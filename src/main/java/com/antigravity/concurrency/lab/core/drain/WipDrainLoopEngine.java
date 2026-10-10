package com.antigravity.concurrency.lab.core.drain;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Level 2b: Cooperative Lock-Free Drain Loop (WIP Trampoline Pattern).
 *
 * <p>Directly inspired by {@code io.servicetalk.concurrent.api.SequentialSubscription}:
 * <ul>
 *   <li>Uses {@link AtomicIntegerFieldUpdater} and {@link AtomicLongFieldUpdater} for zero-allocation
 *       primitive volatile manipulation.</li>
 *   <li>Eliminates dedicated actor worker threads: Producer threads cooperatively execute work.</li>
 *   <li>The first thread to arrive acquires the WIP execution permit ({@code wip.getAndIncrement() == 0}).</li>
 *   <li>Subsequent threads deposit their request and atomically increment WIP, then immediately return.</li>
 *   <li>The executing thread drains all pending accumulated work in a cooperative loop (trampoline).</li>
 *   <li>Guarantees strict single-threaded serialized execution of the state machine without mutexes.</li>
 * </ul>
 */
public final class WipDrainLoopEngine implements StateEngine {

    private static final AtomicIntegerFieldUpdater<WipDrainLoopEngine> WIP_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(WipDrainLoopEngine.class, "wip");

    private static final AtomicLongFieldUpdater<WipDrainLoopEngine> PENDING_DELTA_UPDATER =
            AtomicLongFieldUpdater.newUpdater(WipDrainLoopEngine.class, "pendingDelta");

    private static final class PendingRequest {
        final long delta;
        final CompletableFuture<Long> future;

        PendingRequest(long delta, CompletableFuture<Long> future) {
            this.delta = delta;
            this.future = future;
        }
    }

    private final EngineConfig config;
    private final EngineMetrics metrics = new EngineMetrics();

    // Lock-free queue for request futures tracking
    private final Queue<PendingRequest> requestQueue = new ConcurrentLinkedQueue<PendingRequest>();

    // Cooperative drain fields (updated via AtomicFieldUpdaters)
    private volatile int wip = 0;
    private volatile long pendingDelta = 0L;

    // State machine is mutated strictly during drain loops by the thread holding wip
    private long state = 0L;
    private volatile long readableState = 0L;

    public WipDrainLoopEngine(EngineConfig config) {
        this.config = config;
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();
        final CompletableFuture<Long> future = new CompletableFuture<Long>();
        requestQueue.offer(new PendingRequest(delta, future));

        // Atomically accumulate pending demand (similar to requestedUpdater in SequentialSubscription)
        PENDING_DELTA_UPDATER.addAndGet(this, delta);

        // Cooperative drain permit acquisition:
        // If wip was 0, this thread won the right to drain. Otherwise, another active thread will drain it!
        if (WIP_UPDATER.getAndIncrement(this) == 0) {
            drain();
        }

        return future;
    }

    /**
     * Trampoline drain loop guaranteeing single-threaded execution.
     * Mirrors the re-entrancy / work-in-progress loop in ServiceTalk's SequentialSubscription.
     */
    private void drain() {
        int missed = 1;
        for (;;) {
            // Drain all pending requests from queue
            int processedInBatch = 0;
            PendingRequest req;
            while ((req = requestQueue.poll()) != null) {
                state += req.delta;
                readableState = state;
                processedInBatch++;
                metrics.recordSuccess();
                req.future.complete(state);
            }

            if (processedInBatch > 0) {
                // If I/O persistence is configured, it is executed once per drained batch
                if (config.getSimulatedIoMicros() > 0) {
                    SimulatedIo.execute(config.getSimulatedIoMicros());
                }
                metrics.recordBatchFlush(processedInBatch);
            }

            // Atomically subtract the missed count that was observed
            missed = WIP_UPDATER.addAndGet(this, -missed);
            if (missed == 0) {
                break;
            }
        }
    }

    @Override
    public long getState() {
        return readableState;
    }

    @Override
    public EngineMetrics getMetrics() {
        return metrics;
    }

    @Override
    public EngineType getType() {
        return EngineType.WIP_DRAIN_LOOP;
    }

    @Override
    public void close() {
        // Drain any remaining work
        if (WIP_UPDATER.getAndIncrement(this) == 0) {
            drain();
        }
    }
}
