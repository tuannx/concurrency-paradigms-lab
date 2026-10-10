package com.antigravity.concurrency.lab.core.drain;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Level 2b: Cooperative Lock-Free Drain Loop (WIP Trampoline Pattern).
 *
 * <p>The classic RxJava/Reactor work-in-progress drain. (ServiceTalk's {@code SequentialSubscription} uses a sentinel
 * CAS on {@code main}; PR #3677 proposes a {@code tryAcquireLock}/{@code releaseLock} drain instead.)
 * <ul>
 *   <li>Uses an {@link AtomicIntegerFieldUpdater} on a volatile {@code wip} counter (no {@code AtomicInteger} object);
 *       each request still allocates a future, a request holder and a queue node.</li>
 *   <li>Eliminates dedicated actor worker threads: Producer threads cooperatively execute work.</li>
 *   <li>The first thread to arrive acquires the WIP execution permit ({@code wip.getAndIncrement() == 0}).</li>
 *   <li>Subsequent threads deposit their request and atomically increment WIP, then immediately return.</li>
 *   <li>The executing thread drains all pending accumulated work in a cooperative loop (trampoline).</li>
 *   <li>Guarantees strict single-threaded serialized execution of the state machine without mutexes.</li>
 *   <li>Like the smart-batching engine, a batch is persisted once and its futures complete only after that I/O.</li>
 * </ul>
 */
public final class WipDrainLoopEngine implements StateEngine {

    private static final AtomicIntegerFieldUpdater<WipDrainLoopEngine> WIP_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(WipDrainLoopEngine.class, "wip");

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

    // Cooperative drain permit (updated via AtomicIntegerFieldUpdater)
    private volatile int wip = 0;

    // State machine is mutated strictly during drain loops by the thread holding wip
    private long state = 0L;
    private volatile long readableState = 0L;
    // Reused by the drain owner only (wip hand-off publishes it to the next owner)
    private final List<PendingRequest> batch = new ArrayList<PendingRequest>();

    public WipDrainLoopEngine(EngineConfig config) {
        this.config = config;
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();
        final CompletableFuture<Long> future = new CompletableFuture<Long>();
        requestQueue.offer(new PendingRequest(delta, future));

        // Cooperative drain permit acquisition:
        // If wip was 0, this thread won the right to drain. Otherwise, another active thread will drain it!
        if (WIP_UPDATER.getAndIncrement(this) == 0) {
            drain();
        }

        return future;
    }

    /**
     * Trampoline drain loop guaranteeing single-threaded execution (classic WIP missed-counter loop).
     */
    private void drain() {
        int missed = 1;
        for (;;) {
            // Phase 1: apply every pending request in memory
            PendingRequest req;
            while ((req = requestQueue.poll()) != null) {
                state += req.delta;
                batch.add(req);
            }

            if (!batch.isEmpty()) {
                readableState = state;
                // Phase 2: persist the batch once
                if (config.getSimulatedIoMicros() > 0) {
                    SimulatedIo.execute(config.getSimulatedIoMicros());
                }
                // Phase 3: complete only after the I/O, with the batch's final state (as smart batching does)
                for (PendingRequest done : batch) {
                    done.future.complete(state);
                }
                metrics.recordBatchFlush(batch.size());
                metrics.recordSuccess(batch.size());
                batch.clear();
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
