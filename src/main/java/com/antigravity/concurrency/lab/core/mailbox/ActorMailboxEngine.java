package com.antigravity.concurrency.lab.core.mailbox;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * Level 2a: Classic Actor / Mailbox Model.
 *
 * Characteristics:
 * - Single-threaded execution: A dedicated worker thread processes messages one by one.
 * - Producers enqueue command objects into a shared queue (incurring object allocation and queue CAS).
 * - Consumer executes: Dequeue -> State Transition -> 1-by-1 I/O Persistence -> Complete Future.
 * - Under high load, throughput is bottlenecked by 1-by-1 I/O and queue node contention.
 */
public final class ActorMailboxEngine implements StateEngine {

    private static final class Command {
        final long delta;
        final CompletableFuture<Long> future;

        Command(long delta, CompletableFuture<Long> future) {
            this.delta = delta;
            this.future = future;
        }
    }

    private final EngineConfig config;
    private final EngineMetrics metrics = new EngineMetrics();
    private final BlockingQueue<Command> mailbox;
    private final Thread workerThread;
    private volatile boolean running = true;

    // State is mutated strictly by the single worker thread (no volatile needed for writes)
    private long state = 0L;
    // Volatile read mirror for external readers
    private volatile long readableState = 0L;

    public ActorMailboxEngine(EngineConfig config) {
        this.config = config;
        this.mailbox = new ArrayBlockingQueue<Command>(config.getBufferCapacity());
        this.workerThread = new Thread(this::eventLoop, "Actor-Mailbox-Worker");
        this.workerThread.setDaemon(true);
        this.workerThread.start();
    }

    private void eventLoop() {
        while (running || !mailbox.isEmpty()) {
            Command cmd;
            try {
                cmd = mailbox.take();
            } catch (InterruptedException e) {
                if (!running && mailbox.isEmpty()) {
                    break;
                }
                Thread.currentThread().interrupt();
                continue;
            }

            // Single-writer state transition
            state += cmd.delta;
            readableState = state;

            // 1-by-1 I/O Persistence
            if (config.getSimulatedIoMicros() > 0) {
                SimulatedIo.execute(config.getSimulatedIoMicros());
            }

            metrics.recordSuccess();
            cmd.future.complete(state);
        }
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();
        CompletableFuture<Long> future = new CompletableFuture<Long>();
        Command cmd = new Command(delta, future);

        try {
            mailbox.put(cmd);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
        }

        return future;
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
        return EngineType.ACTOR_MAILBOX;
    }

    @Override
    public void close() {
        running = false;
        workerThread.interrupt();
        try {
            workerThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
