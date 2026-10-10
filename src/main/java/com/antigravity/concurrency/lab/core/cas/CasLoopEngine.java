package com.antigravity.concurrency.lab.core.cas;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Level 1: Multi-Writer Optimistic Concurrency Control (OCC) using atomic CAS retry loop.
 *
 * Characteristics:
 * - Direct compare-and-swap on a shared cache line.
 * - Under high contention across multiple CPU cores, cache lines constantly bounce between
 *   Modified (M) and Invalid (I) states via the MESI/MOESI cache coherence protocol.
 * - Every write requires a serialized hardware memory bus transaction.
 * - Each mutation must execute its own persistence I/O individually (1-by-1 I/O).
 */
public final class CasLoopEngine implements StateEngine {

    private static final AtomicLongFieldUpdater<CasLoopEngine> STATE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(CasLoopEngine.class, "state");

    private final EngineConfig config;
    private final EngineMetrics metrics = new EngineMetrics();

    // The shared state field accessed by all concurrent threads
    private volatile long state = 0L;

    public CasLoopEngine(EngineConfig config) {
        this.config = config;
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();
        long retries = 0;

        for (;;) {
            final long current = state;
            final long next = current + delta;

            if (STATE_UPDATER.compareAndSet(this, current, next)) {
                if (retries > 0) {
                    metrics.recordCasRetries(retries);
                }
                // Each writer is forced to perform its own 1-by-1 I/O persistence
                if (config.getSimulatedIoMicros() > 0) {
                    SimulatedIo.execute(config.getSimulatedIoMicros());
                }
                metrics.recordSuccess();
                return CompletableFuture.completedFuture(next);
            }

            retries++;
        }
    }

    @Override
    public long getState() {
        return state;
    }

    @Override
    public EngineMetrics getMetrics() {
        return metrics;
    }

    @Override
    public EngineType getType() {
        return EngineType.CAS_LOOP;
    }

    @Override
    public void close() {
        // No threads to clean up
    }
}
