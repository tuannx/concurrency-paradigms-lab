package com.antigravity.concurrency.lab.core.striped;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Level 4: Striped Partitioned Counter (LongAdder-style commutative accumulation).
 *
 * <p><b>What this is, and is not:</b>
 * A fixed array of cache-line padded cells; each thread adds to the cell picked by its thread id. Similar in spirit to
 * Java 8's {@code LongAdder}, which additionally grows its cells and re-hashes threads under contention.
 * Additions are commutative ($a + b = b + a$) and associative ($(a + b) + c = a + (b + c)$), but:
 * <ul>
 *   <li><b>Not idempotent:</b> applying the same delta twice counts it twice ($x + x \neq x$).</li>
 *   <li><b>Not a CRDT:</b> there are no replicas and no lattice {@code merge()}. A state-based G-Counter would keep
 *       one entry per replica and merge with a pointwise max.</li>
 *   <li><b>Weakly consistent reads:</b> {@link #getState()} sums the cells non-atomically, so the value returned by
 *       {@link #mutate(long)} is a snapshot, not the value right after this update. CAS returns the exact
 *       post-update value; compare their throughput with that in mind.</li>
 *   <li><b>Memory:</b> 64-byte padding per cell suits long-lived singleton engines, not short-lived per-stream
 *       objects (such as {@code SequentialSubscription}).</li>
 *   <li><b>Collisions:</b> threads whose ids share the low bits share a cell and contend on it.</li>
 * </ul>
 */
public final class StripedCounterEngine implements StateEngine {

    /**
     * Cache-line padded counter cell to eliminate false sharing between stripes.
     */
    static final class PaddedCell {
        volatile long p1, p2, p3, p4, p5, p6, p7;
        volatile long value;
        volatile long p8, p9, p10, p11, p12, p13, p14;

        static final AtomicLongFieldUpdater<PaddedCell> VALUE_UPDATER =
                AtomicLongFieldUpdater.newUpdater(PaddedCell.class, "value");

        void add(long delta) {
            VALUE_UPDATER.addAndGet(this, delta);
        }

        long get() {
            return value;
        }
    }

    private final EngineConfig config;
    private final EngineMetrics metrics = new EngineMetrics();
    private final PaddedCell[] partitions;
    private final int mask;

    public StripedCounterEngine(EngineConfig config) {
        this.config = config;
        int numPartitions = findNextPowerOfTwo(config.getStripeCount());
        this.mask = numPartitions - 1;
        this.partitions = new PaddedCell[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            this.partitions[i] = new PaddedCell();
        }
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();

        // Map calling thread to a stripe (threads with the same low id bits share one)
        int stripe = (int) (Thread.currentThread().getId() & mask);
        partitions[stripe].add(delta);

        if (config.getSimulatedIoMicros() > 0) {
            // Each caller persists on its own thread: I/O runs in parallel and unordered across callers
            SimulatedIo.execute(config.getSimulatedIoMicros());
        }

        metrics.recordSuccess();

        // Non-atomic snapshot sum, not the exact value right after this update
        return CompletableFuture.completedFuture(getState());
    }

    @Override
    public long getState() {
        // Sum of the stripes; cells may change while they are read
        long total = 0L;
        for (PaddedCell cell : partitions) {
            total += cell.get();
        }
        return total;
    }

    @Override
    public EngineMetrics getMetrics() {
        return metrics;
    }

    @Override
    public EngineType getType() {
        return EngineType.STRIPED_COUNTER;
    }

    @Override
    public void close() {
        // No background threads
    }

    private static int findNextPowerOfTwo(int value) {
        int highestOne = Integer.highestOneBit(value);
        return highestOne == value ? value : highestOne << 1;
    }
}
