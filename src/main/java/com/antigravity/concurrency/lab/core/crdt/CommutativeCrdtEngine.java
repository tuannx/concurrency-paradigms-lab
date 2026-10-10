package com.antigravity.concurrency.lab.core.crdt;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Level 4: Striped Partitioned Counter (LongAdder / Commutative Algebraic Pattern).
 *
 * <p><b>ARCHITECTURAL DISTINCTION & CLARIFICATION:</b>
 * This engine implements a partitioned counter (identical in principle to Java 8's {@code LongAdder}).
 * While mutation operations possess <b>Commutativity</b> ($a + b = b + a$) and <b>Associativity</b>
 * ($(a + b) + c = a + (b + c)$):
 * <ul>
 *   <li><b>Not Idempotent:</b> Addition does NOT satisfy $x \sqcup x = x$. Delivering a delta twice corrupts state.</li>
 *   <li><b>Not a Distributed CRDT:</b> There are no multi-node replicas and no explicit {@code merge()} lattice join.</li>
 *   <li><b>Weak Consistency Read:</b> {@link #getState()} performs a non-atomic summation across stripes.
 *       Comparing its raw throughput directly against linearizable CAS represents a classic consistency-versus-speed trade-off.</li>
 *   <li><b>Memory Footprint Trade-off:</b> 64-byte padding per cell is appropriate for long-lived singleton
 *       engines, but creates prohibitive memory bloat for short-lived, high-churn instances (such as
 *       per-stream reactive subscriptions like {@code SequentialSubscription}).</li>
 * </ul>
 */
public final class CommutativeCrdtEngine implements StateEngine {

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

    public CommutativeCrdtEngine(EngineConfig config) {
        this.config = config;
        int numPartitions = findNextPowerOfTwo(config.getCrdtPartitions());
        this.mask = numPartitions - 1;
        this.partitions = new PaddedCell[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            this.partitions[i] = new PaddedCell();
        }
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();

        // Map calling thread to its designated partition stripe
        int stripe = (int) (Thread.currentThread().getId() & mask);
        partitions[stripe].add(delta);

        if (config.getSimulatedIoMicros() > 0) {
            // Local partition persistence without global barrier
            SimulatedIo.execute(config.getSimulatedIoMicros());
        }

        metrics.recordSuccess();

        // In CRDT, reads converge via semilattice join (sum across all stripes)
        return CompletableFuture.completedFuture(getState());
    }

    @Override
    public long getState() {
        // Semilattice join / reconciliation: associative summation
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
        // Stateless partitions require no active background threads
    }

    private static int findNextPowerOfTwo(int value) {
        int highestOne = Integer.highestOneBit(value);
        return highestOne == value ? value : highestOne << 1;
    }
}
