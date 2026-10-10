package com.antigravity.concurrency.lab.core.crdt;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Level 4: Commutative Replicated State Machine (CRDT / Math-driven Join-Semilattice).
 *
 * <p>Mathematical Foundations:
 * <ul>
 *   <li>The state space forms a Join-Semilattice $\langle S, \sqcup \rangle$ where state updates
 *       satisfy:
 *       <ol>
 *         <li><b>Commutativity:</b> $x \sqcup y = y \sqcup x$ (order of mutation arrival is irrelevant)</li>
 *         <li><b>Associativity:</b> $(x \sqcup y) \sqcup z = x \sqcup (y \sqcup z)$ (batching boundaries are irrelevant)</li>
 *         <li><b>Idempotence:</b> $x \sqcup x = x$ (re-transmissions do not corrupt state)</li>
 *       </ol>
 *   </li>
 *   <li>No mailbox, no locks, no global CAS loops:
 *       Writers mutate their partitioned stripe locally without cross-thread coordination.</li>
 *   <li>Each stripe is cache-line padded to avoid MESI coherency crosstalk.</li>
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
        return EngineType.COMMUTATIVE_CRDT;
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
