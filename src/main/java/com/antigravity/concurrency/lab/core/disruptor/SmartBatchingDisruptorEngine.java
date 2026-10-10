package com.antigravity.concurrency.lab.core.disruptor;

import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineMetrics;
import com.antigravity.concurrency.lab.EngineType;
import com.antigravity.concurrency.lab.SimulatedIo;
import com.antigravity.concurrency.lab.StateEngine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * Level 3: Smart-Batching Single-Writer Engine (LMAX Disruptor & TigerBeetle Architecture).
 *
 * <p>Key Principles:
 * <ol>
 *   <li><b>Cache-Line Padding (64 bytes):</b> Prevents False Sharing between producer sequence
 *       and consumer cursor on modern CPU architectures (x86-64 / ARM64).</li>
 *   <li><b>Pre-allocated RingBuffer:</b> Eliminates garbage collection churn during high throughput.</li>
 *   <li><b>Dynamic Cooperative Batching:</b> The consumer drains all contiguous published slots ($N$ events)
 *       in one pass, processing state transitions on CPU L1/L2 cache.</li>
 *   <li><b>Amortized Group Commit:</b> Disk / WAL fsync I/O is executed ONCE per batch of $N$ events,
 *       driving amortized per-transaction I/O overhead toward zero.</li>
 * </ol>
 */
public final class SmartBatchingDisruptorEngine extends PaddedFields implements StateEngine {

    public static final class Slot {
        public long delta;
        public CompletableFuture<Long> future;
    }

    private final EngineConfig config;
    private final EngineMetrics metrics = new EngineMetrics();
    private final int bufferCapacity;
    private final int mask;
    private final Slot[] ringBuffer;
    private final java.util.concurrent.atomic.AtomicLongArray availableBuffer;

    private final Thread consumerThread;
    private volatile boolean running = true;

    // Single-writer state machine - no volatile needed during iteration!
    private long state = 0L;
    private volatile long readableState = 0L;

    public SmartBatchingDisruptorEngine(EngineConfig config) {
        this.config = config;
        this.bufferCapacity = findNextPowerOfTwo(config.getBufferCapacity());
        this.mask = this.bufferCapacity - 1;

        // Pre-allocate slots to prevent runtime object allocations
        this.ringBuffer = new Slot[this.bufferCapacity];
        this.availableBuffer = new java.util.concurrent.atomic.AtomicLongArray(this.bufferCapacity);
        for (int i = 0; i < this.bufferCapacity; i++) {
            this.ringBuffer[i] = new Slot();
            this.availableBuffer.set(i, -1L);
        }

        this.consumerThread = new Thread(this::consumerEventLoop, "Disruptor-Single-Writer");
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        metrics.recordSubmitted();
        final CompletableFuture<Long> future = new CompletableFuture<Long>();

        // 1. Claim next sequence in the ring buffer atomically
        final long claimSeq = CLAIM_UPDATER.getAndIncrement(this);

        // 2. Backpressure / wait if ring buffer is full
        final long wrapPoint = claimSeq - bufferCapacity;
        while (cursor < wrapPoint) {
            LockSupport.parkNanos(100L); // backpressure yield
        }

        // 3. Write data to pre-allocated slot
        final int index = (int) (claimSeq & mask);
        Slot slot = ringBuffer[index];
        slot.delta = delta;
        slot.future = future;

        // 4. Publish availability to consumer (memory release barrier)
        availableBuffer.set(index, claimSeq);

        return future;
    }

    /**
     * Single-Writer Event Loop with Dynamic Batching and Group Commit.
     */
    private void consumerEventLoop() {
        long currentCursor = -1L;
        final Slot[] localRing = this.ringBuffer;
        final int localMask = this.mask;
        final int maxBatch = config.getMaxBatchSize();

        while (running || currentCursor < claimSequence - 1) {
            long nextSeq = currentCursor + 1;
            int availableIndex = (int) (nextSeq & localMask);

            // Check if the next sequence is published
            if (availableBuffer.get(availableIndex) != nextSeq) {
                LockSupport.parkNanos(500L); // short idle pause
                continue;
            }

            // Find how many contiguous events are available (Smart Batching)
            long batchEndSeq = nextSeq;
            while (batchEndSeq - currentCursor < maxBatch) {
                long candidateSeq = batchEndSeq + 1;
                int candidateIdx = (int) (candidateSeq & localMask);
                if (availableBuffer.get(candidateIdx) == candidateSeq) {
                    batchEndSeq = candidateSeq;
                } else {
                    break;
                }
            }

            int batchSize = (int) (batchEndSeq - currentCursor);

            // Phase 1: Fast in-memory state transition across the entire batch (Hot in L1 Cache)
            for (long seq = nextSeq; seq <= batchEndSeq; seq++) {
                int idx = (int) (seq & localMask);
                state += localRing[idx].delta;
            }
            readableState = state;

            // Phase 2: Amortized Group Commit (Fsync once for all N events)
            if (config.getSimulatedIoMicros() > 0) {
                SimulatedIo.execute(config.getSimulatedIoMicros());
            }

            // Phase 3: Complete all futures in the batch
            for (long seq = nextSeq; seq <= batchEndSeq; seq++) {
                int idx = (int) (seq & localMask);
                Slot slot = localRing[idx];
                slot.future.complete(state);
            }

            metrics.recordBatchFlush(batchSize);
            metrics.recordSuccess(batchSize);

            // Update consumer cursor to free ring buffer slots for producers
            currentCursor = batchEndSeq;
            cursor = currentCursor;
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
        return EngineType.SMART_BATCHING_DISRUPTOR;
    }

    @Override
    public void close() {
        running = false;
        consumerThread.interrupt();
        try {
            consumerThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int findNextPowerOfTwo(int value) {
        int highestOne = Integer.highestOneBit(value);
        return highestOne == value ? value : highestOne << 1;
    }
}
