package com.antigravity.concurrency.lab;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metric tracking container for an execution engine.
 */
public final class EngineMetrics {
    private final AtomicLong submittedOps = new AtomicLong();
    private final AtomicLong successfulOps = new AtomicLong();
    private final AtomicLong casRetries = new AtomicLong();
    private final AtomicLong batchFlushes = new AtomicLong();
    private final AtomicLong batchElements = new AtomicLong();

    public void recordSubmitted() {
        submittedOps.incrementAndGet();
    }

    public void recordSuccess() {
        successfulOps.incrementAndGet();
    }

    public void recordSuccess(long count) {
        successfulOps.addAndGet(count);
    }

    public void recordCasRetry() {
        casRetries.incrementAndGet();
    }

    public void recordCasRetries(long count) {
        casRetries.addAndGet(count);
    }

    public void recordBatchFlush(int batchSize) {
        batchFlushes.incrementAndGet();
        batchElements.addAndGet(batchSize);
    }

    public long getSubmittedOps() {
        return submittedOps.get();
    }

    public long getSuccessfulOps() {
        return successfulOps.get();
    }

    public long getCasRetries() {
        return casRetries.get();
    }

    public long getBatchFlushes() {
        return batchFlushes.get();
    }

    public double getAverageBatchSize() {
        long flushes = batchFlushes.get();
        return flushes == 0 ? 1.0 : (double) batchElements.get() / flushes;
    }

    @Override
    public String toString() {
        return String.format(
            "Metrics[ops=%d, retries=%d, flushes=%d, avgBatch=%.1f]",
            successfulOps.get(), casRetries.get(), batchFlushes.get(), getAverageBatchSize()
        );
    }
}
