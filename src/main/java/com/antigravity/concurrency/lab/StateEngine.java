package com.antigravity.concurrency.lab;

import java.util.concurrent.CompletableFuture;

/**
 * Common abstraction for all concurrency state machine implementations.
 * Represents an integer ledger/counter undergoing high-frequency concurrent mutations.
 */
public interface StateEngine extends AutoCloseable {

    /**
     * Submits an atomic state mutation.
     *
     * @param delta value to add or subtract
     * @return CompletableFuture completing with the resulting state value
     */
    CompletableFuture<Long> mutate(long delta);

    /**
     * Reads the current consolidated state.
     *
     * @return current state
     */
    long getState();

    /**
     * Returns the metrics collected by this engine.
     *
     * @return metrics instance
     */
    EngineMetrics getMetrics();

    /**
     * Returns the architectural paradigm type of this engine.
     *
     * @return engine type
     */
    EngineType getType();

    /**
     * Releases any threads, ring buffers, or executor resources.
     */
    @Override
    void close();
}
