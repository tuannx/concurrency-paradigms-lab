package com.antigravity.concurrency.lab;

import com.antigravity.concurrency.lab.core.cas.CasLoopEngine;
import com.antigravity.concurrency.lab.core.crdt.CommutativeCrdtEngine;
import com.antigravity.concurrency.lab.core.disruptor.SmartBatchingDisruptorEngine;
import com.antigravity.concurrency.lab.core.drain.WipDrainLoopEngine;
import com.antigravity.concurrency.lab.core.mailbox.ActorMailboxEngine;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * <h1>Unified Concurrency Engine Endpoint</h1>
 *
 * <p>Single entry point and common facade for all concurrency paradigm implementations.
 * Designed to provide a unified API interface for enterprise systems needing to select,
 * switch, or benchmark state coordination models without altering application code.</p>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * // Instantiate via static factory
 * try (ConcurrencyEngine engine = ConcurrencyEngine.smartBatching(EngineConfig.defaultInMemory())) {
 *     CompletableFuture<Long> future = engine.mutate(100L);
 *     future.thenAccept(balance -> System.out.println("Balance: " + balance));
 *     long currentState = engine.getState();
 * }
 * }</pre>
 */
public final class ConcurrencyEngine implements StateEngine {

    private final StateEngine delegate;

    private ConcurrencyEngine(StateEngine delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
    }

    /**
     * Creates an engine of the specified paradigm type with custom configuration.
     *
     * @param type paradigm model
     * @param config configuration parameters
     * @return unified engine instance
     */
    public static ConcurrencyEngine create(EngineType type, EngineConfig config) {
        Objects.requireNonNull(type, "type cannot be null");
        EngineConfig cfg = config != null ? config : EngineConfig.defaultInMemory();

        switch (type) {
            case CAS_LOOP:
                return new ConcurrencyEngine(new CasLoopEngine(cfg));
            case ACTOR_MAILBOX:
                return new ConcurrencyEngine(new ActorMailboxEngine(cfg));
            case WIP_DRAIN_LOOP:
                return new ConcurrencyEngine(new WipDrainLoopEngine(cfg));
            case SMART_BATCHING_DISRUPTOR:
                return new ConcurrencyEngine(new SmartBatchingDisruptorEngine(cfg));
            case STRIPED_COUNTER:
            case COMMUTATIVE_CRDT:
                return new ConcurrencyEngine(new CommutativeCrdtEngine(cfg));
            default:
                throw new IllegalArgumentException("Unsupported engine type: " + type);
        }
    }

    /**
     * Creates an engine with default in-memory settings.
     */
    public static ConcurrencyEngine create(EngineType type) {
        return create(type, EngineConfig.defaultInMemory());
    }

    public static ConcurrencyEngine cas(EngineConfig config) {
        return create(EngineType.CAS_LOOP, config);
    }

    public static ConcurrencyEngine mailbox(EngineConfig config) {
        return create(EngineType.ACTOR_MAILBOX, config);
    }

    /**
     * Creates a Cooperative Lock-Free Drain Loop engine based on the ServiceTalk SequentialSubscription pattern.
     */
    public static ConcurrencyEngine wipDrain(EngineConfig config) {
        return create(EngineType.WIP_DRAIN_LOOP, config);
    }

    public static ConcurrencyEngine smartBatching(EngineConfig config) {
        return create(EngineType.SMART_BATCHING_DISRUPTOR, config);
    }

    public static ConcurrencyEngine stripedCounter(EngineConfig config) {
        return create(EngineType.STRIPED_COUNTER, config);
    }

    /**
     * @deprecated Use {@link #stripedCounter(EngineConfig)} instead.
     */
    @Deprecated
    public static ConcurrencyEngine crdt(EngineConfig config) {
        return create(EngineType.STRIPED_COUNTER, config);
    }

    @Override
    public CompletableFuture<Long> mutate(long delta) {
        return delegate.mutate(delta);
    }

    @Override
    public long getState() {
        return delegate.getState();
    }

    @Override
    public EngineMetrics getMetrics() {
        return delegate.getMetrics();
    }

    @Override
    public EngineType getType() {
        return delegate.getType();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
