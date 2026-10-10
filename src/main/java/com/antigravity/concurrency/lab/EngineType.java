package com.antigravity.concurrency.lab;

/**
 * Enumeration of architectural concurrency paradigms compared in this lab.
 */
public enum EngineType {
    /**
     * Level 1: Multi-writer Optimistic Concurrency Control (OCC) using atomic CAS retry loop.
     * High cache line contention (MESI invalidation storm) and retry thrashing under load.
     */
    CAS_LOOP,

    /**
     * Level 2a: Classic Actor / Mailbox model.
     * Single worker thread drains a message queue one by one (1-by-1 state transition and I/O).
     */
    ACTOR_MAILBOX,

    /**
     * Level 2b: Cooperative Lock-Free Drain Loop (RxJava / Reactive Streams WIP Trampoline).
     * Zero-allocation, cooperative serialized execution using AtomicIntegerFieldUpdater.
     * Note: Distinct from ServiceTalk's sentinel-CAS pattern.
     */
    WIP_DRAIN_LOOP,

    /**
     * Level 3: Smart-Batching Single-Writer (LMAX Disruptor / TigerBeetle pattern).
     * Single-writer event loop with CPU cache-line padding (64 bytes), dynamic batch draining,
     * and amortized group commit / I/O flush.
     */
    SMART_BATCHING_DISRUPTOR,

    /**
     * Level 4: Striped Partitioned Counter (LongAdder / Commutative Math pattern).
     *
     * <p><b>Important Distinction:</b>
     * This is a striped partitioned accumulator (similar to Java's {@code LongAdder}).
     * While commutative ($a + b = b + a$) and associative ($(a+b)+c = a+(b+c)$), it is <b>NOT</b> a true CRDT:
     * <ul>
     *   <li>Addition is NOT idempotent ($x + x \neq x$).</li>
     *   <li>It lacks an explicit replica {@code merge()} function.</li>
     *   <li>{@code getState()} performs a non-atomic snapshot across stripes (weaker consistency than CAS).</li>
     * </ul>
     */
    STRIPED_COUNTER,

    /**
     * @deprecated Alias for {@link #STRIPED_COUNTER}. Kept for backward compatibility.
     */
    @Deprecated
    COMMUTATIVE_CRDT
}
