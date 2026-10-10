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
     * Level 2b: Cooperative Lock-Free Drain Loop (Work-In-Progress / WIP Trampoline).
     * Inspired by ServiceTalk's SequentialSubscription.
     * Zero-allocation, cooperative serialized execution using AtomicIntegerFieldUpdater.
     */
    WIP_DRAIN_LOOP,

    /**
     * Level 3: Smart-Batching Single-Writer (LMAX Disruptor / TigerBeetle pattern).
     * Single-writer event loop with CPU cache-line padding (64 bytes), dynamic batch draining,
     * and amortized group commit / I/O flush.
     */
    SMART_BATCHING_DISRUPTOR,

    /**
     * Level 4: Commutative Conflict-Free Replicated State Machine (CRDT / PN-Counter).
     * Mathematically commutative and associative join-semilattice. Zero locks, zero coordination on write.
     */
    COMMUTATIVE_CRDT
}
