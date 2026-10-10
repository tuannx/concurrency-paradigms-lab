package com.antigravity.concurrency.lab.core.disruptor;

/**
 * Cache-line padded field hierarchy to prevent CPU False Sharing.
 *
 * <p>Modern CPU cache lines (x86-64 and ARM64) are 64 bytes wide.
 * When multiple CPU cores concurrently write to distinct variables that reside
 * on the SAME 64-byte cache line, the hardware cache coherence protocol (MESI/MOESI)
 * forces the entire cache line to bounce between cores, invalidating L1/L2 caches
 * and crippling multicore scalability.
 *
 * <p>By inserting 7 {@code long} padding fields (56 bytes + 8 bytes object alignment = 64 bytes),
 * we guarantee that {@code claimSequence} (frequently updated by producer threads)
 * and {@code cursor} (frequently updated by the consumer single-writer thread)
 * inhabit completely independent physical cache lines.
 */
abstract class PaddedHead {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class ClaimSequenceField extends PaddedHead {
    protected volatile long claimSequence = 0L;
    static final java.util.concurrent.atomic.AtomicLongFieldUpdater<ClaimSequenceField> CLAIM_UPDATER =
            java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater(ClaimSequenceField.class, "claimSequence");
}

abstract class PaddedMiddle extends ClaimSequenceField {
    protected long p8, p9, p10, p11, p12, p13, p14;
}

abstract class CursorField extends PaddedMiddle {
    protected volatile long cursor = -1L;
}

abstract class PaddedTail extends CursorField {
    protected long p15, p16, p17, p18, p19, p20, p21;
}

public abstract class PaddedFields extends PaddedTail {
}
