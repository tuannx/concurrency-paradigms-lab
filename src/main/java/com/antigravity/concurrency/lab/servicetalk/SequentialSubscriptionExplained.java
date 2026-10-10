package com.antigravity.concurrency.lab.servicetalk;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * <h1>Educational Deep Dive & Bug Post-Mortem: ServiceTalk's SequentialSubscription</h1>
 *
 * <p>Reference: <a href="https://github.com/tuannx/servicetalk/blob/main/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java">SequentialSubscription.java</a><br>
 * Associated Issues: <a href="https://github.com/apple/servicetalk/issues/1672">Issue #1672 (Lost demand)</a> |
 * <a href="https://github.com/apple/servicetalk/pull/3677">PR #3677 (Lock + Mailbox redesign)</a></p>
 *
 * <h2>CRITICAL WARNING / ARCHITECTURAL POST-MORTEM:</h2>
 * <blockquote>
 * <b>DO NOT USE THIS SENTINEL CAS PATTERN IN PRODUCTION AS-IS.</b><br>
 * While theoretically elegant, this sentinel-CAS coordination model contains verified race-condition
 * bugs in {@code apple/servicetalk:main} that led to PR #3677 abandoning pure lock-free sentinel CAS
 * in favor of an explicit lock + serialized mailbox pattern.
 * </blockquote>
 *
 * <h3>Documented Race Conditions in this Implementation:</h3>
 * <ol>
 *   <li><b>Bug 1 (Issue #1672 - Lost Demand):</b>
 *       In {@link #switchTo(Subscription)}, when {@code currSourceRequested == SWITCHING}, the assignment
 *       {@code this.subscription = next;} occurs without CAS synchronization. Concurrent re-entrant or
 *       interleaved calls desynchronize {@code effectiveSourceRequested} and the active {@code subscription}
 *       reference, leading to dropped/lost demand under heavy load (e.g., 121 lost demands per 200k operations).
 *   </li>
 *   <li><b>Bug 2 (Lost Cancel Race):</b>
 *       In {@link #cancel()}, {@code subscription} is read BEFORE {@code getAndSet(this, CANCELLED)}.
 *       If a concurrent {@code switchTo(next)} executes between the read and the atomic write,
 *       {@code cancel()} invokes cancel on the stale subscription while {@code switchTo} may miss the
 *       {@code CANCELLED} state, leaving the new upstream {@code next} subscription leaked and never cancelled
 *       (measured at ~3,027 uncancelled subscriptions per 3,000,000 calls).
 *   </li>
 *   <li><b>Reactive Streams §3.9 Violation:</b>
 *       In the original implementation, non-positive requests ({@code n <= 0}) were sanitized and forwarded
 *       to downstream sources to trigger {@code onError(new IllegalArgumentException())}. Dropping them silently
 *       violates Reactive Streams Specification Rule §3.9.
 *   </li>
 * </ol>
 *
 * <h3>Why CRDT / Join-Semilattices Cannot Solve This Problem:</h3>
 * <ul>
 *   <li><b>Side-Effects vs. State Convergence:</b> Calling {@code source.request(n)} is an external side-effect,
 *       not an idempotent state transition. Delivering demand twice doubles the upstream emission.</li>
 *   <li><b>Reactive Streams §2.7:</b> Mandates that calls to {@code Subscription.request()} MUST be strictly serialized.</li>
 *   <li><b>Non-Monotonicity of {@code switchTo}:</b> Switching the subscription target resets the emitted baseline
 *       and re-points external side-effects. By the CALM Theorem, non-monotonic transitions strictly require coordination.</li>
 * </ul>
 */
public final class SequentialSubscriptionExplained {

    public interface Subscription {
        void request(long n);
        void cancel();
    }

    private static final long SWITCHING = -1L;
    private static final long REQUESTED = -2L;
    private static final long CANCELLED = -3L;

    private static final AtomicLongFieldUpdater<SequentialSubscriptionExplained> REQUESTED_UPDATER =
            newUpdater(SequentialSubscriptionExplained.class, "requested");
    private static final AtomicLongFieldUpdater<SequentialSubscriptionExplained> SOURCE_REQUESTED_UPDATER =
            newUpdater(SequentialSubscriptionExplained.class, "sourceRequested");

    private Subscription subscription;
    private long sourceEmitted;
    private volatile long requested;
    private volatile long sourceRequested;

    public SequentialSubscriptionExplained(Subscription initialSubscription) {
        this.subscription = requireNonNull(initialSubscription, "initialSubscription cannot be null");
    }

    /**
     * Concurrent request method. Can be invoked from any thread.
     * Complies with Reactive Streams §3.9 by preserving non-positive request signals.
     */
    public void request(long n) {
        // Reactive Streams §3.9: n <= 0 must be forwarded to trigger onError(IllegalArgumentException)
        final long currRequested;
        if (n <= 0) {
            currRequested = Long.MIN_VALUE; // Sentinel for invalid request
            this.requested = currRequested;
        } else {
            currRequested = REQUESTED_UPDATER.accumulateAndGet(this, n, (curr, next) -> {
                long res = curr + next;
                return res < 0 ? Long.MAX_VALUE : res; // Overflow protection
            });
        }

        for (;;) {
            final long currSourceRequested = sourceRequested;

            if (currSourceRequested == CANCELLED) {
                break; // No-op if cancelled
            } else if (currSourceRequested < 0) {
                // Subscription switch is underway: flag that new demand arrived
                if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, REQUESTED)) {
                    // Successfully notified the switching thread; it will deliver this demand when done
                    break;
                }
            } else {
                final Subscription currSubscription = subscription;
                if (currRequested < 0) {
                    if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, CANCELLED)) {
                        currSubscription.request(currRequested); // trigger RS §3.9 error
                        break;
                    }
                } else {
                    final long delta = currRequested - currSourceRequested;
                    if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, currSourceRequested + delta)) {
                        if (delta != 0) {
                            currSubscription.request(delta);
                        }
                        break;
                    }
                }
            }
        }
    }

    /**
     * Switches the active upstream subscription. Must be called only from the Subscriber thread.
     *
     * <p><b>KNOWN BUG WARNING (Issue #1672):</b> The un-CAS'd assignment {@code this.subscription = next;}
     * in the re-entry branch and race window with concurrent requests can lose demand.</p>
     */
    public void switchTo(Subscription next) {
        requireNonNull(next, "next subscription cannot be null");
        long effectiveSourceRequested = sourceEmitted;

        for (;;) {
            final long currSourceRequested = sourceRequested;

            if (currSourceRequested == CANCELLED) {
                next.cancel();
                break;
            } else if (currSourceRequested == SWITCHING) {
                // BUG 1 (Issue #1672): Non-atomic reference overwrite without CAS
                this.subscription = next;
                break;
            } else if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, SWITCHING)) {
                final long currRequested = requested;
                final long delta = currRequested - effectiveSourceRequested;
                final Subscription beforeSubscription = this.subscription;

                if (delta > 0) {
                    effectiveSourceRequested = currRequested;
                    next.request(delta);
                }

                final boolean reentry = beforeSubscription != this.subscription;
                if (!reentry) {
                    this.subscription = next;
                }

                // Attempt to restore state from SWITCHING back to currRequested
                if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, SWITCHING, currRequested) && !reentry) {
                    break; // Successfully switched and restored without concurrent requests
                }
                // If CAS failed, someone marked it REQUESTED! Loop again to drain pending demand!
            }
        }
    }

    public void itemReceived() {
        ++sourceEmitted;
    }

    /**
     * Cancels the subscription.
     *
     * <p><b>KNOWN BUG WARNING:</b> Reading {@code subscription} before {@code getAndSet(CANCELLED)}
     * races with a concurrent {@code switchTo}, potentially leaking the new subscription without cancellation.</p>
     */
    public void cancel() {
        // BUG 2: Reading subscription before getAndSet races with concurrent switchTo
        final Subscription currSubscription = subscription;
        final long currSourceRequested = SOURCE_REQUESTED_UPDATER.getAndSet(this, CANCELLED);
        if (currSourceRequested >= 0) {
            currSubscription.cancel();
        }
    }
}
