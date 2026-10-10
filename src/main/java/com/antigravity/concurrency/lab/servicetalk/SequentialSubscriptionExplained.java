package com.antigravity.concurrency.lab.servicetalk;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * <h1>Educational Deep Dive & Bug Post-Mortem: ServiceTalk's SequentialSubscription</h1>
 *
 * <p>Mirrors the sentinel-CAS algorithm of
 * <a href="https://github.com/apple/servicetalk/blob/e1704cb9d14bdc499e2f9bd471f3572592538a52/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java">SequentialSubscription.java @ e1704cb</a>
 * (assertions and overflow helpers inlined).<br>
 * Associated: <a href="https://github.com/apple/servicetalk/issues/1672">Issue #1672 (lost demand)</a> |
 * <a href="https://github.com/apple/servicetalk/pull/3677">PR #3677 (proposed redesign, open and under review)</a></p>
 *
 * <h2>WARNING: reproduces two known races on purpose</h2>
 * <blockquote>
 * Do not use this sentinel-CAS pattern as-is. PR #3677 proposes replacing it with ServiceTalk's non-blocking drain
 * lock ({@code ConcurrentUtils.tryAcquireLock}/{@code releaseLock}: a thread that loses the race marks pending work
 * and returns, it never blocks) plus an atomic mailbox for the next subscription. The PR is not merged.
 * </blockquote>
 *
 * <h3>Known races kept in this class</h3>
 * <ol>
 *   <li><b>Bug 1 (Issue #1672, lost demand):</b>
 *       The {@code SWITCHING} branch of {@link #switchTo(Subscription)} only stores {@code subscription = next}.
 *       It is meant for same-thread re-entry, where the outer frame delivers demand when the stack unwinds. When a
 *       {@code switchTo} from another thread (e.g. a resubscribe scheduled on a timer thread by
 *       {@code retryWhen}/{@code repeatWhen}) overlaps an earlier {@code switchTo} that is still unwinding, it takes
 *       this branch too and the new subscription can end up with no {@code request(n)}.
 *       Measured on ServiceTalk's class (not this copy): 121 of 200,000 iterations lost demand in a local
 *       micro-stress.</li>
 *   <li><b>Bug 2 (lost cancel):</b>
 *       {@link #cancel()} reads {@code subscription} before {@code getAndSet(CANCELLED)}. If a concurrent
 *       {@code switchTo} completes in between, the stale subscription is cancelled and the new one is never
 *       cancelled. Measured on ServiceTalk's class (not this copy): 3,027 of 3,000,000 iterations in a local
 *       two-thread harness.</li>
 * </ol>
 *
 * <h3>Invalid demand (Reactive Streams §3.9)</h3>
 * <p>Like main, {@code n <= 0} is not dropped: it is stored as a negative marker ({@code 0} becomes
 * {@link Long#MIN_VALUE}, a negative {@code n} is kept), later valid requests do not overwrite it, and it is forwarded
 * to the current and every later source so they can signal {@code onError}.</p>
 *
 * <h3>Why a CRDT is not a fit</h3>
 * <ul>
 *   <li><b>Side effects, not state convergence:</b> {@code source.request(n)} is an external, non-idempotent call;
 *       delivering a delta twice doubles upstream demand.</li>
 *   <li><b>Reactive Streams §2.7:</b> a Subscriber must ensure its calls to {@code request} and {@code cancel} are
 *       performed serially. Deciding which thread delivers which delta, exactly once, is a claim (CAS / lock), not a
 *       commutative merge.</li>
 *   <li><b>{@code switchTo} is non-monotonic:</b> it re-points future demand and resets the emitted baseline. By
 *       analogy with the CALM theorem (stated for distributed programs), non-monotonic steps need coordination.</li>
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
     */
    public void request(long n) {
        final long currRequested;
        if (n > 0) {
            // Saturating add that keeps an invalid (negative) marker once set, like main's
            // FlowControlUtils.addWithOverflowProtectionIfNotNegative.
            currRequested = REQUESTED_UPDATER.accumulateAndGet(this, n, (curr, add) -> {
                if (curr < 0) {
                    return curr;
                }
                final long res = curr + add;
                return res < 0 ? Long.MAX_VALUE : res;
            });
        } else {
            currRequested = n == 0 ? Long.MIN_VALUE : n;
            requested = currRequested;
        }

        for (;;) {
            final long currSourceRequested = sourceRequested;
            if (currSourceRequested == CANCELLED) {
                break;
            } else if (currSourceRequested < 0) {
                // A switch is in progress (SWITCHING or REQUESTED): flag it, the switching thread delivers the demand.
                if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, REQUESTED)) {
                    break;
                }
            } else {
                // Read subscription BEFORE the CAS so a switch that made sourceRequested visible also made it visible.
                final Subscription currSubscription = subscription;
                if (currRequested > 0) {
                    final long delta = currRequested - currSourceRequested;
                    if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, currRequested)) {
                        if (delta != 0) {
                            currSubscription.request(delta);
                        }
                        break;
                    }
                } else if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, CANCELLED)) {
                    currSubscription.request(currRequested); // RS §3.9: the source signals onError
                    break;
                }
            }
        }
    }

    /**
     * Switches the active upstream subscription. Must be called only from the Subscriber thread; same-thread
     * re-entry (a source completing inside {@code request(n)}) is allowed.
     *
     * <p><b>KNOWN BUG (Issue #1672):</b> a {@code switchTo} from another thread that overlaps an unwinding switch
     * takes the re-entry branch and can lose demand.</p>
     */
    public void switchTo(Subscription next) {
        requireNonNull(next, "next subscription cannot be null");
        long effectiveSourceRequested = sourceEmitted;
        for (;;) {
            final long currSourceRequested = sourceRequested;
            if (currSourceRequested == CANCELLED) {
                final long currRequested = requested;
                if (currRequested >= 0) {
                    next.cancel();
                } else {
                    next.request(currRequested); // pending invalid demand goes to every later source
                }
                break;
            } else if (currSourceRequested == SWITCHING) {
                // BUG 1 (Issue #1672): meant for same-thread re-entry; the outer frame delivers demand on unwind.
                // A switch from another thread lands here too and nobody requests from `next`.
                subscription = next;
                break;
            } else if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, currSourceRequested, SWITCHING)) {
                final long currRequested = requested;
                if (currRequested < 0) {
                    sourceRequested = CANCELLED;
                    next.request(currRequested);
                    break;
                }

                // effectiveSourceRequested ...[delta]... requested
                final long delta = currRequested - effectiveSourceRequested;
                final Subscription beforeSubscription = subscription;
                if (delta != 0) {
                    // Track what was already requested so a later iteration does not request it again.
                    effectiveSourceRequested = currRequested;
                    next.request(delta);
                }

                final boolean reentry = beforeSubscription != subscription;
                if (reentry) {
                    // A nested switchTo stored a newer subscription: deliver to it on the next iteration,
                    // with the emitted baseline reset for the new source.
                    next = subscription;
                    effectiveSourceRequested = sourceEmitted;
                } else {
                    // Publish the subscription before restoring sourceRequested (the request() thread reads them
                    // in the opposite order).
                    subscription = next;
                }

                // Restore a monotonic sourceRequested. If the CAS fails, a request() or cancel() arrived meanwhile
                // (state is REQUESTED or CANCELLED): loop to handle it.
                if (SOURCE_REQUESTED_UPDATER.compareAndSet(this, SWITCHING, currRequested) && !reentry) {
                    break;
                }
            }
        }
    }

    public void itemReceived() {
        ++sourceEmitted;
    }

    /**
     * Cancels the subscription.
     *
     * <p><b>KNOWN BUG (lost cancel):</b> reading {@code subscription} before {@code getAndSet(CANCELLED)} races with a
     * concurrent {@code switchTo}; the new subscription can be left uncancelled.</p>
     */
    public void cancel() {
        // BUG 2: read before getAndSet. If the state was negative, the switching thread cancels instead.
        final Subscription currSubscription = subscription;
        final long currSourceRequested = SOURCE_REQUESTED_UPDATER.getAndSet(this, CANCELLED);
        if (currSourceRequested >= 0) {
            currSubscription.cancel();
        }
    }
}
