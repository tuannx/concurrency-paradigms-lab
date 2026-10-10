package com.antigravity.concurrency.lab.servicetalk;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * <h1>Educational Deep Dive: ServiceTalk's SequentialSubscription</h1>
 *
 * <p>Reference: <a href="https://github.com/tuannx/servicetalk/blob/main/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java">SequentialSubscription.java</a></p>
 *
 * <h2>Key Concurrency Insights:</h2>
 * <ol>
 *   <li><b>Zero Object-Allocation Updaters:</b> Uses {@link AtomicLongFieldUpdater} directly on
 *       primitive volatile longs. This avoids the object allocation overhead of {@code AtomicLong}.</li>
 *   <li><b>State Machine Encoded in a Single {@code long}:</b>
 *       <ul>
 *         <li>Positive values ($\ge 0$): Represent cumulative monotonic demand requested from the upstream source.</li>
 *         <li>Negative sentinel values:
 *           <ul>
 *             <li>{@code SWITCHING = -1}: Another thread (Subscriber) is currently changing the active upstream subscription.</li>
 *             <li>{@code REQUESTED = -2}: A concurrent request arrived while a switch was in progress.</li>
 *             <li>{@code CANCELLED = -3}: The subscription has been cancelled.</li>
 *           </ul>
 *         </li>
 *       </ul>
 *   </li>
 *   <li><b>Lock-Free Trampoline / Drain Coordination:</b>
 *       When {@code request(n)} is called while {@code switchTo()} is active:
 *       Rather than acquiring a mutex or waiting on a condition variable, the calling thread
 *       CAS-flags the state to {@code REQUESTED}.
 *       When the switching thread finishes, its CAS to restore the monotonic demand fails,
 *       alerting it that concurrent demand arrived. It immediately loops and delivers the pending demand!
 *   </li>
 * </ol>
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
        if (n <= 0) {
            return;
        }

        // Atomically increment requested total
        final long currRequested = REQUESTED_UPDATER.addAndGet(this, n);

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
                // Subscription is stable. Read subscription BEFORE CAS (read barrier)
                final Subscription currSubscription = subscription;
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

    /**
     * Switches the active upstream subscription. Must be called only from the Subscriber thread.
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
                // Re-entrant switch: update field for outer frame
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

    public void cancel() {
        final Subscription currSubscription = subscription;
        final long currSourceRequested = SOURCE_REQUESTED_UPDATER.getAndSet(this, CANCELLED);
        if (currSourceRequested >= 0) {
            currSubscription.cancel();
        }
    }
}
