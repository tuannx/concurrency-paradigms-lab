package com.antigravity.concurrency.lab;

import com.antigravity.concurrency.lab.servicetalk.SequentialSubscriptionExplained;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcurrencyEngineTest {

    @Test
    public void testAllEnginesSingleThreaded() throws Exception {
        for (EngineType type : EngineType.values()) {
            try (ConcurrencyEngine engine = ConcurrencyEngine.create(type)) {
                for (int i = 1; i <= 100; i++) {
                    long res = engine.mutate(1L).get();
                    assertEquals("Mismatch on op " + i + " for " + type, (long) i, res);
                }
                assertEquals("Final state mismatch for " + type, 100L, engine.getState());
            }
        }
    }

    @Test
    public void testAllEnginesConcurrentMutations() throws Exception {
        final int threads = 8;
        final int opsPerThread = 2500;
        final long expectedTotal = (long) threads * opsPerThread;

        for (EngineType type : EngineType.values()) {
            try (ConcurrencyEngine engine = ConcurrencyEngine.create(type)) {
                List<CompletableFuture<Long>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    CompletableFuture<Void> threadWork = CompletableFuture.runAsync(() -> {
                        for (int i = 0; i < opsPerThread; i++) {
                            try {
                                engine.mutate(1L).get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    futures.add(threadWork.thenApply(v -> 0L));
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                assertEquals("State mismatch under concurrency for " + type, expectedTotal, engine.getState());
            }
        }
    }

    @Test
    public void testSequentialSubscriptionExplained() {
        AtomicLong requestedReceived = new AtomicLong();
        SequentialSubscriptionExplained.Subscription mockSub1 = new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {
                requestedReceived.addAndGet(n);
            }

            @Override
            public void cancel() {}
        };

        SequentialSubscriptionExplained seqSub = new SequentialSubscriptionExplained(mockSub1);
        seqSub.request(10);
        seqSub.request(5);
        assertEquals(15L, requestedReceived.get());

        // Switch to sub 2
        AtomicLong requestedReceivedSub2 = new AtomicLong();
        SequentialSubscriptionExplained.Subscription mockSub2 = new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {
                requestedReceivedSub2.addAndGet(n);
            }

            @Override
            public void cancel() {}
        };

        seqSub.switchTo(mockSub2);
        // Sub2 receives the cumulative pending demand since 0 items were emitted
        assertEquals(15L, requestedReceivedSub2.get());

        // Verify Reactive Streams §3.9: non-positive request forwarded to source, sanitized like main
        // (a negative n is kept, 0 becomes Long.MIN_VALUE).
        assertEquals(-10L, invalidRequestForwardedToSource(-10));
        assertEquals(Long.MIN_VALUE, invalidRequestForwardedToSource(0));
    }

    @Test
    public void testWipDrainCompletesOnlyAfterItsBatchIsPersisted() throws Exception {
        final int ioMicros = 200_000; // parks 200 ms per drained batch
        try (ConcurrencyEngine engine = ConcurrencyEngine.create(EngineType.WIP_DRAIN_LOOP,
                EngineConfig.withSimulatedIo(ioMicros))) {
            // The first caller becomes the drainer and sits in its batch I/O; the second caller's request is
            // drained by it in the next batch and must not complete before that batch's I/O.
            CompletableFuture<Long> first = CompletableFuture.supplyAsync(() -> engine.mutate(1L).join());
            Thread.sleep(50);
            long start = System.nanoTime();
            engine.mutate(1L).get();
            long waitedMicros = (System.nanoTime() - start) / 1000;
            first.get();
            // Expected ~150 ms if it completes before its own batch I/O, ~350 ms after it; 250 ms leaves margin both
            // ways (parkNanos may return a little early or late).
            assertTrue("completed before its batch was persisted, waited " + waitedMicros + " us",
                    waitedMicros >= ioMicros * 5L / 4);
        }
    }

    private static long invalidRequestForwardedToSource(long n) {
        final AtomicLong invalidDemandReceived = new AtomicLong();
        SequentialSubscriptionExplained.Subscription mockSub = new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {
                if (n <= 0) {
                    invalidDemandReceived.set(n);
                }
            }

            @Override
            public void cancel() {}
        };
        new SequentialSubscriptionExplained(mockSub).request(n);
        return invalidDemandReceived.get();
    }

    @Test
    public void testSequentialSubscriptionExplainedReentrantSwitchDeliversDemandToLatestSource() {
        final AtomicLong requestedFromLatest = new AtomicLong();
        final SequentialSubscriptionExplained.Subscription latest =
                new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {
                requestedFromLatest.addAndGet(n);
            }

            @Override
            public void cancel() {}
        };
        final SequentialSubscriptionExplained[] seqSub = new SequentialSubscriptionExplained[1];
        final SequentialSubscriptionExplained.Subscription completesSynchronously =
                new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {
                // The source completes inside request(n) and the next source subscribes on the same thread.
                seqSub[0].switchTo(latest);
            }

            @Override
            public void cancel() {}
        };
        seqSub[0] = new SequentialSubscriptionExplained(new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {}
        });
        seqSub[0].request(15);
        seqSub[0].switchTo(completesSynchronously);
        assertEquals(15L, requestedFromLatest.get());
    }
}
