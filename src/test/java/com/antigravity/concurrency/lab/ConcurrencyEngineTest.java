package com.antigravity.concurrency.lab;

import com.antigravity.concurrency.lab.servicetalk.SequentialSubscriptionExplained;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

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

        // Verify Reactive Streams §3.9: non-positive request forwarded to source
        AtomicLong invalidDemandReceived = new AtomicLong();
        SequentialSubscriptionExplained.Subscription mockSub3 = new SequentialSubscriptionExplained.Subscription() {
            @Override
            public void request(long n) {
                if (n <= 0) {
                    invalidDemandReceived.set(n);
                }
            }

            @Override
            public void cancel() {}
        };
        SequentialSubscriptionExplained seqSub3 = new SequentialSubscriptionExplained(mockSub3);
        seqSub3.request(-10);
        assertEquals(Long.MIN_VALUE, invalidDemandReceived.get());
    }
}
