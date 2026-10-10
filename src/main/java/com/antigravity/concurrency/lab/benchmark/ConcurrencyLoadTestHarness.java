package com.antigravity.concurrency.lab.benchmark;

import com.antigravity.concurrency.lab.ConcurrencyEngine;
import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineType;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <h1>Standalone High-Contention Load Test & Benchmark Harness</h1>
 *
 * <p>Executes multi-threaded stress tests across all 5 concurrency paradigms.
 * Records end-to-end latency histograms (P50, P90, P99, P99.9) using HdrHistogram,
 * measures throughput (ops/sec), tracks CAS retries, and measures dynamic batch sizes.</p>
 */
public final class ConcurrencyLoadTestHarness {

    public static final class TestResult {
        public final EngineType type;
        public final String scenario;
        public final long totalOps;
        public final double throughputOpsPerSec;
        public final double meanLatencyMicros;
        public final double p50Micros;
        public final double p90Micros;
        public final double p99Micros;
        public final double p999Micros;
        public final long casRetries;
        public final double avgBatchSize;
        public final boolean stateCorrect;

        public TestResult(EngineType type, String scenario, long totalOps, double throughputOpsPerSec,
                          double meanLatencyMicros, double p50Micros, double p90Micros, double p99Micros,
                          double p999Micros, long casRetries, double avgBatchSize, boolean stateCorrect) {
            this.type = type;
            this.scenario = scenario;
            this.totalOps = totalOps;
            this.throughputOpsPerSec = throughputOpsPerSec;
            this.meanLatencyMicros = meanLatencyMicros;
            this.p50Micros = p50Micros;
            this.p90Micros = p90Micros;
            this.p99Micros = p99Micros;
            this.p999Micros = p999Micros;
            this.casRetries = casRetries;
            this.avgBatchSize = avgBatchSize;
            this.stateCorrect = stateCorrect;
        }
    }

    public static TestResult runBenchmark(
            EngineType type,
            EngineConfig config,
            int threadCount,
            int opsPerThread,
            String scenarioName) throws Exception {

        final long totalOps = (long) threadCount * opsPerThread;
        final Histogram histogram = new Histogram(3600000000000L, 3); // up to 1 hour, 3 sig figs
        final Histogram[] threadHistograms = new Histogram[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threadHistograms[i] = new Histogram(3600000000000L, 3);
        }

        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threadCount);

        try (ConcurrencyEngine engine = ConcurrencyEngine.create(type, config)) {
            for (int t = 0; t < threadCount; t++) {
                final int threadIdx = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        final Histogram localHist = threadHistograms[threadIdx];
                        for (int i = 0; i < opsPerThread; i++) {
                            long start = System.nanoTime();
                            engine.mutate(1L).get();
                            long durationNanos = System.nanoTime() - start;
                            localHist.recordValue(durationNanos);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Warmup pause
            Thread.sleep(100);

            long startTime = System.nanoTime();
            startLatch.countDown();
            doneLatch.await(60, TimeUnit.SECONDS);
            long totalNanos = System.nanoTime() - startTime;

            // Aggregate histograms
            for (Histogram h : threadHistograms) {
                histogram.add(h);
            }

            double durationSeconds = totalNanos / 1_000_000_000.0;
            double throughput = totalOps / durationSeconds;
            long expectedState = totalOps;
            long actualState = engine.getState();
            boolean stateCorrect = (actualState == expectedState);

            return new TestResult(
                    type,
                    scenarioName,
                    totalOps,
                    throughput,
                    histogram.getMean() / 1000.0,
                    histogram.getValueAtPercentile(50.0) / 1000.0,
                    histogram.getValueAtPercentile(90.0) / 1000.0,
                    histogram.getValueAtPercentile(99.0) / 1000.0,
                    histogram.getValueAtPercentile(99.9) / 1000.0,
                    engine.getMetrics().getCasRetries(),
                    engine.getMetrics().getAverageBatchSize(),
                    stateCorrect
            );
        } finally {
            executor.shutdownNow();
        }
    }

    public static void printResults(List<TestResult> results, String title) {
        System.out.println("\n========================================================================================================================");
        System.out.println("  BENCHMARK REPORT: " + title);
        System.out.println("========================================================================================================================");
        System.out.printf("%-26s | %-12s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s%n",
                "Paradigm Engine", "Throughput", "P50 (µs)", "P90 (µs)", "P99 (µs)", "P99.9(µs)", "Retries", "Avg Batch");
        System.out.println("------------------------------------------------------------------------------------------------------------------------");

        for (TestResult r : results) {
            System.out.printf("%-26s | %10.0f/s | %10.1f | %10.1f | %10.1f | %10.1f | %10d | %10.1f%n",
                    r.type.name(),
                    r.throughputOpsPerSec,
                    r.p50Micros,
                    r.p90Micros,
                    r.p99Micros,
                    r.p999Micros,
                    r.casRetries,
                    r.avgBatchSize
            );
        }
        System.out.println("========================================================================================================================\n");
    }

    public static void main(String[] args) throws Exception {
        System.out.println(">>> Starting Concurrency Paradigms Benchmark Suite (Java 8 / Apple Silicon) <<<\n");

        // Scenario 1: Pure In-Memory High Contention (16 Threads, 40,000 ops each = 640,000 total ops)
        System.out.println("Running Scenario 1: High-Contention In-Memory (16 Threads, 640,000 Ops)...");
        List<TestResult> inMemoryResults = new ArrayList<>();
        EngineConfig inMemoryConfig = EngineConfig.defaultInMemory();

        for (EngineType type : EngineType.values()) {
            System.out.print("  -> Testing " + type + "... ");
            System.out.flush();
            TestResult res = runBenchmark(type, inMemoryConfig, 16, 40000, "In-Memory High Contention");
            inMemoryResults.add(res);
            System.out.println("Done! Throughput: " + String.format("%.0f ops/s", res.throughputOpsPerSec));
        }
        printResults(inMemoryResults, "Pure In-Memory High Contention (16 Threads)");

        // Scenario 2: Simulated Persistence / I/O Bottleneck (8 Threads, 2,500 ops each, 20µs I/O delay)
        System.out.println("Running Scenario 2: Simulated I/O Persistence (8 Threads, 20µs latency per commit)...");
        List<TestResult> ioResults = new ArrayList<>();
        EngineConfig ioConfig = EngineConfig.withSimulatedIo(20);

        for (EngineType type : EngineType.values()) {
            System.out.print("  -> Testing " + type + "... ");
            System.out.flush();
            TestResult res = runBenchmark(type, ioConfig, 8, 2500, "Simulated I/O Persistence (20µs)");
            ioResults.add(res);
            System.out.println("Done! Throughput: " + String.format("%.0f ops/s", res.throughputOpsPerSec));
        }
        printResults(ioResults, "Simulated Persistence I/O (8 Threads, 20µs Commit Delay)");
    }
}
