package com.antigravity.concurrency.lab.benchmark;

import com.antigravity.concurrency.lab.ConcurrencyEngine;
import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
public class ConcurrencyParadigmsJmhBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkPlan {
        ConcurrencyEngine casEngine;
        ConcurrencyEngine mailboxEngine;
        ConcurrencyEngine drainEngine;
        ConcurrencyEngine disruptorEngine;
        ConcurrencyEngine crdtEngine;

        @Setup(Level.Trial)
        public void setup() {
            EngineConfig cfg = EngineConfig.defaultInMemory();
            casEngine = ConcurrencyEngine.create(EngineType.CAS_LOOP, cfg);
            mailboxEngine = ConcurrencyEngine.create(EngineType.ACTOR_MAILBOX, cfg);
            drainEngine = ConcurrencyEngine.create(EngineType.WIP_DRAIN_LOOP, cfg);
            disruptorEngine = ConcurrencyEngine.create(EngineType.SMART_BATCHING_DISRUPTOR, cfg);
            crdtEngine = ConcurrencyEngine.create(EngineType.STRIPED_COUNTER, cfg);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            casEngine.close();
            mailboxEngine.close();
            drainEngine.close();
            disruptorEngine.close();
            crdtEngine.close();
        }
    }

    @Benchmark
    public long benchCasLoop(BenchmarkPlan plan) throws Exception {
        return plan.casEngine.mutate(1L).get();
    }

    @Benchmark
    public long benchActorMailbox(BenchmarkPlan plan) throws Exception {
        return plan.mailboxEngine.mutate(1L).get();
    }

    @Benchmark
    public long benchWipDrainLoop(BenchmarkPlan plan) throws Exception {
        return plan.drainEngine.mutate(1L).get();
    }

    @Benchmark
    public long benchSmartBatchingDisruptor(BenchmarkPlan plan) throws Exception {
        return plan.disruptorEngine.mutate(1L).get();
    }

    @Benchmark
    public long benchCommutativeCrdt(BenchmarkPlan plan) throws Exception {
        return plan.crdtEngine.mutate(1L).get();
    }
}
