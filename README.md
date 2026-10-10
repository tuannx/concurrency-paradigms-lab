# Concurrency Paradigms Lab: CAS vs Mailbox vs WIP Drain vs Smart-Batching vs Striped Counter

A high-performance benchmark lab and unified Java 8 library comparing 5 foundational concurrency paradigms for concurrent state coordination, featuring an architectural post-mortem on Apple ServiceTalk's `SequentialSubscription`.

---

## 📂 Project Structure

```
concurrency-paradigms-lab/
├── pom.xml                                    # Maven configuration (Java 8 target, JMH, HdrHistogram)
├── ARCHIFY.md                                 # Archify visual blueprints, ASCII memory maps & Mermaid flows
├── PAPER.md                                   # Academic research paper, post-mortem & microarchitecture analysis
├── README.md                                  # Lab documentation and execution guide
└── src
    ├── main/java/com/antigravity/concurrency/lab
    │   ├── ConcurrencyEngine.java             # Unified Facade Endpoint (Single Common Class)
    │   ├── StateEngine.java                   # Common abstraction interface
    │   ├── EngineType.java                    # Paradigm enum (CAS, Mailbox, WIP Drain, Disruptor, Striped Counter)
    │   ├── EngineConfig.java                  # Configuration (buffer size, simulated I/O latency)
    │   ├── EngineMetrics.java                 # Performance & retry counter metrics
    │   ├── SimulatedIo.java                   # Microsecond-scale simulated persistence / WAL fsync
    │   ├── core/
    │   │   ├── cas/CasLoopEngine.java         # Level 1: Multi-writer CAS loop (OCC)
    │   │   ├── mailbox/ActorMailboxEngine.java# Level 2a: Classic 1-by-1 Actor mailbox
    │   │   ├── drain/WipDrainLoopEngine.java  # Level 2b: Cooperative Lock-Free WIP Drain Loop (RxJava/Reactor pattern)
    │   │   ├── disruptor/                     # Level 3: Smart-Batching Single-Writer
    │   │   │   ├── PaddedFields.java          # 64-Byte CPU cache line padding (prevents false sharing)
    │   │   │   └── SmartBatchingDisruptorEngine.java # RingBuffer with dynamic batching & group commit
    │   │   └── crdt/CommutativeCrdtEngine.java# Level 4: Striped Partitioned Counter (LongAdder pattern)
    │   ├── benchmark/
    │   │   ├── ConcurrencyLoadTestHarness.java# Standalone multi-threaded load test with HdrHistogram
    │   │   └── ConcurrencyParadigmsJmhBenchmark.java # OpenJDK JMH microbenchmark
    │   └── servicetalk/
    │       └── SequentialSubscriptionExplained.java # Post-mortem & bug breakdown of Apple ServiceTalk #1672
    └── test/java/com/antigravity/concurrency/lab
        └── ConcurrencyEngineTest.java         # Verification test suite for all paradigms
```

---

## 🔬 Architectural Post-Mortem: ServiceTalk `SequentialSubscription`

The class `com.antigravity.concurrency.lab.servicetalk.SequentialSubscriptionExplained` breaks down the inner workings and documented production bugs of [`SequentialSubscription.java`](https://github.com/apple/servicetalk/blob/main/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java):

### Documented Production Bugs in Sentinel-CAS:
1. **Issue #1672 (Lost Demand):** In `switchTo(next)`, when `currSourceRequested == SWITCHING`, assigning `this.subscription = next;` without CAS leads to lost demand under concurrent re-entry or interleaved requests.
2. **Lost Cancel Race:** In `cancel()`, reading `subscription` before calling `getAndSet(CANCELLED)` races with concurrent `switchTo(next)`, permanently leaking uncancelled upstream subscriptions.
3. **Reactive Streams §3.9:** Non-positive requests (`n <= 0`) must be forwarded downstream to trigger `onError(IllegalArgumentException)`, not silently dropped.
4. **PR #3677 Resolution:** Apple ServiceTalk resolved these race conditions by abandoning pure lock-free sentinel CAS in favor of an **explicit lock + serialized mailbox pattern**.

### Why `SequentialSubscription` Should NOT Be CRDT-ized:
- **No Multi-Node Replicas:** There is only a single object in a single JVM process.
- **External Side-Effects (Not State Convergence):** `source.request(delta)` is non-idempotent (sending twice doubles demand).
- **Reactive Streams §2.7:** Mandates strict serialization of calls to `Subscription.request()`.
- **Non-Monotonicity of `switchTo`:** Switching upstream sources resets emission baselines and shifts effect targets. By the CALM Theorem, non-monotonic operations mathematically require coordination.
- **Useful Testing Invariant:** While not implementable as a CRDT, viewing demand through a lattice lens yields two crucial test assertions:
  - *Safety:* No source receives more demand than owed (no duplicate demand).
  - *Liveness:* The active source eventually receives all demand owed (no lost demand).

---

## 🚀 Quick Start

### 1. Run Unit Tests & Correctness Verification
```bash
mvn clean test
```

### 2. Run High-Contention Load Test Suite
```bash
mvn exec:java -Dexec.mainClass="com.antigravity.concurrency.lab.benchmark.ConcurrencyLoadTestHarness"
```

This runs:
- **Scenario 1:** Pure in-memory high contention (16 threads, 640,000 operations).
- **Scenario 2:** Simulated disk persistence / I/O latency (8 threads, 20 µs commit latency).
- Automatically prints P50, P90, P99, and P99.9 latency percentiles, throughput (ops/sec), CAS retries, and dynamic batch size statistics.

---

## 💡 Library Usage: Single Unified Endpoint (`ConcurrencyEngine`)

```java
import com.antigravity.concurrency.lab.ConcurrencyEngine;
import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineType;

// 1. Cooperative Lock-Free Drain Loop (RxJava / Reactive Streams WIP Trampoline)
try (ConcurrencyEngine engine = ConcurrencyEngine.wipDrain(EngineConfig.defaultInMemory())) {
    engine.mutate(10L).thenAccept(balance -> {
        System.out.println("New Balance: " + balance);
    });
}

// 2. Smart-Batching Single-Writer (Disruptor / TigerBeetle pattern with WAL fsync simulation)
EngineConfig config = EngineConfig.withSimulatedIo(20); // 20µs fsync
try (ConcurrencyEngine engine = ConcurrencyEngine.smartBatching(config)) {
    engine.mutate(100L).join();
    System.out.println("Consolidated State: " + engine.getState());
}

// 3. Striped Partitioned Counter (LongAdder pattern)
try (ConcurrencyEngine engine = ConcurrencyEngine.create(EngineType.STRIPED_COUNTER)) {
    engine.mutate(5L);
}
```
