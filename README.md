# Concurrency Paradigms Lab: CAS vs Mailbox vs WIP Drain vs Smart-Batching vs CRDT

A high-performance benchmark lab and unified Java 8 library comparing the 5 foundational concurrency paradigms for concurrent state coordination.

---

## 📂 Project Structure

```
concurrency-paradigms-lab/
├── pom.xml                                    # Maven configuration (Java 8 target, JMH, HdrHistogram)
├── ARCHIFY.md                                 # Archify visual blueprints, ASCII memory maps & Mermaid flows
├── PAPER.md                                   # Academic research paper & microarchitecture analysis
├── README.md                                  # Lab documentation and execution guide
└── src
    ├── main/java/com/antigravity/concurrency/lab
    │   ├── ConcurrencyEngine.java             # Unified Facade Endpoint (Single Common Class)
    │   ├── StateEngine.java                   # Common abstraction interface
    │   ├── EngineType.java                    # Paradigm enum (CAS, Mailbox, WIP Drain, Disruptor, CRDT)
    │   ├── EngineConfig.java                  # Configuration (buffer size, simulated I/O latency)
    │   ├── EngineMetrics.java                 # Performance & retry counter metrics
    │   ├── SimulatedIo.java                   # Microsecond-scale simulated persistence / WAL fsync
    │   ├── core/
    │   │   ├── cas/CasLoopEngine.java         # Level 1: Multi-writer CAS loop (OCC)
    │   │   ├── mailbox/ActorMailboxEngine.java# Level 2a: Classic 1-by-1 Actor mailbox
    │   │   ├── drain/WipDrainLoopEngine.java  # Level 2b: Cooperative Lock-Free WIP Drain Loop (ServiceTalk pattern)
    │   │   ├── disruptor/                     # Level 3: Smart-Batching Single-Writer
    │   │   │   ├── PaddedFields.java          # 64-Byte CPU cache line padding (prevents false sharing)
    │   │   │   └── SmartBatchingDisruptorEngine.java # RingBuffer with dynamic batching & group commit
    │   │   └── crdt/CommutativeCrdtEngine.java# Level 4: Math-driven Join-Semilattice (PN-Counter)
    │   ├── benchmark/
    │   │   ├── ConcurrencyLoadTestHarness.java# Standalone multi-threaded load test with HdrHistogram
    │   │   └── ConcurrencyParadigmsJmhBenchmark.java # OpenJDK JMH microbenchmark
    │   └── servicetalk/
    │       └── SequentialSubscriptionExplained.java # Line-by-line educational reproduction of Apple ServiceTalk
    └── test/java/com/antigravity/concurrency/lab
        └── ConcurrencyEngineTest.java         # Verification test suite for all paradigms
```

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

Inspired by the design of high-performance libraries and Apple ServiceTalk, all paradigms are accessible through a single entry point:

```java
import com.antigravity.concurrency.lab.ConcurrencyEngine;
import com.antigravity.concurrency.lab.EngineConfig;
import com.antigravity.concurrency.lab.EngineType;

// 1. Cooperative Lock-Free Drain Loop (ServiceTalk SequentialSubscription pattern)
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

// 3. Mathematical CRDT Semilattice
try (ConcurrencyEngine engine = ConcurrencyEngine.crdt(EngineConfig.defaultInMemory())) {
    engine.mutate(5L);
}
```

---

## 🔬 ServiceTalk `SequentialSubscription` Deep Dive

The class `com.antigravity.concurrency.lab.servicetalk.SequentialSubscriptionExplained` breaks down the inner workings of [`SequentialSubscription.java`](https://github.com/tuannx/servicetalk/blob/main/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java):
1. **Zero-Allocation Updaters:** Uses `AtomicLongFieldUpdater` and `AtomicIntegerFieldUpdater` directly on volatile primitive fields.
2. **Bitmask State Machine:** Encodes states into a single primitive `long`:
   - Positive numbers: Monotonic requested items.
   - Negative control values: `SWITCHING = -1`, `REQUESTED = -2`, `CANCELLED = -3`.
3. **Lock-Free Cooperative Drain Loop:** Eliminates worker threads by turning calling threads into cooperative workers that drain accumulated work without blocking or queueing.
