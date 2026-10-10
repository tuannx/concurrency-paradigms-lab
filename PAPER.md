# A Comparative Rigorous Analysis of Concurrency Paradigms: From Optimistic CAS to Commutative Replicated State Machines

**Author:** Technical Architecture & High-Performance Computing Lab  
**Target Specification:** Java 8+ / Modern Multicore Microarchitectures (x86-64 & AArch64)  
**Reference Codebase & Case Study:** Apple ServiceTalk [`SequentialSubscription.java`](https://github.com/apple/servicetalk/blob/main/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java)  
**Associated Post-Mortem:** [ServiceTalk Issue #1672](https://github.com/apple/servicetalk/issues/1672) | [ServiceTalk PR #3677](https://github.com/apple/servicetalk/pull/3677)

---

## Abstract

State mutation under extreme concurrency is the core bottleneck in high-throughput transactional systems (e.g., financial ledgers, matching engines, low-latency messaging fabrics). For decades, system architects oscillated between **Optimistic Concurrency Control (OCC / CAS Loops)** and **Pessimistic Single-Writer Queues (Actor / Mailbox)**. However, modern multicore hardware exposes profound physical limits: cache-line invalidation storms in CAS, context switches in queues, and serialized disk I/O stalls in one-by-one mailboxes.

This paper provides an end-to-end mathematical, microarchitectural, and empirical evaluation of five distinct concurrency paradigms:
1. **Level 1: Multi-Writer CAS Loop (Optimistic Concurrency Control)**
2. **Level 2a: Classic Actor Mailbox (1-by-1 Queue Serialization)**
3. **Level 2b: Cooperative Lock-Free Drain Loop (Reactive Streams / RxJava WIP Trampoline)**
4. **Level 3: Pipelined Smart-Batching Single-Writer (LMAX Disruptor & TigerBeetle Group Commit)**
5. **Level 4: Striped Partitioned Counter (Commutative Algebra / LongAdder Pattern)**

Furthermore, this paper includes an in-depth **Architectural Post-Mortem** analyzing why Apple ServiceTalk's original sentinel-CAS implementation of `SequentialSubscription` suffered from race conditions (#1672, lost cancels) and why applying Conflict-Free Replicated Data Types (CRDTs) to such stream coordinators is a fundamental category error.

---

## 1. Microarchitectural Realities & Hardware Contention

### 1.1 The MESI/MOESI Cache Coherence Protocol and Cache Bouncing

On modern symmetric multiprocessing (SMP) and Non-Uniform Memory Access (NUMA) architectures, CPU cores interact through hierarchical L1, L2, and shared L3 caches organized in **64-byte cache lines**.

When multiple threads concurrently mutate an `AtomicLong` via CAS (`compareAndSet`):
1. **Invalidation Broadcast:** The executing core must acquire the cache line in the **Modified (M)** state by broadcasting an *Invalidate* signal across the inter-core interconnect (QPI / UPI / ARM Coherent Hub Interface).
2. **Cache Line Invalidation Storm:** All other cores holding that 64-byte line in the **Shared (S)** state transition to the **Invalid (I)** state.
3. **Pipeline Stalls:** Retrying cores suffer L1/L2 cache misses, stalling pipelines while fetching lines from L3 or remote store buffers.

As concurrency $N$ scales, CAS retries explode and CPU utilization becomes dominated by memory-bus coherency transactions rather than computational throughput.

### 1.2 False Sharing and Cache Line Alignment (64 Bytes)

Even when two threads write to completely independent variables (e.g., Producer writing `claimSequence` and Consumer writing `cursor`), if those variables reside within the same 64-byte boundary:

$$\text{Address Offset} \pmod{64} = 0$$

hardware cache controllers treat them as conflicting, triggering false cache invalidations.

> [!NOTE]
> **Allocation & Footprint Trade-off:**
> While 64-byte padding is essential for **long-lived singleton partition engines** (such as Disruptor RingBuffers or TigerBeetle event loops), applying padding to **short-lived, high-churn objects** (such as per-subscription reactive streams instances like `SequentialSubscription`) creates severe heap memory bloat for no gain, since those instances typically experience low cross-core contention and hot-path execution around ~16 ns/op.

---

## 2. In-Depth Paradigm Breakdown

### Level 1: Multi-Writer CAS Loop (OCC)
* **Core Mechanism:** Threads read latest state, compute next state, and execute `compareAndSet()`. If another thread updated state concurrently, CAS fails and caller spins.
* **Failure Mode:** Under high contention, retries explode exponentially. Standalone unbatched I/O syscalls saturate disk queue depths.

### Level 2a: Classic Actor Mailbox (1-by-1 Queue)
* **Core Mechanism:** State is strictly owned by a single dedicated worker thread. Incoming mutations are wrapped into command objects and pushed to an MPSC queue.
* **Limitations:** GC allocation churn, queue CAS contention, context switching, and the **1-by-1 I/O Bottleneck**:
  $$\text{Throughput}_{\max} \le \frac{1}{T_{\text{io}}}$$
  If $T_{\text{io}} = 20\,\mu\text{s}$, throughput cannot exceed $50,000\text{ ops/sec}$.

### Level 2b: Cooperative Lock-Free Drain Loop (RxJava / Reactive Streams WIP Trampoline)
* **Core Mechanism:** Eliminates dedicated background actor threads using a volatile `wip` (Work-In-Progress) integer updater:
  - Thread $A$ registers work and calls `wipUpdater.getAndIncrement(this)`.
  - If `wip == 0`, Thread $A$ becomes the active drain worker.
  - If `wip > 0`, work has been registered; the arriving thread returns immediately without blocking.
  - The active worker executes a cooperative trampoline loop, draining all pending work.
* **Attribution Clarification:** This is the classic RxJava/Reactor WIP drain loop implemented in `WipDrainLoopEngine`. It is distinct from ServiceTalk's sentinel-CAS pattern.

### Level 3: Smart-Batching Single-Writer (Disruptor / LMAX / TigerBeetle)
* **Core Mechanism:** Single-writer operating on a pre-allocated, 64-byte padded RingBuffer.
* **Dynamic Batching Principle:** Consumer drains all contiguous claimed slots ($N$ events) in one pass:
  1. Computes $N$ state transitions consecutively in L1/L2 cache (nanoseconds).
  2. Performs **exactly ONE Group Commit I/O (WAL fsync)** for the entire batch.
* **Amortized Cost:**
  $$\text{Latency}_{\text{amortized}} = \frac{T_{\text{fsync}}}{N} + T_{\text{calc}}$$
  As load increases, $N$ dynamically scales up, driving amortized per-op I/O cost toward zero.

### Level 4: Striped Partitioned Counter (Commutative Algebra / LongAdder Pattern)
* **Core Mechanism:** State is partitioned across independent 64-byte padded cells. Writers update local stripes without cross-core coordination.
* **Important Clarification:** While addition is commutative ($a + b = b + a$) and associative ($(a + b) + c = a + (b + c)$):
  - **Not a CRDT:** Addition is NOT idempotent ($x + x \neq x$). There are no multi-node replicas and no lattice join `merge()` function.
  - **Weak Consistency Read:** `getState()` performs a non-atomic summation across stripes. Comparing its throughput directly against linearizable CAS represents a classic consistency-versus-speed trade-off.

---

## 3. Case Study & Post-Mortem: ServiceTalk's `SequentialSubscription`

### 3.1 The Original Sentinel-CAS Architecture and Its Failure

In Apple ServiceTalk (`io.servicetalk.concurrent.api.SequentialSubscription`), concurrency between `request(n)` and `switchTo(Subscription)` was originally coordinated using negative sentinel values in `volatile long sourceRequested`:
- `SWITCHING = -1`: A subscription switch is in progress.
- `REQUESTED = -2`: Demand arrived during a switch.
- `CANCELLED = -3`: The subscription was cancelled.

#### Documented Production Bugs in the Sentinel-CAS Pattern:
1. **Issue #1672 (Lost Demand):**
   In `switchTo(Subscription next)`, if `currSourceRequested == SWITCHING`, the assignment `this.subscription = next;` was executed without CAS coordination. Under concurrent re-entry or interleaved requests, the baseline `effectiveSourceRequested` and the active subscription reference desynchronized, causing demand to be permanently lost (measured at 121 lost demands per 200,000 operations).
2. **Lost Cancel Race Condition:**
   In `cancel()`, `subscription` was read *before* executing `sourceRequestedUpdater.getAndSet(this, CANCELLED)`. If a concurrent `switchTo` replaced the subscription between the read and the `getAndSet`, the cancel was delivered to the stale subscription while `switchTo` bypassed the cancellation check, permanently leaking uncancelled upstream subscriptions (measured at 3,027 uncancelled subscriptions per 3,000,000 operations).
3. **The Solution (PR #3677):**
   ServiceTalk abandoned pure lock-free sentinel CAS for `SequentialSubscription`, transitioning to an **explicit lock + serialized mailbox pattern** to ensure provable correctness and zero race conditions.

---

### 3.2 Theoretical Analysis: Why CRDTs Cannot Solve `SequentialSubscription`

A tempting architectural hypothesis is whether `SequentialSubscription` could be converted into a Conflict-Free Replicated Data Type (CRDT) to eliminate locks. A rigorous analysis proves that **CRDT-izing `SequentialSubscription` is impossible and conceptually unsound**:

#### 1. No Distributed Replicas to Converge
CRDTs exist to resolve eventual consistency across independent, distributed replicas. In `SequentialSubscription`, there is only a single object residing in a single JVM process. Reducing "CRDT" to a single process merely yields commutative atomic updates, which the class already utilizes where appropriate (e.g., `accumulateAndGet`).

#### 2. Delivering Side-Effects vs. Converging State
The purpose of `SequentialSubscription` is **delivering external side-effects** (`source.request(delta)`, `source.cancel()`), not converging mathematical state:
- `source.request(delta)` is strictly **non-idempotent**: invoking it twice doubles upstream emission.
- **Reactive Streams Specification Rule §2.7** mandates that calls to `Subscription.request()` MUST be strictly serialized.
- Deciding which thread delivers which delta, exactly once, to the active upstream source requires winning an execution turn (`getAndSet` or lock). Commutative algebraic merges cannot make this decision.

#### 3. `switchTo` is Fundamentally Non-Monotonic
`switchTo` alters the destination of future demand and resets the emission baseline (`sourceEmitted`). Under the **CALM Theorem** (Consistency as Logical Monotonicity):
- Monotonic operations do not require coordination.
- **Non-monotonic operations strictly require coordination**.
Because switching upstream sources is non-monotonic, coordination (locks or serialized drain loops) is mathematically unavoidable.

#### 4. Component Monotonicity Breakdown

| Component | Forms a Lattice? | Coordination Required? | Architectural Mechanism |
|---|---|---|---|
| `requested` | **Yes** (Saturating addition; invalid request is top $\top$) | **No** | Atomic `accumulateAndGet` |
| `cancelled` | **Yes** (Monotonic false $\to$ true / boolean OR) | **For effect only** | Atomic `getAndSet` to trigger `cancel()` once |
| `sourceEmitted` | **Yes** (Single-writer on Subscriber thread, monotonic) | **No** | Primitive `++sourceEmitted` |
| `Current source delivered level` | Monotonic only as a pair: $(\text{generation}, \text{delivered})$ | **Yes** | Winning permit to deliver delta |
| **Delivering request / cancel to source** | **NO** (External side-effect, non-idempotent) | **YES** | **Drain loop or Explicit Lock (PR #3677)** |

---

### 3.3 The Useful Takeaway: Lattice-Based Testing Invariants

While `SequentialSubscription` cannot be implemented as a CRDT, viewing demand through a **lattice lens** yields two rigorous mathematical invariants for randomized concurrency testing:

1. **Safety Invariant (No Duplication):**  
   No upstream source receives more demand than the total requested minus what has already been accounted for.
   $$\sum \text{Delivered}(\text{source}_i) \le \text{Requested}_{\text{total}}$$
2. **Liveness Invariant (No Loss):**  
   The active upstream source must eventually receive all outstanding demand:
   $$\lim_{t \to \infty} \text{Delivered}(\text{source}_{\text{active}}) = \text{Requested}_{\text{total}} - \text{Emitted}_{\text{total}}$$

These two invariants capture the dual hazards of "duplicated demand" and "lost demand" and serve as formal assertion predicates for randomized stress harnesses.

---

## 4. Empirical Benchmark Results

Tests executed on macOS AArch64 (Apple Silicon), Java JDK 8 bytecode compatibility, using `HdrHistogram` for microsecond latency tracking.

### 4.1 Scenario 1: Pure In-Memory High Contention (16 Threads, 640,000 Operations)

| Paradigm Engine | Throughput (ops/s) | P50 (µs) | P90 (µs) | P99 (µs) | P99.9 (µs) | CAS Retries | Avg Batch Size |
|---|---|---|---|---|---|---|---|
| **CAS_LOOP** | 3,888,517 | 0.5 | 2.0 | 5.7 | 64.4 | **209,603** | 1.0 |
| **ACTOR_MAILBOX** | 264,014 | 57.4 | 81.8 | 153.2 | 477.4 | 0 | 1.0 |
| **WIP_DRAIN_LOOP** | 817,416 | **0.1** | 56.8 | 93.8 | 579.1 | 0 | 1.5 |
| **SMART_BATCHING** | 272,929 | 51.0 | 70.6 | 159.7 | 1,302.5 | 0 | 10.9 |
| **STRIPED_COUNTER** | 3,595,543 | **0.2** | **0.8** | **2.4** | **29.2** | **0** | 1.0 |

*Key Insights:*
- **The True Cost of CAS:** The CAS loop suffered **209,603 failed retry spins**, wasting CPU cycles on cache line invalidations.
- **WIP Drain Loop:** Achieved a **0.1 µs P50 latency** and **3.1× higher throughput than the classic Actor Mailbox**, proving the efficiency of cooperative trampoline drain without dedicated actor threads.
- **Striped Counter:** Achieved 3.6M ops/s with **zero retries** and an ultra-tight P99 latency of **2.4 µs**, albeit with weak snapshot read consistency.

---

### 4.2 Scenario 2: Simulated I/O Persistence (8 Threads, 20 µs Commit Delay)

| Paradigm Engine | Throughput (ops/s) | P50 (µs) | P90 (µs) | P99 (µs) | P99.9 (µs) | Avg Batch Size |
|---|---|---|---|---|---|---|
| **ACTOR_MAILBOX** | 38,324 | 187.6 | 224.5 | 751.6 | 2,246.7 | 1.0 |
| **SMART_BATCHING** | 129,498 | 56.5 | 76.8 | 179.5 | **521.2** | **4.5** |
| **WIP_DRAIN_LOOP** | 155,273 | 26.9 | 44.9 | 232.1 | 1,399.8 | **7.0** |
| **CAS_LOOP** | 284,804 | 21.6 | 22.7 | 90.2 | 908.3 | 1.0 |
| **STRIPED_COUNTER** | 269,790 | 21.5 | 22.5 | 86.0 | 918.5 | 1.0 |

*Key Insights:*
- **Actor Mailbox Collapse:** Under 20 µs I/O, the 1-by-1 Actor Mailbox collapsed to **38,324 ops/s** because every message suffered serialized disk latency.
- **Smart-Batching & WIP Drain:** Automatically grouped incoming requests into dynamic batches ($N \approx 4.5 - 7.0$), achieving **3.4× to 4.0× higher throughput** and cutting P99.9 tail latency from 2,246 µs down to 521 µs.

---

## 5. Architectural Decision Matrix

```text
                                  STATE COORDINATION DECISION TREE

                             Does the operation require external
                            side-effects & non-monotonic switching?
                                         /        \
                                       YES         NO
                                       /             \
                   Is I/O persistence required?       Use Level 4: Striped Counter
                           /             \            (LongAdder / Commutative metrics)
                         YES              NO
                         /                  \
            Expected ops/sec > 50K?          Lifecycle & Instance Count?
                 /          \                  /           \
               YES           NO          Per-Subscription  Singleton Core
               /               \         (Short-lived)     (Long-lived)
        Use Level 3:        Use Level 2a:      |                 |
        Smart-Batching      Actor Mailbox  Use Lock+Mailbox  Use Level 2b:
        (Disruptor/         (Simple logic) (PR #3677 pattern) WIP Drain Loop
        TigerBeetle)
```

---

## 6. Conclusion & Recommendations

1. **Do not use Sentinel CAS for complex state coordination:** ServiceTalk's experience with Issue #1672 proves that multi-state CAS sentinels without mutual exclusion create treacherous edge-case race conditions.
2. **Do not attempt to CRDT-ize side-effect coordinators:** Reactive Streams demand delivery is an external, non-idempotent side effect governed by §2.7 serialization rules.
3. **Use Explicit Locks + Mailboxes for short-lived, correctness-critical coordinators:** When instances are created frequently and the hot path is tens of nanoseconds, explicit locks with short critical sections (PR #3677) provide provable correctness without memory-padding overhead.
4. **Deploy Smart-Batching for long-lived, high-throughput engines:** For core ledgers and matching engines where persistence dominates, 64-byte padded RingBuffers with group commit deliver optimal hardware utilization and amortization.
