# A Comparative Rigorous Analysis of Concurrency Paradigms: From Optimistic CAS to Commutative Replicated State Machines

**Author:** Technical Architecture & High-Performance Computing Lab  
**Target Specification:** Java 8+ / Modern Multicore Microarchitectures (x86-64 & AArch64)  
**Reference Codebase & Case Study:** Apple ServiceTalk [`SequentialSubscription.java` @ e1704cb](https://github.com/apple/servicetalk/blob/e1704cb9d14bdc499e2f9bd471f3572592538a52/servicetalk-concurrent-api/src/main/java/io/servicetalk/concurrent/api/SequentialSubscription.java) (the version analysed; `main` may change)  
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

Furthermore, this paper includes an in-depth **Architectural Post-Mortem** analyzing the known races in Apple ServiceTalk's sentinel-CAS implementation of `SequentialSubscription` (#1672 lost demand, a lost-cancel window) and why Conflict-Free Replicated Data Types (CRDTs) are not a fit for such stream coordinators.

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
> While 64-byte padding is essential for **long-lived singleton partition engines** (such as Disruptor RingBuffers or TigerBeetle event loops), applying padding to **short-lived, high-churn objects** (such as per-subscription reactive streams instances like `SequentialSubscription`) creates severe heap memory bloat for no gain, since those instances see low cross-core contention and a short hot path (~16 ns/op measured for `request(1)` through the drain loop of a local build of the PR #3677 design, single thread, Apple M1 Max).

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
   In `switchTo(Subscription next)`, the `currSourceRequested == SWITCHING` branch only stores `subscription = next`. It exists for same-thread re-entry (a source completing inside `request(n)`), where the outer frame delivers demand when the stack unwinds. A `switchTo` from *another* thread (e.g. a resubscribe scheduled on a timer thread by `retryWhen`/`repeatWhen`) that overlaps an earlier, still-unwinding `switchTo` takes the same branch, and the new subscription can end up with no `request(n)`: the stream hangs. Measured on ServiceTalk's class: 121 of 200,000 iterations lost demand in a local micro-stress.
2. **Lost Cancel Race Condition:**
   In `cancel()`, `subscription` is read *before* `sourceRequestedUpdater.getAndSet(this, CANCELLED)`. If a concurrent `switchTo` completes in between, the stale subscription is cancelled and the new one is never cancelled. Measured on ServiceTalk's class: 3,027 of 3,000,000 iterations in a local two-thread harness.
3. **Proposed Fix (PR #3677, open and under review):**
   Replace the sentinel CAS with ServiceTalk's non-blocking drain lock (`ConcurrentUtils.tryAcquireLock`/`releaseLock`: a thread that loses the race marks pending work and returns, it never blocks) plus an atomic mailbox for the next subscription, so one drain owner delivers demand. Reviewers asked to keep `cancel()` outside the lock, as on `main`, so a cancel reaches a source that is blocked inside `request(n)`. The PR is not merged; its correctness is still being reviewed.

---

### 3.2 Theoretical Analysis: Why CRDTs Cannot Solve `SequentialSubscription`

A tempting architectural hypothesis is whether `SequentialSubscription` could be converted into a Conflict-Free Replicated Data Type (CRDT) to eliminate locks. The analysis below argues that **a CRDT is not a fit for `SequentialSubscription`**:

#### 1. No Distributed Replicas to Converge
CRDTs exist to resolve eventual consistency across independent, distributed replicas. In `SequentialSubscription`, there is only a single object residing in a single JVM process. Reducing "CRDT" to a single process merely yields commutative atomic updates, which the class already utilizes where appropriate (e.g., `accumulateAndGet`).

#### 2. Delivering Side-Effects vs. Converging State
The purpose of `SequentialSubscription` is **delivering external side-effects** (`source.request(delta)`, `source.cancel()`), not converging mathematical state:
- `source.request(delta)` is strictly **non-idempotent**: invoking it twice doubles upstream emission.
- **Reactive Streams Rule §2.7:** a Subscriber MUST ensure that all calls on its Subscription's `request` and `cancel` methods are performed serially. (`main` lets `cancel()` run concurrently with `request(n)`; PR #3677's reviewers asked it to keep that behaviour for now and to defer this until blocking `request(n)` implementations are audited.)
- Deciding which thread delivers which delta, exactly once, to the active upstream source requires winning an execution turn (`getAndSet` or lock). Commutative algebraic merges cannot make this decision.

#### 3. `switchTo` is Fundamentally Non-Monotonic
`switchTo` alters the destination of future demand and resets the emission baseline (`sourceEmitted`). By analogy with the **CALM Theorem** (Consistency as Logical Monotonicity, stated for distributed programs):
- Monotonic operations can run without coordination.
- **Non-monotonic operations need coordination**.
Because switching upstream sources is non-monotonic, some coordination (a serialized drain owner, or CAS-based claims) is needed.

#### 4. Component Monotonicity Breakdown

| Component | Forms a Lattice? | Coordination Required? | Architectural Mechanism |
|---|---|---|---|
| `requested` | **Yes** (Saturating addition; invalid request is top $\top$) | **No** | Atomic `accumulateAndGet` |
| `cancelled` | **Yes** (Monotonic false $\to$ true / boolean OR) | **For effect only** | Atomic `getAndSet` to trigger `cancel()` once |
| `sourceEmitted` | **Yes** (Single-writer on Subscriber thread, monotonic) | **No** | Primitive `++sourceEmitted` |
| `Current source delivered level` | Monotonic only as a pair: $(\text{generation}, \text{delivered})$ | **Yes** | Winning permit to deliver delta |
| **Delivering request / cancel to source** | **NO** (External side-effect, non-idempotent) | **YES** | **Serialized delivery: sentinel CAS on `main`; non-blocking `tryAcquireLock` drain + mailbox in PR #3677** |

---

### 3.3 The Useful Takeaway: Lattice-Based Testing Invariants

While `SequentialSubscription` cannot be implemented as a CRDT, viewing demand through a **lattice lens** yields two rigorous mathematical invariants for randomized concurrency testing:

Notation: $R$ is the total (valid, saturating) demand requested by the downstream; $B_k$ is `sourceEmitted` at the moment source $k$ is installed (its baseline); $D_k$ is the total demand delivered to source $k$ via `request(n)`.

1. **Safety Invariant (No Duplication):**  
   At all times, no source receives more than it is owed:
   $$\forall k:\; D_k \le R - B_k$$
   (The sum $\sum_k D_k$ may legitimately exceed $R$: demand requested but not yet emitted by a completed source is carried over to the next one.)
2. **Liveness Invariant (No Loss):**  
   Once calls stop, and unless cancelled, the active source $a$ has received everything it is owed (for bounded demand):
   $$D_a = R - B_a$$

These two invariants capture the dual hazards of "duplicated demand" and "lost demand" and serve as formal assertion predicates for randomized stress harnesses.

---

## 4. Empirical Benchmark Results

Run on an Apple M1 Max (macOS, AArch64), OpenJDK 25.0.1 executing Java 8 bytecode, with `HdrHistogram` latency recording (`ConcurrencyLoadTestHarness`). Each scenario was run 3 times; the tables show the median of each metric and the throughput range across the runs. Run-to-run variance is large (up to ~2.5× for some engines), so treat smaller differences as noise.

> **Changes since the first version of these tables:** the WIP drain engine used to complete futures *before* its batch I/O; it now completes them after the I/O, like the mailbox and smart-batching engines, so its Scenario 2 latencies include persistence. `STRIPED_COUNTER` was previously labelled `COMMUTATIVE_CRDT` (same code).

### 4.1 Scenario 1: Pure In-Memory High Contention (16 Threads, 640,000 Operations)

| Paradigm Engine | Throughput (ops/s, median) | Throughput range | P50 (µs) | P90 (µs) | P99 (µs) | P99.9 (µs) | CAS Retries | Avg Batch Size |
|---|---|---|---|---|---|---|---|---|
| **CAS_LOOP** | 3,973,224 | 3.63M – 5.06M | 0.8 | 2.0 | 4.7 | 98.0 | **200,803** | 1.0 |
| **ACTOR_MAILBOX** | 251,796 | 246k – 255k | 59.2 | 77.2 | 194.7 | 709.6 | 0 | 1.0 |
| **WIP_DRAIN_LOOP** | 666,394 | 575k – 1.42M | **0.1** | 60.2 | 138.8 | 639.0 | 0 | 1.5 |
| **SMART_BATCHING** | 222,959 | 148k – 274k | 57.1 | 74.6 | 270.3 | 2,609.2 | 0 | 12.0 |
| **STRIPED_COUNTER** | 5,456,981 | 4.66M – 5.83M | 0.5 | 1.2 | 7.8 | 67.1 | **0** | 1.0 |

*Key Insights:*
- **The Cost of CAS:** about 200,000 failed CAS retries per run, spent on cache-line invalidations.
- **Striped Counter:** the highest throughput with zero retries, but `mutate()` returns a non-atomic snapshot sum, not the exact post-update value that CAS returns (§2, Level 4). The two rows do not measure the same guarantee.
- **WIP Drain Loop:** a **0.1 µs P50** (most callers drain their own request inline) and about **2.6× the mailbox throughput**, with wide run-to-run variance.
- **Smart Batching without I/O:** with nothing to amortize, the producer→consumer hand-off dominates (P50 57 µs); batching pays off in Scenario 2.

---

### 4.2 Scenario 2: Simulated I/O Persistence (8 Threads, 20 µs Commit Delay)

| Paradigm Engine | Throughput (ops/s, median) | Throughput range | P50 (µs) | P90 (µs) | P99 (µs) | P99.9 (µs) | Avg Batch Size |
|---|---|---|---|---|---|---|---|
| **ACTOR_MAILBOX** | 37,110 | 31k – 38k | 188.4 | 211.8 | 663.0 | 4,673.5 | 1.0 |
| **WIP_DRAIN_LOOP** | 108,037 | 68k – 109k | 47.1 | 68.0 | 201.0 | 1,584.1 | **3.3** |
| **SMART_BATCHING** | 123,469 | 62k – 145k | 53.2 | 82.8 | 285.7 | **657.4** | **5.2** |
| **CAS_LOOP** | 195,353 | 193k – 290k | 21.2 | 22.6 | 228.1 | 2,066.4 | 1.0 |
| **STRIPED_COUNTER** | 231,031 | 230k – 270k | 21.3 | 22.9 | 152.7 | 1,559.6 | 1.0 |

*Key Insights:*
- **Actor Mailbox Collapse:** one 20 µs I/O per message caps the mailbox near $1/T_{\text{io}}$ (37,110 ops/s measured, 50,000 theoretical).
- **Smart-Batching & WIP Drain:** both persist once per batch (average batch 5.2 and 3.3) and reach **2.9× to 3.3× the mailbox throughput**; smart batching has the tightest P99.9 (657 µs vs 4,674 µs for the mailbox).
- **Why CAS and the striped counter top this table:** each caller thread performs its own I/O, so up to 8 "commits" run in parallel and in no particular order. The mailbox, WIP drain and smart-batching engines persist one batch at a time. This row compares parallel, unordered persistence against serialized persistence, not the quality of the coordination scheme.

---

## 5. Architectural Decision Matrix

```text
                                  STATE COORDINATION DECISION TREE

                  Does the operation trigger external side effects or a
                  non-monotonic step (re-targeting, resetting state)?
                                    /        \
                                  YES         NO
                                  /             \
              Is I/O persistence required?       Commutative accumulation is enough:
                      /             \            Level 4 Striped Counter (LongAdder),
                    YES              NO          if snapshot (non-atomic) reads are acceptable
                    /                  \
       Expected ops/sec > 50K?       Must effects run one caller at a time?
            /          \               /                      \
          YES           NO           YES                      NO
          /               \           |                        |
   Use Level 3:        Use Level 2a:  Use Level 2b: cooperative  Use Level 1: CAS loop
   Smart-Batching      Actor Mailbox  drain (WIP counter, or     (claim with CAS/getAndSet,
   (Disruptor/         (Simple logic) ServiceTalk tryAcquireLock then act once)
   TigerBeetle)                       + mailbox, PR #3677). Pad
                                      long-lived singletons only.
```

---

## 6. Conclusion & Recommendations

1. **Be wary of multi-sentinel CAS state machines:** Issue #1672 shows how a branch designed for same-thread re-entry silently mis-handles a cross-thread overlap. Such races hide from ordinary tests (121 of 200,000 iterations in a micro-stress).
2. **Do not try to CRDT-ize side-effect coordinators:** Reactive Streams demand delivery is an external, non-idempotent side effect, and §2.7 requires `request`/`cancel` calls to be serial.
3. **Give external effects a single owner:** For short-lived coordinators that must serialize effects, a non-blocking drain (WIP counter or `tryAcquireLock`/`releaseLock`) with an atomic mailbox, as proposed in PR #3677 (under review), avoids both padding and blocking. Check such designs against the safety/liveness invariants of §3.3 instead of claiming correctness by construction.
4. **Deploy Smart-Batching for long-lived, high-throughput engines:** For core ledgers and matching engines where persistence dominates, 64-byte padded RingBuffers with group commit deliver optimal hardware utilization and amortization.
