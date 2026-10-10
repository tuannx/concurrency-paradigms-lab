# Architectural Specification & Visual Blueprints (Archify Edition)
## Comparative Concurrency Paradigms: From CAS to Striped Counters

This document provides architectural blueprints, hardware cache layout diagrams, and state-machine sequence flows for the fundamental concurrency coordination models, accompanied by an architectural post-mortem on Reactive Streams coordinators.

---

## 1. High-Level Evolution Architecture (Archify Overview)

```
====================================================================================================
                        CONCURRENCY & STATE COORDINATION EVOLUTION SPECTRUM
====================================================================================================

  [ LEVEL 1: CAS LOOP ]         [ LEVEL 2a: ACTOR MAILBOX ]     [ LEVEL 2b: WIP DRAIN LOOP ]
  Optimistic Multi-Writer       Pessimistic Single-Writer       Cooperative Trampoline (RxJava/Reactor)
  -----------------------       -------------------------       ---------------------------------------
  Thread 1 ──┐                  Thread 1 ──┐                    Thread 1 ──┐ (Accumulate demand)
  Thread 2 ──┼─► [CAS State]    Thread 2 ──┼─► [Queue] ──► [Actor] Thread 2 ─┼─► [WIP State] ─► [Active]
  Thread 3 ──┘ (MESI Storm)     Thread 3 ──┘   (1-by-1 worker)  Thread 3 ──┘ (Zero thread switch)
       │                                     │                               │
       ▼                                     ▼                               ▼
  Bottleneck: Cache Contention  Bottleneck: Context Switches    Bottleneck: Linear Mutex-free
  & Invalidation Storm          & 1-by-1 Disk I/O Stall         Sequential Processing

                                             │
                                             ▼
  [ LEVEL 3: SMART-BATCHING DISRUPTOR ]                         [ LEVEL 4: STRIPED COUNTER ]
  Mechanical Sympathy & Group Commit                            Partitioned Commutative Math (LongAdder)
  ----------------------------------                            ----------------------------------------
  Thread 1 ──┐  RingBuffer (Padded 64B)   Batch Drain           Thread 1 ──► [Partition 0] ──┐
  Thread 2 ──┼─► [Slot 0][Slot 1][Slot 2] ─────────► [Single]   Thread 2 ──► [Partition 1] ──┼─► Non-atomic
  Thread 3 ──┘   Pre-allocated, Zero-GC               Writer    Thread 3 ──► [Partition 2] ──┘   Snapshot
       │                                                │                    │
       ▼                                                ▼                    ▼
  1 State Transition per Batch (L1 Cache) ──► 1 Amortized Fsync   Zero-Coordination Writes!
  Measured throughput: see PAPER.md §4           Measured throughput: see PAPER.md §4
====================================================================================================
```

---

## 2. Hardware Memory Layout & CPU Cache Line Alignment (64 Bytes)

In multicore architectures (x86-64 and ARM64 / Apple Silicon), the L1/L2 cache operates in **64-byte cache lines**.

### 2.1 The False Sharing Trap in Unpadded Queues & Atomic Fields

```
+---------------------------------------------------------------------------------------+
|                              CPU L1 CACHE LINE (64 BYTES)                             |
|                                                                                       |
|  [ Object Header ] [ Producer claimSequence (8B) ] [ Consumer cursor (8B) ] [ Free ]  |
|       (16B)                   (WRITTEN BY CORE 1)     (WRITTEN BY CORE 2)     (32B)   |
+---------------------------------------------------------------------------------------+
        ▲                                                       ▲
        │ Core 1 writes claimSequence                           │ Core 2 writes cursor
        │ (Issues MESI Invalidate)                              │ (Issues MESI Invalidate)
        ▼                                                       ▼
=========================================================================================
  MESI CACHE BOUNCING STORM:
  Core 1 updates claimSequence -> Marks L1 Cache Line in Core 2 INVALID (I)
  Core 2 updates cursor        -> Marks L1 Cache Line in Core 1 INVALID (I)
  Result: 100x slowdown due to constant inter-core L3 / QPI / Interconnect bus transactions!
=========================================================================================
```

### 2.2 Cache-Line Padding Solution (64-Byte Alignment in Java 8)

```
Cache Line 0 (64 Bytes):
+---------------------------------------------------------------------------------------+
| [Header 16B] | [p1..p6 Padding: 48B]                                                 |
+---------------------------------------------------------------------------------------+

Cache Line 1 (64 Bytes) - DEDICATED TO PRODUCERS:
+---------------------------------------------------------------------------------------+
| [p7 Padding: 8B] | [ volatile long claimSequence: 8B ] | [p8..p13 Padding: 48B]       |
+---------------------------------------------------------------------------------------+
                               ▲ Core 1 updates here without invalidating Cache Line 2

Cache Line 2 (64 Bytes) - DEDICATED TO CONSUMER:
+---------------------------------------------------------------------------------------+
| [p14 Padding: 8B] | [ volatile long cursor: 8B ] | [p15..p20 Padding: 48B]           |
+---------------------------------------------------------------------------------------+
                               ▲ Core 2 updates here without invalidating Cache Line 1
```

> **Microarchitectural Caveat:** 64-byte padding is advantageous for **long-lived singleton partition engines** (Disruptor, TigerBeetle), but represents an anti-pattern for **short-lived per-subscription stream instances** (like `SequentialSubscription`), which are created per subscribe and whose hot path is short (~16 ns/op measured for `request(1)` on a local build of the PR #3677 design, single thread, Apple M1 Max).

---

## 3. Sequence Flows & State Transition Mechanics

### 3.1 Level 1: CAS Loop (Optimistic Concurrency Control)

```mermaid
sequenceDiagram
    autonumber
    actor T1 as Thread 1
    actor T2 as Thread 2
    participant MEM as Shared Memory (volatile state)

    T1->>MEM: Read current = 100
    T2->>MEM: Read current = 100
    Note over T1,T2: Both calculate next = 101 concurrently
    T1->>MEM: CAS(expected=100, next=101) -> SUCCESS
    Note over MEM: State is now 101. Core 2 cache line INVALIDATED!
    T2->>MEM: CAS(expected=100, next=101) -> FAIL!
    Note over T2: CAS RETRY LOOP TRIGGERED!
    T2->>MEM: Read current = 101
    Note over T2: Recalculate next = 102
    T2->>MEM: CAS(expected=101, next=102) -> SUCCESS
    Note over T1,T2: Each thread executes individual 1-by-1 I/O fsync!
```

---

### 3.2 Level 2b: WIP Trampoline Drain Loop (RxJava / Reactor Pattern)

```mermaid
sequenceDiagram
    autonumber
    actor T1 as Producer 1
    actor T2 as Producer 2
    participant WIP as AtomicIntegerFieldUpdater (wip)
    participant ACC as Accumulator (AtomicLong)
    participant SM as Single-Threaded State Machine

    T1->>ACC: addAndGet(10) -> pending = 10
    T1->>WIP: getAndIncrement() -> returns 0 (Turn Acquired!)
    Note over T1: T1 becomes the DRAIN WORKER!

    T2->>ACC: addAndGet(20) -> pending = 30
    T2->>WIP: getAndIncrement() -> returns 1 (>0, Turn Busy!)
    Note over T2: T2 exits immediately! Zero blocking, zero context switch!

    Note over T1: Drain Loop Iteration 1
    T1->>ACC: getAndSet(0) -> drains batch of 30
    T1->>SM: Apply delta (+30) in memory
    T1->>WIP: addAndGet(-missed)
    Note over T1: Checks if new work arrived. If 0 -> terminates cleanly!
```

---

### 3.3 Level 3: Smart-Batching Single-Writer (Disruptor / TigerBeetle)

```mermaid
sequenceDiagram
    autonumber
    actor P1 as Producer 1
    actor P2 as Producer 2
    participant RB as Padded RingBuffer
    participant SW as Single-Writer Event Loop
    participant DISK as Amortized WAL / Fsync

    P1->>RB: Claim slot #101, write payload & future
    P2->>RB: Claim slot #102, write payload & future
    Note over RB: Slots 101 and 102 published!

    Note over SW: Consumer detects available range: [101 .. 102]
    rect rgb(235, 248, 255)
        Note over SW: Batch Size N = 2
        SW->>SW: State transition slot #101 (Hot L1 Cache)
        SW->>SW: State transition slot #102 (Hot L1 Cache)
    end

    rect rgb(255, 243, 205)
        SW->>DISK: SINGLE GROUP COMMIT (1 fsync for both #101 & #102!)
    end

    SW->>P1: Complete Future #101
    SW->>P2: Complete Future #102
    SW->>RB: Update cursor = 102 (Free slots)
```

---

### 3.4 Case Study: Monotonicity Breakdown of `SequentialSubscription`

Why a CRDT is **not a fit** for `SequentialSubscription`:

```mermaid
graph TD
    subgraph Monotonic ["Monotonic Components (No Coordination Needed)"]
        REQ["requested: Saturating Addition (accumulateAndGet)"]
        EMIT["sourceEmitted: Single-writer monotonic increment"]
        CAN["cancelled: Monotonic Boolean OR (False -> True)"]
    end

    subgraph NonMonotonic ["Non-Monotonic & External Effects (Coordination Required)"]
        SW["switchTo(next): Destination change & baseline reset (non-monotonic; CALM by analogy)"]
        DELIV["source.request(delta): External Side-Effect (Non-idempotent, RS §2.7)"]
        CANCEL_EFF["source.cancel(): Exactly-once delivery"]
    end

    REQ -.->|Feeds demand| DELIV
    SW -->|Resets destination| DELIV
    CAN -->|Triggers once| CANCEL_EFF

    style NonMonotonic fill:#ffebee,stroke:#c62828,stroke-width:2px
    style Monotonic fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
```

| Component | Forms a Lattice? | Coordination Required? | Architectural Reason |
|---|---|---|---|
| `requested` | **Yes** (Saturating add; invalid is top $\top$) | **No** | Atomic `accumulateAndGet` |
| `cancelled` | **Yes** (Monotonic false $\to$ true / boolean OR) | **For effect only** | Atomic `getAndSet` to trigger `cancel()` once |
| `sourceEmitted` | **Yes** (Single-writer on Subscriber thread) | **No** | Primitive `++sourceEmitted` |
| `Current source delivered level` | Monotonic only as a pair: $(\text{generation}, \text{delivered})$ | **Yes** | Winning permit to deliver delta |
| **Delivering request / cancel to source** | **NO** (External side-effect, non-idempotent) | **YES** | **Serialized delivery: sentinel CAS on `main`; non-blocking `tryAcquireLock` drain + mailbox in PR #3677** |

---

## 4. Analytical Comparison Table

| Architecture Pillar | CAS Loop (Level 1) | Actor Mailbox (Level 2a) | WIP Drain Loop (Level 2b) | Smart-Batching (Level 3) | Striped Counter (Level 4) |
|---|---|---|---|---|---|
| **Coordination Type** | Optimistic Multi-Writer | Pessimistic Single-Writer | Cooperative Trampoline | Pipelined Single-Writer | Partitioned Commutative Math |
| **Hardware Contention** | Severe (MESI Invalidation) | High (Queue Head/Tail CAS) | Low (Cooperative Hand-off) | Zero (Cache Padded Ring) | Low (cells shared only when thread ids collide) |
| **Thread Context Switches** | 0 (Busy spin) | 2 per message (Producer/Actor) | 0 (Calling thread drains) | Minimal (Dedicated Core) | 0 (Calling thread mutates) |
| **Object Allocation (per op)** | 1 `CompletableFuture` | Future + Command | Future + request + queue node | Future (slots preallocated) | 1 `CompletableFuture` |
| **I/O Amortization** | $1 \times \text{I/O}$ per op, in caller threads (parallel) | $1 \times \text{I/O}$ per op | 1 I/O per drained batch | **1 Group Commit for N ops** | $1 \times \text{I/O}$ per op, in caller threads (parallel, unordered) |
| **Consistency Level** | Linearizable | Serialized | Serialized | Serialized | **Weak (Non-atomic read sum)** |
