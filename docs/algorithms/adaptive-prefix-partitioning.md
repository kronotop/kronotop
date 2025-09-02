# Adaptive Prefix Partitioning (APP) — A Lexicographic Histogram for Byte Arrays on FoundationDB

## Scope

* Works on **byte arrays** (not “strings”), ordered **lexicographically**.
* **No background workers.** All maintenance (split/merge) happens **online** on the write path.
* Supports **add / delete / update** and **range‐selectivity estimation** for predicates like `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`.

## Defaults

* `MAX_DEPTH (D_max) = 3` (optionally 4 for hot subtrees)
* `FANOUT = 4` (quartile split)
* `T_SPLIT = 4096`, `T_MERGE = 1024` (hysteresis 4:1)
* Sharded counters **only** where needed (e.g., 8 shards on hot leaves)

---

## 1) Keyspace Layout (FDB)

(All keys live under a per-index prefix. Tuple layer is illustrative—raw bytes are fine.)

```
HIST/<indexId>/L/<lowerPad><depthByte>                = <meta>   # leaf boundary record
HIST/<indexId>/C/<lowerPad><depthByte>/<shardId>      = <i64>    # leaf counter shards
HIST/<indexId>/F/<lowerPad><depthByte>                = <flags>  # maintenance flags
IDX/<indexId>/<valueBytes>/<docRef>                   = ø        # existing index entries
```

* `lowerPad`: leaf lower bound, **right-padded to `D_max` bytes with 0x00**.
* `depthByte`: 1 byte, `d ∈ [1..D_max]`.
* `meta`: tiny blob (e.g., `{d, checksum?}`).
* `flags`: bitfield `{needs_split, needs_merge, hot_sharded?}`.
* Missing counter shards imply zero.

**Sentinels (depth `d=1`)**

* Global low: `LOW = 0x00…00` (length `D_max`)
* Global high: `HIGH = 0xFF…FF` (conceptual upper fence)

---

## 2) Canonicalization & Leaf Geometry

Let `D_max = MAX_DEPTH`. Treat all byte arrays as **padded** to `D_max` bytes.

* For any key `K`:

    * `K_pad = rightPad(K, D_max, 0x00)`
    * `K_pad_hi = rightPad(K, D_max, 0xFF)` (when an inclusive upper endpoint is needed)

A leaf at depth `d` covers `[L_pad, U_pad)` where:

* `S(d) = 256^(D_max - d)`  (leaf width in “minimal cells”)
* `U_pad = L_pad + S(d)`

This geometry drives both **split** and **estimation interpolation**.

---

## 3) Lookup — `findLeaf(K)`

Goal: find the unique leaf `[L_pad, U_pad)` such that `L_pad ≤ K_pad < U_pad`.

**Transaction steps**

1. Compute `K_pad`.
2. Reverse scan **one** boundary ≤ `K_pad`:

    * `begin = HIST/<indexId>/L/(K_pad ++ 0xFF)`
    * `end   = HIST/<indexId>/L/()` (prefix start)
    * `reverse = true`, `limit = 1`
3. Decode `(L_pad, d)`. Compute `U_pad = L_pad + S(d)`.
4. If `K_pad ≥ U_pad` (rare concurrency edge), forward step to the next boundary and fix up.
5. Return `(leafId=(L_pad,d), L_pad, U_pad)`.

> Only **leaf** boundaries are materialized. Parents/siblings are computed arithmetically (see §6).

---

## 4) Write Path Operations

### 4.1 Add (Insert)

Input: `(K, docRef)`

1. `leaf = findLeaf(K)`
2. Choose shard `s`:

    * If leaf flagged hot/sharded: `s = hash(docRef) & (S-1)` (e.g., `S=8`)
    * Else: `s = 0`
3. `atomicAdd(HIST/.../C/<leaf>/<s>, +1)`
4. Optionally snapshot-sum shards to check split:

    * If `approxTotal + 1 ≥ T_SPLIT` → `set(HIST/.../F/<leaf>, flags|needs_split)`

### 4.2 Delete (Remove)

Input: `(K, docRef)`

1. `leaf = findLeaf(K)`
2. Same shard rule as Add → `atomicAdd(..., -1)`
3. Optionally snapshot-sum shards:

    * If `total ≤ T_MERGE` → `set(HIST/.../F/<leaf>, flags|needs_merge)`

### 4.3 Update

`Delete(K_old)` + `Add(K_new)` within the **same** transaction.

---

## 5) Online Maintenance (No Workers)

### 5.1 Split (quartile geometry, exact recount)

**When:** `needs_split` set **or** observed total ≥ `T_SPLIT`.
**Fanout:** `FANOUT = 4`.

**Single transaction**

1. **Verify leaf** (conflict control): `get(HIST/L/<leafId>)` non-snapshot; abort if missing.
2. Geometry:

    * `S_leaf = S(d)`; `childWidth = S_leaf / 4`
    * Children at depth `d+1`:
      `childL_i = L_pad + i * childWidth` for `i=0..3`
3. **Recount from index** (bounded scan):

    * `begin = IDX/<indexId>/<L_pad>/(-∞ docRef)`
    * `end   = IDX/<indexId>/<U_pad>/(-∞ docRef)`
    * `limit ≈ T_SPLIT + ε`
    * For each `valueBytes`, compute `valueBytes_pad` and bucket into child `i`.
    * Accumulate exact `cnt[0..3]`.
4. **Write children**:

    * `set(HIST/L/<childL_i><d+1>, meta{d+1})`
    * `set(HIST/C/<childL_i><d+1>/0, encode_i64(cnt[i]))`
    * (Don’t pre-shard; shard later if hot.)
5. **Remove parent**:

    * `clear(HIST/L/<leafId>)`
    * `clear_range(HIST/C/<leafId>/, HIST/C/<leafId>/~)`
    * `clear(HIST/F/<leafId>)`
6. Commit.

*If TX nears limits:* skip steps 3–5; only set `needs_split` and exit. A later write retries.

### 5.2 Merge (collapse siblings into parent)

**When:** `needs_merge` set **and** all four siblings at depth `d` have small totals.

**Single transaction**

1. Compute parent:

    * `S_parent = 4 * S(d)`
    * `parentL = floor((L_pad - ROOT_L)/S_parent)*S_parent + ROOT_L` (with `ROOT_L = 0x00…00`)
    * Siblings: `sibL_i = parentL + i * S(d)`
2. Ensure all four children exist as leaves: `get(HIST/L/<sibL_i><d>)`.
3. Snapshot-sum all shards of each child → `sumChildren`.
4. If `sumChildren ≤ T_MERGE`:

    * `set(HIST/L/<parentL><d-1>, meta{d-1})`
    * `set(HIST/C/<parentL><d-1>/0, encode_i64(sumChildren))`
    * `clear` all child boundaries, counters, flags.
5. Commit.

---

## 6) Deriving Parents & Siblings (Arithmetic Only)

* `S(d) = 256^(D_max - d)`
* `S_parent = 4 * S(d)`
* `parentL = floor(L_pad / S_parent) * S_parent`
* `sibL_i = parentL + i * S(d)`, for `i = 0..3`

No internal nodes are stored.

---

## 7) Estimation Algorithm

Given predicate range `[A, B)`:

1. Canonicalize:

    * `A_pad = rightPad(A, D_max, 0x00)`
    * `B_pad = rightPad(B, D_max, 0x00)`  (half-open interval)
2. Find first leaf: reverse scan one boundary ≤ `A_pad`.
3. Iterate forward while `L_pad < B_pad`:

    * `U_pad = L_pad + S(d)`
    * `covered = max(0, min(B_pad, U_pad) - max(A_pad, L_pad))`
    * `ratio = covered / (U_pad - L_pad)`  (denominator = `S(d)`)
    * `countLeaf = sum_shards_snapshot(HIST/C/<leafId>/*)`
    * `estimate += countLeaf * ratio`
    * Move to next boundary.
4. Return `estimate`.

**Equality (`=K`)**

* Use histogram only as a hint. For plan correctness, do a tiny index peek:

    * `getRange(IDX/<indexId>/<K>/(-∞), IDX/<indexId>/<K>/(+∞), small limit)`.

---

## 8) Concurrency & Hotspots

* **Atomic adds** prevent write-write conflicts but can heat a single key.

    * Mitigate by enabling **sharded counters** on hot leaves (set a flag; future writes pick shards).
* **Split/Merge races** are resolved by reading the leaf boundary **non-snapshot** and modifying it in the same TX → optimistic concurrency aborts one side.
* **Flags** allow deferring heavy work without daemons.
* Standard FDB retry loop everywhere.

---

## 9) Cost Profile

* **Add/Delete:** 1–2 small reads + 1 atomic add (+ optional tiny flag write).
* **Split:** deterministic geometry + scan up to \~`T_SPLIT` index entries + a handful of writes.
* **Merge:** read ≤4 leaves’ counters, write one parent, clear four children.

Choose `T_SPLIT` so recount scan fits FDB TX limits (e.g., 4–8K KVs worst case).

---

## 10) Tuning & Heuristics

* Start with `D_max = 3`. Allow `4` **only** on branches that keep hitting `T_SPLIT` despite sharding.
* Shard counters lazily when a leaf becomes hot; initialize children **unsharded** after splits.
* Keep `T_MERGE` well below `T_SPLIT` to avoid oscillation.
* Simple sizing check: with total entries `N`,
  `target_depth ≈ ceil(log_4(N / T_SPLIT))`, then clamp to `D_max`.

---

## 11) Micro-Example (D\_max = 3)

* Address space: `[00 00 00, FF FF FF]`.
* Leaf at depth `d=2` has width `S(2)=256`. Example leaf:
  `[L_pad="ab 00 00", U_pad="ab 01 00")`.
* Query `[A="ab 00 40", B="ab 00 80")`:

    * `covered = 0x40`, `S=0x100`, `ratio=0.25`
    * If `countLeaf=400` → contribution `100`.

Split:

* Children (width `64`): `ab 00 00`, `ab 00 40`, `ab 00 80`, `ab 00 C0` at depth `d=3`.
* Recount from `IDX` within `[ab 00 00, ab 01 00)`, write children, clear parent.

---

## 12) Failure & Recovery (No Workers)

* **Split TX too big:** set `needs_split`, return. Later hot writes retry.
* **Leaf vanished:** someone else restructured; abort and retry.
* **Flag drift:** harmless. Attempt maintenance once per write; clear on success.

---

## 13) Why This Works

* Deterministic **equal-width geometry**; **exact recount** on split prevents drift.
* No internal nodes—parents/siblings via arithmetic.
* All heavy work is **opportunistic**; no schedulers.
* Planner gets stable range selectivity via **partial coverage ratios**.

---

# Implementer’s Checklist (Step-by-Step)

## Initialization

* [ ] Pick `D_max=3`, `FANOUT=4`, `T_SPLIT=4096`, `T_MERGE=1024`.
* [ ] Create two sentinel leaves at `d=1` covering the whole space:

    * [ ] `set(HIST/L/<LOW><1>, meta{1})` where `LOW=0x00…00`
    * [ ] `set(HIST/L/<HIGH><1>, meta{1})` (conceptual upper fence; you may only materialize the first real leaf starting at `LOW` if you prefer)
* [ ] Ensure `IDX/<indexId>/…` exists and is lexicographically ordered by `<valueBytes>,<docRef>`.

## Common Helpers

* [ ] `rightPad(bytes, D_max, padByte)`
* [ ] `S(d) = 256^(D_max - d)`
* [ ] `findLeaf(K)` using a **reverse one-key scan** on `HIST/L`.

## Insert (Add)

* [ ] TX start
* [ ] `leaf = findLeaf(K)`
* [ ] Pick shard (`0` or `hash(docRef) & (S-1)` if hot)
* [ ] `atomicAdd(HIST/C/<leaf>/<shard>, +1)`
* [ ] (Optional) Snapshot-sum shards; if `≥ T_SPLIT` → `set(HIST/F/<leaf>, needs_split)`
* [ ] TX commit (retry on conflict)

## Delete (Remove)

* [ ] TX start
* [ ] `leaf = findLeaf(K)`
* [ ] Pick shard consistently
* [ ] `atomicAdd(HIST/C/<leaf>/<shard>, -1)`
* [ ] (Optional) Snapshot-sum shards; if `≤ T_MERGE` → `set(HIST/F/<leaf>, needs_merge)`
* [ ] TX commit

## Update

* [ ] TX start
* [ ] `Delete(K_old)` steps
* [ ] `Add(K_new)` steps
* [ ] TX commit

## Split (on write path when `needs_split` or high count)

* [ ] TX start
* [ ] `get(HIST/L/<leafId>)` **non-snapshot**; abort if missing
* [ ] Compute children (`childL_i = L_pad + i*(S(d)/4)`, depth `d+1`)
* [ ] `getRange` over `IDX/<indexId>` within `[L_pad, U_pad)` with limit ≈ `T_SPLIT + ε`, bucket into `cnt[0..3]`
* [ ] For each child:

    * [ ] `set(HIST/L/<childL_i><d+1>, meta{d+1})`
    * [ ] `set(HIST/C/<childL_i><d+1>/0, encode_i64(cnt[i]))`
* [ ] `clear(HIST/L/<leafId>)`
* [ ] `clear_range(HIST/C/<leafId>/, HIST/C/<leafId>/~)`
* [ ] `clear(HIST/F/<leafId>)`
* [ ] TX commit
  *If size/latency too high: abort; in a prior TX just set `needs_split` and return.*

## Merge (on write path when `needs_merge`)

* [ ] TX start
* [ ] Compute `parentL`, `sibL_i` via §6 arithmetic
* [ ] Verify all 4 siblings exist at depth `d`
* [ ] Snapshot-sum all shards of the 4 siblings → `sumChildren`
* [ ] If `sumChildren ≤ T_MERGE`:

    * [ ] `set(HIST/L/<parentL><d-1>, meta{d-1})`
    * [ ] `set(HIST/C/<parentL><d-1>/0, encode_i64(sumChildren))`
    * [ ] `clear` all sibling `HIST/L`, `HIST/C`, `HIST/F`
* [ ] TX commit

## Estimation

* [ ] Input `[A,B)`:

    * [ ] `A_pad = rightPad(A, D_max, 0x00)`
    * [ ] `B_pad = rightPad(B, D_max, 0x00)`
* [ ] Reverse scan one boundary ≤ `A_pad` → start leaf
* [ ] Loop: for each leaf with `L_pad < B_pad`

    * [ ] `U_pad = L_pad + S(d)`
    * [ ] `covered = max(0, min(B_pad, U_pad) - max(A_pad, L_pad))`
    * [ ] `ratio = covered / (U_pad - L_pad)`
    * [ ] `countLeaf = snapshot sum of HIST/C/<leaf>/*`
    * [ ] `estimate += countLeaf * ratio`
    * [ ] Move to next boundary
* [ ] Return `estimate`
  *Equality: do a tiny `IDX` peek instead of relying on histogram.*

## Hotspot Mitigation

* [ ] If a leaf gets high RPS, set its `hot_sharded` flag and start writing to multiple shards.
* [ ] After splitting a hot leaf, initialize children unsharded; they can shard if they become hot.

## Safety & Retries

* [ ] Use standard FDB retry loops for all TXs.
* [ ] Treat `needs_*` flags as hints; clear after successful maintenance.
* [ ] Keep `T_MERGE << T_SPLIT` to avoid oscillation.

That’s the Markdown version plus a concrete checklist you can hand to the implementer.

# APP Histogram — Verification & Testing Plan

Validate correctness from two angles: **(1) data-structure invariants** (leaf geometry, split/merge, count conservation) and **(2) selectivity estimation** (compare against ground truth). Below is a detailed plan with acceptance criteria and an implementer’s checklist.

---

## 1) Invariants & Structural Correctness

### 1.1. Helper functions (unit tests)

* `rightPad`, `S(d) = 256^(D_max - d)`, `parentL`, `sibL_i`, `U_pad = L_pad + S(d)`.
* **Expectations**

    * `U_pad - L_pad == S(d)`
    * `parentL ≤ L_pad < parentL + 4*S(d)`
    * Siblings: `sibL_i = parentL + i*S(d)` for `i=0..3`.

### 1.2. Leaf coverage & disjointness

* **Test:** Enumerate all leaf intervals `[L, U)` from `HIST/L/*`.
* **Expectations**

    * Intervals are **sorted** and **non-overlapping**.
    * Adjacent leaves are touching (no gaps) unless sentinels define outer fences.
    * The space is fully covered from global `LOW` to `HIGH` (conceptual fence).

### 1.3. Count mass conservation

* The index `IDX` is the ground-truth multiset.
* **Test:** `Sum(leaf.count) == |IDX|` after each operation round.
* For sharded counters: `sum(shards) == leaf.count`.

### 1.4. Split correctness (exact recount)

* **Setup:** Small dataset with low `T_SPLIT` (e.g., 8) to force splits.
* **Check:** Before split, parent count `C_p`; after split, `Σ C_i` over 4 children.
* **Expectation:** `Σ C_i == C_p`; parent boundary removed/disabled; children boundaries/counters created with correct geometry (`L_child_i = L + i*S(d)/4`).

### 1.5. Merge correctness

* **Setup:** Four siblings with totals ≤ `T_MERGE`.
* **Expectations:** Parent leaf materialized with `C_parent = Σ C_children`; child boundaries/counters/flags cleared; parent depth `d-1` at `parentL` computed arithmetically.

### 1.6. Flag idempotence

* Repeatedly setting `needs_split/needs_merge` on the same leaf is a **no-op** after the first successful maintenance.
* **Expectation:** No net data change; flags cleared upon success.

### 1.7. Edge cases

* Empty index.
* Single key; heavy duplicates of the same key.
* Extreme keys (`00…00`, `FF…FF`).
* Short vs. long keys (padding correctness).
* Hot single prefix (split cascade; depth cap respected).

---

## 2) Selectivity Estimation Validation

### 2.1. Ground-truth oracle

* **Model:** In-memory ordered multiset (e.g., `TreeMap<byte[], List<docRef>>`).
* **Truth for `[A, B)`:** Two binary searches and index-difference.

### 2.2. Data & query distributions

* **Data distributions**

    * Uniform random bytes
    * Zipf/skew (heavy single-prefix concentration)
    * Clustered (long shared prefixes)
    * Mixed (uniform + clusters)
    * Very sparse (few records over huge space)
* **Queries**

    * Random ranges with varied widths
    * Edge-hugging ranges (touching `LOW`/`HIGH`)
    * Many partial-coverage cases (range starts/ends inside leaves)
    * Equality `=K` (treat as **hint** only; planner should peek `IDX`)

### 2.3. Error metrics & acceptance

* Per query: `err_rel = |estimate − truth| / max(1, truth)`

* **Segmented reporting**

    * By range width (bins like `[1,4,16,64,…]` minimal cells): P50/P90/P99
    * Per data distribution

* **Initial targets**

    * Uniform: P90 ≤ **15%**, P99 ≤ **30%**
    * Skew/clustered: P90 ≤ **25%**, P99 ≤ **50%**
    * Very small ranges/equality: histogram is coarse; planner correctness relies on `IDX` peek

* Optionally report WLS or MAPE across queries.

### 2.4. Metamorphic tests (semantic relations)

* **Monotonicity:** If `[A,B) ⊆ [C,D)`, then `est(A,B) ≤ est(C,D)`.
* **Additivity:** For `M ∈ (A,B)`, `est(A,B) ≈ est(A,M) + est(M,B)` (minor rounding allowed).
* **Linearity:** Duplicate the dataset (distinct `docRef`s): both truth and estimate should \~double.
* **Order-preserving transform:** Prepend the same fixed prefix to all keys (increase `D_max` by one). Mapped range estimates remain consistent proportionally.
* **Padding invariance:** Using `A_pad/B_pad` directly must not change results.

---

## 3) Concurrency & Progress

### 3.1. Single-thread golden traces

* Small scripted scenarios with expected boundary/counter tables:

    * Adds K1..K8 → Split → verify exact post-state
    * Deletes K3..K6 → Merge → verify exact post-state

### 3.2. Multi-thread fuzz

* N threads perform random `Add/Delete/Update`; writes opportunistically perform split/merge when flags are seen.
* **Periodic checks**

    * `Sum(leaf.count) == |IDX|`
    * Leaf disjointness/order
    * Flag backlog drains (flags do not persist indefinitely when writes continue)

### 3.3. “Large transaction” simulation

* Force a split to exceed TX limits (mock big index range), abort split and only set `needs_split`.
* Next write to that leaf should successfully complete the split.

---

## 4) Test Mechanics (Java suggestion)

* **Unit & property-based:** JUnit 5 + jqwik (or QuickTheories).
* **Deterministic RNG:** One seed per suite; reproducible failures.
* **In-memory model:** Ordered multiset for truth.
* **FDB integration**

    * Single-process local cluster during tests.
    * Use Directory/Subspace for scoping.
    * Wrap standard FDB retry loops in helpers.

---

## 5) Planner Integration Checks

* For **equality** predicates, verify the planner does a **tiny `IDX` peek** (does not rely on histogram).
* For `<, ≤, ≥, BETWEEN`, ensure selectivity-driven index choice toggles around expected thresholds (e.g., `selectivity < τ` picks index-driven plan).
* When residual predicates apply (partial coverage), confirm operator choice matches estimated cost.

---

## 6) Metrics & Observability

* Split/merge frequency; average TX durations; split-abort rate.
* Hot-leaf counter sharding impact (hotspot reduction).
* Histogram depth distribution (counts at d=1/2/3/4).

---

## 7) Example Acceptance Criteria (v1)

* **Invariants:** Always pass after each test epoch.
* **Estimation**

    * Uniform + 10k random queries: MAPE ≤ **10%**, P90 ≤ **15%**
    * Zipf + 10k queries: MAPE ≤ **18%**, P90 ≤ **25%**
    * Very small ranges/equality excluded from these thresholds (planner handles via `IDX` peek)
* **Robustness:** 5-minute multi-thread fuzz (≥100k ops) ends with mass conservation and zero lingering flags.

---

## Implementer’s Test Checklist

* [ ] Arithmetic: `S(d)`, `parentL`, `sibL_i`, `U_pad` are correct.
* [ ] Leaf intervals are disjoint, ordered; outer coverage holds.
* [ ] **Mass conservation:** `Σ leaf.count == |IDX|` at all checkpoints.
* [ ] **Split:** `Σ child == parent`; parent cleared; children geometry exact.
* [ ] **Merge:** `parent == Σ children`; children cleared.
* [ ] Flags (`needs_*`) are idempotent and clear after success.
* [ ] Shard totals equal leaf count where sharding is enabled.
* [ ] Planner performs `IDX` peek for equality predicates.
* [ ] Range estimates reported vs. truth with P50/P90/P99; targets met.
* [ ] Metamorphic checks: monotonicity, additivity, linearity, padding invariance.
* [ ] Under fuzz, mass conservation and leaf disjointness remain intact.
* [ ] Split-abort scenario covered: flag then later successful split.
