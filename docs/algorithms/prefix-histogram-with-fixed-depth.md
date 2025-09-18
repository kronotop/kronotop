# Prefix Histogram with Fixed Depth (FDB-Ready, No Background Workers)

## Overview

A lightweight histogram structure for **approximate range selectivity estimation** over string or binary keys in FoundationDB-like systems.

* Each **depth** corresponds to one byte of the input key (max 8 bytes).
* Each **bin** is identified by `(depth, byteValue)`.
* Writes increment counters along the path of the key.
* Reads use these counters to approximate selectivity for range predicates.
* **Equality (`= v`) is not estimated here** — handled by the secondary index directly.

---

## Parameters

* **Bin size:** 1 byte per depth
* **Max depth:** 8

---

## Data Layout

For each depth `d` and byte value `b`:

```
HIST/<index>/D/<d>/B/<b> = <i64 counter>
```

* Incremented for every key whose prefix at `d` equals `b`.
* Atomic adds ensure no write conflicts.

---

## Write Path

### Insert(key)

For `d = 1..min(len(key), MAX_DEPTH)`:

1. `b = key[d−1]` (one byte) sample input [256, 23, 123] depth is 3 here. first bin: 256, second bin: 23, third bin: 123.
2. `ADD(HIST/D/d/B/b, +1)`

### Delete(key)

Same as insert, but `−1`.

---

## Estimation Primitives

The core operation is:

```
estimateRange(A, B) → approximate count of keys ∈ [A, B)
```

* **If A is null** → `(−∞, B)`
* **If B is null** → `[A, ∞)`
* **If both null** → total count

---

### Algorithm (high-level)

1. Compare first differing byte of A and B.
2. If they differ:

    * Use counters at that depth:

        * Tail of A-bin → recurse downwards.
        * Full counts from middle bins.
        * Head of B-bin → recurse downwards.
3. If identical at this depth:

    * Recurse into deeper depth (up to max depth).
4. If depth > max → stop (approximate as 0).

---

## Derived Predicates

All inequalities are defined via `estimateRange`:

* `< v` = `estimateRange(null, v)`
* `<= v` = `estimateRange(null, incrementKey(v))`
* `> v` = `estimateRange(incrementKey(v), null)`
* `>= v` = `estimateRange(v, null)`

**Notes:**

* `Eq(v)` is *not* estimated here; use the index.
* `>= v` can be computed as `Eq(v)` (from index) + `> v` (from histogram).

---

## Complexity

* **Insert/Delete:** O(depth) ≤ 8 atomic adds.
* **Estimate:** O(depth + number of bins in range). In practice, ≤ 8 levels scanned.
* **FDB round trips:** one `getRange` per level.

---

## Accuracy

* Uniform distributions → error typically <1%.
* Skewed distributions (e.g., Zipf) → error may be high without MCV or type hints.
* Histogram is best-effort; heavy hitters should be handled separately.

