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

* **Bin size**: 1 byte per depth
* **Max depth**: 8
* **Value padding**: if the key is shorter than `depth`, pad with `0x00`.

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

1. `b = key[d-1] if d <= len(key) else 0x00` => 1 byte
2. `ADD(HIST/D/d/B/b, +1)`

### Delete(key)

Same as insert, but `-1`.

---

## Estimation Primitives

The core operation is:

```
estimateRange(A, B) → approximate count of keys ∈ [A, B)
```

* **If A is null** → `(−∞, B)`
* **If B is null** → `[A, ∞)`
* **If both null** → total count

### Algorithm (high-level)

1. Compare first differing byte of A and B.
2. If they differ:

    * Use counters at that depth:

        * Partial contribution from `A` bin (fraction above A).
        * Full counts from middle bins.
        * Partial contribution from `B` bin (fraction below B).
3. If identical at this depth:

    * Recurse into deeper depth (up to max depth).
4. If depth > max → stop (approximate as 0).

Fractions are computed as:

* **Left fraction**: `(256 − nextByte(A)) / 256`
* **Right fraction**: `nextByte(B) / 256`

---

## Derived Predicates

All inequalities are defined via `estimateRange`:

* `< v` = `estimateRange(null, v)`
* `<= v` = `estimateRange(null, incrementKey(v))`
* `> v` = `estimateRange(incrementKey(v), null)`
* `>= v` = `estimateRange(v, null)`

**Note:**

* `Eq(v)` is *not* estimated here; use the index.
* `>= v` = `Eq(v)` (from index) + `> v` (from histogram).

---

## Complexity

* **Insert/Delete**: O(depth) ≤ 8 atomic adds.
* **Estimate**: O(depth + number of bins in range). In practice, ≤ 8 levels scanned.
* **FDB round trips**: one `getRange` per level.

---

## Accuracy

* Uniform distributions → low error (<1% typical).
* Skewed distributions (e.g., Zipf) → error can be large without MCV/hints.
* Histogram is best effort; for hot values, fall back to **MCV tables** or **index**.
