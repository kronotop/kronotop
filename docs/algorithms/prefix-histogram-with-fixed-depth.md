# Prefix Histogram with Fixed Depth (FDB-Ready, No Background Workers)

## Overview

We define a **prefix-based histogram** over arbitrary byte arrays (string or binary keys).

* Each histogram node corresponds to a prefix of length *d* (1 ≤ *d* ≤ `MAX_DEPTH`).
* Each node stores a counter of how many keys share that prefix.
* A key contributes to all its prefixes up to `MAX_DEPTH`.
* At query time, selectivity is estimated using counters from the coarsest sufficient level, ensuring **bounded read cost**.

This approach requires no background workers, split/merge logic, or dynamic rebalancing. All updates are handled by atomic increments.

---

## Parameters

* **BUCKET\_SIZE:** 1 byte (each prefix level adds one byte)
* **MAX\_DEPTH:** 8 (maximum prefix length considered)

---

## FoundationDB Schema

```
HIST/<depth>/<prefix> = <i64 counter>
```

* `<depth>`: integer in \[1, MAX\_DEPTH]
* `<prefix>`: the first `depth` bytes of the key
* Counter encoding: little-endian `i64` (compatible with FDB atomic ADD)

---

## Write Path

For key `k` of length `ℓ`:

1. For each depth `d = 1 .. min(ℓ, MAX_DEPTH)`

    * `prefix = k[0..d-1]`
    * `tr.mutate(ADD, HIST/d/prefix, +1)`

2. Delete = same loop with `-1`

3. Update = delete(oldKey) + insert(newKey) in the same transaction

👉 Complexity: ≤ 8 atomic increments per key (bounded)

---

## Equality Estimation (`= v`)

1. Let `depth = min(len(v), MAX_DEPTH)`
2. Read counter `HIST/depth/prefix(v)`
3. Estimate:

   ```
   est = count(prefix(v)) / totalDocs
   ```

---

## Range Estimation `[A,B)`

1. **Root-level counters**

    * Retrieve all depth=1 counters with a single `getRange(HIST/1, HIST/2)`
    * At most 256 counters returned

2. **Decompose the range**

    * **Left edge**: use `count(prefix(A)) * fracLeft(A)`
    * **Right edge**: use `count(prefix(B)) * fracRight(B)`
    * **Middle bins**: sum all fully covered root-level bins between prefix(A) and prefix(B)

3. **Optional refinement**

    * If needed, read deeper prefix levels for edge bins only (depth up to 8)

---

## Estimation Formula

```
est = count(prefix(A)) * fracLeft(A)
    + Σ count(middleBins)
    + count(prefix(B)) * fracRight(B)
```

* `fracLeft(A)` = portion of values in prefix(A) greater than A (uniform assumption)
* `fracRight(B)` = portion of values in prefix(B) less than B (uniform assumption)

---

## Example Walkthrough

### Query: `> "burak"`

1. **Root-level read**

    * Call `getRange(HIST/1, HIST/2)` → retrieve all 256 one-byte counters.

2. **Left edge**

    * The relevant root bin is `"b"`.
    * From `count("b")`, we need only the fraction greater than `"burak"`.
    * If resolution is insufficient, descend to deeper prefix:

        * Read `HIST/2/bu`, `HIST/3/bur`, … up to `HIST/5/burak`.
        * Estimate how many values in `"bur*"` are strictly greater than `"burak"`.

3. **Middle bins**

    * Add counts for `"c" .. "z"` root-level bins (fully included in the range).

4. **Result**

    * Final estimate = (fraction of `"bur*"` above `"burak"`) + (sum of `"c".."z"` bins).

👉 Complexity:

* **Typical case:** 1 `getRange` (256 counters).
* **If refinement is needed:** +1 `getRange` for the `"bur*"` subtree.
* Still bounded (≤ 2 round-trips).

---

## Properties

* **Bounded write cost:** ≤ 8 atomic increments per insert/delete
* **Bounded read cost:** root-level query = ≤ 256 counters in one `getRange`
* **No background workers:** structure is static, prefix assignment deterministic
* **Adaptive resolution:** refine edges at deeper levels if needed

---

## Limitations

* **Uniformity assumption** inside each bucket may cause error under skew
* **Write amplification:** up to 8 increments per insert

---

✅ This is the finalized version: a **fixed-depth prefix histogram** with 1-byte buckets, maximum depth 8, atomic increments for writes, and bounded-cost range/equality estimation.

---