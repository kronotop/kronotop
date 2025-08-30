# Fixed N-Byte Prefix Histogram (FoundationDB, **no sharding**)

## Summary

* Treat the first **N bytes** of each value (byte array) as a **bucket**.
* On **Add/Delete**, update that bucket’s counter via **atomic add**.
* For **range** predicates, sum bucket counters; for edge buckets, use a simple **(N+1)th-byte fraction**.
* For **equality** (`= v`), do a small **index peek** first; only if it overflows the peek limit, fall back to the histogram.
* No background workers, no split/merge, **no sharding**.

---

## Parameters

* `N = 2` (use `N = 3` per field if you observe heavy skew)
* `PEEK_CAP = 1024` (max KV to read for equality peek)

---

## FoundationDB Keyspace

Tuple-like notation (use Directory/Subspace/Tuple in code):

```
# Per-bucket total counter
HIST/<idx>/N/<N>/T/<pN>                 = <i64>

# Existing secondary index (already in your system)
IDX/<idx>/<valueBytes>/<docRef>         = ø
```

* `pN`: first `N` bytes of the value; if the value is shorter than `N`, **right-pad with 0x00** and then take the first `N`.

---

## Helper Functions

* `rightPad(bytes, N, 0x00)` → ensures length ≥ `N`
* `pN(bytes, N) = firstN(rightPad(bytes, N, 0x00))`
* `(N+1)th byte`: `b = (len(key) > N) ? key[N] : 0x00`
* `nextFixedN(pN)` / `prevFixedN(pN)` → big-endian increment/decrement of an `N`-byte id (for inner range bounds)
* `lexSuccessor(v_padFF)` if you need `[v, v_next)` style half-open bounds

---

## Write Path (single transaction, standard FDB retry loop)

### Add(key, docRef)

1. `p = pN(key, N)`
2. `tr.mutate(ADD, HIST/.../T/p, +1_i64)`
3. Commit (retry on conflict)

### Delete(key, docRef)

1. `p = pN(key, N)`
2. `tr.mutate(ADD, HIST/.../T/p, -1_i64)`
3. Commit

### Update(oldKey → newKey)

* In the **same** transaction: `Delete(oldKey)` then `Add(newKey)`.

> Reads for estimation can be **snapshot** to avoid unnecessary conflicts.

---

## Equality Estimation (`= v`) — *peek first*

1. **Index peek (exact up to a cap)**

    * `begin = IDX/<idx>/<v>/(-∞ docRef)`
    * `end   = IDX/<idx>/<v>/(+∞ docRef)`
    * `limit = PEEK_CAP + 1`
    * If returned KV count `≤ PEEK_CAP` → **EXACT = count**.
2. **If limit hit** → treat as “large set” and estimate via histogram:

    * Estimate the half-open range `[v_pad00..., v_padFF..._next)` using the **Range Estimation** below (result is **APPROX**).

---

## Range Estimation `[A, B)` (half-open)

1. **Bucket ids**

    * `a = pN(A, N)`
    * `b = pN(B, N)`

2. **Edge fractions (simple and fast)**

    * `fracLeft(A)  = (256 - byteAt(A, N, default=0)) / 256.0`
    * `fracRight(B) = (byteAt(B, N, default=0)) / 256.0`

3. **Sum**

    * If `a == b` (range fully inside one bucket):

        * `ratio = clamp01(fracRight(B) - (1 - fracLeft(A)))`
        * `est = T[a] * ratio`
    * Else:

        * `Ta = get(HIST/.../T/a, snapshot=true)` (0 if missing)
          `Tb = get(HIST/.../T/b, snapshot=true)`
        * `sumMid = Σ T[p]` for all `p ∈ (a, b)`
          (FDB range: `HIST/.../T/nextFixedN(a)` → `HIST/.../T/b`, `StreamingMode.ITERATOR`)
        * `est = Ta * fracLeft(A) + sumMid + Tb * fracRight(B)`

---

## Concurrency & Hot Buckets

* **Atomic add** avoids write-write conflicts, but a single hot bucket can still concentrate writes on one key.
* Since sharding is **disabled**, mitigate hotspots by:

    * Increasing `N` for that field (e.g., `N=3`) to spread writes across more buckets.
    * (Optional operational) Throttle extreme producer workloads or batch writes at the application layer.

---

# Implementation Checklist (FoundationDB, no sharding)

## A) Schema & Setup

* [ ] Create/open Directory/Subspaces:

    * [ ] `dirHist = DL.createOrOpen("HIST", idx, "N", N)`
    * [ ] `subT   = dirHist.subspace(Tuple.from("T"))`
    * [ ] `dirIdx = DL.open("IDX", idx)` (already present)
* [ ] Ensure value encoding for counters is **little-endian i64** (what FDB `ADD` expects).
* [ ] Implement helpers: `rightPad`, `pN`, `nextFixedN`, `byteAtOr0`.

## B) Transaction Utilities

* [ ] Wrap all write ops in the standard FDB **retry loop** with backoff.
* [ ] Use **snapshot** reads where appropriate (estimation paths).
* [ ] For range reads on `T/*`, use `StreamingMode.ITERATOR` and a sensible batch size.

## C) Write Path

* [ ] **add(key, docRef)**

    * [ ] `p = pN(key, N)`
    * [ ] `ADD subT.pack(p), +1`
* [ ] **del(key, docRef)**

    * [ ] `p = pN(key, N)`
    * [ ] `ADD subT.pack(p), -1`
* [ ] **update(oldKey → newKey)** in one transaction: `del(oldKey)` then `add(newKey)`.

## D) Equality (`= v`)

* [ ] **peekIndexEq(v)**

    * [ ] `Range r = dirIdx.subspace(v).range()`
    * [ ] `getRange(r.begin, r.end, limit=PEEK_CAP+1, snapshot=true)`
    * [ ] If `count ≤ PEEK_CAP` → **EXACT**
    * [ ] Else → **APPROX** via `estimateRange(v_pad00..., v_padFF..._next)`

## E) Range Estimation

* [ ] `estimateRange(A, B)`

    * [ ] `a = pN(A, N)`, `b = pN(B, N)`
    * [ ] Compute `fracLeft/fracRight` from the `(N+1)th` byte (or `0x00` if absent)
    * [ ] If `a == b`: single-bucket formula
    * [ ] Else:

        * [ ] `Ta = get(subT.pack(a), snapshot=true)`
        * [ ] `Tb = get(subT.pack(b), snapshot=true)`
        * [ ] `sumMid = fold(getRange(subT.range(nextFixedN(a), b), snapshot=true))`
        * [ ] `est = Ta*fracLeft + sumMid + Tb*fracRight`

## F) Tests (minimum to ship)

* [ ] **Mass conservation:** `Σ T[p] == |IDX|` after mutation rounds.
* [ ] **Equality:** small sets → **EXACT**; large sets → **APPROX** within target error.
* [ ] **Range:** P50/P90/P99 error reports for Uniform/Zipf datasets meet thresholds (e.g., P90 ≤ 15% uniform; ≤ 25% skewed).
* [ ] **Metamorphic:** monotonicity, additivity, linearity, padding invariance.
* [ ] **Concurrency fuzz:** multi-threaded Add/Del/Update preserves mass and consistent reads.

## G) Operations / Tuning

* [ ] Basic metrics: `adds/sec`, `deletes/sec`, `estimate_latency_ms`, `peek_hit_rate`.
* [ ] If a bucket gets hot, consider **raising `N`** for that field (migrate via dual-write to `N=3` counters, then read-switch, then retire `N=2`).

---

## Tiny Pseudocode Stub

```java
byte[] pN(byte[] k, int N) { return firstN(rightPad(k, N, (byte)0x00)); }

void add(byte[] k)    { tr.mutate(ADD, T.pack(pN(k,N)), encodeI64(+1)); }
void del(byte[] k)    { tr.mutate(ADD, T.pack(pN(k,N)), encodeI64(-1)); }

Estimate estimateEq(byte[] v) {
  long peek = countRange(idxRangeFor(v), PEEK_CAP + 1); // snapshot
  if (peek <= PEEK_CAP) return Estimate.exact(peek);
  return Estimate.approx(Math.round(estimateRange(v, lexSuccessor(vPadFF))));
}

double estimateRange(byte[] A, byte[] B) {
  var a = pN(A,N), b = pN(B,N);
  int aNextByte = (A.length > N) ? (A[N] & 0xFF) : 0;
  int bNextByte = (B.length > N) ? (B[N] & 0xFF) : 0;
  double fracLeft = (256.0 - aNextByte) / 256.0;
  double fracRight = bNextByte / 256.0;

  long Ta = getOrZero(T.pack(a), snapshot=true);
  if (Arrays.equals(a, b)) {
    double ratio = clamp01(fracRight - (1.0 - fracLeft));
    return Ta * ratio;
  }
  long Tb = getOrZero(T.pack(b), snapshot=true);
  long sumMid = sumRange(T.range(nextFixedN(a), b), snapshot=true);
  return Ta*fracLeft + sumMid + Tb*fracRight;
}
```

That’s the lean, FDB-ready spec—**no sharding**, just atomic counters per fixed-prefix bucket, equality peek, and simple range math.
