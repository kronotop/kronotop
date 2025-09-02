# Logarithmic Histogram for Selectivity Estimation in FoundationDB

## Table of Contents
- [1. Purpose](#1-purpose)
- [2. Data Representation](#2-data-representation)
  - [2.1 Positive and Negative Histograms](#21-positive-and-negative-histograms)
  - [2.2 Logarithmic Bucketing](#22-logarithmic-bucketing)
  - [2.3 Stored Counters](#23-stored-counters)
- [3. Write Path](#3-write-path)
  - [3.1 Insert](#31-insert)
  - [3.2 Delete](#32-delete)
  - [3.3 Update](#33-update)
- [4. Read Path](#4-read-path)
  - [4.1 Greater-than Query (Pvalue--t)](#41-greater-than-query-pvalue--t)
  - [4.2 Range Query (pa--value--b)](#42-range-query-pa--value--b)
  - [4.3 Cost Bound](#43-cost-bound)
- [5. Concurrency and Correctness](#5-concurrency-and-correctness)
- [6. Keyspace Layout (Conceptual)](#6-keyspace-layout-conceptual)
- [7. Parameters](#7-parameters)
- [8. Characteristics](#8-characteristics)
- [9. Limitations and Design Choices](#9-limitations-and-design-choices)
- [10. Summary](#10-summary)

---

## 1. Purpose
Query planners rely on approximate selectivity estimates to decide which index access paths are efficient. The goal of this design is to maintain **lightweight, bounded-cost histograms** inside FoundationDB (FDB) that provide estimates such as `P(value > t)` or `P(a ≤ value < b)` for indexed fields.

The approach is explicitly designed for:

- **Write efficiency**: O(1) atomic updates per insert/delete/update of a row
- **Read efficiency**: bounded number of key reads, independent of total dataset size
- **Scalability**: handles wide numeric ranges and negative values
- **FDB compliance**: only uses atomic `ADD` mutations and snapshot reads, avoiding conflicts

---

## 2. Data Representation

### 2.1 Positive and Negative Histograms
- **Positive values (`v > 0`)** are stored in a `pos` histogram
- **Negative values (`v < 0`)** are stored in a `neg` histogram, bucketed by their **magnitude** (`|v|`)
- **Zero (`v = 0`)** is tracked in a dedicated counter

### 2.2 Logarithmic Bucketing
- Values are mapped by their base-10 logarithm
- For a given magnitude `x`:
  - **Decade (`d`)** = `floor(log10(x))`
  - **Sub-bucket (`j`)** = subdivision of the decade into `m` equal log-space intervals
  - **Group (`g`)** = coarser subdivision of sub-buckets to accelerate queries

### 2.3 Stored Counters
For each value added, the following are incremented:

- **Sub-bucket counter** `(d,j)`
- **Decade sum** (total count of values in decade `d`)
- **Group sum** (total count for sub-bucket group within `d`)
- **Zero counter** if `v == 0`

Totals across the histogram are derived by summing decade/group counters; no dedicated global counter is required.

---

## 3. Write Path

### 3.1 Insert
- Map the value to its histogram type (`pos`, `neg`, or `zero`)
- Compute `(d,j,g)` if applicable
- Perform atomic increments (`ADD(+1)`) to the relevant counters

### 3.2 Delete
- Apply the same mapping as insert
- Perform atomic decrements (`ADD(-1)`) to the same counters
- Ensures deletes always undo prior inserts

### 3.3 Update
- When a field value changes (`v_old → v_new`), both operations are performed in a single transaction:
  - Decrement counters for `v_old`
  - Increment counters for `v_new`
- Keeps histogram consistent under concurrent updates

---

## 4. Read Path

### 4.1 Greater-than Query (`P(value > t)`)
- **If `t > 0`**:
  - Examine only the positive histogram
  - Accumulate counts from decades above `d_t`, group sums and sub-buckets within `d_t`, plus partial interpolation inside the boundary bucket
- **If `t = 0`**:
  - All positive counts included; negatives excluded
- **If `t < 0`**:
  - Include all positives, plus negative values with magnitude less than `|t|`

### 4.2 Range Query (`P(a ≤ value < b)`)
- Computed as `P(value ≥ a) − P(value ≥ b)`

### 4.3 Cost Bound
Reads touch:

- A handful of decade sums (`≤ W` active decades)
- Group sums within the boundary decade (`≤ m / groupSize`)
- A small number of sub-bucket counters (≤ groupSize)
- The zero counter

Total: **20–60 keys** per estimate, independent of dataset size.

---

## 5. Concurrency and Correctness
- All updates use **atomic `ADD`**; concurrent writers never conflict
- Reads use **snapshot isolation**; estimates may lag slightly but are consistent
- Deletes and updates are symmetric and idempotent
- Negative counts may occur if the application issues duplicate deletes/updates; background checks can detect anomalies

---

## 6. Keyspace Layout (Conceptual)

```
/stats/{index}/{field}/
pos/log10/m/{m}/d/{d}/j/{j} -> int64
pos/log10/m/{m}/d/{d}/sum -> int64
pos/log10/m/{m}/d/{d}/g/{g} -> int64

neg/log10/m/{m}/d/{d}/j/{j} -> int64
neg/log10/m/{m}/d/{d}/sum -> int64
neg/log10/m/{m}/d/{d}/g/{g} -> int64

zero_count -> int64
meta -> parameters (m, groupSize, W, etc.)
```


---

## 7. Parameters
- **m**: sub-buckets per decade (e.g., 16)
- **groupSize**: sub-buckets per group (e.g., 4)
- **W**: active decades maintained (e.g., 8)
- **Overflow/underflow**: optional summaries of decades beyond the active window
- **Versioning**: parameters are tied to a versioned prefix for schema evolution

---

## 8. Characteristics
- **Write complexity**: O(1), constant number of atomic increments per row
- **Read complexity**: O(W + m/groupSize + groupSize), bounded
- **Storage size**: proportional to `W × m` per histogram (positive and negative), independent of dataset size
- **Accuracy**: log-space bucketing balances range adaptability with bounded storage
- **Scalability**: millions of inserts/deletes per second if FDB cluster is provisioned

---

## 9. Limitations and Design Choices
- High-frequency updates on the same value can stress the same sub-bucket key; considered unrealistic for ACID workloads
- Estimates are approximate, not exact; precision depends on `m` and `W`
- Negative counts possible if application issues duplicate deletes/updates
- Janitor/compaction may be added later to bound key counts; orthogonal to insert/delete/update

---

## 10. Summary
This design implements an **FDB-native logarithmic histogram** for selectivity estimation:

- Symmetric insert/delete/update using atomic `ADD`
- Positive/negative domains separated, zero tracked independently
- Logarithmic bucketing across decades with sub-buckets and groups
- Bounded read and write costs, independent of dataset size
- FDB-friendly: no read-modify-writes, only atomic increments and snapshot reads

It provides query planners with fast, approximate selectivity estimates while remaining scalable and simple to operate within FoundationDB.
