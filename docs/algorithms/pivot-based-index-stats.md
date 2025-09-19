# Pivot-Based Index Statistics Design for Kronotop Query Planner

## Overview

This document outlines the design of a pivot-based index statistics system for Kronotop. The goal is to provide high-fidelity, low-overhead, and incrementally maintainable statistics that guide the query planner's decisions, especially in the absence of sampling or full index scans. The system is deterministic, adaptive, and robust against data skew and drift.

## Motivation

Traditional systems like PostgreSQL rely on random sampling and periodic ANALYZE operations to collect statistics. These approaches:

* Can miss skewed or localized patterns
* Require full scans or table sampling
* Age poorly with changing data

In contrast, Kronotop benefits from a deterministic insertion order (via backpointers) and fully ordered indexes. This allows us to implement a pivot-driven statistics mechanism that is:

* Incremental and low-cost
* Localized and drift-aware
* Suited for real-time systems and document workloads

## Design Components

### 1. Pivot Placement

Pivots are lightweight markers placed every *N* entries (e.g., every 10k inserts). Each pivot stores:

```java
record IndexPivot {
  long offset;       // Insertion-order position
  byte[] key;        // Index key
  Object value;      // Parsed value
  Instant createdAt;
}
```

Pivot placement can be:

* Fixed interval (e.g., every 10k entries)
* Prefix-based (e.g., key/value prefix change)
* Adaptive (e.g., entropy or cardinality spikes)

### 2. Pivot Analysis

Each pivot undergoes localized analysis over a window of ±K entries (e.g., ±2k). This window is scanned to compute:

```java
record PivotProfile {
  long pivotOffset;
  Object minValue;
  Object maxValue;
  List<Double> quantiles;           // e.g., P10, P25, ..., P90
  Map<Object, Double> mcv;          // Most common values with relative frequencies
  int nDistinct;
  double stddev;
  Instant analyzedAt;
}
```

These profiles represent local distributions around the pivot.

### 3. Drift Detection

To avoid unnecessary re-analysis, pivot profiles are only recomputed if drift is detected via periodic probing.

```java
boolean isDrifted(PivotProfile previous, ProbeResult current) {
  return abs(current.mean - previous.mean) > ε
      || abs(current.nDistinct - previous.nDistinct) > δ;
}
```

Alternative approaches include histogram divergence (e.g., KL-divergence) or MCV change detection.

### 4. Global Summary Construction

All active pivot profiles are periodically merged into a global summary used by the query planner:

```java
record IndexStatisticsSummary {
  List<Double> globalQuantiles;
  Map<Object, Double> globalMCV;
  double averageStddev;
  int globalDistinctEstimate;
  List<MinMaxWindow> pivotRanges;
  Instant lastUpdated;
}
```

* Quantiles are merged from local histograms
* MCVs are aggregated and normalized
* Pivot min-max windows are retained for fast predicate matching

### 5. Planner Integration

At planning time, the query planner estimates selectivity using:

* MCV hit: if predicate value exists in any pivot's MCV list
* Quantile estimation: if value falls within pivot's min–max window
* Fallback: use `1/nDistinct` if no match is found

To speed up lookup:

* A `NavigableMap<Object, PivotProfile>` can be maintained to find pivot windows covering the predicate value

### 6. Self-Healing and Adaptive Maintenance

The system is self-correcting:

* New pivots are added as the index grows
* Old/stale pivots are re-analyzed or evicted if drift is detected
* Global summary is recomputed incrementally

Pivot metadata is minimal and can be compressed (e.g., only store value hash + offset)

## Advantages

| Feature                   | Benefit                                       |
| ------------------------- | --------------------------------------------- |
| Deterministic             | No random sampling required                   |
| Local & Global Visibility | Combines micro and macro statistical views    |
| Drift Detection           | Keeps statistics accurate under changing data |
| Incremental Updates       | No full index scan required                   |
| Planner-Oriented          | Fast, accurate selectivity estimation         |

## Future Work

* Use t-digest or HDR histogram for better quantile merging
* Introduce entropy-based pivot placement heuristics
* Integrate with query feedback loop for runtime statistics correction

## Conclusion

This pivot-based approach offers a powerful foundation for Kronotop’s indexing subsystem to guide query planning. It blends deterministic design with adaptive behavior and offers production-grade introspection at scale, without the cost of traditional sampling or full analysis sweeps.
