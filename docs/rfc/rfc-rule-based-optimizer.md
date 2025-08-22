# RFC: Rule-Based Physical Plan Optimizer for Kronotop Query Planner

## Abstract

This RFC proposes a rule-based optimizer framework for the Kronotop query planner to enhance query execution performance through systematic physical plan optimizations. The optimizer operates after the physical planning phase, applying transformation rules to optimize the generated PhysicalNode execution plans before they reach the Plan Executor.

## Background

Currently, Kronotop employs a four-phase query planning architecture:

1. **BQL Parser** → Logical Plan (LogicalNode tree)
2. **Physical Planner** → Physical Plan (PhysicalNode tree)
3. **Plan Executor** → Results

The Physical Planner currently implements a single rule: "if an index exists for a selector, use PhysicalIndexScan; otherwise, use PhysicalFullScan." While this produces correct execution plans, there are significant opportunities for optimization at the physical plan level that could dramatically improve query performance.

## Motivation

### Current Limitations

1. **No Physical Plan Optimization**: Physical plans are executed as-is without further optimization
2. **Suboptimal Execution Strategies**: Missing optimizations like:
   - Index intersection for multiple indexed fields
   - Range scan consolidation for same-field conditions
   - Scan order optimization based on selectivity
   - Redundant scan elimination
   - Cost-based scan strategy selection
3. **Limited Index Utilization**: No consideration of multiple index usage patterns
4. **No Performance-Based Decisions**: No consideration of selectivity, cardinality, or execution cost

### Proposed Benefits

- **Improved Query Performance**: Optimized physical execution plans with better resource utilization
- **Intelligent Index Usage**: Advanced index intersection and selection strategies
- **Extensible Framework**: Foundation for adding sophisticated physical optimization rules
- **Future-Proof Design**: Architecture ready for cost-based optimization and statistics

## Design Overview

### Architecture

```
BQL Query → LogicalPlanner → PhysicalPlanner → PhysicalOptimizer → Plan Executor
                ↓                ↓                    ↓               ↓
         LogicalNode Tree   PhysicalNode Tree   Optimized Physical   Execution
                                                     Plan
```

### Core Components

#### 1. Physical Plan Optimizer Framework

```java
public class PhysicalPlanOptimizer {
    private final List<PhysicalOptimizationRule> rules;
    
    public PhysicalNode optimize(PhysicalNode plan, BucketMetadata metadata) {
        PhysicalNode current = plan;
        boolean changed;
        int iterations = 0;
        
        do {
            changed = false;
            for (PhysicalOptimizationRule rule : rules) {
                PhysicalNode optimized = rule.apply(current, metadata);
                if (!optimized.equals(current)) {
                    current = optimized;
                    changed = true;
                }
            }
            iterations++;
        } while (changed && iterations < maxOptimizationPasses);
        
        return current;
    }
}
```

#### 2. Physical Optimization Rule Interface

```java
public interface PhysicalOptimizationRule {
    PhysicalNode apply(PhysicalNode node, BucketMetadata metadata);
    String getName();
    int getPriority(); // Higher priority rules execute first
    boolean canApply(PhysicalNode node); // Quick applicability check
}
```

#### 3. Physical Optimization Categories

**Index Optimization Rules:**
- Index intersection for multiple indexed AND conditions
- Range scan consolidation for same-field conditions
- Index selection based on estimated selectivity

**Scan Strategy Rules:**
- Convert multiple PhysicalIndexScans to single optimized scan
- Eliminate redundant scans in OR conditions
- Optimize scan order based on estimated cost

**Execution Order Rules:**
- Reorder AND/OR children by estimated selectivity
- Push most selective conditions first
- Optimize nested query execution order

## Proposed Physical Optimization Rules

### Phase 1: Index Optimization Rules (High Priority)

#### 1. Range Scan Consolidation Rule
```java
public class RangeScanConsolidationRule implements PhysicalOptimizationRule {
    // Consolidate multiple PhysicalIndexScans on same field with range operators
    // Example: AND(PhysicalIndexScan(age >= 18), PhysicalIndexScan(age < 65))
    //       → PhysicalRangeScan(age, [18, 65))
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return node instanceof PhysicalAnd && 
               hasMultipleIndexScansOnSameField(node);
    }
}
```

#### 2. Index Intersection Rule
```java
public class IndexIntersectionRule implements PhysicalOptimizationRule {
    // Combine multiple indexed conditions into optimized intersection scan
    // Example: AND(PhysicalIndexScan(name="john"), PhysicalIndexScan(age=25))
    //       → PhysicalIndexIntersection([name_index, age_index], [filters])
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return node instanceof PhysicalAnd && 
               hasMultipleIndexScans(node);
    }
}
```

#### 3. Redundant Scan Elimination Rule
```java
public class RedundantScanEliminationRule implements PhysicalOptimizationRule {
    // Eliminate duplicate scans in OR conditions
    // Example: OR(PhysicalIndexScan(status="active"), PhysicalIndexScan(status="active"))
    //       → PhysicalIndexScan(status="active")
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return (node instanceof PhysicalOr || node instanceof PhysicalAnd) &&
               hasDuplicateScans(node);
    }
}
```

### Phase 2: Execution Order Optimization Rules (Medium Priority)

#### 4. Selectivity-Based Ordering Rule
```java
public class SelectivityOrderingRule implements PhysicalOptimizationRule {
    // Reorder AND/OR children by estimated selectivity (most selective first)
    // Uses operator selectivity heuristics: EQ > RANGE > IN > NE > EXISTS
    // IndexScans typically before FullScans
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return (node instanceof PhysicalAnd || node instanceof PhysicalOr) &&
               needsReordering(node);
    }
}
```

#### 5. Index vs Full Scan Strategy Rule
```java
public class ScanStrategyOptimizationRule implements PhysicalOptimizationRule {
    // Choose optimal scan strategy based on estimated costs
    // May prefer FullScan over multiple IndexScans for low-selectivity queries
    // Consider: index count, estimated result size, scan complexity
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return hasSuboptimalScanStrategy(node);
    }
}
```

### Phase 3: Advanced Physical Rules (Low Priority)

#### 6. Nested Query Flattening Rule
```java
public class NestedQueryFlatteningRule implements PhysicalOptimizationRule {
    // Flatten nested AND/OR structures where beneficial
    // Example: AND(a, AND(b, c)) → AND(a, b, c)
    // Only when it improves execution efficiency
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return hasNestedSameTypeOperators(node);
    }
}
```

#### 7. Filter Predicate Optimization Rule
```java
public class FilterPredicateOptimizationRule implements PhysicalOptimizationRule {
    // Optimize filter predicates for better execution
    // Example: Convert IN with single value to EQ
    // Example: Optimize range predicates for index usage
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return hasOptimizableFilters(node);
    }
}
```

#### 8. Short-Circuit Optimization Rule
```java
public class ShortCircuitOptimizationRule implements PhysicalOptimizationRule {
    // Add short-circuit hints for boolean operations
    // Place high-selectivity conditions early in AND operations
    // Place low-selectivity conditions early in OR operations
    
    @Override
    public boolean canApply(PhysicalNode node) {
        return canBenefitFromShortCircuit(node);
    }
}
```

## Integration Points

### 1. Query Pipeline Integration

```java
public class QueryPlanner {
    private final LogicalPlanner logicalPlanner;
    private final PhysicalPlanner physicalPlanner;
    private final PhysicalPlanOptimizer optimizer;
    private final PlanExecutor planExecutor;
    
    public Results executeQuery(String bql, BucketMetadata metadata) {
        // Step 1: Parse BQL to LogicalNode tree
        BqlExpr expr = BqlParser.parse(bql);
        LogicalNode logical = logicalPlanner.plan(expr);
        
        // Step 2: Generate initial PhysicalNode tree
        PhysicalNode physical = physicalPlanner.plan(metadata, logical);
        
        // Step 3: Optimize the physical plan (NEW STEP)
        PhysicalNode optimized = optimizer.optimize(physical, metadata);
        
        // Step 4: Execute the optimized plan
        return planExecutor.execute(optimized, metadata);
    }
}
```

### 2. New Physical Node Types for Optimization

```java
// New physical node for range scans
public record PhysicalRangeScan(
    String selector,
    Object lowerBound,
    Object upperBound,
    boolean includeLower,
    boolean includeUpper,
    IndexDefinition index
) implements PhysicalNode {
    @Override
    public <R> R accept(PhysicalPlanVisitor<R> visitor) {
        return visitor.visitRangeScan(this);
    }
}

// New physical node for index intersections
public record PhysicalIndexIntersection(
    List<IndexDefinition> indexes,
    List<PhysicalFilter> filters
) implements PhysicalNode {
    @Override
    public <R> R accept(PhysicalPlanVisitor<R> visitor) {
        return visitor.visitIndexIntersection(this);
    }
}
```

### 3. Optimizer Configuration

```java
public class PhysicalOptimizerConfig {
    private boolean enableOptimizer = true;
    private int maxOptimizationPasses = 5;
    private Set<String> disabledRules = new HashSet<>();
    private Map<String, Integer> rulePriorities = new HashMap<>();
    
    // Optimization thresholds
    private int minIndexIntersectionBenefit = 2; // Min indexes for intersection
    private double fullScanThresholdRatio = 0.8; // When to prefer full scan
    private boolean enableRangeScanOptimization = true;
    private boolean enableSelectivityOrdering = true;
}
```

### 4. Metrics and Observability

```java
public class PhysicalOptimizerMetrics {
    private long totalOptimizationTime;
    private Map<String, Integer> ruleApplicationCounts;
    private Map<String, Long> ruleExecutionTimes;
    private int plansOptimized;
    private int plansUnchanged;
    
    // Performance impact metrics
    private double averageOptimizationTimeMs;
    private Map<String, Double> estimatedPerformanceGains;
    
    // Integration with existing metrics system
}
```

## Implementation Plan

### Phase 1: Foundation Framework (2-3 weeks)
- Implement PhysicalPlanOptimizer framework and rule interface
- Add basic redundant scan elimination rule
- Integrate optimizer into query pipeline after PhysicalPlanner
- Add configuration support and metrics collection
- Update PhysicalNode interface to support new node types

### Phase 2: Core Index Optimizations (3-4 weeks)
- Implement range scan consolidation rule
- Add PhysicalRangeScan node type and executor support
- Implement selectivity-based ordering rule
- Add comprehensive testing for index optimization scenarios
- Performance testing and benchmarking

### Phase 3: Advanced Index Strategies (3-4 weeks)
- Implement index intersection optimization
- Add PhysicalIndexIntersection node type and executor support
- Implement scan strategy optimization rule
- Add filter predicate optimization rules
- Advanced performance testing with complex queries

### Phase 4: Production Readiness (2-3 weeks)
- Add comprehensive observability and metrics
- Performance regression testing
- Documentation and examples
- Configuration tuning guidelines
- Rollout strategy and feature flags

## Testing Strategy

### 1. Unit Tests
- Individual rule testing with isolated PhysicalNode trees
- Rule applicability testing (canApply method)
- Edge case handling (empty plans, single nodes, complex nesting)
- Rule interaction and convergence testing

### 2. Integration Tests
- End-to-end optimization with PhysicalPlannerWithIndexTest scenarios
- Correctness verification: optimized plans produce same results
- Performance comparison: before/after optimization execution times
- Complex query optimization testing (nested AND/OR with mixed indexes)

### 3. Performance Tests
- Optimization overhead measurement (should be < 1ms for typical queries)
- Query execution time improvements (target: 20-50% for multi-index queries)
- Memory usage impact analysis
- Throughput impact testing under load

### 4. Regression Tests
- Ensure optimization never produces incorrect results
- Performance regression detection
- Backward compatibility with existing query patterns

## Backward Compatibility

- **Full Compatibility**: Existing queries continue to work unchanged
- **Opt-in Design**: Optimizer can be disabled via configuration
- **Gradual Rollout**: Rules can be enabled/disabled individually
- **Fallback Strategy**: If optimization fails, execute original physical plan
- **Zero Breaking Changes**: No changes to existing APIs or query syntax

## Optimization Examples

### Example 1: Range Query Consolidation
```java
// Before Optimization:
PhysicalAnd([
    PhysicalIndexScan(age_index, PhysicalFilter("age", GTE, 18)),
    PhysicalIndexScan(age_index, PhysicalFilter("age", LT, 65))
])

// After RangeScanConsolidationRule:
PhysicalRangeScan("age", 18, 65, inclusive=true, exclusive=true, age_index)
```

### Example 2: Index Intersection
```java
// Before Optimization:
PhysicalAnd([
    PhysicalIndexScan(name_index, PhysicalFilter("name", EQ, "john")),
    PhysicalIndexScan(status_index, PhysicalFilter("status", EQ, "active")),
    PhysicalFullScan(PhysicalFilter("score", GT, 80))
])

// After IndexIntersectionRule:
PhysicalAnd([
    PhysicalIndexIntersection([name_index, status_index], [
        PhysicalFilter("name", EQ, "john"),
        PhysicalFilter("status", EQ, "active")
    ]),
    PhysicalFullScan(PhysicalFilter("score", GT, 80))
])
```

### Example 3: Selectivity-Based Reordering
```java
// Before Optimization:
PhysicalAnd([
    PhysicalFullScan(PhysicalFilter("description", NE, "test")),     // Low selectivity
    PhysicalIndexScan(user_id_index, PhysicalFilter("user_id", EQ, "123")) // High selectivity
])

// After SelectivityOrderingRule:
PhysicalAnd([
    PhysicalIndexScan(user_id_index, PhysicalFilter("user_id", EQ, "123")), // High selectivity first
    PhysicalFullScan(PhysicalFilter("description", NE, "test"))
])
```

## Future Enhancements

1. **Statistics-Based Optimization**: Collect and use index/table statistics for cost estimation
2. **Adaptive Optimization**: Machine learning-based rule selection and parameter tuning
3. **Join Optimization**: Advanced rules for multi-collection query optimization
4. **Materialized View Matching**: Automatic view selection for query acceleration  
5. **Parallel Execution Planning**: Rules for parallelizable scan strategies
6. **Cache-Aware Optimization**: Consider data locality and caching in optimization decisions

## Risks and Mitigation

### Risk 1: Optimization Overhead
- **Mitigation**: Fast rule applicability checks (`canApply()` method)
- **Monitoring**: Track optimization time per query
- **Threshold**: Skip optimization for simple queries

### Risk 2: Incorrect Optimizations
- **Mitigation**: Comprehensive correctness testing
- **Fallback**: Graceful degradation to unoptimized plan
- **Validation**: Result set comparison in testing

### Risk 3: Performance Regression
- **Mitigation**: Continuous performance benchmarking
- **Rollback**: Feature flags for quick disable
- **Monitoring**: Query execution time tracking

### Risk 4: Complexity Growth
- **Mitigation**: Clear rule interfaces and documentation
- **Testing**: Isolated rule testing and integration tests
- **Modularity**: Independent rule development and deployment

## Success Metrics

### Performance Targets
- **Optimization Overhead**: < 1ms for 95% of queries
- **Execution Improvement**: 20-50% faster for multi-index queries  
- **Range Query Improvement**: 30-70% faster for range conditions
- **Index Intersection Benefit**: 40-80% faster for multiple indexed conditions

### Operational Metrics
- **Rule Application Rate**: % of queries benefiting from optimization
- **False Positive Rate**: % of rules applied with no benefit
- **Optimization Stability**: < 0.1% optimization failures
- **Memory Overhead**: < 5% additional memory usage

## Conclusion

The proposed physical plan optimizer provides a robust foundation for systematic query performance improvement in Kronotop. By operating on PhysicalNode trees after the PhysicalPlanner, it can apply sophisticated optimizations while maintaining correctness and compatibility.

The rule-based approach ensures extensibility and maintainability, allowing incremental addition of new optimization strategies. The framework's design supports both simple heuristic optimizations and future cost-based optimization approaches.

Key benefits include:
- **Significant Performance Gains**: Especially for complex queries with multiple indexes
- **Maintainable Architecture**: Clear separation of optimization logic from planning
- **Production Ready**: Comprehensive testing, metrics, and fallback strategies
- **Future Extensible**: Foundation for advanced optimization techniques

This optimizer will dramatically improve query performance for complex workloads while providing a solid architecture for continued optimization enhancements.