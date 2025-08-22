# LogicalPlanner Architecture & Design Document

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Optimization Pipeline](#optimization-pipeline)
5. [Node Hierarchy](#node-hierarchy)
6. [Validation System](#validation-system)
7. [Extension Points](#extension-points)
8. [Performance Considerations](#performance-considerations)
9. [Testing Strategy](#testing-strategy)
10. [Future Development Guidelines](#future-development-guidelines)

## Overview

The LogicalPlanner is a sophisticated query optimization engine that transforms BQL (Bucket Query Language) expressions into optimized logical plans. It implements a multi-pass optimization pipeline with advanced techniques including contradiction detection, tautology elimination, redundant condition removal, and constant folding.

### Key Characteristics
- **Multi-pass optimization pipeline** with 6 distinct transformation phases
- **Research-grade query optimization** with advanced algebraic simplifications
- **Comprehensive validation system** for structural and semantic correctness
- **Extensible architecture** supporting custom optimization transforms
- **MongoDB MQL-compatible** syntax with BSON document processing

## Architecture

### High-Level Flow
```
BQL Expression → Raw Logical Plan → Optimization Pipeline → Validated Logical Plan
     ↓                    ↓                     ↓                      ↓
BqlAnd/BqlOr/etc    LogicalAnd/LogicalOr    6 Transform Passes    ValidationResult
```

### Core Design Principles
1. **Immutable Node Structure**: All LogicalNode instances are immutable records
2. **Visitor Pattern**: Recursive tree traversal using pattern matching
3. **Sealed Interfaces**: Type-safe node hierarchy with compile-time completeness
4. **Functional Transformation**: Each pass creates new trees rather than mutating
5. **Separation of Concerns**: Clear distinction between optimization and validation

## Core Components

### 1. LogicalPlanner Class
**Location**: `com.kronotop.bucket.planner2.logical.LogicalPlanner`

```java
public final class LogicalPlanner {
    private final List<LogicalTransform> pipeline;
    private final LogicalPlanValidator validator;
}
```

**Responsibilities**:
- BQL-to-LogicalNode conversion
- Orchestrating optimization pipeline
- Plan validation
- Public API for query planning

**Key Methods**:
- `plan(BqlExpr)`: Creates optimized logical plan
- `planAndValidate(BqlExpr)`: Plans and validates, throwing exception on failure
- `validate(LogicalNode)`: Validates existing plan
- `isWellFormed(LogicalNode)`: Quick structural validation

### 2. LogicalNode Hierarchy
**Location**: `com.kronotop.bucket.planner2.logical.LogicalNode`

```java
public sealed interface LogicalNode 
    permits LogicalFilter, LogicalAnd, LogicalOr, LogicalNot, 
            LogicalElemMatch, LogicalTrue, LogicalFalse
```

**Node Types**:
- **LogicalFilter**: Field-level predicates (field op value)
- **LogicalAnd**: Conjunction of child nodes
- **LogicalOr**: Disjunction of child nodes  
- **LogicalNot**: Negation of child node
- **LogicalElemMatch**: Array element matching with sub-plan
- **LogicalTrue**: Constant TRUE (optimization result)
- **LogicalFalse**: Constant FALSE (optimization result)

### 3. TransformUtils (Shared Utilities)
**Location**: `LogicalPlanner.TransformUtils` (nested class)

**Key Utilities**:
```java
// Value extraction from BQL wrappers
static Object extractValue(Object operand)

// Type checking and conversion
static boolean areNumericOperands(Object op1, Object op2)
static Number extractNumericValue(Object operand)

// Common processing patterns
static LogicalNode processAndChildren(List<LogicalNode> children, Function<LogicalNode, LogicalNode> rewriter)
static LogicalNode processOrChildren(List<LogicalNode> children, Function<LogicalNode, LogicalNode> rewriter)

// Analysis utilities
static Map<String, List<LogicalFilter>> groupFiltersByField(List<LogicalNode> children)
static boolean hasFieldProperty(List<LogicalFilter> filters, BiPredicate<LogicalFilter, LogicalFilter> checker)
```

## Optimization Pipeline

The pipeline consists of 6 sequential transformation passes:

### 1. FlattenAndOrTransform
**Purpose**: Eliminates nested logical operators
**Example**: `AND(a, AND(b, c))` → `AND(a, b, c)`
**Complexity**: O(n) where n is node count

### 2. RemoveDoubleNotTransform  
**Purpose**: Eliminates double negation
**Example**: `NOT(NOT(condition))` → `condition`
**Complexity**: O(n)

### 3. ContradictionDetectionTransform
**Purpose**: Detects impossible conditions and replaces with FALSE
**Examples**:
- `AND(field = A, field = B)` → `FALSE`
- `AND(field > 100, field < 50)` → `FALSE`
- `AND(field = A, field != A)` → `FALSE`

**Algorithm**:
1. Group filters by field name
2. For each field, check all filter pairs for contradictions
3. Support for equality, inequality, and numeric range contradictions
4. Replace entire AND with LogicalFalse if contradiction found

### 4. TautologyEliminationTransform
**Purpose**: Detects always-true conditions and replaces with TRUE
**Examples**:
- `OR(field = A, field != A)` → `TRUE`
- `OR(field > 5, field <= 5)` → `TRUE`

**Algorithm**:
1. Check for tautological patterns in OR contexts
2. Handle complementary range conditions
3. Replace entire OR with LogicalTrue if tautology found

### 5. RedundantConditionEliminationTransform
**Purpose**: Removes subsumed and duplicate conditions
**Examples**:
- `AND(field > 5, field > 3)` → `field > 5` (more restrictive)
- `OR(field < 10, field < 15)` → `field < 15` (less restrictive)
- `AND(field = A, field = A)` → `field = A` (duplicates)

**Algorithm**:
1. Group filters by field within AND/OR contexts
2. Apply context-specific subsumption rules:
   - **AND context**: Keep more restrictive conditions
   - **OR context**: Keep less restrictive conditions
3. Support for numeric ranges and set operations (IN/NIN/ALL)

### 6. ConstantFoldingTransform
**Purpose**: Simplifies expressions involving constants
**Examples**:
- `AND(TRUE, condition)` → `condition`
- `OR(FALSE, condition)` → `condition`
- `AND(TRUE, TRUE)` → `TRUE`

**Algorithm**:
1. Process constant nodes (LogicalTrue/LogicalFalse)
2. Apply boolean algebra rules
3. Simplify parent nodes based on children

## Node Hierarchy

### Sealed Interface Design
```java
public sealed interface LogicalNode 
    permits LogicalFilter, LogicalAnd, LogicalOr, LogicalNot, 
            LogicalElemMatch, LogicalTrue, LogicalFalse {
    // Marker interface - no methods
}
```

### Node Implementations

#### LogicalFilter
```java
public record LogicalFilter(String field, Operator op, Object operand) 
    implements LogicalNode
```
- Represents field-level predicates
- Supports all BQL operators (EQ, NE, GT, GTE, LT, LTE, IN, NIN, ALL, SIZE, EXISTS)
- Operand can be BqlValue wrapper or native Java type

#### LogicalAnd/LogicalOr
```java
public record LogicalAnd(List<LogicalNode> children) implements LogicalNode
public record LogicalOr(List<LogicalNode> children) implements LogicalNode
```
- Immutable child lists
- Support arbitrary nesting depth
- Empty lists handled by optimization passes

#### LogicalNot
```java
public record LogicalNot(LogicalNode child) implements LogicalNode
```
- Single child negation
- Participates in double-negation elimination

#### LogicalElemMatch
```java
public record LogicalElemMatch(String field, LogicalNode subPlan) 
    implements LogicalNode
```
- Array element matching with nested conditions
- Recursive sub-plan processing

#### LogicalTrue/LogicalFalse
```java
public record LogicalTrue() implements LogicalNode {
    public static final LogicalTrue INSTANCE = new LogicalTrue();
}
public record LogicalFalse() implements LogicalNode {
    public static final LogicalFalse INSTANCE = new LogicalFalse();
}
```
- Singleton constant nodes
- Results of optimization transformations
- Enable advanced boolean algebra

## Validation System

### LogicalPlanValidator
**Location**: `com.kronotop.bucket.planner2.logical.LogicalPlanValidator`

**Design Philosophy**: Post-optimization validation focusing on structural integrity rather than semantic analysis.

### Validation Categories

#### 1. Structural Validity
- Null pointer detection
- Malformed tree structures
- Missing required fields

#### 2. Operator Compatibility
- Type checking for operators (SIZE requires integer, EXISTS requires boolean)
- Warning for potentially problematic combinations
- List validation for IN/NIN/ALL operators

#### 3. Optimization Quality
- Detection of unoptimized patterns that should have been transformed
- Single-child AND/OR nodes
- Nested structures that should be flattened
- Double negations

#### 4. Constant Node Usage
- Proper LogicalTrue/LogicalFalse usage
- Detection of missed optimization opportunities

### ValidationResult Structure
```java
public record ValidationResult(boolean valid, List<ValidationIssue> issues) {
    public boolean hasErrors()
    public boolean hasWarnings()
}

public record ValidationIssue(
    Severity severity,
    String message, 
    String field,
    LogicalNode node
)
```

## Extension Points

### 1. Custom Optimization Transforms
Implement the `LogicalTransform` interface:
```java
public interface LogicalTransform {
    LogicalNode transform(LogicalNode root);
}
```

**Guidelines**:
- Always return new immutable trees
- Handle all node types in switch statements
- Use TransformUtils for common patterns
- Consider both performance and correctness

### 2. Custom Operators
1. Add to `Operator` enum
2. Update BQL parser to recognize operator
3. Add conversion logic in `convert()` method
4. Update optimization transforms if needed
5. Add validation rules in LogicalPlanValidator

### 3. Custom Node Types
1. Add to sealed interface permits list
2. Implement as immutable record
3. Update all transform classes to handle new type
4. Add validation logic
5. Update test suites

### 4. Custom Validation Rules
Extend LogicalPlanValidator with additional validation methods following the pattern:
```java
private void checkCustomRules(LogicalNode node, List<ValidationIssue> issues) {
    // Custom validation logic
}
```

## Performance Considerations

### 1. Algorithmic Complexity
- **Overall Pipeline**: O(n × p) where n = nodes, p = passes (6)
- **Individual Transforms**: Mostly O(n) with some O(n²) for comparison operations
- **Memory Usage**: Creates new trees, old trees eligible for GC

### 2. Optimization Strategies
- **Shared Utilities**: Common patterns extracted to TransformUtils
- **Early Returns**: Contradictions and tautologies short-circuit processing
- **Constant Folding**: Reduces tree size early in pipeline
- **Immutable Structures**: Enable safe concurrent access

### 3. Bottlenecks
- **Field Grouping**: O(n) operation repeated across transforms
- **Pairwise Comparisons**: O(n²) for contradiction/tautology detection
- **Deep Recursion**: Potential stack overflow on very deep trees

### 4. Memory Patterns
- **Tree Creation**: Each transform creates new tree
- **Singleton Constants**: LogicalTrue.INSTANCE, LogicalFalse.INSTANCE
- **Shared Utilities**: Static methods minimize object creation

## Testing Strategy

### 1. Test Structure
- **Unit Tests**: Individual transform testing
- **Integration Tests**: Full pipeline testing  
- **Edge Case Tests**: Empty plans, single nodes, deep nesting
- **Property-Based Tests**: Random query generation

### 2. Key Test Categories

#### Optimization Tests (`LogicalPlannerOptimizationTest`)
- 32 comprehensive tests covering all transforms
- Contradiction detection scenarios
- Tautology elimination cases
- Constant folding validation

#### Redundancy Tests (`RedundantConditionEliminationTest`)  
- 25+ tests for subsumption logic
- Numeric range redundancy
- Set operation redundancy
- Context-specific behavior (AND vs OR)

#### Integration Tests (`LogicalPlannerTest`)
- End-to-end BQL processing
- Order-independent assertions
- Real-world query patterns

### 3. Test Data Management
- **BqlTestDataBuilder**: Fluent API for complex test data
- **Parameterized Tests**: Coverage of operator combinations
- **Edge Case Generation**: Systematic boundary testing

## Future Development Guidelines

### 1. Adding New Optimizations

**Process**:
1. Implement LogicalTransform interface
2. Add to pipeline in appropriate order
3. Create comprehensive test suite
4. Update this documentation
5. Consider impact on existing transforms

**Considerations**:
- **Transform Order**: Later transforms can build on earlier ones
- **Idempotency**: Transforms should be safe to run multiple times
- **Correctness**: Extensive testing required for soundness
- **Performance**: Measure impact on overall pipeline

### 2. Extending Node Types

**Required Changes**:
1. Update sealed interface permits clause
2. Modify all transform classes
3. Add validation rules
4. Update conversion logic
5. Create test coverage

**Design Guidelines**:
- Use immutable records
- Include all necessary data in constructor
- Consider toString() for debugging
- Follow existing naming conventions

### 3. Performance Optimization

**Opportunities**:
- **Memoization**: Cache transform results for identical subtrees
- **Parallel Processing**: Independent subtrees could be processed concurrently
- **Incremental Updates**: Partial re-optimization for plan modifications
- **Profile-Guided Optimization**: Reorder transforms based on real query patterns

### 4. Advanced Features

**Potential Enhancements**:
- **Cost-Based Optimization**: Estimate execution costs and choose best plans
- **Statistics Integration**: Use data statistics for better optimization decisions
- **Rule-Based Extensions**: Pluggable optimization rule system
- **Query Hints**: Allow manual optimization control

### 5. Maintenance Guidelines

**Code Quality**:
- **Immutability**: Never modify existing nodes
- **Type Safety**: Leverage sealed interfaces and pattern matching
- **Documentation**: Update JavaDoc for all public methods
- **Testing**: Maintain >90% code coverage

**Debugging Support**:
- **Logging**: Add structured logging for optimization decisions
- **Visualization**: Consider plan visualization tools
- **Profiling**: Instrument performance-critical paths
- **Validation**: Comprehensive error messages

## Conclusion

The LogicalPlanner represents a sophisticated, production-ready query optimization engine with advanced algebraic optimization techniques. Its extensible architecture, comprehensive validation system, and robust testing infrastructure make it suitable for complex query processing requirements while maintaining high performance and correctness standards.

The clean separation between optimization logic and validation, combined with the use of modern Java features like sealed interfaces and pattern matching, creates a maintainable and type-safe foundation for future development.