# BQL2 Parser Architecture & Design Document

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [AST Node Hierarchy](#ast-node-hierarchy)
5. [Parser Implementation](#parser-implementation)
6. [Error Handling](#error-handling)
7. [Integration Points](#integration-points)
8. [Extension Points](#extension-points)
9. [Performance Characteristics](#performance-characteristics)
10. [Testing Strategy](#testing-strategy)
11. [Future Development Guidelines](#future-development-guidelines)

## Overview

The BQL2 (Bucket Query Language 2) parser is a sophisticated JSON-based query parser that implements MongoDB-compatible syntax. It converts BSON documents into a strongly-typed Abstract Syntax Tree (AST) using a recursive descent parsing approach, providing the foundation for Kronotop's query processing pipeline.

### Key Characteristics
- **MongoDB MQL-compatible** syntax with full BSON support
- **Streaming parser** using BSON BsonReader for memory efficiency
- **Strongly-typed AST** with sealed interfaces and immutable records
- **Thread-safe** stateless design with no shared state
- **Comprehensive error handling** with detailed exception messages
- **Extensible architecture** supporting new operators and value types

## Architecture

### High-Level Flow
```
JSON Query String → BSON Document → BsonReader → Recursive Parser → BQL AST → LogicalPlanner
        ↓                ↓              ↓              ↓              ↓              ↓
   "{"status":"active"}" Document    Streaming    BqlEq/BqlAnd    Immutable    LogicalFilter
```

### Core Design Principles
1. **Streaming Processing**: Memory-efficient parsing using BSON streaming API
2. **Type Safety**: Sealed interfaces ensure compile-time completeness
3. **Immutability**: All AST nodes are immutable records
4. **Stateless Design**: No parser state or configuration options
5. **Error Transparency**: Clear error messages with detailed context

## Core Components

### 1. BqlParser Class
**Location**: `com.kronotop.bucket.bql2.BqlParser`

```java
public final class BqlParser {
    // Main parsing entry point
    public static BqlExpr parse(String query) throws BqlParseException
    
    // AST explanation utility
    public static String explain(BqlExpr expr)
}
```

**Key Responsibilities**:
- JSON string validation and BSON conversion
- Recursive AST construction
- Error handling and exception wrapping
- Public API for query parsing

**Parsing Workflow**:
1. **Input Validation**: Parse JSON string to BSON Document
2. **Reader Creation**: Convert Document to streaming BsonReader
3. **Recursive Parsing**: Build AST using `readExpr()` method
4. **Error Handling**: Wrap exceptions in BqlParseException

### 2. Internal Parser Methods

#### Core Parsing Methods
```java
// Top-level document parsing
private static BqlExpr readExpr(BsonReader reader)

// Field vs operator disambiguation
private static BqlExpr parseFieldOrOperator(BsonReader reader, String name)

// Field-specific operator parsing
private static BqlExpr parseFieldOperator(BsonReader reader, String op, String field)

// Nested field expression parsing
private static BqlExpr readFieldExpression(BsonReader reader, String field)

// Array element matching
private static BqlExpr readElemMatchExpr(BsonReader reader, String field)

// BSON value conversion
private static BqlValue readValue(BsonReader reader)

// Array processing
private static List<BqlValue> readArray(BsonReader reader)
```

#### Parsing Algorithm
1. **Document Level**: Reads top-level document structure
2. **Name Resolution**: Distinguishes between field names and operators (`$and`, `$or`, etc.)
3. **Operator Processing**: Delegates to specific operator handlers
4. **Value Conversion**: Converts BSON values to typed BqlValue objects
5. **Recursion**: Processes nested documents and arrays recursively

## AST Node Hierarchy

### Base Interfaces

```java
// Main expression interface - sealed for type safety
public sealed interface BqlExpr 
    permits BqlAnd, BqlOr, BqlNot, BqlEq, BqlGt, BqlLt, BqlGte, BqlLte, 
            BqlNe, BqlIn, BqlNin, BqlExists, BqlAll, BqlSize, BqlElemMatch {
    String toJson();
}

// Value wrapper interface
public sealed interface BqlValue 
    permits StringVal, IntVal, DoubleVal, BooleanVal, ArrayVal, DocumentVal {
    String toJson();
}
```

### Expression Node Types

#### Logical Operators
- **BqlAnd**: `record BqlAnd(List<BqlExpr> children)`
  - Represents conjunction of multiple expressions
  - Maps to MongoDB `$and` operator
  - Supports arbitrary nesting depth

- **BqlOr**: `record BqlOr(List<BqlExpr> children)`
  - Represents disjunction of multiple expressions
  - Maps to MongoDB `$or` operator
  - Supports arbitrary nesting depth

- **BqlNot**: `record BqlNot(BqlExpr expr)`
  - Represents negation of single expression
  - Maps to MongoDB `$not` operator
  - Single child structure

#### Comparison Operators
- **BqlEq**: `record BqlEq(String field, BqlValue value)` - Equality (`field: value`)
- **BqlNe**: `record BqlNe(String field, BqlValue value)` - Inequality (`field: {$ne: value}`)
- **BqlGt**: `record BqlGt(String field, BqlValue value)` - Greater than (`field: {$gt: value}`)
- **BqlGte**: `record BqlGte(String field, BqlValue value)` - Greater than or equal (`field: {$gte: value}`)
- **BqlLt**: `record BqlLt(String field, BqlValue value)` - Less than (`field: {$lt: value}`)
- **BqlLte**: `record BqlLte(String field, BqlValue value)` - Less than or equal (`field: {$lte: value}`)

#### Array Operators
- **BqlIn**: `record BqlIn(String field, List<BqlValue> values)`
  - Value membership test (`field: {$in: [values]}`)
  - Supports heterogeneous value types

- **BqlNin**: `record BqlNin(String field, List<BqlValue> values)`
  - Value non-membership test (`field: {$nin: [values]}`)
  - Negation of `$in` operator

- **BqlAll**: `record BqlAll(String field, List<BqlValue> values)`
  - Array contains all values (`field: {$all: [values]}`)
  - Requires field to be array containing all specified values

#### Special Operators
- **BqlExists**: `record BqlExists(String field, boolean exists)`
  - Field existence check (`field: {$exists: true/false}`)
  - Boolean parameter controls existence vs non-existence

- **BqlSize**: `record BqlSize(String field, int size)`
  - Array size check (`field: {$size: number}`)
  - Requires field to be array of exact specified length

- **BqlElemMatch**: `record BqlElemMatch(String field, BqlExpr expr)`
  - Array element matching (`field: {$elemMatch: {expr}}`)
  - Supports nested expressions for array element filtering

### Value Node Types

#### Primitive Values
- **StringVal**: `record StringVal(String value)` - String literals with UTF-8 support
- **IntVal**: `record IntVal(int value)` - 32-bit signed integers
- **DoubleVal**: `record DoubleVal(double value)` - IEEE 754 double precision floats
- **BooleanVal**: `record BooleanVal(boolean value)` - Boolean true/false values

#### Complex Values
- **ArrayVal**: `record ArrayVal(List<BqlValue> values)`
  - Nested array structures
  - Supports heterogeneous element types
  - Recursive BqlValue containment

- **DocumentVal**: `record DocumentVal(Map<String, BqlValue> fields)`
  - Nested document structures
  - String field names mapped to BqlValue objects
  - Supports arbitrary nesting depth

## Parser Implementation

### Grammar Rules (Implicit)

The parser implements MongoDB's query grammar through recursive descent:

```
Document     ::= '{' (Field | Operator)* '}'
Operator     ::= LogicalOp | FieldOp
LogicalOp    ::= '$and' ':' '[' Document* ']' |
                 '$or' ':' '[' Document* ']' |
                 '$not' ':' Document
Field        ::= FieldName ':' (Value | FieldExpression)
FieldExpression ::= '{' FieldOperator+ '}'
FieldOperator   ::= ComparisonOp | ArrayOp | SpecialOp
ComparisonOp    ::= ('$gt' | '$gte' | '$lt' | '$lte' | '$eq' | '$ne') ':' Value
ArrayOp         ::= ('$in' | '$nin' | '$all') ':' '[' Value* ']'
SpecialOp       ::= '$exists' ':' Boolean |
                    '$size' ':' Integer |
                    '$elemMatch' ':' Document
Value           ::= String | Number | Boolean | Array | Document
```

### Operator Precedence and Associativity

MongoDB queries use structural precedence rather than traditional operator precedence:

1. **Document Level**: Top-level fields are implicitly AND-ed
2. **Logical Level**: `$and`, `$or`, `$not` operators
3. **Field Level**: Field-specific operators (`$gt`, `$eq`, etc.)
4. **Value Level**: Literal values and nested structures

### Type System

#### BSON to BqlValue Mapping
```java
private static BqlValue readValue(BsonReader reader) {
    return switch (reader.getCurrentBsonType()) {
        case STRING -> new StringVal(reader.readString());
        case INT32 -> new IntVal(reader.readInt32());
        case DOUBLE -> new DoubleVal(reader.readDouble());
        case BOOLEAN -> new BooleanVal(reader.readBoolean());
        case ARRAY -> new ArrayVal(readArray(reader));
        case DOCUMENT -> new DocumentVal(readDocument(reader));
        // Additional BSON types handled appropriately
    };
}
```

#### Type Validation
- **Strict Type Checking**: Each operator validates argument types
- **BSON Compatibility**: Full support for BSON type system
- **Error Context**: Detailed error messages with field/operator context

## Error Handling

### Exception Hierarchy

```java
public class BqlParseException extends KronotopException {
    public BqlParseException(String message)
    public BqlParseException(String message, Throwable cause)
}
```

### Error Categories

#### 1. Syntax Errors
- **Invalid JSON**: Malformed JSON structure
- **BSON Format Errors**: Invalid BSON document structure
- **Missing Values**: Required operator arguments missing

#### 2. Semantic Errors
- **Unknown Operators**: Unsupported operator names (`$xyz`)
- **Unknown Field Operators**: Invalid field-level operators
- **Type Mismatches**: Wrong argument types for operators

#### 3. Structural Errors
- **Empty Operator Documents**: Field operators with no content
- **Invalid Nesting**: Malformed nested structures
- **Array Structure**: Invalid array content for array operators

### Error Messages

**Examples of Generated Error Messages**:
```java
// Type validation errors
"$size expects an integer, but got: string"
"$exists expects a boolean, but got: integer"

// Structure validation errors  
"Empty field operator document for field: status"
"Unknown operator: $invalidOp"

// BSON processing errors
"Invalid BSON format: Unexpected character at position 15"
"BSON processing error: [detailed technical message]"
```

### Error Handling Strategy
1. **Early Detection**: Validate during parsing, not after
2. **Context Preservation**: Include field names and operator context
3. **Exception Chaining**: Preserve original BSON parsing exceptions
4. **User-Friendly Messages**: Clear descriptions of what went wrong

## Integration Points

### LogicalPlanner Integration

**Primary Integration**: BQL2 AST converts to LogicalPlanner input

```java
// In LogicalPlanner.java
public LogicalNode plan(BqlExpr root) {
    // Step 1: Convert BQL AST to LogicalNode tree
    LogicalNode logical = convert(root);
    
    // Step 2: Apply optimization pipeline
    for (LogicalTransform pass : pipeline) {
        logical = pass.transform(logical);
    }
    
    return logical;
}
```

#### AST to LogicalNode Conversion
```java
private LogicalNode convert(BqlExpr expr) {
    return switch (expr) {
        case BqlAnd(List<BqlExpr> children) -> 
            new LogicalAnd(convertList(children));
        case BqlOr(List<BqlExpr> children) -> 
            new LogicalOr(convertList(children));
        case BqlEq(String field, BqlValue value) -> 
            toFilter(field, Operator.EQ, value);
        // ... additional conversions
    };
}
```

### Query Handler Integration

**Current Status**: BQL2 system exists but integration with main query handlers appears to be in development.

**Test Integration**: Comprehensive test coverage shows full BQL2 → LogicalPlanner pipeline working.

### Explain Utility Integration

**Location**: `com.kronotop.bucket.bql2.Explain`

```java
public final class Explain {
    public static String explain(BqlExpr expr) {
        return explain(expr, 0);
    }
    
    private static String explain(BqlExpr expr, int depth) {
        // Recursive AST visualization
    }
}
```

**Usage**: Provides human-readable AST structure for debugging and analysis.

## Extension Points

### Adding New Operators

#### Step 1: Create AST Node
```java
public record BqlRegex(String field, String pattern, String options) 
    implements BqlExpr {
    
    @Override
    public String toJson() {
        return "{\"" + field + "\": {\"$regex\": \"" + pattern + 
               "\", \"$options\": \"" + options + "\"}}";
    }
}
```

#### Step 2: Update BqlExpr Interface
```java
public sealed interface BqlExpr 
    permits BqlAnd, BqlOr, BqlNot, BqlEq, BqlGt, BqlLt, BqlGte, BqlLte,
            BqlNe, BqlIn, BqlNin, BqlExists, BqlAll, BqlSize, BqlElemMatch,
            BqlRegex // Add new operator here
{
    String toJson();
}
```

#### Step 3: Add Parser Support
```java
private static BqlExpr parseFieldOperator(BsonReader reader, String op, String field) {
    return switch (op) {
        case "$eq" -> new BqlEq(field, readValue(reader));
        case "$ne" -> new BqlNe(field, readValue(reader));
        // ... existing cases
        case "$regex" -> {
            // Parse regex pattern and options
            String pattern = reader.readString();
            String options = reader.readString(); // or default
            yield new BqlRegex(field, pattern, options);
        }
        default -> throw new BqlParseException("Unknown field operator: " + op);
    };
}
```

#### Step 4: Update Explain Utility
```java
// Add case to Explain.explain() method
case BqlRegex regex -> {
    return pad + "BqlRegex(field=" + regex.field() + 
           ", pattern=" + regex.pattern() + 
           ", options=" + regex.options() + ")";
}
```

#### Step 5: Update LogicalPlanner
```java
// Add conversion in LogicalPlanner.convert()
case BqlRegex(String field, String pattern, String options) -> {
    // Convert to appropriate LogicalFilter with regex operator
    return toFilter(field, Operator.REGEX, 
                   Map.of("pattern", pattern, "options", options));
}
```

### Adding New Value Types

#### Step 1: Create Value Record
```java
public record DateVal(LocalDateTime value) implements BqlValue {
    @Override
    public String toJson() {
        return "{\"$date\": \"" + value.toString() + "\"}";
    }
}
```

#### Step 2: Update BqlValue Interface
```java
public sealed interface BqlValue 
    permits StringVal, IntVal, DoubleVal, BooleanVal, ArrayVal, DocumentVal,
            DateVal // Add new value type here
{
    String toJson();
}
```

#### Step 3: Add Parser Support
```java
private static BqlValue readValue(BsonReader reader) {
    return switch (reader.getCurrentBsonType()) {
        case STRING -> new StringVal(reader.readString());
        case INT32 -> new IntVal(reader.readInt32());
        // ... existing cases
        case DATE_TIME -> new DateVal(
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(reader.readDateTime()), 
                ZoneOffset.UTC));
        default -> throw new BqlParseException(
            "Unsupported BSON type: " + reader.getCurrentBsonType());
    };
}
```

### Extension Patterns

1. **Sealed Interfaces**: Ensure compile-time completeness for new additions
2. **Record Classes**: Immutable data structures with built-in methods
3. **Switch Expressions**: Exhaustive pattern matching with compiler verification
4. **Factory Methods**: Static creation methods for complex value types
5. **Visitor Pattern**: Explain utility demonstrates traversal patterns

## Performance Characteristics

### Parsing Performance

#### Algorithmic Complexity
- **Time Complexity**: O(n) where n is the input size
- **Space Complexity**: O(d) where d is the nesting depth
- **Memory Usage**: Streaming parser uses constant memory for large documents

#### Performance Optimizations
- **Streaming Processing**: BsonReader processes documents without full materialization
- **Single Pass**: No backtracking or multiple parsing passes required
- **Lazy Evaluation**: Values are parsed on-demand during AST construction
- **Direct Construction**: AST nodes created directly without intermediate structures

### Memory Usage

#### AST Memory Characteristics
- **Proportional Size**: AST memory usage proportional to query complexity
- **Immutable Structures**: No memory leaks from mutable references
- **Primitive Storage**: Numbers and booleans stored as primitives in records
- **Collection Wrapping**: Lists and Maps use standard Java collections

#### Garbage Collection Impact
- **Short-lived Objects**: Parser creates temporary objects during parsing
- **Long-lived AST**: AST typically lives for query execution duration
- **No Caching**: No parser-level caching of intermediate results

### Concurrency

#### Thread Safety
- **Stateless Parser**: All parsing methods are static with no shared state
- **Immutable AST**: Record-based AST is immutable after construction
- **Concurrent Parsing**: Multiple threads can parse queries simultaneously
- **Reader Isolation**: Each parse operation uses its own BsonReader

#### Scalability
- **Linear Scaling**: Parsing performance scales linearly with input size
- **Memory Bounded**: Memory usage bounded by query complexity, not data size
- **CPU Intensive**: Parsing is CPU-bound, not I/O bound

## Testing Strategy

### Test Infrastructure

#### BqlTestDataBuilder
**Location**: `com.kronotop.bucket.bql2.BqlTestDataBuilder`

Fluent API for constructing complex test queries:
```java
BqlTestDataBuilder.query()
    .field("status").eq("active")
    .and()
    .field("score").gt(80)
    .or()
    .field("premium").exists(true)
    .build();
```

#### BqlQueryGenerator
**Location**: `com.kronotop.bucket.bql2.BqlQueryGenerator`

Random query generation for property-based testing:
```java
String randomQuery = BqlQueryGenerator.generateRandomQuery(
    maxDepth, maxFields, operators);
```

### Test Categories

#### 1. Unit Tests (`BqlParserTest`)
- **Single Operator Tests**: Each operator in isolation
- **Type Validation**: All value types with all compatible operators
- **Error Conditions**: Invalid syntax and semantic errors
- **Edge Cases**: Empty documents, null values, extreme nesting

#### 2. Complex Integration Tests
- **Deep Nesting**: Heavily nested logical operators
- **Mixed Operators**: Combinations of different operator types
- **Large Queries**: Performance with complex query structures
- **Real-World Patterns**: Common MongoDB query patterns

#### 3. Concurrency Tests (`BqlParserConcurrencyTest`)
- **Thread Safety**: Parallel parsing from multiple threads
- **Stress Testing**: High-volume concurrent parsing
- **Memory Consistency**: Verify immutable AST behavior

#### 4. Property-Based Tests (`BqlParserPropertyBasedTest`)
- **Round-Trip**: Parse → toJson() → parse consistency
- **Random Generation**: Automated test case generation
- **Invariant Checking**: AST structure invariants maintained

#### 5. Performance Tests (`BqlParserPerformanceTest`)
- **Parsing Speed**: Throughput measurements
- **Memory Usage**: Heap allocation patterns
- **Scalability**: Performance with increasing query complexity

#### 6. Error Message Tests (`BqlParserErrorMessageTest`)
- **Message Quality**: Clear, actionable error messages
- **Context Information**: Field names and operators in error messages
- **Exception Chaining**: Proper cause preservation

### Test Data Patterns

#### Systematic Coverage
```java
@ParameterizedTest
@ValueSource(strings = {"$gt", "$gte", "$lt", "$lte", "$eq", "$ne"})
void testComparisonOperators(String operator) {
    // Test each comparison operator systematically
}
```

#### Edge Case Generation
```java
@Test
void testEdgeCases() {
    // Empty documents: "{}"
    // Single fields: "{\"field\": \"value\"}"
    // Maximum nesting: deep recursive structures
    // Type boundaries: min/max values for numeric types
}
```

#### Error Condition Coverage
```java
@Test
void testErrorConditions() {
    // Malformed JSON
    // Unknown operators
    // Type mismatches
    // Structural errors
}
```

## Future Development Guidelines

### Adding New MongoDB Operators

#### Research Phase
1. **MongoDB Documentation**: Study official MongoDB operator documentation
2. **Semantic Analysis**: Understand operator behavior and edge cases
3. **Type Requirements**: Identify required argument types and validation rules
4. **Integration Impact**: Assess impact on LogicalPlanner and execution engine

#### Implementation Phase
1. **AST Node Creation**: Follow existing record pattern
2. **Parser Integration**: Add parsing logic with proper validation
3. **Error Handling**: Implement appropriate error messages
4. **Test Coverage**: Create comprehensive test suite
5. **Documentation Update**: Update this architecture document

### Performance Optimization Opportunities

#### Parser Optimizations
1. **Operator Dispatch**: Replace string matching with more efficient dispatch
2. **Memory Pooling**: Reuse objects for high-frequency parsing scenarios
3. **Streaming Improvements**: Further optimize BsonReader usage
4. **Validation Caching**: Cache validation results for repeated patterns

#### AST Optimizations
1. **Intern Strings**: String interning for common field names
2. **Compact Representations**: More memory-efficient node representations
3. **Lazy Loading**: Defer expensive operations until needed
4. **Structure Sharing**: Share common sub-expressions

### Extension Architecture

#### Plugin System Design
```java
public interface BqlOperatorPlugin {
    String getOperatorName();
    BqlExpr parseOperator(BsonReader reader, String field);
    void validateOperator(BqlExpr expr) throws BqlParseException;
}

public final class BqlParserWithPlugins {
    private final List<BqlOperatorPlugin> plugins;
    
    public BqlExpr parseWithPlugins(String query) {
        // Extended parsing with plugin support
    }
}
```

#### Custom Value Type System
```java
public interface BqlValuePlugin {
    Set<BsonType> getSupportedBsonTypes();
    BqlValue parseValue(BsonReader reader);
    boolean canHandle(BsonType type);
}
```

### Maintenance Guidelines

#### Code Quality Standards
1. **Immutability**: All AST nodes must be immutable
2. **Type Safety**: Leverage sealed interfaces and exhaustive pattern matching
3. **Error Handling**: Provide clear, actionable error messages
4. **Test Coverage**: Maintain >95% line coverage with meaningful tests
5. **Documentation**: Keep JavaDoc updated for all public methods

#### Debugging and Monitoring
1. **Logging Integration**: Add structured logging for parsing decisions
2. **Metrics Collection**: Track parsing performance and error rates
3. **Profiling Support**: Instrument performance-critical parsing paths
4. **Debug Utilities**: Extend explain() functionality for debugging

#### Version Compatibility
1. **Backward Compatibility**: New operators should not break existing queries
2. **Migration Support**: Provide migration tools for query format changes
3. **Feature Toggles**: Use feature flags for experimental operators
4. **Documentation**: Maintain compatibility matrix for different versions

## Conclusion

The BQL2 parser represents a sophisticated, production-ready query parsing system that successfully combines MongoDB compatibility with strong type safety and excellent performance characteristics. Its streaming architecture, comprehensive error handling, and extensible design make it well-suited for complex query processing requirements.

The clean separation between parsing logic and AST representation, combined with modern Java features like sealed interfaces and records, creates a maintainable and type-safe foundation that can evolve with changing requirements while preserving correctness and performance.