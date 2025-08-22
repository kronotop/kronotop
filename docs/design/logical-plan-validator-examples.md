# LogicalPlanValidator Examples

This document demonstrates the LogicalPlanValidator capabilities with practical examples.

## Basic Usage

```java
LogicalPlanner planner = new LogicalPlanner();
LogicalPlanValidator validator = new LogicalPlanValidator();

// Method 1: Validate after planning
BqlExpr expr = BqlParser.parse("{ \"status\": \"active\" }");
LogicalNode plan = planner.plan(expr);
ValidationResult result = planner.validate(plan);

if (result.isValid()) {
    System.out.println("Plan is valid!");
} else {
    System.out.println("Plan has issues:");
    for (ValidationIssue issue : result.getIssues()) {
        System.out.println("  " + issue);
    }
}

// Method 2: Plan and validate in one step (throws exception if invalid)
try {
    LogicalNode validatedPlan = planner.planAndValidate(expr);
    // Use validatedPlan...
} catch (LogicalPlanValidationException e) {
    System.err.println("Invalid plan: " + e.getMessage());
    // Handle validation errors...
}
```

## Contradictory Conditions Detection

### Example 1: Field Equality Contradiction
```java
// BQL: { "$and": [{ "status": "active" }, { "status": "inactive" }] }
// Result: ERROR - Contradictory conditions: status EQ "active" AND status EQ "inactive"
```

### Example 2: Numeric Range Contradiction
```java
// BQL: { "$and": [{ "price": { "$gt": 100 } }, { "price": { "$lt": 50 } }] }
// Result: ERROR - Contradictory conditions: price GT 100 AND price LT 50
```

### Example 3: Equality vs Not-Equal Contradiction
```java
// BQL: { "$and": [{ "category": "electronics" }, { "category": { "$ne": "electronics" } }] }
// Result: ERROR - Contradictory conditions: category EQ "electronics" AND category NE "electronics"
```

## Invalid Operator Combinations

### Example 1: Wrong Operand Type for IN
```java
// Invalid: IN operator with non-list operand
LogicalNode plan = new LogicalFilter("field", Operator.IN, new StringVal("value"));
// Result: ERROR - IN operator requires list operand
```

### Example 2: Wrong Operand Type for SIZE
```java
// Invalid: SIZE operator with string operand
LogicalNode plan = new LogicalFilter("items", Operator.SIZE, new StringVal("not_a_number"));
// Result: ERROR - SIZE operator requires integer operand
```

### Example 3: Negative SIZE Value
```java
// Invalid: SIZE operator with negative value
LogicalNode plan = new LogicalFilter("items", Operator.SIZE, new IntVal(-5));
// Result: ERROR - SIZE operator requires non-negative integer
```

## Malformed Plan Structures

### Example 1: Null Field
```java
// Invalid: Filter with null field name
LogicalNode plan = new LogicalFilter(null, Operator.EQ, new StringVal("value"));
// Result: ERROR - LogicalFilter has null field
```

### Example 2: Null Children in AND
```java
// Invalid: AND with null children list
LogicalNode plan = new LogicalAnd(null);
// Result: ERROR - LogicalAnd has null children list
```

### Example 3: Null Child in NOT
```java
// Invalid: NOT with null child
LogicalNode plan = new LogicalNot(null);
// Result: ERROR - LogicalNot has null child
```

## Warnings (Valid but Suboptimal)

### Example 1: Empty List in IN Operator
```java
// Valid but suboptimal: IN operator with empty list
LogicalNode plan = new LogicalFilter("field", Operator.IN, Arrays.asList());
// Result: WARNING - IN operator with empty list
```

### Example 2: Comparison with List
```java
// Valid but unusual: GT operator with list operand
LogicalNode plan = new LogicalFilter("field", Operator.GT, Arrays.asList(1, 2, 3));
// Result: WARNING - GT operator with list operand may not behave as expected
```

### Example 3: Double Negation
```java
// Valid but suboptimal: NOT(NOT(...))
LogicalNode plan = new LogicalNot(new LogicalNot(filter));
// Result: WARNING - Double negation detected (NOT(NOT(...)))
```

### Example 4: Empty Collections
```java
// Valid but potentially unintended: Empty AND (always true)
LogicalNode plan = new LogicalAnd(Arrays.asList());
// Result: WARNING - Empty AND condition (always true)

// Valid but potentially unintended: Empty OR (always false)
LogicalNode plan = new LogicalOr(Arrays.asList());
// Result: WARNING - Empty OR condition (always false)
```

## Complex Validation Scenarios

### Example 1: Valid Complex Query
```java
BqlExpr expr = BqlParser.parse("""
    {
      "$and": [
        { "tenant_id": "123" },
        {
          "$or": [
            { "role": "admin" },
            {
              "$and": [
                { "permissions": { "$in": ["read", "write"] } },
                { "active": true }
              ]
            }
          ]
        },
        { "created_at": { "$gte": "2023-01-01" } }
      ]
    }
    """);

LogicalNode plan = planner.plan(expr);
ValidationResult result = planner.validate(plan);
// Result: VALID (no issues)
```

### Example 2: Mixed Valid and Invalid Conditions
```java
BqlExpr expr = BqlParser.parse("""
    {
      "$and": [
        { "status": "active" },           // Valid
        { "status": "deleted" },          // Contradiction with above
        { "items": { "$size": -1 } },     // Invalid negative size
        { "tags": { "$in": [] } }         // Warning: empty list
      ]
    }
    """);

LogicalNode plan = planner.plan(expr);
ValidationResult result = planner.validate(plan);
// Result: INVALID 
// Issues:
//   ERROR: Contradictory conditions: status EQ "active" AND status EQ "deleted"
//   ERROR: SIZE operator requires non-negative integer
//   WARNING: IN operator with empty list
```

## Integration with Planning Pipeline

The validator integrates seamlessly with the logical planning pipeline:

```java
// The validator is automatically integrated into LogicalPlanner
LogicalPlanner planner = new LogicalPlanner();

// Standard planning (no validation)
LogicalNode plan = planner.plan(expr);

// Validation after planning
ValidationResult result = planner.validate(plan);

// Planning with validation (throws exception if invalid)
LogicalNode validatedPlan = planner.planAndValidate(expr);

// Convenience methods
boolean isWellFormed = planner.isWellFormed(plan);
List<ValidationIssue> contradictions = planner.findContradictions(plan);
```

## Best Practices

1. **Use `planAndValidate()` in production** to catch invalid plans early
2. **Handle `LogicalPlanValidationException`** gracefully in user-facing code
3. **Log validation warnings** to help identify suboptimal queries
4. **Use `validate()` method** for detailed analysis and debugging
5. **Check `result.hasErrors()`** vs `result.hasWarnings()` for different handling
6. **Use convenience methods** like `findContradictions()` for specific checks

## Performance Notes

- Validation is lightweight and designed for production use
- The validator uses a single-pass tree traversal
- Contradiction detection is O(n²) per field within AND contexts
- Memory usage is minimal - only collects issues found
- No performance impact on the planning pipeline itself