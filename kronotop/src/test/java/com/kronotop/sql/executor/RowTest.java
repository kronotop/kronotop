package com.kronotop.sql.executor;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The RowTest class tests the functionality of the put method in the Row class.
 */
public class RowTest {

    /**
     * This test method checks whether a new field-value pair is correctly added to a Row.
     */
    @Test
    public void testPut() {
        // Setup
        Row<RexLiteral> testRow = new Row<>();
        String testField = "testField";
        Integer testIndex = 0;
        SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT);

        RexLiteral testRex = RexLiteral.fromJdbcString(sqlTypeFactory.createSqlType(SqlTypeName.CHAR), SqlTypeName.CHAR, "testRexValue");

        // Act
        RexLiteral returnedRex = testRow.put(testField, testIndex, testRex);

        // Assert
        // Check whether the return value of the put method matches the input RexLiteral.
        assertEquals(testRex, returnedRex,
                "The returned RexLiteral from the put method does not match the input RexLiteral.");

        // Check whether the new field-value pair has been added to the Row.
        RexLiteral resultRex = testRow.get(testField);
        assertNotNull(resultRex,
                "The get method returns null for the new field, indicating that the field-value pair was not added.");

        // Check whether the value of the new field matches the input RexLiteral.
        assertEquals(testRex, resultRex,
                "The RexLiteral obtained from the Row does not match the input RexLiteral.");
    }

    @Test
    public void testHasFieldWhenFieldExists() {
        //Setup
        String field = "existingField";
        Row<RexLiteral> row = new Row<>();
        row.put(field, 1, null);

        // Execute & Verify
        assertTrue(row.hasField(field), "Expected hasField to return true when field exists");
    }

    @Test
    public void testHasFieldWhenFieldDoesNotExist() {
        //Setup
        String field = "nonExistingField";
        Row<RexLiteral> row = new Row<>();

        // Execute & Verify
        assertFalse(row.hasField(field), "Expected hasField to return false when field does not exist");
    }

    /**
     * This test validates the remove method in the Row class.
     */
    @Test
    void testRemove() {
        // Initialize a Row and a RexLiteral using arbitrary values
        Row<RexLiteral> row = new Row<>();
        SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT);
        RexLiteral rexLiteral = RexLiteral.fromJdbcString(sqlTypeFactory.createSqlType(SqlTypeName.CHAR), SqlTypeName.CHAR, "testRexValue");

        // Add a row using the put method
        String testField = "TestField";
        row.put(testField, 0, rexLiteral);

        // Confirm that the row has been added
        assertTrue(row.hasField(testField));

        // Remove the row we just created
        RexLiteral removedItem = row.remove(testField);

        // Validate that the items removed were the same ones added
        assertEquals(removedItem, rexLiteral);

        // Validate that the row no longer exists
        assertFalse(row.hasField(testField));
    }

    /**
     * Test the getByIndex method when the requested index is associated with a field.
     */
    @Test
    public void testGetByIndexWithValidIndex() {
        // Arrange
        Row<RexLiteral> row = new Row<>();
        Integer index = 1;
        String field = "testField";

        SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT);
        RexLiteral rexLiteral = RexLiteral.fromJdbcString(sqlTypeFactory.createSqlType(SqlTypeName.CHAR), SqlTypeName.CHAR, "testRexValue");

        row.put(field, index, rexLiteral);

        // Act
        RexLiteral result = row.getByIndex(index);

        // Assert
        assertEquals(rexLiteral, result);
    }

    /**
     * Test the getByIndex method when the requested index is not associated with a field.
     */
    @Test
    public void testGetByIndexWithInvalidIndex() {
        // Arrange
        Row<RexLiteral> row = new Row<>();
        Integer validIndex = 1;
        Integer invalidIndex = 2;
        String field = "testField";

        SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT);
        RexLiteral rexLiteral = RexLiteral.fromJdbcString(sqlTypeFactory.createSqlType(SqlTypeName.CHAR), SqlTypeName.CHAR, "testRexValue");

        row.put(field, validIndex, rexLiteral);

        // Act
        RexLiteral result = row.getByIndex(invalidIndex);

        // Assert
        assertNull(result);
    }
}