/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.sql.optimizer;

import com.kronotop.sql.KronotopSchema;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.Parser;
import com.kronotop.sql.ddl.model.ColumnModel;
import com.kronotop.sql.ddl.model.TableModel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OptimizerUnitTest {
    private static Optimizer optimizer;

    @BeforeAll
    static void setUp() {
        TableModel tableModel = new TableModel();
        tableModel.setSchema("myschema");
        tableModel.setTable("mytable");
        ColumnModel columnModel = new ColumnModel();
        columnModel.setName("myfield");
        columnModel.setDataType(SqlTypeName.DECIMAL);
        tableModel.setColumnList(List.of(columnModel));

        KronotopSchema schema = new KronotopSchema("myschema");
        schema.getTableMap().put(tableModel.getTable(), new KronotopTable(tableModel, new byte[]{0}));
        optimizer = new Optimizer(schema);
    }

    @Test
    void validate_ValidNode_NoExceptionThrown() {
        try {
            SqlNode node = Parser.parse("SELECT * FROM myschema.mytable");
            SqlNode result = optimizer.validate(node);
            assertNotNull(result, "The method has returned a null value");
            assertEquals(node, result, "The validated node differs from the original node.");
        } catch (Exception e) {
            fail("This method should not throw any exceptions when provided with a valid SqlNode.", e);
        }
    }

    @Test
    void validate_InvalidNode_ExceptionThrown() {
        String invalidQuery = "Invalid Query";
        assertThrows(Exception.class, () -> {
            SqlNode invalidNode = Parser.parse(invalidQuery);
            optimizer.validate(invalidNode);
        }, "Expected to throw an exception when provided with an invalid SqlNode but it didn't.");
    }
}