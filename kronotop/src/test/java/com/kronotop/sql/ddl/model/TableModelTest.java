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

package com.kronotop.sql.ddl.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TableModelTest {
    @Test
    public void encode_then_decode() throws JsonProcessingException {
        TableModel tableModel = new TableModel();
        tableModel.setOperator(SqlKind.CREATE_TABLE);
        tableModel.setSchema("public");
        tableModel.setTable("users");

        ColumnModel columnModel = new ColumnModel();
        columnModel.setName("id");
        columnModel.setDataType(SqlTypeName.INTEGER);
        columnModel.setStrategy(ColumnStrategy.NULLABLE);
        List<ColumnModel> columnList = new ArrayList<>(List.of(columnModel));
        tableModel.setColumnList(columnList);

        ObjectMapper objectMapper = new ObjectMapper();
        String encoded = objectMapper.writeValueAsString(tableModel);
        TableModel decoded = objectMapper.readValue(encoded, TableModel.class);
        assertThat(tableModel).usingRecursiveComparison().isEqualTo(decoded);
    }
}