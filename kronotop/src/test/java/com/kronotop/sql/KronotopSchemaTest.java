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

package com.kronotop.sql;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for {@link KronotopSchema} class.
 *
 */
public class KronotopSchemaTest {

    /**
     * Test scenario where the method is called with valid parameters.
     * setSubSchema should add the provided schema to the map of subSchemas and no exceptions should be thrown.
     */
    @Test
    void test_KronotopSchema() {
        ConcurrentHashMap<String, Table> tableMap = new ConcurrentHashMap<>();
        KronotopSchema schema = new KronotopSchema("TestSchema", tableMap);
        assertEquals("TestSchema", schema.getName());
        assertNotNull(schema.getTableMap());
    }
}