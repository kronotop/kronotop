/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.index;

import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompoundIndexNameGeneratorTest {

    @Test
    void shouldGenerateNameFromTwoFields() {
        // Behavior: Generated name follows the pattern compound:selector.TYPE_selector.TYPE.
        List<CompoundIndexField> fields = List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        );
        String name = CompoundIndexNameGenerator.generate(fields);
        assertEquals("compound:category.STRING_price.DOUBLE", name);
    }

    @Test
    void shouldGenerateNameFromThreeFields() {
        // Behavior: Generated name joins all fields with underscores.
        List<CompoundIndexField> fields = List.of(
                new CompoundIndexField("a", BsonType.STRING, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.BOOLEAN, false)
        );
        String name = CompoundIndexNameGenerator.generate(fields);
        assertEquals("compound:a.STRING_b.INT32_c.BOOLEAN", name);
    }
}
