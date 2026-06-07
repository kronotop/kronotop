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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VectorIndexNameGeneratorTest {

    @Test
    void shouldGenerateExpectedFormat() {
        // Behavior: Generated name follows the pattern vector:selector.dimensions:N.distance:FUNC.
        String name = VectorIndexNameGenerator.generate("embedding", 1536, DistanceFunction.COSINE);
        assertEquals("vector:embedding.dimensions:1536.distance:COSINE", name);
    }

    @Test
    void shouldGenerateForDotProduct() {
        // Behavior: DOT_PRODUCT distance function is included in the generated name.
        String name = VectorIndexNameGenerator.generate("vec_field", 768, DistanceFunction.DOT_PRODUCT);
        assertEquals("vector:vec_field.dimensions:768.distance:DOT_PRODUCT", name);
    }
}
