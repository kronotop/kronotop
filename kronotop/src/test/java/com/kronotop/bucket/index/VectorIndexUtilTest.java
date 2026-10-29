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

import com.kronotop.bucket.VectorFieldValidationException;
import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VectorIndexUtilTest {

    private static VectorIndexDefinition definition(DistanceFunction distance) {
        return VectorIndexDefinition.create("test", "embedding", 3, distance, IndexStatus.READY);
    }

    private static BsonArray vector(double... values) {
        BsonArray array = new BsonArray();
        for (double v : values) {
            array.add(new BsonDouble(v));
        }
        return array;
    }

    @Test
    void shouldRejectNonFiniteComponent() {
        // Behavior: NaN, Infinity and doubles that overflow float range are rejected for every distance function.
        for (DistanceFunction distance : DistanceFunction.values()) {
            VectorIndexDefinition def = definition(distance);
            assertThrows(VectorFieldValidationException.class, () -> VectorIndexUtil.validateVectorField(def, vector(Double.NaN, 0.0, 0.0)));
            assertThrows(VectorFieldValidationException.class, () -> VectorIndexUtil.validateVectorField(def, vector(0.0, Double.POSITIVE_INFINITY, 0.0)));
            assertThrows(VectorFieldValidationException.class, () -> VectorIndexUtil.validateVectorField(def, vector(0.0, 0.0, 1e300)));
        }
    }

    @Test
    void shouldApplyZeroVectorRuleByDistance() {
        // Behavior: The zero vector is rejected for COSINE and accepted for EUCLIDEAN.
        BsonArray zero = vector(0.0, 0.0, 0.0);
        assertThrows(VectorFieldValidationException.class, () -> VectorIndexUtil.validateVectorField(definition(DistanceFunction.COSINE), zero));
        assertDoesNotThrow(() -> VectorIndexUtil.validateVectorField(definition(DistanceFunction.EUCLIDEAN), zero));
    }

    @Test
    void shouldRequireUnitLengthForDotProduct() {
        // Behavior: DOT_PRODUCT accepts only unit-length vectors; longer vectors and the zero vector are rejected.
        VectorIndexDefinition def = definition(DistanceFunction.DOT_PRODUCT);
        assertDoesNotThrow(() -> VectorIndexUtil.validateVectorField(def, vector(0.6, 0.8, 0.0)));
        assertThrows(VectorFieldValidationException.class, () -> VectorIndexUtil.validateVectorField(def, vector(3.0, 4.0, 0.0)));
        assertThrows(VectorFieldValidationException.class, () -> VectorIndexUtil.validateVectorField(def, vector(0.0, 0.0, 0.0)));
    }
}
