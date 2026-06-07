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

package com.kronotop.bucket;

import com.kronotop.KronotopException;

/**
 * Thrown when a document's vector field fails validation against its vector index definition.
 */
public class VectorFieldValidationException extends KronotopException {

    public VectorFieldValidationException(String message) {
        super(message);
    }

    public static VectorFieldValidationException notAnArray(String field) {
        return new VectorFieldValidationException("Vector field '" + field + "' must be an array");
    }

    public static VectorFieldValidationException wrongDimensions(String field, int expected, int actual) {
        return new VectorFieldValidationException(
                "Vector field '" + field + "' requires " + expected + " dimensions but got " + actual);
    }

    public static VectorFieldValidationException elementsNotDouble(String field) {
        return new VectorFieldValidationException("Vector field '" + field + "' must contain only double elements");
    }
}
