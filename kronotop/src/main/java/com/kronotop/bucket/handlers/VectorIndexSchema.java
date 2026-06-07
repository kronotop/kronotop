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

package com.kronotop.bucket.handlers;

import com.kronotop.KronotopException;
import com.kronotop.bucket.index.DistanceFunction;
import tools.jackson.databind.annotation.JsonDeserialize;

/**
 * Jackson POJO representing a vector index entry from the {@code $vector} directive.
 */
class VectorIndexSchema {
    private String name;
    private String field;
    private int dimensions;

    @JsonDeserialize(using = DistanceFunctionDeserializer.class)
    private DistanceFunction distance;

    VectorIndexSchema() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public int getDimensions() {
        return dimensions;
    }

    public void setDimensions(int dimensions) {
        this.dimensions = dimensions;
    }

    public DistanceFunction getDistance() {
        return distance;
    }

    public void setDistance(DistanceFunction distance) {
        this.distance = distance;
    }

    public void validate() {
        if (field == null || field.isEmpty()) {
            throw new KronotopException("Vector index 'field' cannot be null or empty");
        }
        if (dimensions < 1) {
            throw new KronotopException("Vector index 'dimensions' must be >= 1, got " + dimensions);
        }
        if (distance == null) {
            throw new KronotopException("Vector index 'distance' cannot be null");
        }
    }
}
