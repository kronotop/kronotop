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

/**
 * Constants and utilities for the primary index on the "_id" field.
 */
public final class PrimaryIndex {
    public static final String NAME = "primary-index";
    public static final String SELECTOR = "_id";
    public static final BsonType BSON_TYPE = BsonType.OBJECT_ID;

    private PrimaryIndex() {
    }

    /**
     * Checks if the given index definition represents the primary index.
     */
    public static boolean isPrimary(SingleFieldIndexDefinition definition) {
        return NAME.equals(definition.name()) && SELECTOR.equals(definition.selector());
    }

    /**
     * Creates a new primary index definition with READY status.
     */
    public static SingleFieldIndexDefinition createDefinition() {
        return SingleFieldIndexDefinition.create(NAME, SELECTOR, BSON_TYPE, false, IndexStatus.READY);
    }
}
