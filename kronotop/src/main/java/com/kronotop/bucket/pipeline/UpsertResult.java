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

package com.kronotop.bucket.pipeline;

import org.bson.types.ObjectId;

/**
 * Holds the result of an upsert operation.
 */
public class UpsertResult {
    private final ObjectId objectId;

    public UpsertResult(ObjectId objectId) {
        this.objectId = objectId;
    }

    /**
     * Returns the ObjectId of the upserted document.
     */
    public ObjectId getObjectId() {
        return objectId;
    }
}
