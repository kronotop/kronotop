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

import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.volume.EntryMetadata;
import org.bson.types.ObjectId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents document location information for retrieval from storage.
 */
public class DocumentLocation implements DocumentRef {
    private final ObjectId objectId;
    private final int shardId;
    private final EntryMetadata entryMetadata;
    private BqlValue cursorIndexValue;

    public DocumentLocation(
            @Nonnull ObjectId objectId,
            int shardId,
            @Nonnull EntryMetadata entryMetadata
    ) {
        this.objectId = objectId;
        this.shardId = shardId;
        this.entryMetadata = entryMetadata;

        Objects.requireNonNull(objectId);
        Objects.requireNonNull(entryMetadata);
    }

    @Override
    public ObjectId objectId() {
        return objectId;
    }

    @Override
    public int shardId() {
        return shardId;
    }

    @Override
    public EntryMetadata entryMetadata() {
        return entryMetadata;
    }

    @Override
    public BqlValue cursorIndexValue() {
        return cursorIndexValue;
    }

    public void setCursorIndexValue(BqlValue cursorIndexValue) {
        this.cursorIndexValue = cursorIndexValue;
    }
}
