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

import java.nio.ByteBuffer;

/**
 * View over a document that includes the _id field and content.
 * <p>
 * This class is mutable to allow reuse within a single transform operation, minimizing GC pressure.
 */
public class DocumentView {
    private ObjectId id;
    private ByteBuffer content;

    /**
     * Resets the view for a new document.
     *
     * @param id      the document's ObjectId (_id field)
     * @param content the physical document content
     */
    public void reset(ObjectId id, ByteBuffer content) {
        this.id = id;
        this.content = content;
    }

    /**
     * Returns the document's ObjectId (_id field).
     */
    public ObjectId getId() {
        return id;
    }

    /**
     * Returns the physical document content.
     */
    public ByteBuffer getContent() {
        return content;
    }
}
