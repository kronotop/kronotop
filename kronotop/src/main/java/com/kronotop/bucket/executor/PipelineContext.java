/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.executor;

import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;

import java.util.concurrent.ConcurrentHashMap;

public class PipelineContext {
    private final Context context;
    private final BucketMetadata metadata;
    private final ConcurrentHashMap<Integer, CursorState> cursor = new ConcurrentHashMap<>();

    public PipelineContext(Context context, BucketMetadata metadata) {
        this.context = context;
        this.metadata = metadata;
    }

    public void setCursor(int nodeId, CursorState state) {
        cursor.put(nodeId, state);
    }

    public CursorState getCursor(int nodeId) {
        return cursor.get(nodeId);
    }

    public Context context() {
        return context;
    }

    public BucketMetadata getMetadata() {
        return metadata;
    }
}

