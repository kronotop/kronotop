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

package com.kronotop.bucket.pipeline;

public abstract class AbstractPipelineNode {
    private final int id;
    private volatile PipelineNode next;

    public AbstractPipelineNode(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public PipelineNode next() {
        return next;
    }

    public void connectNext(PipelineNode node) {
        if (next != null) {
            throw new IllegalStateException("next node has already been set for id=" + id);
        }
        next = node;
    }
}
