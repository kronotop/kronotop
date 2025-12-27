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

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.BucketMetadata;

import java.util.concurrent.atomic.AtomicInteger;

public class PlannerContext {
    // Note: AtomicInteger uses a 32-bit signed int under the hood.
    // When incrementing past Integer.MAX_VALUE (2_147_483_647),
    // the value wraps around to Integer.MIN_VALUE (-2_147_483_648)
    // and continues from there. No exception is thrown — standard
    // Java int overflow semantics apply.
    private final AtomicInteger nextId = new AtomicInteger(1);
    private BucketMetadata metadata;

    public PlannerContext() {
    }

    public PlannerContext(BucketMetadata metadata) {
        this.metadata = metadata;
    }

    public BucketMetadata getMetadata() {
        return metadata;
    }

    public int nextId() {
        return nextId.getAndIncrement();
    }
}