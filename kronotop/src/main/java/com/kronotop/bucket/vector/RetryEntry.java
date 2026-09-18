/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.vector;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.UUID;

/**
 * A vector node that could not be added to the on-heap graph and is kept for a later retry.
 *
 * @param versionstamp    the versionstamp of the add operation
 * @param collectedVector the vector and document metadata to add again
 */
public record RetryEntry(
        String namespace,
        String bucket,
        UUID bucketUuid,
        Versionstamp versionstamp,
        CollectedVector collectedVector) {
}
