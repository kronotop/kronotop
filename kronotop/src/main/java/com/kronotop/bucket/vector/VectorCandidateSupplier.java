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

package com.kronotop.bucket.vector;

import com.kronotop.bucket.pipeline.CandidateSupplier;
import com.kronotop.bucket.pipeline.DocumentLocation;
import org.bson.types.ObjectId;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Supplies vector search candidates using resumable graph search.
 * Each call to {@link #fetch()} resumes the search from where the previous call left off,
 * returning only new (unseen) candidates and populating a score map.
 */
public class VectorCandidateSupplier implements CandidateSupplier, Closeable {
    private final VectorSearchSession session;
    private final Map<ObjectId, Float> scoreMap;
    private final Set<ObjectId> seen = new HashSet<>();
    private final int batchSize;
    private final float threshold;
    private final float overquery;
    private final int maxScanCandidates;
    private boolean exhausted;

    public VectorCandidateSupplier(VectorSearchSession session, int batchSize, float threshold,
                                   Map<ObjectId, Float> scoreMap, int maxScanCandidates, float overquery) {
        this.session = session;
        this.batchSize = batchSize;
        this.threshold = threshold;
        this.overquery = overquery;
        this.scoreMap = scoreMap;
        this.maxScanCandidates = maxScanCandidates;
    }

    @Override
    public List<DocumentLocation> fetch() {
        if (exhausted) {
            return List.of();
        }

        List<MergedNodeScore> results = session.search(batchSize, threshold, overquery);

        List<DocumentLocation> newCandidates = new ArrayList<>();
        for (MergedNodeScore merged : results) {
            ObjectId objectId = merged.location().objectId();
            if (seen.add(objectId)) {
                newCandidates.add(merged.location());
                scoreMap.put(objectId, merged.score());
            }
        }

        if (results.size() < batchSize || seen.size() >= maxScanCandidates) {
            exhausted = true;
        }

        return newCandidates;
    }

    @Override
    public void close() throws IOException {
        session.close();
    }
}
