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
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mutable, per-node state that a pipeline node carries across paginated executions.
 * <p>
 * Every node in the query pipeline owns one instance, stored inside the cursor so it
 * survives between ADVANCE calls. The state captures where a scan left off (bounds and
 * selectors), how many results the node may still produce (limit), whether the
 * underlying data source is fully consumed (exhausted flag), and which documents have
 * already been emitted (for deduplication across pages). Union nodes additionally
 * snapshot their children's checkpoints before execution so they can rewind a child
 * that overproduced.
 *
 * @see QueryContext#getOrCreateExecutionState(int)
 */
public class ExecutionState {
    private final AtomicInteger limit = new AtomicInteger();
    private volatile Bound lower;
    private volatile Bound upper;
    private volatile boolean exhausted;
    private volatile SelectorPair selector;
    private volatile Roaring64Bitmap returnedHandles;
    private volatile Set<ObjectId> returnedObjectIds;
    private volatile Map<Integer, SavedCursorCheckpoint> savedChildCheckpoints;
    private volatile boolean scanReversed;

    public boolean isEmpty() {
        return upper == null && lower == null;
    }

    public Bound getUpper() {
        return upper;
    }

    public void setUpper(Bound upper) {
        this.upper = upper;
    }

    public Bound getLower() {
        return lower;
    }

    public void setLower(Bound lower) {
        this.lower = lower;
    }

    public int getLimit() {
        return limit.get();
    }

    public void setLimit(int limit) {
        this.limit.set(limit);
    }

    public boolean isExhausted() {
        return exhausted;
    }

    public void setExhausted(boolean exhausted) {
        this.exhausted = exhausted;
    }

    public SelectorPair getSelector() {
        return selector;
    }

    public void setSelector(SelectorPair selector) {
        this.selector = selector;
    }

    public Roaring64Bitmap getReturnedHandles() {
        return returnedHandles;
    }

    public void setReturnedHandles(Roaring64Bitmap returnedHandles) {
        this.returnedHandles = returnedHandles;
    }

    public Set<ObjectId> getReturnedObjectIds() {
        return returnedObjectIds;
    }

    public void setReturnedObjectIds(Set<ObjectId> returnedObjectIds) {
        this.returnedObjectIds = returnedObjectIds;
    }

    public Map<Integer, SavedCursorCheckpoint> getSavedChildCheckpoints() {
        return savedChildCheckpoints;
    }

    public void setSavedChildCheckpoints(Map<Integer, SavedCursorCheckpoint> savedChildCheckpoints) {
        this.savedChildCheckpoints = savedChildCheckpoints;
    }

    public boolean isScanReversed() {
        return scanReversed;
    }

    public void setScanReversed(boolean scanReversed) {
        this.scanReversed = scanReversed;
    }

    /**
     * Snapshot of a child node's cursor position taken before a union execution round,
     * used to rewind the child if it produces more entries than the union budget allows.
     */
    public record SavedCursorCheckpoint(Bound lower, Bound upper, SelectorPair selector) {
    }
}
