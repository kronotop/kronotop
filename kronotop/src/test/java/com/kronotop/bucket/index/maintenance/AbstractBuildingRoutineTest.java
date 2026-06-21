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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.FDBException;
import com.kronotop.KronotopException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractBuildingRoutineTest {

    @Test
    void shouldRetryOnNotCommittedConflict() {
        // Behavior: a commit conflict (FDB 1020 not_committed) is retriable, so the batch can be retried in place.
        KronotopException exp = new KronotopException(new FDBException("not_committed", 1020));
        assertTrue(AbstractBuildingRoutine.isRetriableConflict(exp));
    }

    @Test
    void shouldNotRetryOnNonRetriableFdbError() {
        // Behavior: a non-transient FDB error code is not retried; only the transaction-conflict set qualifies.
        KronotopException exp = new KronotopException(new FDBException("non_retriable", 9999));
        assertFalse(AbstractBuildingRoutine.isRetriableConflict(exp));
    }

    @Test
    void shouldNotRetryWhenCauseIsNotFdbException() {
        // Behavior: a non-FDB cause is not a transient conflict, so it propagates instead of retrying.
        KronotopException exp = new KronotopException("boom", new IllegalStateException("boom"));
        assertFalse(AbstractBuildingRoutine.isRetriableConflict(exp));
    }

    @Test
    void shouldNotRetryWhenNoCause() {
        // Behavior: an exception without an FDB cause (e.g. a routine/validation error) propagates, never retries.
        KronotopException exp = new IndexMaintenanceRoutineException("no index found");
        assertFalse(AbstractBuildingRoutine.isRetriableConflict(exp));
    }
}
