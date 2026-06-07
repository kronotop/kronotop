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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ScanBudgetTest {

    private static long farDeadline() {
        return System.nanoTime() + Duration.ofHours(1).toNanos();
    }

    @Test
    void shouldStartWithUserLimit() {
        // Behavior: Initial budget equals the user-specified limit
        ScanBudget budget = new ScanBudget(10, farDeadline());
        assertEquals(10, budget.current());
    }

    @Test
    void shouldJumpToMinBudgetOnFirstGrow() {
        // Behavior: When budget is below MIN_BUDGET, grow() jumps directly to MIN_BUDGET
        ScanBudget budget = new ScanBudget(2, farDeadline());
        assertEquals(2, budget.current());

        budget.grow();
        assertEquals(ScanBudget.MIN_BUDGET, budget.current());
    }

    @Test
    void shouldGrowByFiftyPercent() {
        // Behavior: When budget >= MIN_BUDGET, grow() increases by 50%
        ScanBudget budget = new ScanBudget(ScanBudget.MIN_BUDGET, farDeadline());
        budget.grow();
        assertEquals(1500, budget.current());

        budget.grow();
        assertEquals(2250, budget.current());
    }

    @Test
    void shouldCapAtMaxIterBudget() {
        // Behavior: Budget per iteration never exceeds MAX_ITER_BUDGET
        ScanBudget budget = new ScanBudget(8000, farDeadline());
        budget.grow(); // 8000 * 1.5 = 12000, capped to 10000
        assertEquals(ScanBudget.MAX_ITER_BUDGET, budget.current());

        budget.grow(); // stays at 10000
        assertEquals(ScanBudget.MAX_ITER_BUDGET, budget.current());
    }

    @Test
    void shouldTrackTotalScanned() {
        // Behavior: totalScanned accumulates across iterations
        ScanBudget budget = new ScanBudget(100, farDeadline());
        budget.recordScanned(100);
        budget.recordScanned(500);
        assertEquals(600, budget.getTotalScanned());
    }

    @Test
    void shouldExhaustTotalBudget() {
        // Behavior: totalBudgetExhausted returns true when total reaches MAX_TOTAL_BUDGET
        ScanBudget budget = new ScanBudget(100, farDeadline());
        assertFalse(budget.totalBudgetExhausted());

        budget.recordScanned(ScanBudget.MAX_TOTAL_BUDGET);
        assertTrue(budget.totalBudgetExhausted());
    }

    @Test
    void shouldNotExceedRemainingTotalBudget() {
        // Behavior: current() is capped by remaining total budget
        ScanBudget budget = new ScanBudget(ScanBudget.MAX_ITER_BUDGET, farDeadline());
        budget.recordScanned(ScanBudget.MAX_TOTAL_BUDGET - 500);

        // Only 500 remaining in total budget, even though iter budget is 10000
        assertEquals(500, budget.current());
    }

    @Test
    void shouldGrowFromSmallLimitToMaxInExpectedSteps() {
        // Behavior: Growth sequence from limit=2 reaches MAX_ITER_BUDGET
        ScanBudget budget = new ScanBudget(2, farDeadline());
        assertEquals(2, budget.getCurrentBudget());

        budget.grow(); // -> 1000
        assertEquals(ScanBudget.MIN_BUDGET, budget.getCurrentBudget());

        budget.grow(); // -> 1500
        assertEquals(1500, budget.getCurrentBudget());

        budget.grow(); // -> 2250
        assertEquals(2250, budget.getCurrentBudget());

        budget.grow(); // -> 3375
        assertEquals(3375, budget.getCurrentBudget());

        budget.grow(); // -> 5062
        assertEquals(5062, budget.getCurrentBudget());

        budget.grow(); // -> 7593
        assertEquals(7593, budget.getCurrentBudget());

        budget.grow(); // -> 10000 (capped)
        assertEquals(ScanBudget.MAX_ITER_BUDGET, budget.getCurrentBudget());
    }

    @Test
    void shouldExhaustTimeBudget() {
        // Behavior: timeBudgetExhausted returns true when deadline is in the past
        long pastDeadline = System.nanoTime() - Duration.ofSeconds(1).toNanos();
        ScanBudget budget = new ScanBudget(100, pastDeadline);
        assertTrue(budget.timeBudgetExhausted());
    }

    @Test
    void shouldNotExhaustTimeBudgetWhenDeadlineInFuture() {
        // Behavior: timeBudgetExhausted returns false when deadline is far in the future
        ScanBudget budget = new ScanBudget(100, farDeadline());
        assertFalse(budget.timeBudgetExhausted());
    }

    @Test
    void shouldReportAnyBudgetExhaustedOnTimeBudget() {
        // Behavior: anyBudgetExhausted returns true when time budget is exhausted even if row budget is not
        long pastDeadline = System.nanoTime() - Duration.ofSeconds(1).toNanos();
        ScanBudget budget = new ScanBudget(100, pastDeadline);
        assertFalse(budget.totalBudgetExhausted());
        assertTrue(budget.anyBudgetExhausted());
    }

    @Test
    void shouldReportAnyBudgetExhaustedOnRowBudget() {
        // Behavior: anyBudgetExhausted returns true when row budget is exhausted even if time budget is not
        ScanBudget budget = new ScanBudget(100, farDeadline());
        budget.recordScanned(ScanBudget.MAX_TOTAL_BUDGET);
        assertFalse(budget.timeBudgetExhausted());
        assertTrue(budget.anyBudgetExhausted());
    }
}
