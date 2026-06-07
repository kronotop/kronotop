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

/**
 * Manages adaptive scan budget for FDB getRange calls within a single ADVANCE call.
 *
 * <p>Decouples the internal FDB scan limit with the user-facing result limit. When residual
 * filtering discards most scanned entries, the budget grows to reduce FDB round-trips.
 * Growth is capped per-iteration and in total to respect FDB transaction time limits.</p>
 */
public class ScanBudget {
    static final int MIN_BUDGET = 1_000;
    static final int MAX_ITER_BUDGET = 10_000;
    static final int MAX_TOTAL_BUDGET = 50_000;

    private final long deadlineNanos;
    private final boolean unbounded;
    private int currentBudget;
    private int totalScanned;

    public ScanBudget(int userLimit, long deadlineNanos) {
        this(userLimit, deadlineNanos, false);
    }

    private ScanBudget(int userLimit, long deadlineNanos, boolean unbounded) {
        this.currentBudget = userLimit;
        this.totalScanned = 0;
        this.deadlineNanos = deadlineNanos;
        this.unbounded = unbounded;
    }

    /**
     * Creates a budget with no scan or time limits. Used for vector search where
     * the candidate supplier manages its own bounds.
     */
    public static ScanBudget unbounded(int userLimit) {
        return new ScanBudget(userLimit, 0, true);
    }

    /**
     * Returns the budget for the next getRange call, capped by the remaining total budget.
     */
    public int current() {
        return Math.min(currentBudget, MAX_TOTAL_BUDGET - totalScanned);
    }

    /**
     * Records entries scanned in the last iteration.
     */
    public void recordScanned(int count) {
        totalScanned += count;
    }

    /**
     * Grows the budget for the next iteration. Jumps to MIN_BUDGET if below it,
     * otherwise grows by 50%, capped at MAX_ITER_BUDGET.
     */
    public void grow() {
        if (currentBudget < MIN_BUDGET) {
            currentBudget = MIN_BUDGET;
        } else {
            currentBudget = (int) Math.min((long) currentBudget * 3 / 2, MAX_ITER_BUDGET);
        }
    }

    public boolean timeBudgetExhausted() {
        // deadlineNanos can be zero; this means no time budget
        if (deadlineNanos == 0) {
            return false;
        }
        return System.nanoTime() >= deadlineNanos;
    }

    public boolean totalBudgetExhausted() {
        if (unbounded) {
            return false;
        }
        return totalScanned >= MAX_TOTAL_BUDGET;
    }

    public boolean anyBudgetExhausted() {
        return totalBudgetExhausted() || timeBudgetExhausted();
    }

    int getCurrentBudget() {
        return currentBudget;
    }

    int getTotalScanned() {
        return totalScanned;
    }
}
