/*
 * Copyright (c) 2023 Kronotop
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


package com.kronotop.sql;

import com.kronotop.sql.plan.PlanCache;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PlanCacheTest {

    @Test
    public void test_putPlan_then_getPlan() {
        PlanCache planCache = new PlanCache(10);
        assertDoesNotThrow(() -> planCache.putPlan("public", "select 1", new Plan(null)));

        assertNotNull(planCache.getPlan("public", "select 1"));
        assertNull(planCache.getPlan("public", "select 2"));
    }

    @Test
    public void test_shrinkIfNeeded() throws InterruptedException {
        PlanCache planCache = new PlanCache(10);

        for (int i = 0; i < 15; i++) {
            planCache.putPlan("public", "select " + i, new Plan(null));
            Thread.sleep(10);
        }

        int found = 0;
        for (int i = 0; i < 15; i++) {
            Plan plan = planCache.getPlan("public", "select " + i);
            if (plan != null) {
                found++;
            }
        }

        assertEquals(10, found);
    }

    @Test
    public void test_invalidate_the_oldest_plans() throws InterruptedException {
        PlanCache planCache = new PlanCache(10);

        for (int i = 0; i < 15; i++) {
            planCache.putPlan("public", "select " + i, new Plan(null));
            Thread.sleep(10);
        }

        Set<String> expectedQueries = new HashSet<>(List.of("select 0", "select 1", "select 2", "select 3", "select 4"));
        Set<String> queries = new HashSet<>();
        for (int i = 0; i < 15; i++) {
            String query = "select " + i;
            Plan plan = planCache.getPlan("public", query);
            if (plan == null) {
                queries.add(query);
            }
        }
        assertEquals(expectedQueries, queries);
    }

    @Test
    public void test_putPlan_then_invalidateQuery() {
        PlanCache planCache = new PlanCache(10);
        assertDoesNotThrow(() -> planCache.putPlan("public", "select 1", new Plan(null)));
        assertDoesNotThrow(() -> planCache.invalidateQuery("public", "select 1"));

        assertNull(planCache.getPlan("public", "select 1"));
    }

    @Test
    public void test_putPlan_then_invalidateSchema() {
        PlanCache planCache = new PlanCache(10);
        assertDoesNotThrow(() -> planCache.putPlan("public", "select 1", new Plan(null)));
        assertDoesNotThrow(() -> planCache.invalidateSchema("public"));

        assertNull(planCache.getPlan("public", "select 1"));
    }
}