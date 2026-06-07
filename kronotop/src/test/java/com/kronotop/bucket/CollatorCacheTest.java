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

package com.kronotop.bucket;

import com.ibm.icu.text.Collator;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CollatorCacheTest {

    private static Collation spec(String locale, int strength) {
        return Collation.create(locale, strength, null, null, null, null, null, null, null);
    }

    private static int acquireIdentityHash(CollatorCache cache, Collation spec) {
        return System.identityHashCode(cache.acquire(spec));
    }

    @Test
    void shouldReturnSameCollatorInstanceForEqualSpecs() {
        // Behavior: Two acquire calls with value-equal Collation specs return the exact same Collator reference.
        CollatorCache cache = new CollatorCache();
        Collator a = cache.acquire(spec("en", 3));
        Collator b = cache.acquire(spec("en", 3));
        assertSame(a, b);
    }

    @Test
    void shouldReturnDistinctCollatorsForDifferentSpecs() {
        // Behavior: Specs that differ in any field produce distinct Collator instances.
        CollatorCache cache = new CollatorCache();
        Collator en = cache.acquire(spec("en", 3));
        Collator tr = cache.acquire(spec("tr", 3));
        Collator enSecondary = cache.acquire(spec("en", 2));
        assertNotSame(en, tr);
        assertNotSame(en, enSecondary);
        assertNotSame(tr, enSecondary);
    }

    @Test
    void shouldReturnFrozenCollator() {
        // Behavior: Cached collators are frozen, required for safe sharing across threads.
        CollatorCache cache = new CollatorCache();
        Collator c = cache.acquire(spec("en", 3));
        assertTrue(c.isFrozen());
    }

    @Test
    void shouldApplyStrengthMapping() {
        // Behavior: Collation.strength 1..5 maps to ICU PRIMARY..IDENTICAL.
        CollatorCache cache = new CollatorCache();
        assertEquals(Collator.PRIMARY, cache.acquire(spec("en", 1)).getStrength());
        assertEquals(Collator.SECONDARY, cache.acquire(spec("en", 2)).getStrength());
        assertEquals(Collator.TERTIARY, cache.acquire(spec("en", 3)).getStrength());
        assertEquals(Collator.QUATERNARY, cache.acquire(spec("en", 4)).getStrength());
        assertEquals(Collator.IDENTICAL, cache.acquire(spec("en", 5)).getStrength());
    }

    @Test
    void shouldRejectInvalidStrength() {
        // Behavior: build() throws on strength outside 1..5.
        CollatorCache cache = new CollatorCache();
        assertThrows(IllegalArgumentException.class,
                () -> cache.acquire(spec("en", 0)));
        assertThrows(IllegalArgumentException.class,
                () -> cache.acquire(spec("en", 6)));
    }

    @Test
    void shouldApplyCaseFirstUpper() {
        // Behavior: case_first="upper" makes uppercase sort before lowercase at tertiary strength.
        CollatorCache cache = new CollatorCache();
        Collation s = Collation.create("en", 3, null, "upper", null, null, null, null, null);
        Collator c = cache.acquire(s);
        assertTrue(c.compare("A", "a") < 0);
    }

    @Test
    void shouldApplyNumericOrdering() {
        // Behavior: numericOrdering=true compares embedded digits by numeric value, so "9" < "10".
        CollatorCache cache = new CollatorCache();
        Collation s = Collation.create("en", 3, null, null, true, null, null, null, null);
        Collator c = cache.acquire(s);
        assertTrue(c.compare("9", "10") < 0);
        Collator baseline = cache.acquire(spec("en", 3));
        assertTrue(baseline.compare("9", "10") > 0);
    }

    @Test
    void shouldTreatPrimaryStrengthAsCaseInsensitive() {
        // Behavior: strength=PRIMARY treats case and accent differences as equal.
        CollatorCache cache = new CollatorCache();
        Collator c = cache.acquire(spec("en", 1));
        assertEquals(0, c.compare("cafe", "CAFE"));
        assertEquals(0, c.compare("cafe", "café"));
    }

    @Test
    void shouldEvictUnreachableCollator() {
        // Behavior: Once the last strong reference to a cached Collator dies, the entry is reaped
        // on the next write-path acquire and a fresh Collator is built. GC is non-deterministic,
        // so we poll with Awaitility.
        CollatorCache cache = new CollatorCache();
        Collation s = spec("en", 3);
        int firstIdentity = acquireIdentityHash(cache, s);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> {
                    System.gc();
                    return acquireIdentityHash(cache, s) != firstIdentity;
                });
    }

    @Test
    void shouldDeduplicateAcrossConcurrentAcquirers() throws InterruptedException {
        // Behavior: N threads calling acquire with the same spec all receive the same Collator instance,
        // and build() runs at most once per unique spec under contention.
        AtomicInteger builds = new AtomicInteger();
        CollatorCache cache = new CollatorCache() {
            @Override
            protected Collator build(Collation spec) {
                builds.incrementAndGet();
                return super.build(spec);
            }
        };
        Collation s = spec("en", 3);

        int threadCount = 32;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        Set<Collator> observed = java.util.Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < threadCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    observed.add(cache.acquire(s));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertEquals(1, observed.size());
        assertEquals(1, builds.get());
    }

    @Test
    void shouldApplyMaxVariableSpace() {
        // Behavior: With alternate=shifted and maxVariable=space, only spaces are ignorable;
        // punctuation remains significant.
        CollatorCache cache = new CollatorCache();
        Collation s = Collation.create("en", 3, null, null, null, "shifted", null, null, "space");
        Collator c = cache.acquire(s);
        assertEquals(0, c.compare("black bird", "blackbird"));
        assertNotEquals(0, c.compare("black-bird", "blackbird"));
    }

    @Test
    void shouldApplyMaxVariablePunct() {
        // Behavior: With alternate=shifted and maxVariable=punct, both spaces and punctuation
        // are ignorable at strengths below quaternary.
        CollatorCache cache = new CollatorCache();
        Collation s = Collation.create("en", 3, null, null, null, "shifted", null, null, "punct");
        Collator c = cache.acquire(s);
        assertEquals(0, c.compare("black-bird", "blackbird"));
        assertEquals(0, c.compare("black bird", "blackbird"));
    }
}
