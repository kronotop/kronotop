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

package com.kronotop.bucket;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;

/**
 * Thread-safe cache of ICU4J {@link Collator} instances keyed by {@link Collation} spec.
 *
 * <p>Callers retain strong references to returned collators (typically via a field on a
 * long-lived metadata object). When the last strong reference disappears, the collator
 * becomes weakly reachable and is enqueued for removal; stale entries are reaped on the
 * next write-path visit.
 */
public class CollatorCache {
    private final Map<Collation, Entry> map = new HashMap<>();
    private final ReferenceQueue<Collator> queue = new ReferenceQueue<>();
    private final StampedLock lock = new StampedLock();

    private static int mapStrength(int strength) {
        return switch (strength) {
            case 1 -> Collator.PRIMARY;
            case 2 -> Collator.SECONDARY;
            case 3 -> Collator.TERTIARY;
            case 4 -> Collator.QUATERNARY;
            case 5 -> Collator.IDENTICAL;
            default -> throw new IllegalArgumentException("invalid collation strength: " + strength);
        };
    }

    public Collator acquire(Collation spec) {
        long stamp = lock.tryOptimisticRead();
        Entry entry = map.get(spec);
        Collator collator = entry != null ? entry.get() : null;
        if (lock.validate(stamp) && collator != null) {
            return collator;
        }

        stamp = lock.readLock();
        try {
            entry = map.get(spec);
            collator = entry != null ? entry.get() : null;
            if (collator != null) {
                return collator;
            }
        } finally {
            lock.unlockRead(stamp);
        }

        stamp = lock.writeLock();
        try {
            entry = map.get(spec);
            if (entry != null) {
                collator = entry.get();
                if (collator != null) {
                    return collator;
                }
            }
            drainStaleEntries();
            collator = build(spec);
            collator.freeze();
            map.put(spec, new Entry(spec, collator, queue));
            return collator;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private void drainStaleEntries() {
        Reference<? extends Collator> ref;
        while ((ref = queue.poll()) != null) {
            Entry dead = (Entry) ref;
            map.remove(dead.key, dead);
        }
    }

    protected Collator build(Collation spec) {
        Collator collator = Collator.getInstance(new ULocale(spec.locale()));
        collator.setStrength(mapStrength(spec.strength()));
        if (collator instanceof RuleBasedCollator rbc) {
            rbc.setCaseLevel(spec.caseLevel());
            switch (spec.caseFirst()) {
                case "upper" -> rbc.setUpperCaseFirst(true);
                case "lower" -> rbc.setLowerCaseFirst(true);
                default -> {
                }
            }
            rbc.setNumericCollation(spec.numericOrdering());
            if ("shifted".equals(spec.alternate())) {
                rbc.setAlternateHandlingShifted(true);
                int maxVar = "space".equals(spec.maxVariable())
                        ? Collator.ReorderCodes.SPACE
                        : Collator.ReorderCodes.PUNCTUATION;
                rbc.setMaxVariable(maxVar);
            }
            rbc.setFrenchCollation(spec.backwards());
            rbc.setDecomposition(spec.normalization()
                    ? Collator.CANONICAL_DECOMPOSITION
                    : Collator.NO_DECOMPOSITION);
        }
        return collator;
    }

    private static final class Entry extends WeakReference<Collator> {
        final Collation key;

        Entry(Collation key, Collator referent, ReferenceQueue<Collator> queue) {
            super(referent, queue);
            this.key = key;
        }
    }
}
