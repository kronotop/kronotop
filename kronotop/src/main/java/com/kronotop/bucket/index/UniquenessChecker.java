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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.DuplicateKeyException;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Collects uniqueness checks for a single write operation and runs them together.
 * <p>
 * Each check issues its FoundationDB read immediately, so the reads run in parallel.
 * {@link #await()} then waits for all of them at once and reports the first violation.
 * This keeps the checks off the critical write path: they are started up front and
 * drained just before the volume append.
 * <p>
 * An instance is used for one operation and thrown away. It is not thread-safe.
 */
public final class UniquenessChecker {
    private final Transaction tr;
    private final List<CompletableFuture<Void>> pending = new ArrayList<>();

    public UniquenessChecker(Transaction tr) {
        this.tr = tr;
    }

    /**
     * Checks that no primary index entry already exists for the given ObjectId.
     * The read is issued immediately; the violation is raised at {@link #await()}.
     *
     * @param indexSubspace the primary index subspace
     * @param objectId      the document's ObjectId
     */
    public void checkPrimaryKey(DirectorySubspace indexSubspace, ObjectId objectId) {
        // Primary Index Key Structure: (ENTRIES, ObjectId)
        Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId.toByteArray());
        byte[] key = indexSubspace.pack(tuple);
        CompletableFuture<Void> check = tr.get(key).thenAccept(existing -> {
            if (existing != null) {
                throw new DuplicateKeyException(objectId);
            }
        });
        pending.add(check);
    }

    /**
     * Checks that no other document already holds the given value in a unique single field index.
     * The read is issued immediately over the {@code (ENTRIES, value)} prefix; the violation is
     * raised at {@link #await()}.
     * <p>
     * Entries whose trailing ObjectId equals {@code selfObjectId} are ignored, so a document does not
     * conflict with its own existing entry (relevant on update). At most two entries are read: with
     * multi-key unique indexes disallowed, a single document contributes at most one entry per value,
     * so two entries are enough to tell "only self" from "someone else".
     *
     * @param indexSubspace the single field index subspace
     * @param valuePrefix   the packed {@code (ENTRIES, encodedValue)} prefix
     * @param selfObjectId  the writing document's ObjectId as bytes, excluded from the match
     * @param violation     supplies the exception, built only on the rare violation branch
     */
    public void checkFieldValue(
            DirectorySubspace indexSubspace,
            byte[] valuePrefix,
            byte[] selfObjectId,
            Supplier<DuplicateKeyException> violation
    ) {
        CompletableFuture<Void> check = tr.getRange(valuePrefix, ByteArrayUtil.strinc(valuePrefix), 2)
                .asList().thenAccept(entries -> {
                    for (KeyValue kv : entries) {
                        Tuple unpacked = indexSubspace.unpack(kv.getKey());
                        byte[] objectId = unpacked.getBytes(unpacked.size() - 1);
                        if (!Arrays.equals(objectId, selfObjectId)) {
                            throw violation.get();
                        }
                    }
                });
        pending.add(check);
    }

    /**
     * Waits for all issued checks to complete and throws the first violation found.
     */
    public void await() {
        if (pending.isEmpty()) {
            return;
        }
        CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
    }
}
