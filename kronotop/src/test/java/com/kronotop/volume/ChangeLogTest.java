/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.kronotop.volume.Subspaces.CHANGELOG_SUBSPACE;
import static com.kronotop.volume.Subspaces.MUTATION_TRIGGER;
import static org.junit.jupiter.api.Assertions.*;

class ChangeLogTest extends BaseStandaloneInstanceTest {

    private EntryMetadata createTestMetadata() {
        Prefix prefix = new Prefix("test-prefix");
        return new EntryMetadata(1L, prefix.asBytes(), 100L, 50L, 1);
    }

    @Test
    void shouldRecordAppendOperation_withUserVersion() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] value = results.get(0).getValue();
            Tuple valueTuple = Tuple.fromBytes(value);
            assertEquals(metadata.segmentId(), valueTuple.getLong(0));
            assertEquals(metadata.position(), valueTuple.getLong(1));
            assertEquals(metadata.length(), valueTuple.getLong(2));
            assertEquals(prefix.asLong(), valueTuple.getLong(3));
        }
    }

    @Test
    void shouldRecordAppendOperation_withVersionstamp() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] value = results.get(0).getValue();
            Tuple valueTuple = Tuple.fromBytes(value);
            assertEquals(metadata.segmentId(), valueTuple.getLong(0));
            assertEquals(metadata.position(), valueTuple.getLong(1));
            assertEquals(metadata.length(), valueTuple.getLong(2));
            assertEquals(prefix.asLong(), valueTuple.getLong(3));
        }
    }

    @Test
    void shouldRecordDeleteOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");
        Versionstamp versionstamp = TestUtil.generateVersionstamp(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.deleteOperation(tr, metadata, prefix, versionstamp);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(1, results.size());

            byte[] key = results.get(0).getKey();
            Tuple keyTuple = subspace.unpack(key);
            long rawOpKind = keyTuple.getLong(3);
            OperationKind opKind = OperationKind.valueOf((byte) rawOpKind);
            assertEquals(OperationKind.DELETE, opKind);
        }
    }

    @Test
    void shouldTriggerWatchers_onAppendOperation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        EntryMetadata metadata = createTestMetadata();
        Prefix prefix = new Prefix("test-prefix");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            changeLog.appendOperation(tr, metadata, prefix, 0);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] triggerKey = subspace.pack(MUTATION_TRIGGER);
            byte[] value = tr.get(triggerKey).join();
            assertNotNull(value);
        }
    }

    @Test
    void shouldMaintainChronologicalOrder_forMultipleOperations() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-changelog");
        ChangeLog changeLog = new ChangeLog(context, subspace);
        Prefix prefix = new Prefix("test-prefix");

        List<EntryMetadata> metadataList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            EntryMetadata metadata = new EntryMetadata((long) i, prefix.asBytes(), 100L * i, 50L, i);
            metadataList.add(metadata);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 0; i < metadataList.size(); i++) {
                changeLog.appendOperation(tr, metadataList.get(i), prefix, i);
            }
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Range range = subspace.range(Tuple.from(CHANGELOG_SUBSPACE));
            List<KeyValue> results = tr.getRange(range).asList().join();

            assertEquals(5, results.size());

            long previousLogSeq = -1;
            for (KeyValue kv : results) {
                Tuple keyTuple = subspace.unpack(kv.getKey());
                long logSeq = keyTuple.getLong(2);
                assertTrue(logSeq > previousLogSeq);
                previousLogSeq = logSeq;
            }
        }
    }
}