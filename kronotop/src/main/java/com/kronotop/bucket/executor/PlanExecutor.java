/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.values.VersionstampVal;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.planner.Bound;
import com.kronotop.bucket.planner.Bounds;
import com.kronotop.bucket.planner.physical.PhysicalFullScan;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.Prefix;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code PlanExecutor} class is responsible for executing physical query plans and retrieving
 * corresponding data from a key-value store. It provides support for operations such as full scans
 * and index scans on defined subspaces and prefixes, using transaction contexts.
 * <p>
 * This class operates within the context of a specific shard and namespace and interacts with the
 * storage volume through its {@code ExecutorContext} and {@code Context} dependencies. The methods
 * in this class are used to process query plans, retrieve index entries, and fetch corresponding data
 * from the volume.
 */
public class PlanExecutor {
    private final PlanExecutorConfig config;
    private final PlanExecutorEnvironment env;

    public PlanExecutor(PlanExecutorConfig config) {
        this.config = config;
        this.env = config.environment();
    }

    /**
     * Reads entries from the volume associated with the given prefix and list of index entries.
     * This method retrieves data for each index entry from the volume and constructs a map
     * where the versionstamp is used as the key and the corresponding ByteBuffer as the value.
     * If any entry cannot be found in the volume, a KronotopException is thrown.
     *
     * @param prefix  the prefix identifying the namespace and shard key space from which entries
     *                are to be retrieved
     * @param entries a map where the key is an integer ID, and the value is an {@code IndexEntry}
     *                containing the index details and metadata for the entries to be read
     * @return a map of versionstamps to ByteBuffer objects containing the data for the retrieved entries
     * @throws IOException       if an I/O error occurs during the retrieval from the volume
     * @throws KronotopException if an indexed entry cannot be found in the volume
     */
    private Map<Versionstamp, ByteBuffer> readEntriesFromVolume(Prefix prefix, Map<Integer, EntryCandidate> entries) throws IOException {
        Map<Versionstamp, ByteBuffer> result = new LinkedHashMap<>();
        for (Map.Entry<Integer, EntryCandidate> entry : entries.entrySet()) {
            EntryCandidate indexEntry = entry.getValue();
            ByteBuffer buffer = env.shard().volume().get(prefix, indexEntry.versionstamp(), indexEntry.metadata());
            if (buffer == null) {
                // Kill the query, something went seriously wrong.
                throw new KronotopException(String.format("Indexed entry could not be found in volume: '%s', Versionstamp: '%s'",
                        env.shard().volume().getConfig().name(),
                        VersionstampUtil.base32HexEncode(indexEntry.versionstamp())
                ));
            }
            result.put(indexEntry.versionstamp(), buffer);
        }
        return result;
    }

    private Map<Versionstamp, ByteBuffer> doPhysicalFullScan(Transaction tr, PhysicalFullScan physicalFullScan) throws IOException {
        // TODO: Review this
        DirectorySubspace indexSubspace = env.metadata().indexes().getSubspace(DefaultIndexDefinition.ID);
        byte[] b = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(b);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(b));
        IndexRange range = new IndexRange(begin, end);
        Map<Integer, EntryCandidate> entries = getEntriesFromIndex(tr, indexSubspace, range);
        return readEntriesFromVolume(env.metadata().volumePrefix(), entries);
    }

    /**
     * Retrieves a map of index entries from the specified index range within the provided index subspace.
     * The method unpacks index keys and decodes associated metadata to construct the resulting map.
     *
     * @param tr            the transaction context used to access the key-value range
     * @param indexSubspace the subspace representing the index structure
     * @param range         the index range specifying the beginning and end key selectors for the operation
     * @return a map where the key is an integer ID from the decoded metadata and the value
     * is an {@code IndexEntry} containing the unpacked index and metadata
     */
    private LinkedHashMap<Integer, EntryCandidate> getEntriesFromIndex(Transaction tr, DirectorySubspace indexSubspace, IndexRange range) {
        LinkedHashMap<Integer, EntryCandidate> result = new LinkedHashMap<>();
        for (KeyValue keyValue : tr.getRange(range.begin(), range.end(), config.limit(), config.reverse())) {
            Tuple keyTuple = indexSubspace.unpack(keyValue.getKey());

            // TODO: ShardId ignored for now
            IndexEntry indexEntry = IndexEntry.decode(keyValue.getValue());
            EntryMetadata metadata = EntryMetadata.decode(ByteBuffer.wrap(indexEntry.entryMetadata()));

            Versionstamp key = (Versionstamp) keyTuple.get(1);
            result.put(metadata.id(), new EntryCandidate(key, metadata));
        }
        return result;
    }

    private Bound getLowerBound(PhysicalIndexScan node) {
        Bounds savedBounds = config.cursor().bounds().get(node.getIndex());
        if (Objects.nonNull(savedBounds) && Objects.nonNull(savedBounds.lower())) {
            return savedBounds.lower();
        }
        return node.getBounds().lower();
    }

    private Bound getUpperBound(PhysicalIndexScan node) {
        Bounds savedBounds = config.cursor().bounds().get(node.getIndex());
        if (Objects.nonNull(savedBounds) && Objects.nonNull(savedBounds.upper())) {
            return savedBounds.upper();
        }
        return node.getBounds().upper();
    }

    /**
     * Constructs an {@link IndexRange} object defining the start and end key selectors
     * for an index scan operation based on the provided bounds and index metadata.
     *
     * @param physicalIndexScan the physical index scan object containing index metadata
     *                          and operator types for determining the range
     * @param indexSubspace     the subspace representing the specific index structure
     *                          and containing the key-value pairs to be scanned
     * @return an {@link IndexRange} containing the begin and end key selectors for the
     * index scan operation
     */
    private IndexRange getIndexRange(PhysicalIndexScan physicalIndexScan, DirectorySubspace indexSubspace) {
        KeySelector begin = null;
        Bound lower = getLowerBound(physicalIndexScan);
        if (lower != null) {
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lower.bqlValue().value());
            byte[] key = indexSubspace.pack(tuple);
            if (lower.type().equals(OperatorType.GT)) {
                begin = KeySelector.firstGreaterThan(key);
            } else if (lower.type().equals(OperatorType.GTE)) {
                begin = KeySelector.firstGreaterOrEqual(key);
            } else if (lower.type().equals(OperatorType.EQ)) {
                begin = KeySelector.firstGreaterOrEqual(key);
            }
        } else {
            begin = KeySelector.firstGreaterOrEqual(indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue())));
        }

        KeySelector end = null;
        Bound upper = getUpperBound(physicalIndexScan);
        if (upper == null) {
            end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()))));
        } else {
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), upper.bqlValue().value());
            byte[] key = indexSubspace.pack(tuple);
            if (upper.type().equals(OperatorType.LT)) {
                end = KeySelector.firstGreaterOrEqual(key);
            } else if (upper.type().equals(OperatorType.LTE)) {
                end = KeySelector.firstGreaterThan(key);
            } else if (upper.type().equals(OperatorType.EQ)) {
                end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(key));
            }
        }
        return new IndexRange(begin, end);
    }

    private Map<Versionstamp, ByteBuffer> doPhysicalIndexScan(Transaction tr, PhysicalIndexScan physicalIndexScan) throws IOException {
        DirectorySubspace indexSubspace = env.metadata().indexes().getSubspace(physicalIndexScan.getIndex());
        IndexRange range = getIndexRange(physicalIndexScan, indexSubspace);
        LinkedHashMap<Integer, EntryCandidate> entries = getEntriesFromIndex(tr, indexSubspace, range);

        Map<Versionstamp, ByteBuffer> result = readEntriesFromVolume(env.metadata().volumePrefix(), entries);

        if (entries.isEmpty()) {
            return result;
        }
        // rearrange index boundaries, only for the lower bound for testing purposes
        Bound lower = null;
        if (physicalIndexScan.getBounds().lower() != null) {
            Bound previousLowerBound = physicalIndexScan.getBounds().lower();
            OperatorType operatorType = previousLowerBound.type();
            if (previousLowerBound.type().equals(OperatorType.GTE)) {
                operatorType = OperatorType.GT;
            }
            if (Objects.requireNonNull(previousLowerBound.bqlValue()) instanceof VersionstampVal) {
                Versionstamp versionstamp = entries.lastEntry().getValue().versionstamp();
                lower = new Bound(operatorType, new VersionstampVal(versionstamp));
            } else {
                throw new KronotopException("Unsupported index");
            }
        }
        config.cursor().bounds().put(physicalIndexScan.getIndex(), new Bounds(lower, null));
        return result;
    }

    /**
     * Executes a physical plan on the given transaction. The method determines the type of physical plan
     * (e.g., physical full scan, physical index scan) and performs the respective operation to retrieve
     * data from the bucket subspace. The execution results in a map where the keys are versionstamps and
     * the values are ByteBuffer objects representing the retrieved data.
     *
     * @param tr the transaction within which the operation is executed
     * @return a map of versionstamps to ByteBuffer objects representing the results of the executed plan
     * @throws IOException if an I/O error occurs during execution
     */
    public Map<Versionstamp, ByteBuffer> execute(Transaction tr) throws IOException {
        PhysicalNode plan = env.plan();
        return switch (plan) {
            case PhysicalFullScan physicalFullScan -> doPhysicalFullScan(tr, physicalFullScan);
            case PhysicalIndexScan physicalIndexScan -> doPhysicalIndexScan(tr, physicalIndexScan);
            default -> throw new IllegalStateException("Unexpected value: " + plan);
        };
    }
}
