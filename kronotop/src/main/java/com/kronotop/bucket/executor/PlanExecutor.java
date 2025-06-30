// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketPrefix;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.DefaultIndex;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.bucket.index.UnpackedIndex;
import com.kronotop.bucket.planner.Bound;
import com.kronotop.bucket.planner.physical.PhysicalFullScan;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.internal.VersionstampUtils;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.Prefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(PlanExecutor.class);
    private final Context context;
    private final PlanExecutorEnvironment executorContext;

    public PlanExecutor(Context context, PlanExecutorEnvironment executorContext) {
        this.context = context;
        this.executorContext = executorContext;
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
    private Map<Versionstamp, ByteBuffer> readEntriesFromVolume(Prefix prefix, Map<Integer, IndexEntry> entries) throws IOException {
        Map<Versionstamp, ByteBuffer> result = new LinkedHashMap<>();
        for (Map.Entry<Integer, IndexEntry> entry : entries.entrySet()) {
            IndexEntry indexEntry = entry.getValue();
            ByteBuffer buffer = executorContext.shard().volume().get(prefix, indexEntry.index().versionstamp(), indexEntry.metadata());
            if (buffer == null) {
                // Kill the query, something went seriously wrong.
                throw new KronotopException(String.format("Indexed entry could not be found in volume: '%s', Versionstamp: '%s'",
                        executorContext.shard().volume().getConfig().name(),
                        VersionstampUtils.base32HexEncode(indexEntry.index().versionstamp())
                ));
            }
            result.put(indexEntry.index().versionstamp(), buffer);
        }
        return result;
    }

    /**
     * Performs a physical full scan operation on the provided bucket subspace using the specified transaction
     * and prefix. This method determines the appropriate scan approach based on the operator type of
     * the {@code PhysicalFullScan} object and retrieves matching entries from the index or underlying volume.
     *
     * @param tr               the transaction context in which the scan is executed
     * @param subspace         the bucket subspace containing the index and related metadata
     * @param prefix           the prefix representing the namespace and shard key space for the scan
     * @param physicalFullScan the object detailing the full scan operation, including operator type and criteria
     * @return a map of version stamps to ByteBuffer objects containing the results of the scan
     * @throws IOException if an I/O error occurs during the scan process
     */
    private Map<Versionstamp, ByteBuffer> doPhysicalFullScan(Transaction tr, BucketSubspace subspace, Prefix prefix, PhysicalFullScan physicalFullScan) throws IOException {
        // TODO: Review this
        Subspace indexSubspace = subspace.getBucketIndexSubspace(executorContext.shard().id(), prefix);
        KeySelector begin = KeySelector.firstGreaterOrEqual(indexSubspace.pack(IndexBuilder.beginningOfIndexRange(DefaultIndex.ID)));
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(indexSubspace.pack(IndexBuilder.beginningOfIndexRange(DefaultIndex.ID))));
        IndexRange range = new IndexRange(begin, end);
        Map<Integer, IndexEntry> entries = getEntriesFromIndex(tr, indexSubspace, range);
        return readEntriesFromVolume(prefix, entries);
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
    private Map<Integer, IndexEntry> getEntriesFromIndex(Transaction tr, Subspace indexSubspace, IndexRange range) {
        Map<Integer, IndexEntry> result = new LinkedHashMap<>();
        for (KeyValue keyValue : tr.getRange(range.begin(), range.end())) {
            UnpackedIndex unpackedIndex = IndexBuilder.unpackIndex(indexSubspace, keyValue.getKey());
            EntryMetadata metadata = EntryMetadata.decode(ByteBuffer.wrap(keyValue.getValue()));
            result.put(metadata.id(), new IndexEntry(unpackedIndex, metadata));
        }
        return result;
    }

    private byte[] beginningOfIndexRange(Subspace indexSubspace, Index index) {
        return indexSubspace.pack(IndexBuilder.beginningOfIndexRange(index));
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
    private IndexRange getIndexRange(PhysicalIndexScan physicalIndexScan, Subspace indexSubspace) {
        KeySelector begin = null;
        Bound lower = physicalIndexScan.getBounds().lower();
        if (lower != null) {
            Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(lower.bqlValue().value());
            byte[] key = indexSubspace.pack(tuple);
            if (lower.type().equals(OperatorType.GT)) {
                begin = KeySelector.firstGreaterThan(key);
            } else if (lower.type().equals(OperatorType.GTE)) {
                begin = KeySelector.firstGreaterOrEqual(key);
            } else if (lower.type().equals(OperatorType.EQ)) {
                begin = KeySelector.firstGreaterOrEqual(key);
            }
        } else {
            begin = KeySelector.firstGreaterOrEqual(beginningOfIndexRange(indexSubspace, physicalIndexScan.getIndex()));
        }

        KeySelector end = null;
        Bound upper = physicalIndexScan.getBounds().upper();
        if (upper == null) {
            end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(beginningOfIndexRange(indexSubspace, physicalIndexScan.getIndex())));
        } else {
            Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(upper.bqlValue().value());
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

    /**
     * Executes a physical index scan on the specified subspace, retrieving and reading entries
     * from the index and the associated volume. This operation identifies an index subspace,
     * determines an index range, retrieves index entries, and reads the corresponding data from the volume.
     *
     * @param tr                the transaction context in which the operation is executed
     * @param subspace          the bucket subspace containing the bucket index and related metadata
     * @param prefix            the prefix representing the namespace and shard key space for the operation
     * @param physicalIndexScan the physical index scan operation containing details of the index to be scanned
     * @return a map of version stamps to ByteBuffer objects representing the results of the index scan
     * @throws IOException if an I/O error occurs during processing
     */
    private Map<Versionstamp, ByteBuffer> doPhysicalIndexScan(Transaction tr, BucketSubspace subspace, Prefix prefix, PhysicalIndexScan physicalIndexScan) throws IOException {
        Subspace indexSubspace = subspace.getBucketIndexSubspace(executorContext.shard().id(), prefix);
        IndexRange range = getIndexRange(physicalIndexScan, indexSubspace);
        Map<Integer, IndexEntry> entries = getEntriesFromIndex(tr, indexSubspace, range);
        return readEntriesFromVolume(prefix, entries);
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
        Prefix prefix = BucketPrefix.getOrSetBucketPrefix(context, tr, executorContext.subspace(), executorContext.bucket());
        PhysicalNode plan = executorContext.plan();
        return switch (plan) {
            case PhysicalFullScan physicalFullScan ->
                    doPhysicalFullScan(tr, executorContext.subspace(), prefix, physicalFullScan);
            case PhysicalIndexScan physicalIndexScan ->
                    doPhysicalIndexScan(tr, executorContext.subspace(), prefix, physicalIndexScan);
            default -> throw new IllegalStateException("Unexpected value: " + plan);
        };
    }
}
