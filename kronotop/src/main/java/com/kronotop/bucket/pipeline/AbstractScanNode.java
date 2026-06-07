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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonType;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public abstract class AbstractScanNode extends AbstractPipelineNode implements ScanNode {

    protected AbstractScanNode(int id) {
        super(id);
    }

    /**
     * Converts a raw FoundationDB Tuple element to a typed {@link BqlValue} based on the
     * BSON type declared in the index definition.
     *
     * <p>Secondary index keys are structured as {@code BqlValue | Versionstamp}. This method
     * takes the value portion (tuple position 1) and wraps it in the matching {@link BqlValue}
     * subtype. The conversion is necessary because FoundationDB's Tuple layer uses its own
     * type system (e.g. all integers are stored as longs, Decimal128 as strings).
     *
     * <p><b>Notable type mappings:</b>
     * <ul>
     *   <li>{@code INT32} — FoundationDB stores as long; narrowed to int here</li>
     *   <li>{@code DECIMAL128} — stored as string in the Tuple layer; parsed to BigDecimal</li>
     *   <li>{@code OBJECT_ID} — stored as 12-byte array; reconstructed into {@link ObjectIdVal}</li>
     *   <li>{@code BINARY} — yields {@link VersionstampVal} when the value is a
     *       {@link Versionstamp} (primary _id index), otherwise {@link BinaryVal}</li>
     *   <li>{@code NULL} or a {@code null} value — returns {@link NullVal}</li>
     * </ul>
     *
     * <p>Used by scan nodes to construct checkpoint bounds and evaluate predicates
     * during index traversal.
     *
     * @param value    the raw value extracted from tuple position 1 (may be {@code null})
     * @param bsonType the BSON type from the index definition
     * @return the typed BqlValue wrapper for query operations
     * @throws IllegalArgumentException if the BSON type is not supported for indexing
     * @see RangeScanNode
     * @see IndexScanNode
     */
    BqlValue createBqlValueFromIndexValue(Object value, BsonType bsonType, Collation collation) {
        if (Objects.isNull(value)) {
            return new NullVal();
        }
        return switch (bsonType) {
            case OBJECT_ID -> {
                if (value instanceof byte[] bytes) {
                    yield new ObjectIdVal(new ObjectId(bytes));
                }
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid value for BSON type %s: expected byte[], but got %s",
                                bsonType,
                                value.getClass().getName()
                        )
                );
            }
            case STRING -> {
                if (collation != null && value instanceof byte[] bytes) {
                    yield new BinaryVal(bytes);
                }
                yield new StringVal((String) value);
            }
            case INT32 -> new Int32Val(((Long) value).intValue()); // Index stores INT32 as long
            case INT64 -> new Int64Val((Long) value);
            case DOUBLE -> new DoubleVal((Double) value);
            case BOOLEAN -> new BooleanVal((Boolean) value);
            case DATE_TIME -> new DateTimeVal((Long) value);
            case TIMESTAMP -> new TimestampVal((Long) value);
            case DECIMAL128 -> new Decimal128Val(new BigDecimal((String) value));
            case BINARY -> {
                if (value instanceof Versionstamp) {
                    yield new VersionstampVal((Versionstamp) value);
                } else {
                    yield new BinaryVal((byte[]) value);
                }
            }
            case NULL -> new NullVal();
            default -> throw new IllegalArgumentException("Unsupported BSON type for index value: " + bsonType);
        };
    }

    protected SingleFieldScanSetup setupSingleFieldScan(QueryContext ctx, SingleFieldIndexDefinition index) {
        ctx.setScannedIndexField(index.selector());
        ctx.setScannedIndexIsMultiKey(index.multiKey());

        Index indexRecord = ctx.metadata().indexes().getIndex(index.selector(), IndexSelectionPolicy.READ);
        if (indexRecord == null) {
            throw new IllegalStateException("Index not found for selector: " + index.selector());
        }
        DirectorySubspace indexSubspace = indexRecord.subspace();
        ExecutionState state = ctx.getOrCreateExecutionState(id());

        boolean shouldReverse = getShouldReverse(ctx, index.selector());
        state.setScanReversed(shouldReverse);
        SortDirection effectiveDirection = getEffectiveSortDirection(ctx, shouldReverse);

        return new SingleFieldScanSetup(indexSubspace, state, ctx.getParameters(), shouldReverse, effectiveDirection);
    }

    protected AsyncIterable<KeyValue> getRange(Transaction tr, QueryContext ctx,
                                               KeySelector begin, KeySelector end, int limit, boolean reverse) {
        if (ctx.isSnapshotRead()) {
            return tr.snapshot().getRange(begin, end, limit, reverse);
        }
        return tr.getRange(begin, end, limit, reverse);
    }

    boolean getShouldReverse(QueryContext ctx, String selector) {
        boolean shouldReverse = ctx.options().isReverse();
        if (ctx.options().getSortByField() != null) {
            // Only reverse the scan when the sort field matches the scanned index field
            shouldReverse = ctx.options().isReverse() && selector.equals(ctx.options().getSortByField());
        }
        return shouldReverse;
    }

    SortDirection getEffectiveSortDirection(QueryContext ctx, boolean shouldReverse) {
        return shouldReverse ? ctx.options().getSortDirection() : SortDirection.ASC;
    }

    record SingleFieldScanSetup(DirectorySubspace indexSubspace, ExecutionState state,
                                List<BqlValue> parameters, boolean shouldReverse,
                                SortDirection effectiveDirection) {
    }
}