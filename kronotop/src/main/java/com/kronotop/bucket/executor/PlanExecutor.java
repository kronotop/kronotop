// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketPrefix;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.bucket.index.UnpackedIndex;
import com.kronotop.bucket.planner.physical.PhysicalFullScan;
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
import java.util.Objects;

public class PlanExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlanExecutor.class);
    private final Context context;
    private final ExecutorContext executorContext;

    public PlanExecutor(Context context, ExecutorContext executorContext) {
        this.context = context;
        this.executorContext = executorContext;
    }

    private Map<Versionstamp, ByteBuffer> doPhysicalFullScan(Transaction tr, BucketSubspace subspace, Prefix prefix, PhysicalFullScan physicalFullScan) throws IOException {
        Map<Versionstamp, ByteBuffer> result = new LinkedHashMap<>();
        if (Objects.isNull(physicalFullScan.getOperatorType())) {
            // There is no operator in the plan. Do a physical full scan on the given bucket.
            Subspace indexSubspace = subspace.getBucketIndexSubspace(executorContext.shard().id(), prefix);
            Range range = new Range(indexSubspace.pack(), ByteArrayUtil.strinc(indexSubspace.pack()));
            for (KeyValue keyValue : tr.getRange(range)) {
                UnpackedIndex unpackedIndex = IndexBuilder.unpackIndex(indexSubspace, keyValue.getKey());
                EntryMetadata metadata = EntryMetadata.decode(ByteBuffer.wrap(keyValue.getValue()));
                ByteBuffer buffer = executorContext.shard().volume().get(prefix, unpackedIndex.versionstamp(), metadata);
                if (buffer == null) {
                    LOGGER.error("Indexed entry could not be found in volume: {}, Versionstamp: {}",
                            executorContext.shard().volume().getConfig().name(),
                            VersionstampUtils.base32HexEncode(unpackedIndex.versionstamp())
                    );
                    continue;
                }
                result.put(unpackedIndex.versionstamp(), buffer);
            }
        }
        return result;
    }

    public Map<Versionstamp, ByteBuffer> execute(Transaction tr) throws IOException {
        Prefix prefix = BucketPrefix.getOrSetBucketPrefix(context, tr, executorContext.subspace(), executorContext.bucket());
        PhysicalNode plan = executorContext.plan();
        switch (plan) {
            case PhysicalFullScan physicalFullScan:
                return doPhysicalFullScan(tr, executorContext.subspace(), prefix, physicalFullScan);
            default:
                throw new IllegalStateException("Unexpected value: " + plan);
        }
    }
}
