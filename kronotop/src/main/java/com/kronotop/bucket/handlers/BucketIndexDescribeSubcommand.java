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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

class BucketIndexDescribeSubcommand implements SubcommandHandler {
    private final Context context;

    BucketIndexDescribeSubcommand(Context context) {
        this.context = context;
    }

    private static FullBulkStringRedisMessage bulkString(String input) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(input.getBytes(StandardCharsets.UTF_8)));
    }

    private static MapRedisMessage getCollationMap(Collation collation) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        if (collation == null) {
            map.put(bulkString("locale"), NullRedisMessage.INSTANCE);
            map.put(bulkString("strength"), NullRedisMessage.INSTANCE);
            map.put(bulkString("case_level"), NullRedisMessage.INSTANCE);
            map.put(bulkString("case_first"), NullRedisMessage.INSTANCE);
            map.put(bulkString("numeric_ordering"), NullRedisMessage.INSTANCE);
            map.put(bulkString("alternate"), NullRedisMessage.INSTANCE);
            map.put(bulkString("backwards"), NullRedisMessage.INSTANCE);
            map.put(bulkString("normalization"), NullRedisMessage.INSTANCE);
            map.put(bulkString("max_variable"), NullRedisMessage.INSTANCE);
        } else {
            map.put(bulkString("locale"), bulkString(collation.locale()));
            map.put(bulkString("strength"), new IntegerRedisMessage(collation.strength()));
            map.put(bulkString("case_level"), collation.caseLevel() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
            map.put(bulkString("case_first"), bulkString(collation.caseFirst()));
            map.put(bulkString("numeric_ordering"), collation.numericOrdering() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
            map.put(bulkString("alternate"), bulkString(collation.alternate()));
            map.put(bulkString("backwards"), collation.backwards() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
            map.put(bulkString("normalization"), collation.normalization() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
            map.put(bulkString("max_variable"), bulkString(collation.maxVariable()));
        }
        return new MapRedisMessage(map);
    }

    private static Map<RedisMessage, RedisMessage> getSingleFieldDescription(SingleFieldIndexDefinition definition, IndexStatistics statistics) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(bulkString("index_type"), bulkString("single_field"));
        description.put(bulkString("id"), new IntegerRedisMessage(definition.id()));
        description.put(bulkString("selector"), bulkString(definition.selector()));
        description.put(bulkString("bson_type"), bulkString(definition.bsonType().name()));
        description.put(bulkString("status"), bulkString(definition.status().name()));
        description.put(bulkString("collation"), getCollationMap(definition.collation()));

        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(bulkString("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        description.put(bulkString("statistics"), new MapRedisMessage(stats));

        return description;
    }

    private static Map<RedisMessage, RedisMessage> getVectorDescription(VectorIndexDefinition definition, IndexStatistics statistics) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(bulkString("index_type"), bulkString("vector"));
        description.put(bulkString("id"), new IntegerRedisMessage(definition.id()));
        description.put(bulkString("selector"), bulkString(definition.selector()));
        description.put(bulkString("dimensions"), new IntegerRedisMessage(definition.dimensions()));
        description.put(bulkString("distance"), bulkString(definition.distance().name()));
        description.put(bulkString("status"), bulkString(definition.status().name()));

        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(bulkString("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        description.put(bulkString("statistics"), new MapRedisMessage(stats));

        return description;
    }

    private static Map<RedisMessage, RedisMessage> getCompoundDescription(CompoundIndexDefinition definition, IndexStatistics statistics) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(bulkString("index_type"), bulkString("compound"));
        description.put(bulkString("id"), new IntegerRedisMessage(definition.id()));

        List<RedisMessage> fieldsList = new ArrayList<>();
        for (CompoundIndexField field : definition.fields()) {
            Map<RedisMessage, RedisMessage> fieldMap = new LinkedHashMap<>();
            fieldMap.put(bulkString("selector"), bulkString(field.selector()));
            fieldMap.put(bulkString("bson_type"), bulkString(field.bsonType().name()));
            fieldsList.add(new MapRedisMessage(fieldMap));
        }
        description.put(bulkString("fields"), new ArrayRedisMessage(fieldsList));

        description.put(bulkString("status"), bulkString(definition.status().name()));
        description.put(bulkString("collation"), getCollationMap(definition.collation()));

        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(bulkString("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        description.put(bulkString("statistics"), new MapRedisMessage(stats));

        return description;
    }

    @Override
    public void execute(Request request, Response response) {
        DescribeParameters parameters = new DescribeParameters(request.getParams());
        supplyAsync(context, response, () -> {
            Session session = request.getSession();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, parameters.bucket);

                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                if (vectorIndex != null) {
                    VectorIndexDefinition definition = VectorIndexUtil.loadIndexDefinition(tr, vectorIndex.subspace());
                    IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                    return getVectorDescription(definition, statistics);
                }

                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                if (compoundIndex != null) {
                    CompoundIndexDefinition definition = CompoundIndexUtil.loadIndexDefinition(tr, compoundIndex.subspace());
                    IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                    return getCompoundDescription(definition, statistics);
                }

                DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), parameters.index);
                SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, indexSubspace);
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                return getSingleFieldDescription(definition, statistics);
            }
        }, response::writeMap);
    }

    private static class DescribeParameters {
        private final String bucket;
        private final String index;

        DescribeParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
            index = ProtocolMessageUtil.readAsString(params.get(2));
        }
    }
}
