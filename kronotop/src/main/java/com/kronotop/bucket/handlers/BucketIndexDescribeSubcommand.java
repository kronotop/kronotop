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
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.booleanMessage;
import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.formatMapMessage;

class BucketIndexDescribeSubcommand implements SubcommandHandler {
    private final Context context;

    BucketIndexDescribeSubcommand(Context context) {
        this.context = context;
    }

    private static RedisMessage getCollation(Collation collation, RESPVersion version) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        if (collation == null) {
            return formatMapMessage(map, version);
        }

        map.put(bulkString("locale"), bulkString(collation.locale()));
        map.put(bulkString("strength"), new IntegerRedisMessage(collation.strength()));
        map.put(bulkString("case_level"), booleanMessage(collation.caseLevel(), version));
        map.put(bulkString("case_first"), bulkString(collation.caseFirst()));
        map.put(bulkString("numeric_ordering"), booleanMessage(collation.numericOrdering(), version));
        map.put(bulkString("alternate"), bulkString(collation.alternate()));
        map.put(bulkString("backwards"), booleanMessage(collation.backwards(), version));
        map.put(bulkString("normalization"), booleanMessage(collation.normalization(), version));
        map.put(bulkString("max_variable"), bulkString(collation.maxVariable()));
        return formatMapMessage(map, version);
    }

    private static RedisMessage getStatistics(IndexStatistics statistics, RESPVersion version) {
        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(bulkString("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        return formatMapMessage(stats, version);
    }

    private static RedisMessage getSingleFieldDescription(SingleFieldIndexDefinition definition, IndexStatistics statistics, RESPVersion version) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(bulkString("index_type"), bulkString("single_field"));
        description.put(bulkString("id"), new IntegerRedisMessage(definition.id()));
        description.put(bulkString("selector"), bulkString(definition.selector()));
        description.put(bulkString("bson_type"), bulkString(definition.bsonType().name()));
        description.put(bulkString("status"), bulkString(definition.status().name()));
        description.put(bulkString("unique"), booleanMessage(definition.unique(), version));
        description.put(bulkString("collation"), getCollation(definition.collation(), version));
        description.put(bulkString("statistics"), getStatistics(statistics, version));
        return formatMapMessage(description, version);
    }

    private static RedisMessage getVectorDescription(VectorIndexDefinition definition, IndexStatistics statistics, RESPVersion version) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(bulkString("index_type"), bulkString("vector"));
        description.put(bulkString("id"), new IntegerRedisMessage(definition.id()));
        description.put(bulkString("selector"), bulkString(definition.selector()));
        description.put(bulkString("dimensions"), new IntegerRedisMessage(definition.dimensions()));
        description.put(bulkString("distance"), bulkString(definition.distance().name()));
        description.put(bulkString("status"), bulkString(definition.status().name()));
        description.put(bulkString("statistics"), getStatistics(statistics, version));
        return formatMapMessage(description, version);
    }

    private static RedisMessage getCompoundDescription(CompoundIndexDefinition definition, IndexStatistics statistics, RESPVersion version) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(bulkString("index_type"), bulkString("compound"));
        description.put(bulkString("id"), new IntegerRedisMessage(definition.id()));

        List<RedisMessage> fieldsList = new ArrayList<>();
        for (CompoundIndexField field : definition.fields()) {
            Map<RedisMessage, RedisMessage> fieldMap = new LinkedHashMap<>();
            fieldMap.put(bulkString("selector"), bulkString(field.selector()));
            fieldMap.put(bulkString("bson_type"), bulkString(field.bsonType().name()));
            fieldsList.add(formatMapMessage(fieldMap, version));
        }
        description.put(bulkString("fields"), new ArrayRedisMessage(fieldsList));

        description.put(bulkString("status"), bulkString(definition.status().name()));
        description.put(bulkString("unique"), booleanMessage(definition.unique(), version));
        description.put(bulkString("collation"), getCollation(definition.collation(), version));
        description.put(bulkString("statistics"), getStatistics(statistics, version));
        return formatMapMessage(description, version);
    }

    @Override
    public void execute(Request request, Response response) {
        DescribeParameters parameters = new DescribeParameters(request.getParams());
        supplyAsync(context, response, () -> {
            Session session = request.getSession();
            RESPVersion protoVer = session.protocolVersion();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, parameters.bucket);

                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                if (vectorIndex != null) {
                    VectorIndexDefinition definition = VectorIndexUtil.loadIndexDefinition(tr, vectorIndex.subspace());
                    IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                    return getVectorDescription(definition, statistics, protoVer);
                }

                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                if (compoundIndex != null) {
                    CompoundIndexDefinition definition = CompoundIndexUtil.loadIndexDefinition(tr, compoundIndex.subspace());
                    IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                    return getCompoundDescription(definition, statistics, protoVer);
                }

                DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), parameters.index);
                SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, indexSubspace);
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                return getSingleFieldDescription(definition, statistics, protoVer);
            }
        }, response::writeRedisMessage);
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
