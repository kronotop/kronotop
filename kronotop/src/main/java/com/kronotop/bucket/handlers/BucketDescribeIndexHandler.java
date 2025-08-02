// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketDescribeIndexMessage;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketDescribeIndexMessage.COMMAND)
@MinimumParameterCount(BucketDescribeIndexMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketDescribeIndexMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketDescribeIndexHandler extends BaseBucketHandler implements Handler {
    public BucketDescribeIndexHandler(BucketService service) {
        super(service);
    }

    private static Map<RedisMessage, RedisMessage> getRedisMessageRedisMessageMap(IndexDefinition definition, IndexStatistics statistics) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(
                new SimpleStringRedisMessage("field"),
                new SimpleStringRedisMessage(definition.field())
        );
        description.put(
                new SimpleStringRedisMessage("bson_type"),
                new SimpleStringRedisMessage(definition.bsonType().name())
        );
        description.put(
                new SimpleStringRedisMessage("sort_order"),
                new SimpleStringRedisMessage(definition.sortOrder().name())
        );

        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(new SimpleStringRedisMessage("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        description.put(new SimpleStringRedisMessage("statistics"), new MapRedisMessage(stats));

        return description;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETDESCRIBEINDEX).set(new BucketDescribeIndexMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketDescribeIndexMessage message = request.attr(MessageTypes.BUCKETDESCRIBEINDEX).get();
            Session session = request.getSession();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());

                DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), message.getIndex());
                IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, indexSubspace, definition.id());
                return getRedisMessageRedisMessageMap(definition, statistics);
            }
        }, response::writeMap);
    }
}
