// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketListIndexesMessage;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.*;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketListIndexesMessage.COMMAND)
@MinimumParameterCount(BucketListIndexesMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketListIndexesMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketListIndexesHandler extends BaseBucketHandler implements Handler {
    public BucketListIndexesHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETLISTINDEXES).set(new BucketListIndexesMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketListIndexesMessage message = request.attr(MessageTypes.BUCKETLISTINDEXES).get();
            Session session = request.getSession();
            List<RedisMessage> children = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());
                List<String> names = IndexUtil.list(tr, metadata.subspace());
                for (String name : names) {
                    Map<RedisMessage, RedisMessage> item = new LinkedHashMap<>();
                    item.put(new SimpleStringRedisMessage("name"), new SimpleStringRedisMessage(name));
                    children.add(new MapRedisMessage(item));
                }
            }
            return children;
        }, response::writeArray);
    }
}
