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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketLocateMessage;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.network.Address;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;

@Command(BucketLocateMessage.COMMAND)
@MinimumParameterCount(BucketLocateMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketLocateMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketLocateHandler extends AbstractBucketHandler implements Handler {

    public BucketLocateHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETLOCATE).set(new BucketLocateMessage(request));
    }

    /**
     * Renders the member table: the member id followed by its external advertise addresses.
     * Each member is written once, no matter how many shards it owns.
     */
    private List<RedisMessage> renderMembers(Map<String, Member> members) {
        List<RedisMessage> result = new ArrayList<>(members.size() * 2);
        for (Map.Entry<String, Member> entry : members.entrySet()) {
            result.add(bulkString(entry.getKey()));

            List<Address> advertise = entry.getValue().getExternalAdvertise();
            List<RedisMessage> addresses = new ArrayList<>(advertise.size());
            for (Address address : advertise) {
                addresses.add(bulkString(address.toString()));
            }
            result.add(new ArrayRedisMessage(addresses));
        }
        return result;
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketLocateMessage message = request.attr(MessageTypes.BUCKETLOCATE).get();
            List<RedisMessage> routes = new ArrayList<>();
            Map<String, Member> members = new LinkedHashMap<>();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), message.getBucket());
                for (int shardId : metadata.shards()) {
                    Route route = service.findRoute(shardId);
                    if (route == null) {
                        // we can't locate the shard
                        continue;
                    }
                    routes.add(new IntegerRedisMessage(shardId));

                    Member primary = route.primary();
                    members.putIfAbsent(primary.getId(), primary);
                    routes.add(bulkString(primary.getId()));

                    List<RedisMessage> standbyIds = new ArrayList<>();
                    for (Member standby : route.standbys()) {
                        members.putIfAbsent(standby.getId(), standby);
                        standbyIds.add(bulkString(standby.getId()));
                    }
                    routes.add(new ArrayRedisMessage(standbyIds));
                }
            }
            return List.of(
                    (RedisMessage) new ArrayRedisMessage(routes),
                    new ArrayRedisMessage(renderMembers(members))
            );
        }, response::writeArray);
    }
}
