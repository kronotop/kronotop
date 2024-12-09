/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.redis.handlers.generic;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.redis.NoAvailableShardException;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.generic.protocol.ScanMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.index.Projection;
import com.kronotop.redis.storage.index.impl.FlakeIdGenerator;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(ScanMessage.COMMAND)
@MinimumParameterCount(ScanMessage.MINIMUM_PARAMETER_COUNT)
public class ScanHandler extends BaseHandler implements Handler {
    public ScanHandler(RedisService service) {
        super(service);
    }

    private List<RedisMessage> prepareResponse(Response response, long cursor, List<RedisMessage> children) {
        List<RedisMessage> parent = new ArrayList<>();
        ByteBuf buf = response.getChannelContext().alloc().buffer();
        parent.add(new FullBulkStringRedisMessage(buf.writeBytes(Long.toString(cursor).getBytes())));
        parent.add(new ArrayRedisMessage(children));
        return parent;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SCAN).set(new ScanMessage(request));
    }

    private int findHostedShardId(int initial) {
        for (int shardId = initial; shardId < service.getNumberOfShards(); shardId++) {
            RedisShard shard = service.getServiceContext().shards().get(shardId);
            if (shard == null || shard.storage().isEmpty()) {
                continue;
            }
            return shardId;
        }
        throw new NoAvailableShardException();
    }

    @Override
    public void execute(Request request, Response response) {
        ScanMessage scanMessage = request.attr(MessageTypes.SCAN).get();

        int shardId;
        if (scanMessage.getCursor() == 0) {
            try {
                shardId = findHostedShardId(0);
            } catch (NoAvailableShardException e) {
                response.writeArray(prepareResponse(response, 0, new ArrayList<>()));
                return;
            }
        } else {
            long[] parsedCursor = FlakeIdGenerator.parse(scanMessage.getCursor());
            // This will never overflow. Maximum shard id is 2**14;
            shardId = Math.toIntExact(parsedCursor[0]);
        }

        RedisShard shard = service.findShard(shardId, ShardStatus.READONLY);
        List<RedisMessage> children = new ArrayList<>();

        Projection projection = shard.index().getProjection(scanMessage.getCursor(), scanMessage.getCount());
        if (projection.getKeys().isEmpty()) {
            response.writeArray(prepareResponse(response, projection.getCursor(), children));
            return;
        }

        Iterable<ReadWriteLock> locks = shard.striped().bulkGet(projection.getKeys());
        for (ReadWriteLock lock : locks) {
            lock.readLock().lock();
        }
        try {
            for (String key : projection.getKeys()) {
                if (shard.storage().containsKey(key)) {
                    ByteBuf buf = response.getChannelContext().alloc().buffer();
                    buf.writeBytes(key.getBytes());
                    children.add(new FullBulkStringRedisMessage(buf));
                }
            }
        } finally {
            for (ReadWriteLock lock : locks) {
                lock.readLock().unlock();
            }
        }

        if (projection.getCursor() == 0) {
            try {
                int nextShardId = findHostedShardId(shardId + 1);
                RedisShard nextShard = service.findShard(nextShardId, ShardStatus.READONLY);
                if (nextShard != null) {
                    response.writeArray(prepareResponse(response, nextShard.index().head(), children));
                    return;
                }
            } catch (NoAvailableShardException e) {
                response.writeArray(prepareResponse(response, 0, children));
                return;
            }
        }

        response.writeArray(prepareResponse(response, projection.getCursor(), children));
    }
}
