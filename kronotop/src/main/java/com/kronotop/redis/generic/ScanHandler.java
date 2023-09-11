/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.redis.generic;

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.NoAvailablePartitionException;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.generic.protocol.ScanMessage;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.index.Projection;
import com.kronotop.redis.storage.index.impl.FlakeIdGenerator;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@Command(ScanMessage.COMMAND)
@MinimumParameterCount(ScanMessage.MINIMUM_PARAMETER_COUNT)
public class ScanHandler extends BaseHandler implements Handler {
    public ScanHandler(RedisService service) {
        super(service);
    }

    private List<RedisMessage> prepareResponse(long cursor, List<RedisMessage> children) {
        List<RedisMessage> parent = new ArrayList<>();
        // TODO: Cursor has to be a bulk string message
        parent.add(new IntegerRedisMessage(cursor));
        parent.add(new ArrayRedisMessage(children));
        return parent;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SCAN).set(new ScanMessage(request));
    }

    private int findPartitionId(Response response, int initial) {
        LogicalDatabase logicalDatabase = service.getLogicalDatabase(getCurrentLogicalDatabaseIndex(response.getContext()));
        for (int partitionId = initial; partitionId < service.getPartitionCount(); partitionId++) {
            Partition partition = logicalDatabase.getPartitions().get(partitionId);
            if (partition == null || partition.isEmpty()) {
                continue;
            }
            return partitionId;
        }
        throw new NoAvailablePartitionException();
    }

    @Override
    public void execute(Request request, Response response) {
        ScanMessage scanMessage = request.attr(MessageTypes.SCAN).get();

        int partitionId;
        if (scanMessage.getCursor() == 0) {
            try {
                partitionId = findPartitionId(response, 0);
            } catch (NoAvailablePartitionException e) {
                response.writeArray(prepareResponse(0, new ArrayList<>()));
                return;
            }
        } else {
            long[] parsedCursor = FlakeIdGenerator.parse(scanMessage.getCursor());
            // This will never overflow. Maximum partition id is 2**14;
            partitionId = Math.toIntExact(parsedCursor[0]);
        }

        Partition partition = service.getPartition(getCurrentLogicalDatabaseIndex(response.getContext()), partitionId);
        List<RedisMessage> children = new ArrayList<>();

        Projection projection = partition.getIndex().getProjection(scanMessage.getCursor(), 10);
        if (projection.getKeys().isEmpty()) {
            response.writeArray(prepareResponse(projection.getCursor(), children));
            return;
        }

        Iterable<ReadWriteLock> locks = partition.getStriped().bulkGet(projection.getKeys());
        try {
            for (ReadWriteLock lock : locks) {
                lock.readLock().lock();
            }
            for (String key : projection.getKeys()) {
                if (partition.containsKey(key)) {
                    ByteBuf buf = response.getContext().alloc().buffer();
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
                int nextPartitionId = findPartitionId(response, partitionId + 1);
                Partition nextPartition = service.getPartition(getCurrentLogicalDatabaseIndex(response.getContext()), nextPartitionId);
                if (nextPartition != null) {
                    response.writeArray(prepareResponse(nextPartition.getIndex().head(), children));
                    return;
                }
            } catch (NoAvailablePartitionException e) {
                response.writeArray(prepareResponse(0, children));
                return;
            }
        }

        response.writeArray(prepareResponse(projection.getCursor(), children));
    }
}
