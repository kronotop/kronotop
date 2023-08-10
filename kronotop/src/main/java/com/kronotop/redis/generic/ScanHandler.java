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
import com.kronotop.redis.RedisService;
import com.kronotop.redis.generic.protocol.ScanMessage;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.index.Projection;
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

    private List<RedisMessage> prepareResponse(Projection projection, List<RedisMessage> children) {
        List<RedisMessage> parent = new ArrayList<>();
        // TODO: Cursor has to be a bulk string message
        parent.add(new IntegerRedisMessage(projection.getCursor()));
        parent.add(new ArrayRedisMessage(children));
        return parent;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SCAN).set(new ScanMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        ScanMessage scanMessage = request.attr(MessageTypes.SCAN).get();

        LogicalDatabase storage = getLogicalDatabase(response.getContext());

        List<RedisMessage> children = new ArrayList<>();
        Projection projection = storage.getIndex().getProjection(scanMessage.getCursor(), 10);
        if (projection.getKeys().size() == 0) {
            response.writeArray(prepareResponse(projection, children));
            return;
        }

        Iterable<ReadWriteLock> locks = storage.getStriped().bulkGet(projection.getKeys());
        try {
            for (ReadWriteLock lock : locks) {
                lock.readLock().lock();
            }
            for (String key : projection.getKeys()) {
                if (storage.containsKey(key)) {
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

        response.writeArray(prepareResponse(projection, children));
    }
}
