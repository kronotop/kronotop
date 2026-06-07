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

package com.kronotop.stash.handlers.hash;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.stash.StashService;
import com.kronotop.stash.handlers.hash.protocol.FieldValuePair;
import com.kronotop.stash.handlers.hash.protocol.HSetNXMessage;
import com.kronotop.stash.storage.StashShard;
import com.kronotop.stash.storage.StashValueContainer;
import com.kronotop.stash.storage.StashValueKind;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.stash.StashService.checkStashValueKind;

@Command(HSetNXMessage.COMMAND)
@MaximumParameterCount(HSetNXMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(HSetNXMessage.MINIMUM_PARAMETER_COUNT)
public class HSetNXHandler extends BaseHashHandler implements Handler {
    public HSetNXHandler(StashService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HSETNX).set(new HSetNXMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(request.attr(MessageTypes.HSETNX).get().getKey());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HSetNXMessage message = request.attr(MessageTypes.HSETNX).get();

        StashShard shard = service.findShard(message.getKey(), ShardStatus.READWRITE);
        ReadWriteLock lock = shard.striped().get(message.getKey());
        lock.writeLock().lock();
        int result = 0;
        try {
            HashValue hashValue;
            StashValueContainer previous = shard.storage().get(message.getKey());
            if (previous == null) {
                hashValue = new HashValue();
                shard.storage().put(message.getKey(), new StashValueContainer(hashValue));
            } else {
                checkStashValueKind(previous, StashValueKind.HASH);
                hashValue = previous.hash();
            }
            FieldValuePair fieldValuePair = message.getFieldValuePairs().getFirst();
            boolean exists = hashValue.containsKey(fieldValuePair.getField());
            if (!exists) {
                hashValue.put(fieldValuePair.getField(), fieldValuePair.getValue());
                syncHashField(shard, message.getKey(), fieldValuePair);
                result = 1;
            }
        } finally {
            lock.writeLock().unlock();
        }

        response.writeInteger(result);
    }
}
