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
import com.kronotop.stash.handlers.BaseHandler;
import com.kronotop.stash.handlers.hash.protocol.HStrlenMessage;
import com.kronotop.stash.storage.StashShard;
import com.kronotop.stash.storage.StashValueContainer;
import com.kronotop.stash.storage.StashValueKind;

import java.util.concurrent.locks.ReadWriteLock;

import static com.kronotop.stash.StashService.checkStashValueKind;

@Command(HStrlenMessage.COMMAND)
@MinimumParameterCount(HStrlenMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(HStrlenMessage.MAXIMUM_PARAMETER_COUNT)
public class HStrlenHandler extends BaseHandler implements Handler {
    public HStrlenHandler(StashService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HSTRLEN).set(new HStrlenMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HStrlenMessage hstrlenMessage = request.attr(MessageTypes.HSTRLEN).get();

        StashShard shard = service.findShard(hstrlenMessage.getKey(), ShardStatus.READONLY);
        ReadWriteLock lock = shard.striped().get(hstrlenMessage.getKey());
        lock.readLock().lock();
        try {
            StashValueContainer container = shard.storage().get(hstrlenMessage.getKey());
            if (container == null) {
                response.writeInteger(0);
                return;
            }
            checkStashValueKind(container, StashValueKind.HASH);
            HashFieldValue hashField = container.hash().get(hstrlenMessage.getField());
            if (hashField != null) {
                response.writeInteger(hashField.value().length);
            } else {
                response.writeInteger(0);
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
