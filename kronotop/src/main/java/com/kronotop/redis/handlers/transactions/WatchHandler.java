/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.handlers.transactions;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.transactions.protocol.WatchMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import io.netty.util.Attribute;

import java.util.HashMap;
import java.util.List;

@Command(WatchMessage.COMMAND)
@MinimumParameterCount(WatchMessage.MINIMUM_PARAMETER_COUNT)
public class WatchHandler implements Handler {
    private final RedisService service;

    public WatchHandler(RedisService service) {
        this.service = service;
    }

    @Override
    public List<String> getKeys(Request request) {
        return request.attr(MessageTypes.WATCH).get().getKeys();
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.WATCH).set(new WatchMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        Session session = request.getSession();
        Attribute<HashMap<String, Long>> watchedKeysAttr = session.attr(SessionAttributes.WATCHED_KEYS);
        HashMap<String, Long> watchedKeys = watchedKeysAttr.get();
        if (watchedKeys == null) {
            watchedKeys = new HashMap<>();
            watchedKeysAttr.set(watchedKeys);
        }

        WatchMessage watchMessage = request.attr(MessageTypes.WATCH).get();
        for (String key : watchMessage.getKeys()) {
            watchedKeys.compute(key, (k, version) -> {
                if (version == null) {
                    // Watch the key here.
                    return service.getWatcher().watchKey(session.getClientId(), key);
                }
                // Already watched.
                return version;
            });
        }
        response.writeOK();
    }
}

