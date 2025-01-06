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

package com.kronotop.task.handlers;

import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.task.TaskService;

import java.util.LinkedHashMap;
import java.util.Map;

public class ListSubcommand extends BaseHandler implements SubcommandHandler {

    public ListSubcommand(TaskService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        service.tasks().forEach(task -> {
            Map<RedisMessage, RedisMessage> item = new LinkedHashMap<>();
            item.put(
                    new SimpleStringRedisMessage("running"),
                    task.running() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
            );
            item.put(
                    new SimpleStringRedisMessage("completed"),
                    task.completed() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
            );
            item.put(new SimpleStringRedisMessage("started_at"), new IntegerRedisMessage(task.startedAt()));
            item.put(new SimpleStringRedisMessage("last_run"), new IntegerRedisMessage(task.lastRun()));
            result.put(new SimpleStringRedisMessage(task.name()), new MapRedisMessage(item));
        });
        response.writeMap(result);
    }
}
