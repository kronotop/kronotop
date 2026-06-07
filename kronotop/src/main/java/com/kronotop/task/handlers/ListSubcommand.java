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

package com.kronotop.task.handlers;

import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.BooleanRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.task.TaskService;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.wrapBytes;

public class ListSubcommand extends BaseHandler implements SubcommandHandler {
    private static final byte[] RUNNING_BYTES = "running".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FINISHED_BYTES = "finished".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STARTED_AT_BYTES = "started_at".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LAST_RUN_BYTES = "last_run".getBytes(StandardCharsets.UTF_8);

    public ListSubcommand(TaskService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        service.tasks().forEach(task -> {
            Map<RedisMessage, RedisMessage> item = new LinkedHashMap<>();
            item.put(
                    wrapBytes(RUNNING_BYTES),
                    task.running() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
            );
            item.put(
                    wrapBytes(FINISHED_BYTES),
                    task.finished() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
            );
            item.put(wrapBytes(STARTED_AT_BYTES), new IntegerRedisMessage(task.startedAt()));
            item.put(wrapBytes(LAST_RUN_BYTES), new IntegerRedisMessage(task.lastRun()));
            result.put(bulkString(task.name()), new MapRedisMessage(item));
        });
        response.writeMap(result);
    }
}
