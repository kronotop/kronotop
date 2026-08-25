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

import com.kronotop.KronotopException;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.task.ObservedTask;
import com.kronotop.task.TaskService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.server.RESPUtil.booleanMessage;
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

    /**
     * Builds the fields of a single task. The field order is the same for both protocol versions.
     */
    private Map<RedisMessage, RedisMessage> fields(ObservedTask task, RESPVersion version) {
        Map<RedisMessage, RedisMessage> item = new LinkedHashMap<>();
        item.put(wrapBytes(RUNNING_BYTES), booleanMessage(task.running(), version));
        item.put(wrapBytes(FINISHED_BYTES), booleanMessage(task.finished(), version));
        item.put(wrapBytes(STARTED_AT_BYTES), new IntegerRedisMessage(task.startedAt()));
        item.put(wrapBytes(LAST_RUN_BYTES), new IntegerRedisMessage(task.lastRun()));
        return item;
    }

    @Override
    public void execute(Request request, Response response) {
        RESPVersion protoVer = request.getSession().protocolVersion();
        if (protoVer.equals(RESPVersion.RESP3)) {
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            service.tasks().forEach(task ->
                    result.put(bulkString(task.name()), new MapRedisMessage(fields(task, protoVer)))
            );
            response.writeMap(result);
        } else if (protoVer.equals(RESPVersion.RESP2)) {
            List<RedisMessage> result = new ArrayList<>();
            service.tasks().forEach(task -> {
                List<RedisMessage> item = new ArrayList<>();
                fields(task, protoVer).forEach((key, value) -> {
                    item.add(key);
                    item.add(value);
                });
                result.add(bulkString(task.name()));
                result.add(new ArrayRedisMessage(item));
            });
            response.writeArray(result);
        } else {
            throw new KronotopException("Unknown protocol version " + protoVer.getValue());
        }
    }
}
