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

package com.kronotop.task.handlers;

import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.task.TaskService;
import com.kronotop.task.handlers.protocol.TaskAdminMessage;
import com.kronotop.task.handlers.protocol.TaskAdminSubcommand;

import java.util.EnumMap;

@Command(TaskAdminMessage.COMMAND)
@MinimumParameterCount(TaskAdminMessage.MINIMUM_PARAMETER_COUNT)
public class TaskAdminHandler extends BaseHandler implements Handler {
    private final EnumMap<TaskAdminSubcommand, SubcommandHandler> handlers = new EnumMap<>(TaskAdminSubcommand.class);

    public TaskAdminHandler(TaskService service) {
        super(service);

        handlers.put(TaskAdminSubcommand.LIST, new ListSubcommand(service));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.TASKADMIN).set(new TaskAdminMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        TaskAdminMessage message = request.attr(MessageTypes.TASKADMIN).get();
        SubcommandHandler executor = handlers.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
