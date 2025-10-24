/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketIndexMessage;
import com.kronotop.bucket.handlers.protocol.BucketIndexSubcommand;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;

@Command(BucketIndexMessage.COMMAND)
@MinimumParameterCount(BucketIndexMessage.MINIMUM_PARAMETER_COUNT)
public class BucketIndexHandler extends AbstractBucketHandler implements Handler {
    private final EnumMap<BucketIndexSubcommand, SubcommandHandler> handlers = new EnumMap<>(BucketIndexSubcommand.class);

    public BucketIndexHandler(BucketService service) {
        super(service);

        handlers.put(BucketIndexSubcommand.TASKS, new BucketIndexTasksSubcommand(context));
        handlers.put(BucketIndexSubcommand.CREATE, new BucketIndexCreateSubcommand(context));
        handlers.put(BucketIndexSubcommand.LIST, new BucketIndexListSubcommand(context));
        handlers.put(BucketIndexSubcommand.DESCRIBE, new BucketIndexDescribeSubcommand(context));
        handlers.put(BucketIndexSubcommand.DROP, new BucketIndexDropSubcommand(context));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETINDEX).set(new BucketIndexMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketIndexMessage message = request.attr(MessageTypes.BUCKETINDEX).get();
        SubcommandHandler executor = handlers.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
