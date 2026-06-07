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

package com.kronotop.volume.handlers;

import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.VolumeService;

import java.util.ArrayList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;

class ListSubcommand extends BaseSubcommandHandler implements SubcommandHandler {

    public ListSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        supplyAsync(context, response, () -> {
            List<RedisMessage> volumes = new ArrayList<>();
            service.list().forEach(volume -> {
                volumes.add(bulkString(volume.getConfig().name()));
            });
            return volumes;
        }, response::writeArray);

    }
}
