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

package com.kronotop.foundationdb.namespace;

import com.kronotop.Context;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

class CurrentSubcommand extends BaseSubcommand implements SubcommandExecutor {

    CurrentSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        String namespace = response.getChannelContext().channel().attr(ChannelAttributes.CURRENT_NAMESPACE).get();
        if (namespace == null || namespace.isBlank()) {
            response.writeError("current namespace is empty, blank or null");
            return;
        }
        response.writeRedisMessage(new SimpleStringRedisMessage(namespace));
    }
}
