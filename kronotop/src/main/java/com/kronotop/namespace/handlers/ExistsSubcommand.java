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

package com.kronotop.namespace.handlers;

import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;

import java.util.List;

class ExistsSubcommand extends BaseSubcommand implements SubcommandHandler {
    ExistsSubcommand(Context context) {
        super(context);
    }

    @Override
    public void execute(Request request, Response response) {
        ExistsParameters parameters = new ExistsParameters(request);
        AsyncCommandExecutor.supplyAsync(context, response,
                () -> NamespaceUtil.exists(context, parameters.subpath),
                response::writeBoolean);
    }

    private class ExistsParameters {
        private final List<String> subpath;

        private ExistsParameters(Request request) {
            if (request.getParams().size() <= 1) {
                throw wrongNumberOfArguments(request, NamespaceSubcommand.EXISTS);
            }
            subpath = readSubpath(request.getParams().get(1));
            validateSubpath(subpath);
        }
    }
}
