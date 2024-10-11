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

package com.kronotop.redis.handlers.cluster.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownSubcommandException;

import java.util.List;

public class ClusterMessage implements KronotopMessage<String> {
    public static final String COMMAND = "CLUSTER";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private ClusterSubcommand subcommand;
    private String key = null;

    public ClusterMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawSubcommand = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawSubcommand);
        String cmd = new String(rawSubcommand);
        try {
            subcommand = ClusterSubcommand.valueOf(cmd.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnknownSubcommandException(cmd);
        }
        if (request.getParams().size() > 1) {
            byte[] rawKey = new byte[request.getParams().get(1).readableBytes()];
            request.getParams().get(1).readBytes(rawKey);
            key = new String(rawKey);
        }
    }

    public ClusterSubcommand getSubcommand() {
        return subcommand;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }
}
