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

package com.kronotop.core.handlers.pubsub.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;

public class SUnsubscribeMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "SUNSUBSCRIBE";

    private final Request request;
    private final List<String> channels = new ArrayList<>();

    public SUnsubscribeMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        for (int i = 0; i < request.getParams().size(); i++) {
            channels.add(request.getParams().get(i).toString(CharsetUtil.UTF_8));
        }
    }

    public List<String> getChannels() {
        return channels;
    }

    @Override
    public String getKey() {
        return channels.isEmpty() ? null : channels.getFirst();
    }

    @Override
    public List<String> getKeys() {
        return channels;
    }
}
