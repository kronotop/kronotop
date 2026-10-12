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

package com.kronotop.core.handlers;

import com.kronotop.Context;
import com.kronotop.core.handlers.protocol.InfoMessage;
import com.kronotop.metrics.InfoReport;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

@Command(InfoMessage.COMMAND)
public class InfoHandler implements Handler {
    private static final Set<String> ALL_SECTIONS = Set.of("all", "default", "everything");

    private final InfoReport report;

    public InfoHandler(Context context) {
        this.report = new InfoReport(context);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.INFO).set(new InfoMessage(request));
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void execute(Request request, Response response) {
        InfoMessage message = request.attr(MessageTypes.INFO).get();
        String body = report.render(sectionFilter(message));
        response.writeVerbatimString(Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8)));
    }

    private Set<String> sectionFilter(InfoMessage message) {
        if (message.getSections().isEmpty()) {
            return null;
        }
        Set<String> filter = new HashSet<>();
        for (String section : message.getSections()) {
            String name = section.toLowerCase();
            if (ALL_SECTIONS.contains(name)) {
                return null;
            }
            filter.add(name);
        }
        return filter;
    }
}
