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

package com.kronotop.foundationdb.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class SnapshotReadMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "SNAPSHOT_READ";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 1;
    public static final String ON_KEYWORD = "ON";
    public static final String OFF_KEYWORD = "OFF";
    private final Request request;
    private String option;

    public SnapshotReadMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        ByteBuf buf = request.getParams().get(0);
        byte[] rawOpt = new byte[buf.readableBytes()];
        buf.readBytes(rawOpt);
        String opt = new String(rawOpt);
        if (opt.equalsIgnoreCase(ON_KEYWORD) || opt.equalsIgnoreCase(OFF_KEYWORD)) {
            option = opt;
        } else {
            throw new IllegalArgumentException(String.format("illegal argument for SNAPSHOT_READ: '%s'", opt));
        }
    }

    public String getOption() {
        return option;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }
}