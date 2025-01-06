/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.foundationdb.zmap.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;

import java.util.List;

public class ZSetMessage implements KronotopMessage<byte[]> {
    public static final String COMMAND = "ZSET";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private byte[] key;
    private byte[] value;

    public ZSetMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(key);

        value = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(value);
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public List<byte[]> getKeys() {
        return null;
    }

    public byte[] getValue() {
        return value;
    }
}