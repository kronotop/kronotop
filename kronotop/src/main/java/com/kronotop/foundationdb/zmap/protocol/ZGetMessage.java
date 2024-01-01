/*
 * Copyright (c) 2023 Kronotop
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

public class ZGetMessage implements KronotopMessage<byte[]> {
    public static final String COMMAND = "ZGET";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private byte[] namespace;
    private byte[] key;

    public ZGetMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        namespace = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(namespace);

        key = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(key);
    }

    public String getNamespace() {
        return new String(namespace);
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public List<byte[]> getKeys() {
        return null;
    }


}