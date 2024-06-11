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

package com.kronotop.foundationdb.zmap.protocol;

import com.apple.foundationdb.MutationType;
import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Locale;

public class ZMutateMessage implements KronotopMessage<byte[]> {
    public static final String COMMAND = "ZMUTATE";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private byte[] key;
    private byte[] param;
    private MutationType mutationType;

    public ZMutateMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(key);

        param = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(param);

        ByteBuf buf = request.getParams().get(2);
        byte[] rawItem = new byte[buf.readableBytes()];
        buf.readBytes(rawItem);
        String mt = new String(rawItem);
        mutationType = ZMutationType.getMutationType(ZMutationType.valueOf(mt.toUpperCase(Locale.ROOT)));
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public List<byte[]> getKeys() {
        return null;
    }


    public byte[] getParam() {
        return param;
    }

    public MutationType getMutationType() {
        return mutationType;
    }
}