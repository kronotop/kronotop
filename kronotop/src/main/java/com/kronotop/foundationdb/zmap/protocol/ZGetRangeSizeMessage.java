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

import com.kronotop.server.resp.KronotopMessage;
import com.kronotop.server.resp.Request;

import java.util.List;

public class ZGetRangeSizeMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "ZGETRANGESIZE";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private byte[] namespace;
    private byte[] begin;
    private byte[] end;

    public ZGetRangeSizeMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        namespace = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(namespace);

        begin = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(begin);

        end = new byte[request.getParams().get(2).readableBytes()];
        request.getParams().get(2).readBytes(end);
    }

    public String getNamespace() {
        return new String(namespace);
    }

    public byte[] getBegin() {
        return begin;
    }

    public byte[] getEnd() {
        return end;
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