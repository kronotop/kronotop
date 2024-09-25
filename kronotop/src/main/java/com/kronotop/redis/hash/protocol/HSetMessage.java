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

package com.kronotop.redis.hash.protocol;

import com.kronotop.redis.hash.HashFieldValue;
import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import com.kronotop.server.WrongNumberOfArgumentsException;

import java.util.List;

public class HSetMessage extends SyncableHashMessage implements KronotopMessage<String> {
    public static final String COMMAND = "HSET";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private String key;

    public HSetMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if ((request.getParams().size() - 1) % 2 != 0) {
            throw new WrongNumberOfArgumentsException(
                    String.format("wrong number of arguments for '%s' command", request.getCommand())
            );
        }

        byte[] rawKey = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(rawKey);
        key = new String(rawKey);

        for (int i = 1; i < request.getParams().size(); i = i + 2) {
            byte[] rawField = new byte[request.getParams().get(i).readableBytes()];
            request.getParams().get(i).readBytes(rawField);
            String field = new String(rawField);

            byte[] value = new byte[request.getParams().get(i + 1).readableBytes()];
            request.getParams().get(i + 1).readBytes(value);

            FieldValuePair fieldValuePair = new FieldValuePair(field, new HashFieldValue(value));
            getFieldValuePairs().add(fieldValuePair);
        }
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
