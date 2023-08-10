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

package com.kronotop.redis.string.protocol;

import com.kronotop.server.resp.KronotopMessage;
import com.kronotop.server.resp.Request;

import java.util.ArrayList;
import java.util.List;

public class MSetMessage implements KronotopMessage<String> {
    public static final String COMMAND = "MSET";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private final List<Pair> pairs = new ArrayList<>();

    public MSetMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        for (int i = 0; i < request.getParams().size(); i = i + 2) {
            byte[] key = new byte[request.getParams().get(i).readableBytes()];
            request.getParams().get(i).readBytes(key);

            byte[] value = new byte[request.getParams().get(i + 1).readableBytes()];
            request.getParams().get(i + 1).readBytes(value);

            Pair keyValue = new Pair(new String(key), value);
            pairs.add(keyValue);
        }
    }

    public List<Pair> getPairs() {
        return pairs;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public List<String> getKeys() {
        List<String> result = new ArrayList<>();
        for (Pair pair : pairs) {
            result.add(pair.getKey());
        }
        return result;
    }


    public static class Pair {
        private String key;
        private byte[] value;

        public Pair(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }
    }
}