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


package com.kronotop.bucket.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public abstract class AbstractBucketMessage implements ProtocolMessage<Void> {

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return List.of();
    }

    private QueryArgumentKey valueOfArgument(String raw) {
        try {
            return QueryArgumentKey.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
        }
    }

    protected QueryArguments parseCommonQueryArguments(Request request, int index) {
        int limit = 0;
        boolean reverse = false;
        for (int i = index; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            QueryArgumentKey argument = valueOfArgument(raw);
            switch (argument) {
                case LIMIT -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be followed by a positive integer");
                    }
                    limit = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (limit < 0) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be a non-negative integer");
                    }
                    i++;
                }
                case REVERSE -> {
                    reverse = true;
                }
            }
        }
        return new QueryArguments(limit, reverse);
    }
}
