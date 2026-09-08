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


package com.kronotop.bucket.handlers.protocol;

import com.kronotop.bucket.handlers.CollationHelper;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Set;

public abstract class AbstractBucketMessage implements ProtocolMessage<Void> {
    protected static final String COLLATION_SPECIFICATION = "a collation specification";
    private static final String FIELD_AND_DIRECTION = "a field name and direction (ASC or DESC)";
    private static final String SORT_DIRECTION = "sort direction";

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
            return QueryArgumentKey.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
        }
    }

    protected QueryArguments parseCommonQueryArguments(Request request, int index, Set<QueryArgumentKey> supportedArguments) {
        QueryArguments arguments = new QueryArguments();
        long seen = 0;
        for (int i = index; i < request.getParams().size(); i++) {
            String raw = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(request.getParams().get(i)));
            QueryArgumentKey argument = valueOfArgument(raw);
            seen = ProtocolMessageUtil.markArgumentSeen(seen, argument, argument.name());
            switch (argument) {
                case BATCH -> {
                    if (!supportedArguments.contains(QueryArgumentKey.BATCH)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.BATCH);
                    }
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getParams(), i, argument.name(), ProtocolMessageUtil.NON_NEGATIVE_INTEGER);
                    int batch = ProtocolMessageUtil.readAsInteger(value);
                    if (batch < 0) {
                        throw ProtocolMessageUtil.illegalValue(argument.name(), ProtocolMessageUtil.NON_NEGATIVE_INTEGER);
                    }
                    arguments.setBatch(batch);
                    i++;
                }
                case LIMIT -> {
                    if (!supportedArguments.contains(QueryArgumentKey.LIMIT)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.LIMIT);
                    }
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getParams(), i, argument.name(), ProtocolMessageUtil.NON_NEGATIVE_INTEGER);
                    int limit = ProtocolMessageUtil.readAsInteger(value);
                    if (limit < 0) {
                        throw ProtocolMessageUtil.illegalValue(argument.name(), ProtocolMessageUtil.NON_NEGATIVE_INTEGER);
                    }
                    arguments.setLimit(limit);
                    i++;
                }
                case SORTBY -> {
                    if (!supportedArguments.contains(QueryArgumentKey.SORTBY)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.SORTBY);
                    }
                    if (request.getParams().size() <= i + 2) {
                        throw ProtocolMessageUtil.illegalValue(argument.name(), FIELD_AND_DIRECTION);
                    }
                    arguments.setSortBy(ProtocolMessageUtil.readAsString(request.getParams().get(i + 1)));
                    arguments.setSortDirection(ProtocolMessageUtil.readEnum(
                            SortDirection.class, request.getParams().get(i + 2), SORT_DIRECTION));
                    i += 2;
                }
                case RESULTSORT -> {
                    if (!supportedArguments.contains(QueryArgumentKey.RESULTSORT)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.RESULTSORT);
                    }
                    if (request.getParams().size() <= i + 2) {
                        throw ProtocolMessageUtil.illegalValue(argument.name(), FIELD_AND_DIRECTION);
                    }
                    arguments.setResultSortBy(ProtocolMessageUtil.readAsString(request.getParams().get(i + 1)));
                    arguments.setResultSortDirection(ProtocolMessageUtil.readEnum(
                            SortDirection.class, request.getParams().get(i + 2), SORT_DIRECTION));
                    i += 2;
                }
                case PROJECTION -> {
                    if (!supportedArguments.contains(QueryArgumentKey.PROJECTION)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.PROJECTION);
                    }
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getParams(), i, argument.name(), "a projection specification");
                    arguments.setProjection(ProtocolMessageUtil.readAsByteArray(value));
                    i++;
                }
                case COLLATION -> {
                    if (!supportedArguments.contains(QueryArgumentKey.COLLATION)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.COLLATION);
                    }
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getParams(), i, argument.name(), COLLATION_SPECIFICATION);
                    byte[] data = ProtocolMessageUtil.readAsByteArray(value);
                    arguments.setCollation(CollationHelper.deserializeAndValidate(data));
                    i++;
                }
            }
        }
        return arguments;
    }
}
