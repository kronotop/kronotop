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

import com.kronotop.KronotopException;
import com.kronotop.bucket.handlers.CollationHelper;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;
import java.util.Set;

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
            return QueryArgumentKey.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
        }
    }

    protected QueryArguments parseCommonQueryArguments(Request request, int index, Set<QueryArgumentKey> supportedArguments) {
        QueryArguments arguments = new QueryArguments();
        for (int i = index; i < request.getParams().size(); i++) {
            String raw = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(request.getParams().get(i)));
            QueryArgumentKey argument = valueOfArgument(raw);
            switch (argument) {
                case LIMIT -> {
                    if (!supportedArguments.contains(QueryArgumentKey.LIMIT)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.LIMIT);
                    }
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be followed by a positive integer");
                    }
                    int limit = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (limit < 0) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be a non-negative integer");
                    }
                    arguments.setLimit(limit);
                    i++;
                }
                case SORTBY -> {
                    if (!supportedArguments.contains(QueryArgumentKey.SORTBY)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.SORTBY);
                    }
                    if (request.getParams().size() <= i + 2) {
                        throw new IllegalCommandArgumentException("SORTBY argument must be followed by a field name and direction (ASC or DESC)");
                    }
                    arguments.setSortBy(ProtocolMessageUtil.readAsString(request.getParams().get(i + 1)));
                    String rawDirection = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(request.getParams().get(i + 2)));
                    try {
                        arguments.setSortDirection(SortDirection.valueOf(rawDirection));
                    } catch (IllegalArgumentException e) {
                        throw new KronotopException("Invalid sort direction: " + rawDirection);
                    }
                    i += 2;
                }
                case RESULTSORT -> {
                    if (!supportedArguments.contains(QueryArgumentKey.RESULTSORT)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.RESULTSORT);
                    }
                    if (request.getParams().size() <= i + 2) {
                        throw new IllegalCommandArgumentException("RESULTSORT argument must be followed by a field name and direction (ASC or DESC)");
                    }
                    arguments.setResultSortBy(ProtocolMessageUtil.readAsString(request.getParams().get(i + 1)));
                    String rawResultSortDirection = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(request.getParams().get(i + 2)));
                    try {
                        arguments.setResultSortDirection(SortDirection.valueOf(rawResultSortDirection));
                    } catch (IllegalArgumentException e) {
                        throw new KronotopException("Invalid sort direction: " + rawResultSortDirection);
                    }
                    i += 2;
                }
                case PROJECTION -> {
                    if (!supportedArguments.contains(QueryArgumentKey.PROJECTION)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.PROJECTION);
                    }
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("PROJECTION argument must be followed by a projection specification");
                    }
                    arguments.setProjection(ProtocolMessageUtil.readAsByteArray(request.getParams().get(i + 1)));
                    i++;
                }
                case COLLATION -> {
                    if (!supportedArguments.contains(QueryArgumentKey.COLLATION)) {
                        throw new UnsupportedArgumentException(QueryArgumentKey.COLLATION);
                    }
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("COLLATION argument must be followed by a collation specification");
                    }
                    byte[] data = ProtocolMessageUtil.readAsByteArray(request.getParams().get(i + 1));
                    arguments.setCollation(CollationHelper.deserializeAndValidate(data));
                    i++;
                }
            }
        }
        return arguments;
    }
}
