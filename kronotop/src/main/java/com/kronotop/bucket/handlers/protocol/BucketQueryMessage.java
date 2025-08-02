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
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class BucketQueryMessage extends BaseBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.QUERY";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private String query;
    private String bucket;
    private QueryArguments arguments;

    public BucketQueryMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));
        query = ProtocolMessageUtil.readAsString(request.getParams().get(1));
        arguments = parseCommonQueryArguments(request, 2);
    }

    public QueryArguments getArguments() {
        return arguments;
    }

    public String getQuery() {
        return query;
    }

    public String getBucket() {
        return bucket;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return List.of();
    }
}
