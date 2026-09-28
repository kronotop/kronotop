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

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.EnumSet;
import java.util.Set;

public class BucketUpdateMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.UPDATE";
    public static final int MINIMUM_ARGUMENT_COUNT = 3;
    public static final int MAXIMUM_ARGUMENT_COUNT = 14;
    private static final Set<QueryArgumentKey> supportedArguments = EnumSet.of(
            QueryArgumentKey.SORTBY,
            QueryArgumentKey.BATCH,
            QueryArgumentKey.COLLATION,
            QueryArgumentKey.LIMIT,
            QueryArgumentKey.NAMESPACE,
            QueryArgumentKey.CLOSE
    );
    private final Request request;
    private String bucket;
    private byte[] query;
    private byte[] update;
    private QueryArguments arguments;

    public BucketUpdateMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getArguments().get(0));
        query = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(1));
        update = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(2));
        if (update.length == 0) {
            throw new IllegalCommandArgumentException("update argument cannot be empty");
        }
        arguments = parseCommonQueryArguments(request, 3, supportedArguments);
    }

    public QueryArguments getArguments() {
        return arguments;
    }

    public String getBucket() {
        return bucket;
    }

    public byte[] getQuery() {
        return query;
    }

    public byte[] getUpdate() {
        return update;
    }
}
