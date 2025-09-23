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

import com.kronotop.KronotopException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

public class BucketUpdateMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.UPDATE";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    public static final int MAXIMUM_PARAMETER_COUNT = 6;
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
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));
        query = ProtocolMessageUtil.readAsByteArray(request.getParams().get(1));
        update = ProtocolMessageUtil.readAsByteArray(request.getParams().get(2));
        if (update.length == 0) {
            throw new KronotopException("update parameter cannot be empty");
        }
        arguments = parseCommonQueryArguments(request, 3);
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
