/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

public class BucketCursorsMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.CURSORS";
    public static final int MAXIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private BucketOperation operation;

    public BucketCursorsMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (!request.getParams().isEmpty()) {
            String rawAction = ProtocolMessageUtil.readAsString(request.getParams().getFirst());
            try {
                operation = BucketOperation.valueOf(rawAction.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' action", rawAction));
            }
        }
    }

    public BucketOperation getOperation() {
        return operation;
    }
}
