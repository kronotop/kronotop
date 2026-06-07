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
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class BucketCreateMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.CREATE";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private final List<Integer> shards = new ArrayList<>();
    private byte[] indexes;
    private byte[] collation;
    private String bucket;
    private boolean ifNotExists = false;

    public BucketCreateMessage(Request request) {
        this.request = request;
        parse();
    }

    private CreateArgumentKey tryParseKey(String raw) {
        return CreateArgumentKey.findByName(raw);
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));

        CreateArgumentKey currentKey = null;
        for (int i = 1; i < request.getParams().size(); i++) {
            ByteBuf buf = request.getParams().get(i);

            buf.markReaderIndex();
            String raw = ProtocolMessageUtil.readAsString(buf);
            CreateArgumentKey parsedKey = tryParseKey(raw);
            if (parsedKey != null) {
                if (parsedKey == CreateArgumentKey.IF_NOT_EXISTS) {
                    ifNotExists = true;
                    continue;
                }
                currentKey = parsedKey;
                continue;
            }
            buf.resetReaderIndex();

            if (currentKey == null) {
                throw new IllegalCommandArgumentException(
                        String.format("Unknown '%s' argument", raw));
            }

            switch (currentKey) {
                case SHARDS -> shards.add(ProtocolMessageUtil.readAsInteger(buf));
                case INDEXES -> indexes = ProtocolMessageUtil.readAsByteArray(buf);
                case COLLATION -> collation = ProtocolMessageUtil.readAsByteArray(buf);
            }
        }
    }

    public String getBucket() {
        return bucket;
    }

    public List<Integer> getShards() {
        return shards;
    }

    public byte[] getIndexes() {
        return indexes;
    }

    public byte[] getCollation() {
        return collation;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }
}
