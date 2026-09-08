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
import com.kronotop.internal.StringUtil;
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

    /**
     * Reads shard ids that follow the SHARDS keyword until the next keyword or the end of the command.
     *
     * @return the index of the last shard id
     */
    private int readShards(int index, String keyword) {
        int last = index;
        for (int i = index + 1; i < request.getParams().size(); i++) {
            ByteBuf buf = request.getParams().get(i);
            buf.markReaderIndex();
            if (tryParseKey(ProtocolMessageUtil.readAsString(buf)) != null) {
                buf.resetReaderIndex();
                break;
            }
            buf.resetReaderIndex();
            shards.add(ProtocolMessageUtil.readAsInteger(buf));
            last = i;
        }
        if (last == index) {
            throw ProtocolMessageUtil.illegalValue(keyword, "one or more shard ids");
        }
        return last;
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));

        long seen = 0;
        for (int i = 1; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            CreateArgumentKey key = tryParseKey(raw);
            if (key == null) {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
            }
            String keyword = StringUtil.toUpperCaseAscii(raw);
            seen = ProtocolMessageUtil.markArgumentSeen(seen, key, keyword);
            switch (key) {
                case IF_NOT_EXISTS -> ifNotExists = true;
                case SHARDS -> i = readShards(i, keyword);
                case INDEXES -> {
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getParams(), i, keyword, "an index specification");
                    indexes = ProtocolMessageUtil.readAsByteArray(value);
                    i++;
                }
                case COLLATION -> {
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getParams(), i, keyword, COLLATION_SPECIFICATION);
                    collation = ProtocolMessageUtil.readAsByteArray(value);
                    i++;
                }
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
