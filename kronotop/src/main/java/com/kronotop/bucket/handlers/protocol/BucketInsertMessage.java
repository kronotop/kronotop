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

import java.util.LinkedList;
import java.util.List;

public class BucketInsertMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.INSERT";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private final List<byte[]> documents = new LinkedList<>();
    private String bucket;
    private InsertArguments arguments;

    public BucketInsertMessage(Request request) {
        this.request = request;
        parse();
    }

    private void readDocuments(int index) {
        for (int i = index; i < request.getParams().size(); i++) {
            byte[] document = ProtocolMessageUtil.readAsByteArray(request.getParams().get(i));
            documents.add(document);
        }
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().getFirst());
        int shard = -1;
        for (int i = 1; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            try {
                InsertArgumentKey argument = InsertArgumentKey.valueOf(raw.toUpperCase());
                if (argument.equals(InsertArgumentKey.DOCS)) {
                    readDocuments(i + 1);
                    break;
                }

                if (argument.equals(InsertArgumentKey.SHARD)) {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("SHARD argument must be followed by a positive integer");
                    }
                    shard = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (shard < 0) {
                        throw new IllegalCommandArgumentException("SHARD argument must be a non-negative integer");
                    }
                    i++;
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
            }
        }
        arguments = new InsertArguments(shard);
    }

    public InsertArguments getArguments() {
        return arguments;
    }

    public List<byte[]> getDocuments() {
        return documents;
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
