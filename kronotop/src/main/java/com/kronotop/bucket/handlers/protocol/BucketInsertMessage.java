// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
