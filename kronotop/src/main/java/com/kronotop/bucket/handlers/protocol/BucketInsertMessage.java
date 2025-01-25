// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;

import java.util.LinkedList;
import java.util.List;

public class BucketInsertMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "BUCKET.INSERT";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private final List<byte[]> documents = new LinkedList<>();
    private String bucket;

    public BucketInsertMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawBucket = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(rawBucket);
        bucket = new String(rawBucket);

        for (int i = 1; i < request.getParams().size(); i++) {
            byte[] document = new byte[request.getParams().get(i).readableBytes()];
            request.getParams().get(i).readBytes(document);
            documents.add(document);
        }
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
