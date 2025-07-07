// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class BucketAdvanceMessage extends BaseBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.ADVANCE";
    private final Request request;
    private Arguments arguments;

    public BucketAdvanceMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        arguments = parseCommonQueryArguments(request, 0);
    }

    public Arguments getArguments() {
        return arguments;
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
