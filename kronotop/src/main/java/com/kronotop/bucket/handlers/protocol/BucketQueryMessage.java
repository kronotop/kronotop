// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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
