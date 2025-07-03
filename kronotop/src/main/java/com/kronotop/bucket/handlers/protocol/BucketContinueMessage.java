// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.internal.ByteBufUtils;
import com.kronotop.server.IllegalCommandParameterException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class BucketContinueMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.CONTINUE";
    private final Request request;
    private Parameters parameters;

    public BucketContinueMessage(Request request) {
        this.request = request;
        parse();
    }

    private BucketQueryParameter valueOfParameter(String raw) {
        try {
            return BucketQueryParameter.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandParameterException(String.format("Unknown '%s' parameter: %s", COMMAND, raw));
        }
    }

    private void parse() {
        int limit = 0;
        for (int i = 0; i < request.getParams().size(); i++) {
            String raw = ByteBufUtils.readAsString(request.getParams().get(i));
            BucketQueryParameter parameter = valueOfParameter(raw);
            switch (parameter) {
                case LIMIT -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandParameterException("LIMIT parameter must be followed by a positive integer");
                    }
                    limit = ByteBufUtils.readAsInteger(request.getParams().get(i + 1));
                    if (limit < 0) {
                        throw new IllegalCommandParameterException("LIMIT parameter must be a non-negative integer");
                    }
                    i++;
                }
            }
        }
        parameters = new Parameters(limit);
    }

    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return List.of();
    }

    public record Parameters(int limit) {
    }
}
