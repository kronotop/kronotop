// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.KronotopException;
import com.kronotop.internal.ByteBufUtils;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class BucketQueryMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.QUERY";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private String query;
    private String bucket;
    private int limit = 0;

    public BucketQueryMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        bucket = ByteBufUtils.readAsString(request.getParams().get(0));
        query = ByteBufUtils.readAsString(request.getParams().get(1));

        if (request.getParams().size() > MINIMUM_PARAMETER_COUNT) {
            for (int i = 2; i < request.getParams().size(); i++) {
                String raw = ByteBufUtils.readAsString(request.getParams().get(i));
                try {
                    BucketQueryParameter parameter = BucketQueryParameter.valueOf(raw.toUpperCase());
                    switch (parameter) {
                        case LIMIT -> {
                            if (request.getParams().size() <= i + 1) {
                                throw new IllegalArgumentException("LIMIT parameter must be followed by a number");
                            }
                            limit = ByteBufUtils.readAsInteger(request.getParams().get(i + 1));
                            i++;
                        }
                        default -> throw new IllegalArgumentException("Unknown parameter: " + parameter);
                    }
                } catch (IllegalArgumentException e) {
                    throw new KronotopException(String.format("Unknown '%s' parameter: %s", COMMAND, raw));
                }
            }
        }
    }

    public int getLimit() {
        return limit;
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
