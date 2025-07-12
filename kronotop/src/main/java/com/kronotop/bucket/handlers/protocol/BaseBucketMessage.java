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
import com.kronotop.server.Request;

public class BaseBucketMessage {

    private QueryArgumentKey valueOfArgument(String raw) {
        try {
            return QueryArgumentKey.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
        }
    }

    protected QueryArguments parseCommonQueryArguments(Request request, int index) {
        int limit = 0;
        int shard = -1;
        boolean reverse = false;
        for (int i = index; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            QueryArgumentKey argument = valueOfArgument(raw);
            switch (argument) {
                case LIMIT -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be followed by a positive integer");
                    }
                    limit = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (limit < 0) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be a non-negative integer");
                    }
                    i++;
                }
                case SHARD -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("SHARD argument must be followed by a positive integer");
                    }
                    shard = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (shard < 0) {
                        throw new IllegalCommandArgumentException("SHARD argument must be a non-negative integer");
                    }
                    i++;
                }
                case REVERSE -> {
                    reverse = true;
                }
            }
        }
        return new QueryArguments(shard, limit, reverse);
    }
}
