// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.


package com.kronotop.bucket.handlers.protocol;

import com.kronotop.internal.ByteBufUtils;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.Request;

public class BaseBucketMessage {

    private QueryArgument valueOfArgument(String raw) {
        try {
            return QueryArgument.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
        }
    }

    protected Arguments parseCommonQueryArguments(Request request, int index) {
        int limit = 0;
        boolean reverse = false;
        for (int i = index; i < request.getParams().size(); i++) {
            String raw = ByteBufUtils.readAsString(request.getParams().get(i));
            QueryArgument argument = valueOfArgument(raw);
            switch (argument) {
                case LIMIT -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be followed by a positive integer");
                    }
                    limit = ByteBufUtils.readAsInteger(request.getParams().get(i + 1));
                    if (limit < 0) {
                        throw new IllegalCommandArgumentException("LIMIT argument must be a non-negative integer");
                    }
                    i++;
                }
                case REVERSE -> {
                    reverse = true;
                }
            }
        }
        return new Arguments(limit, reverse);
    }
}
