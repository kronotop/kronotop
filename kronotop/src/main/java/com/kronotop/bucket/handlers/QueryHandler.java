// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.QueryMessage;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

// Alias for BUCKET.QUERY

@Command(QueryMessage.COMMAND)
@MaximumParameterCount(QueryMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(QueryMessage.MINIMUM_PARAMETER_COUNT)
public class QueryHandler extends BucketQueryHandler {
    public QueryHandler(BucketService service) {
        super(service);
    }
}
