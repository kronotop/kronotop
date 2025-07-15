// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketCreateIndexMessage;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;


@Command(BucketCreateIndexMessage.COMMAND)
@MinimumParameterCount(BucketCreateIndexMessage.MINIMUM_PARAMETER_COUNT)
public class BucketCreateIndexHandler extends BaseBucketHandler implements Handler {
    public BucketCreateIndexHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETCREATEINDEX).set(new BucketCreateIndexMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketCreateIndexMessage message = request.attr(MessageTypes.BUCKETCREATEINDEX).get();
        System.out.println(message.getIndex());
        response.writeOK();
    }
}
