// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketContinueMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;

@Command(BucketContinueMessage.COMMAND)
public class BucketContinueHandler extends BaseBucketHandler {
    public BucketContinueHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETCONTINUE).set(new BucketContinueMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketContinueMessage message = request.attr(MessageTypes.BUCKETCONTINUE).get();
        response.writeOK();
    }
}
