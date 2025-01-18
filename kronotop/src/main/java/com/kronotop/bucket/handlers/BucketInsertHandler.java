// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.BSONUtils;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import org.bson.Document;

import java.util.Arrays;

@Command(BucketInsertMessage.COMMAND)
@MinimumParameterCount(BucketInsertMessage.MINIMUM_PARAMETER_COUNT)
public class BucketInsertHandler extends BaseBucketHandler implements Handler {

    public BucketInsertHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETINSERT).set(new BucketInsertMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketInsertMessage message = request.attr(MessageTypes.BUCKETINSERT).get();
        DirectorySubspace subspace = getBucketSubspace(request, message.getBucket());
        for (byte[] data : message.getDocuments()) {
            System.out.println(new String(data));
            Document document = Document.parse(new String(data));
            byte[] bsonDoc = BSONUtils.toBytes(document);
            System.out.println(Arrays.toString(bsonDoc));
        }
        response.writeOK();
    }
}
