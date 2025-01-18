// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.DirectorySubspaceCache;
import com.kronotop.KronotopService;
import com.kronotop.bucket.handlers.BucketInsertHandler;
import com.kronotop.server.ServerKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Bucket";
    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);
    private final BucketSubspaceCache bucketSubspaceCache;

    public BucketService(Context context) {
        super(context, NAME);

        bucketSubspaceCache = new BucketSubspaceCache(context);

        handlerMethod(ServerKind.EXTERNAL, new BucketInsertHandler(this));
    }

    public DirectorySubspace getBucketSubspace(String namespace, String bucket) {
        return bucketSubspaceCache.get(namespace, bucket);
    }

    public void start() {

    }
}
