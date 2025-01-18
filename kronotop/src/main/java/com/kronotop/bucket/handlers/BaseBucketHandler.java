// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.


package com.kronotop.bucket.handlers;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.bucket.BucketService;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Request;
import com.kronotop.volume.Prefix;

public class BaseBucketHandler {
    protected final BucketService service;
    protected final Context context;

    public BaseBucketHandler(BucketService service) {
        this.service = service;
        this.context = service.getContext();
    }

    protected Prefix getBucketSubspace(Request request, String bucket) {
        String namespace = request.getChannelContext().channel().attr(ChannelAttributes.CURRENT_NAMESPACE).get();
        return service.getPrefix(namespace, bucket);
    }
}
