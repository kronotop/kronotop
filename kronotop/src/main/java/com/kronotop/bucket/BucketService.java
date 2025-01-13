// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Bucket";
    private static final Logger LOGGER = LoggerFactory.getLogger(BucketService.class);

    public BucketService(Context context) {
        super(context, NAME);
    }

    public void start() {

    }
}
