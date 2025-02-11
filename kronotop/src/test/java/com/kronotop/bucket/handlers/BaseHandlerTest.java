// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.BaseTest;
import com.kronotop.KronotopTestInstance;
import com.typesafe.config.Config;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

public class BaseHandlerTest extends BaseTest {
    protected KronotopTestInstance instance;
    protected EmbeddedChannel channel;

    protected EmbeddedChannel newChannel() {
        return instance.newChannel();
    }

    protected void setupCommon(Config config) throws UnknownHostException, InterruptedException {
        instance = new KronotopTestInstance(config);
        instance.start();
        channel = instance.getChannel();
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = loadConfig("test.conf");
        setupCommon(config);
    }

    @AfterEach
    public void tearDown() {
        if (instance == null) {
            return;
        }
        instance.shutdown();
    }
}