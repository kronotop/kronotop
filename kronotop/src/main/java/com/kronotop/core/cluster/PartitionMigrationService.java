/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.core.cluster;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class PartitionMigrationService implements KronotopService {
    public static final String NAME = "Partition Migration";
    private static final Logger logger = LoggerFactory.getLogger(PartitionMigrationService.class);
    private final Context context;
    private final ScheduledThreadPoolExecutor scheduler;
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("kr.partition-migration-%d").build();
    private volatile boolean isShutdown;

    public PartitionMigrationService(Context context) {
        this.context = context;
        this.scheduler = new ScheduledThreadPoolExecutor(2, namedThreadFactory);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public void start() {
        logger.info("Partition Migration Service has been started");
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                logger.warn(String.format("%s cannot be stopped gracefully", NAME));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
