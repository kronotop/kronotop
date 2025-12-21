/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.worker;

import com.kronotop.bucket.BucketEventsWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WorkerUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(WorkerUtil.class);

    public static void shutdownThenAwait(String prefix, List<Worker> workers) {
        for (Worker worker : workers) {
            worker.shutdown();
        }
        try (var virtual = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Worker worker : workers) {
                virtual.submit(() -> {
                    try {
                        worker.await(3, TimeUnit.SECONDS);
                    } catch (InterruptedException exp) {
                        Thread.currentThread().interrupt();
                        LOGGER.debug("Worker await interrupted for prefix: {}, tag: {}", prefix, worker.getTag(), exp);
                    } catch (Exception exp) {
                        LOGGER.debug("Worker await failed for prefix: {}, tag: {}", prefix, worker.getTag(), exp);
                    }
                });
            }
        }
    }
}
