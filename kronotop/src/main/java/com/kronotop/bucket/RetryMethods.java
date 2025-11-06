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

package com.kronotop.bucket;

import com.apple.foundationdb.FDBException;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceRoutineShutdownException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

public class RetryMethods {
    public static final String INDEX_MAINTENANCE_ROUTINE = "INDEX_MAINTENANCE_ROUTINE";
    public static final String TRANSACTION = "TRANSACTION";
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryMethods.class);
    private static final RetryRegistry retryRegistry;

    static {
        Map<String, RetryConfig> configs = new HashMap<>();
        configs.put(INDEX_MAINTENANCE_ROUTINE,
                RetryConfig.custom()
                        .maxAttempts(10)
                        .waitDuration(Duration.ofMillis(100))
                        .retryOnException(ex -> !(ex instanceof IndexMaintenanceRoutineShutdownException))
                        .build());

        configs.put(TRANSACTION,
                RetryConfig.custom()
                        .maxAttempts(10)
                        .waitDuration(Duration.ofMillis(100))
                        .retryOnException(e -> {
                            if (e instanceof CompletionException) {
                                Throwable cause = e.getCause();
                                if (cause instanceof FDBException) {
                                    int code = ((FDBException) cause).getCode();
                                    // 1007: transaction_too_old
                                    // 1020: not_committed
                                    return code == 1007 || code == 1020;
                                }
                            }
                            return false;
                        }).build());
        retryRegistry = RetryRegistry.of(configs);

        retryRegistry.getEventPublisher().onEntryAdded(event -> {
            Retry retry = event.getAddedEntry();
            retry.getEventPublisher()
                    .onRetry(ev -> {
                        if (ev.getLastThrowable() != null) {
                            LOGGER.trace("Retry attempt #{} for [{}] due to {}",
                                    ev.getNumberOfRetryAttempts(),
                                    ev.getName(),
                                    ev.getLastThrowable().toString());
                        }
                    })
                    .onError(ev -> {
                        LOGGER.debug("All retries failed for [{}] after {} attempts",
                                ev.getName(),
                                ev.getNumberOfRetryAttempts(),
                                ev.getLastThrowable());
                    });
        });
    }

    public static Retry retry(String name) {
        return retryRegistry.retry(name);
    }
}
