/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.volume;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicationContext {
    private final Semaphore semaphore = new Semaphore(1);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<Future<?>> snapshotFuture = new AtomicReference<>();
    private final AtomicReference<Future<?>> changeDataCaptureFuture = new AtomicReference<>();

    public Semaphore semaphore() {
        return semaphore;
    }

    public ExecutorService executor() {
        return executor;
    }

    public AtomicReference<Future<?>> snapshotFuture() {
        return snapshotFuture;
    }

    public AtomicReference<Future<?>> changeDataCaptureFuture() {
        return changeDataCaptureFuture;
    }
}
