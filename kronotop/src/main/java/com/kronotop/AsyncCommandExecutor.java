/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop;

import com.kronotop.server.Response;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The AsyncCommandExecutor class provides utility methods for executing tasks asynchronously
 * using virtual threads. These methods ensure efficient concurrency and proper result handling.
 * Responses are written back to the appropriate Netty thread that handled the request.
 */
public class AsyncCommandExecutor {
    /**
     * Executes a given supplier asynchronously using virtual threads and handles its result
     * with the provided action within the context of a Kronotop instance.
     * This method ensures efficient concurrency by leveraging virtual threads
     * and writes the response back to the appropriate Netty thread.
     *
     * @param <U>      the type of the result supplied by the supplier
     * @param context  the Kronotop context containing resources for task execution
     * @param response the response object used to write back results or errors
     * @param supplier the supplier function providing the result to be processed
     * @param action   the consumer function to process the result provided by the supplier
     */
    public static <U> void supplyAsync(Context context, Response response, Supplier<U> supplier, Consumer<? super U> action) {
        // Use virtual threads to ensure efficient concurrency
        // Write response back to Netty's thread that handled the request
        CompletableFuture.supplyAsync(supplier, context.getVirtualThreadPerTaskExecutor()).thenAcceptAsync(action, response.getCtx().executor()).exceptionallyAsync(ex -> {
            response.writeError(ex);
            return null;
        }, response.getCtx().executor());
    }

    /**
     * Executes two runnable tasks asynchronously using virtual threads and ensures proper handling
     * of errors. The first runnable task is executed in a virtual thread, and upon its completion,
     * the second runnable task is executed. The response object is used to write back any errors
     * to the client using the Netty thread that handled the request.
     *
     * @param context  the execution context providing an executor service with virtual threads
     * @param response the response object used to handle errors and write back to the client
     * @param runnable the first runnable task to execute asynchronously in a virtual thread
     * @param action   the subsequent runnable task to execute once the first task completes
     */
    public static void runAsync(Context context, Response response, Runnable runnable, Runnable action) {
        // Use virtual threads to ensure efficient concurrency
        // Write response back to Netty's thread that handled the request
        CompletableFuture.runAsync(runnable, context.getVirtualThreadPerTaskExecutor()).thenRunAsync(action, response.getCtx().executor()).exceptionallyAsync(ex -> {
            response.writeError(ex);
            return null;
        }, response.getCtx().executor());
    }
}
