/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.vector;

import com.kronotop.Context;

import java.util.concurrent.BlockingQueue;

public class VectorSyncMaintainer extends BaseVectorMaintainer<VectorSyncTask> {

    public VectorSyncMaintainer(Context context) {
        super(context, "sync", "kr.vector-sync-maintainer-%d",
                context.getConfig().getLong("bucket.vector.sync_drain_timeout_seconds"));
    }

    @Override
    protected BlockingQueue<VectorSyncTask> queue() {
        return service.getVectorSyncQueue();
    }

    @Override
    protected void process(VectorSyncTask task) {
    }

    @Override
    protected void onFailure(VectorSyncTask task, Exception exp) {
        logger.error("Vector sync task failed", exp);
    }
}
