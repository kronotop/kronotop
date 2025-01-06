/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.redis.storage.syncer.jobs;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.storage.syncer.VolumeSyncSession;

public class DeleteByVersionstampJob extends BaseJob implements VolumeSyncJob {
    private final Versionstamp versionstamp;

    public DeleteByVersionstampJob(Versionstamp versionstamp) {
        this.versionstamp = versionstamp;
        this.hashCode = versionstamp.hashCode();
    }

    @Override
    public void run(VolumeSyncSession session) {
        session.delete(versionstamp);
    }

    @Override
    public void postHook(VolumeSyncSession session) {

    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
