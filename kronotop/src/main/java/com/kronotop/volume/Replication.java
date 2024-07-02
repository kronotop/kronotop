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

import com.kronotop.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private volatile boolean isClosed = false;

    private final Context context;
    private final Volume volume;

    public Replication(Context context, Volume volume) {
        this.context = context;
        this.volume = volume;
    }

    private void checkStandbyOwnership() {
        Host owner = volume.getMetadata().getOwner();
        if (owner.member().getId().equals(context.getMember().getId())) {
            isClosed = true;
            throw new IllegalStateException("Volume owner cannot run volume replication");
        }

        for (Host host : volume.getMetadata().getStandbyHosts()) {
            if (host.member().getId().equals(context.getMember().getId())) {
                return;
            }
        }
        isClosed = true;
        throw new IllegalStateException("This member is not listed as a standby");
    }

    public void start() {
        checkStandbyOwnership();
    }

    public void stop() {
        isClosed = true;
    }
}
