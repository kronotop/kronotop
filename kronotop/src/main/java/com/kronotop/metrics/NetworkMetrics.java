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

package com.kronotop.metrics;

import com.kronotop.server.ServerKind;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class NetworkMetrics {
    private final LongAdder externalReadBytes = new LongAdder();
    private final LongAdder externalWrittenBytes = new LongAdder();
    private final LongAdder internalReadBytes = new LongAdder();
    private final LongAdder internalWrittenBytes = new LongAdder();

    private final LongAdder externalTotalCommandsProcessed = new LongAdder();
    private final LongAdder internalTotalCommandsProcessed = new LongAdder();

    public NetworkMetrics() {
    }

    public void increaseReadBytes(ServerKind serverKind, long delta) {
        if (serverKind == ServerKind.EXTERNAL) {
            externalReadBytes.add(delta);
        } else {
            internalReadBytes.add(delta);
        }
    }

    public void increaseWrittenBytes(ServerKind serverKind, long delta) {
        if (serverKind == ServerKind.EXTERNAL) {
            externalWrittenBytes.add(delta);
        } else {
            internalWrittenBytes.add(delta);
        }
    }

    public long getReadBytes(ServerKind serverKind) {
        if (serverKind == ServerKind.EXTERNAL) {
            return externalReadBytes.sum();
        }
        return internalReadBytes.sum();
    }

    public long getWrittenBytes(ServerKind serverKind) {
        if (serverKind == ServerKind.EXTERNAL) {
            return externalWrittenBytes.sum();
        }
        return internalWrittenBytes.sum();
    }

    public void increaseTotalCommandsProcessed(ServerKind serverKind) {
        if (serverKind == ServerKind.EXTERNAL) {
            externalTotalCommandsProcessed.increment();
        } else {
            internalTotalCommandsProcessed.increment();
        }
    }

    public long getTotalCommandsProcessed(ServerKind serverKind) {
        if (serverKind == ServerKind.EXTERNAL) {
            return externalTotalCommandsProcessed.sum();
        }
        return internalTotalCommandsProcessed.sum();
    }
}
