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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

class Vacuum {
    private final Context context;
    private final Volume volume;
    private final long readVersion;
    private final byte[] readVersionKey;

    protected Vacuum(Context context, Volume volume) {
        this.context = context;
        this.volume = volume;
        this.readVersion = getOrLoadReadVersion();
        this.readVersionKey = volume.getConfig().subspace().pack(Tuple.from("vacuum", "readVersion"));
    }

    private long getOrLoadReadVersion() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] rawReadVersion = tr.get(readVersionKey).join();
            if (rawReadVersion == null || readVersion <= 0) {
                long readVersion = tr.getReadVersion().join();
                tr.set(readVersionKey, ByteBuffer.allocate(8).putLong(readVersion).array());
                tr.commit().join();
                return readVersion;
            }
            return ByteBuffer.wrap(rawReadVersion).getLong();
        }
    }

    public List<SegmentAnalysis> analyze() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return volume.analyze(tr);
        }
    }

    public void vacuum() throws IOException {
        List<SegmentAnalysis> segmentAnalysisList = analyze();
        for (SegmentAnalysis segmentAnalysis : segmentAnalysisList) {
            if (segmentAnalysis.garbageRatio() < volume.getConfig().allowedGarbageRatio()) {
                continue;
            }
            volume.vacuumSegment(segmentAnalysis.name(), readVersion);
        }
    }

    public void reset() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(readVersionKey);
            tr.commit().join();
        }
    }
}
