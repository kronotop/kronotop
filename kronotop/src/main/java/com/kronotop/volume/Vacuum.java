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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

class Vacuum {
    private static final Logger LOGGER = LoggerFactory.getLogger(Vacuum.class);
    private final Context context;
    private final Volume volume;
    private final long readVersion;
    private final byte[] readVersionKey;
    private volatile boolean stop;
    private volatile VacuumContext vacuumContext;

    protected Vacuum(Context context, Volume volume) {
        this.context = context;
        this.volume = volume;
        this.readVersion = getOrLoadReadVersion();
        DirectorySubspace volumeSubspace = volume.getConfig().subspace();
        this.readVersionKey = volumeSubspace.pack(
                Tuple.from(
                        VolumeSubspaceConstants.VACUUM_SUBSPACE,
                        VolumeSubspaceConstants.VACUUM_READ_VERSION_KEY
                )
        );
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

    private List<SegmentAnalysis> analyze() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return volume.analyze(tr);
        }
    }

    void start() throws IOException {
        LOGGER.info("Starting Vacuum on volume {}", volume.getConfig().name());
        List<SegmentAnalysis> segmentAnalysisList = analyze();
        for (SegmentAnalysis segmentAnalysis : segmentAnalysisList) {
            if (stop) {
                LOGGER.info("Stopping Vacuum on volume {}", volume.getConfig().name());
                break;
            }
            if (segmentAnalysis.garbageRatio() < volume.getConfig().allowedGarbageRatio()) {
                continue;
            }
            LOGGER.debug("Vacuuming segment: {} on volume {}", segmentAnalysis.name(), volume.getConfig().name());
            vacuumContext = new VacuumContext(segmentAnalysis.name(), readVersion);
            volume.vacuumSegment(vacuumContext);
        }
    }

    void stop() {
        stop = true;
        vacuumContext.stop();
    }
}
