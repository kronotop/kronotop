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
import java.util.concurrent.atomic.AtomicBoolean;

class Vacuum {
    private static final Logger LOGGER = LoggerFactory.getLogger(Vacuum.class);
    private final Context context;
    private final Volume volume;
    private final double allowedGarbageRatio;
    private final byte[] readVersionKey;
    private final AtomicBoolean stop = new AtomicBoolean();
    private volatile VacuumContext vacuumContext;

    protected Vacuum(Context context, Volume volume, double allowedGarbageRatio) {
        this.context = context;
        this.volume = volume;
        this.allowedGarbageRatio = allowedGarbageRatio;
        DirectorySubspace volumeSubspace = volume.getConfig().subspace();
        this.readVersionKey = volumeSubspace.pack(
                Tuple.from(
                        VolumeSubspaceConstants.VACUUM_SUBSPACE,
                        VolumeSubspaceConstants.VACUUM_READ_VERSION_KEY
                )
        );
    }

    /**
     * Retrieves the cached read version from the database or generates and stores a new read version if not present.
     * This method ensures that a consistent read version is available for further operations.
     *
     * @return the read version, either retrieved from the database or freshly loaded and stored.
     */
    private long getOrLoadReadVersion() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] rawReadVersion = tr.get(readVersionKey).join();
            if (rawReadVersion == null) {
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

    /**
     * Initiates the vacuum process on a specified volume. The vacuum process
     * identifies and processes segments with a garbage ratio exceeding the
     * allowed threshold, reclaiming resources and improving efficiency.
     * <p>
     * The method retrieves or generates the current read version, analyzes the
     * volume's segments, and iterates through each analyzed segment to evaluate
     * its garbage ratio. Segments that surpass the allowed garbage ratio are
     * vacuumed, while those that do not are skipped. It logs relevant information
     * for each step, such as the read version, volume, and vacuum progress.
     * <p>
     * If the stop signal is triggered during the vacuum process, the method
     * gracefully terminates, ensuring that no further segments are processed.
     *
     * @throws IOException if an I/O error occurs during the vacuum process.
     */
    void start() throws IOException {
        long readVersion = getOrLoadReadVersion();
        List<SegmentAnalysis> segmentAnalysisList = analyze();
        if (segmentAnalysisList.isEmpty()) {
            LOGGER.info("No segments found for read version {} on volume {}", readVersion, volume.getConfig().name());
            return;
        }
        LOGGER.info("Starting Vacuum on volume {}", volume.getConfig().name());
        for (SegmentAnalysis segmentAnalysis : segmentAnalysisList) {
            if (stop.get()) {
                LOGGER.info("Stopping Vacuum on volume {}", volume.getConfig().name());
                break;
            }
            if (segmentAnalysis.garbageRatio() < allowedGarbageRatio) {
                LOGGER.debug("Garbage ratio doesn't exceed the allowed garbage ratio, skipping Vacuum on segment: {} on volume {}",
                        segmentAnalysis.name(),
                        volume.getConfig().name()
                );
                continue;
            }
            LOGGER.debug("Vacuuming segment: {} on volume {}", segmentAnalysis.name(), volume.getConfig().name());
            vacuumContext = new VacuumContext(segmentAnalysis.name(), readVersion, stop);
            volume.vacuumSegment(vacuumContext);
        }
    }

    /**
     * Stops the vacuum process gracefully by setting the stop signal.
     * This method triggers a flag indicating that the vacuum process should terminate.
     * Once invoked, the stop signal allows ongoing operations to detect the shutdown request
     * and halt further processing accordingly.
     */
    void stop() {
        stop.set(true);
    }
}
