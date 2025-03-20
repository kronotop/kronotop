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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a vacuum process for cleaning and optimizing a specified volume.
 * The Vacuum class supports analyzing and processing segments within a volume
 * to reclaim resources by identifying and addressing garbage data.
 * <p>
 * The Vacuum is initialized with a given context, volume, and metadata,
 * and can be started or stopped as required.
 */
class Vacuum {
    private static final Logger LOGGER = LoggerFactory.getLogger(Vacuum.class);
    private final Context context;
    private final VolumeService service;
    private final Volume volume;
    private final VacuumMetadata vacuumMetadata;
    private final AtomicBoolean stop = new AtomicBoolean();

    protected Vacuum(@Nonnull Context context, @Nonnull Volume volume, @Nonnull VacuumMetadata vacuumMetadata) {
        this.context = context;
        this.service = context.getService(VolumeService.NAME);
        this.volume = volume;
        this.vacuumMetadata = vacuumMetadata;
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
     * @return a list of stale segment names that were cleaned up, or an empty list if no segments were found during analysis.
     * @throws IOException if an I/O error occurs during the vacuum process.
     */
    List<String> start() throws IOException {
        List<SegmentAnalysis> segmentAnalysisList = analyze();
        if (segmentAnalysisList.isEmpty()) {
            LOGGER.warn("No segments found on volume '{}'", volume.getConfig().name());
            return List.of();
        }

        LOGGER.info("Starting Vacuum on volume '{}'", volume.getConfig().name());
        for (SegmentAnalysis segmentAnalysis : segmentAnalysisList) {
            if (stop.get()) {
                LOGGER.info("Stopping Vacuum on volume '{}'", volume.getConfig().name());
                return List.of();
            }
            if (segmentAnalysis.garbageRatio() < vacuumMetadata.getAllowedGarbageRatio()) {
                LOGGER.info("Garbage ratio doesn't exceed the allowed garbage ratio, skipping Vacuum on segment: {} on volume '{}'",
                        segmentAnalysis.name(),
                        volume.getConfig().name()
                );
                continue;
            }
            LOGGER.info("Vacuuming segment: '{}' on volume '{}'", segmentAnalysis.name(), volume.getConfig().name());
            VacuumContext vacuumContext = new VacuumContext(segmentAnalysis.name(), stop);
            volume.vacuumSegment(vacuumContext);
        }
        return volume.cleanupStaleSegments();
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
