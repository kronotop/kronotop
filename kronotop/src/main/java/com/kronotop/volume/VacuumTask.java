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
import com.kronotop.Context;
import com.kronotop.task.Task;
import com.kronotop.volume.segment.SegmentAnalysis;

import java.util.List;

public class VacuumTask implements Task {
    private final Context context;
    private final String volumeName;

    public VacuumTask(Context context, String volumeName) {
        this.context = context;
        this.volumeName = volumeName;
    }


    @Override
    public String name() {
        return "vacuum:" + volumeName;
    }

    private List<SegmentAnalysis> analyze(Volume volume) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return volume.analyze(tr);
        }
    }

    @Override
    public void run() {
        VolumeService service = context.getService(VolumeService.NAME);
        Volume volume = service.findVolume(volumeName);

        List<SegmentAnalysis> analysis;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            analysis = volume.analyze(tr);
        }
        for (SegmentAnalysis segmentAnalysis : analysis) {
            if (segmentAnalysis.garbageRatio() < volume.getConfig().allowedGarbageRatio()) {
                continue;
            }
            volume.vacuumSegment(segmentAnalysis.name(), readVersion);
        }
    }

    @Override
    public void shutdown() {

    }
}
