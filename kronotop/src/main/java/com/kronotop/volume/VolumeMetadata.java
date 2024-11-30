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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.kronotop.JSONUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

@JsonIgnoreProperties(ignoreUnknown = true)
public class VolumeMetadata {
    @JsonIgnore
    private static final String VOLUME_METADATA_KEY = "volume-metadata";

    private final List<Long> segments = new ArrayList<>();

    public static VolumeMetadata load(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(VOLUME_METADATA_KEY);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return new VolumeMetadata();
        }
        return JSONUtils.readValue(value, VolumeMetadata.class);
    }

    public static VolumeMetadata compute(Transaction tr, DirectorySubspace subspace, Consumer<VolumeMetadata> remappingFunction) {
        byte[] key = subspace.pack(VOLUME_METADATA_KEY);
        VolumeMetadata volumeMetadata;
        byte[] value = tr.get(key).join();
        if (value == null) {
            volumeMetadata = new VolumeMetadata();
        } else {
            volumeMetadata = JSONUtils.readValue(value, VolumeMetadata.class);
        }
        remappingFunction.accept(volumeMetadata);
        tr.set(key, volumeMetadata.toByte());
        return volumeMetadata;
    }

    public void addSegment(long segmentId) {
        for (long id : segments) {
            if (id == segmentId) {
                throw new IllegalArgumentException("Duplicate segment");
            }
        }
        segments.add(segmentId);
        segments.sort(Comparator.naturalOrder());
    }

    public void removeSegment(long segmentId) {
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i) == segmentId) {
                segments.remove(i);
                return;
            }
        }
        throw new IllegalArgumentException("No such segment");
    }

    public List<Long> getSegments() {
        return Collections.unmodifiableList(segments);
    }

    public byte[] toByte() {
        return JSONUtils.writeValueAsBytes(this);
    }
}