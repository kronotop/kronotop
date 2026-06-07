/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

import javax.annotation.Nonnull;

import java.util.Objects;

import static com.kronotop.volume.Subspaces.*;

public class VolumeSubspace {
    private final DirectorySubspace subspace;

    public VolumeSubspace(@Nonnull DirectorySubspace subspace) {
        Objects.requireNonNull(subspace);
        this.subspace = subspace;
    }

    public DirectorySubspace getDirectorySubspace() {
        return subspace;
    }

    byte[] packEntryKeyPrefix(Prefix prefix) {
        return subspace.pack(
                Tuple.from(
                        ENTRY_SUBSPACE,
                        prefix.asBytes()
                )
        );
    }

    byte[] packEntryKey(Prefix prefix, Versionstamp key) {
        return subspace.pack(
                Tuple.from(
                        ENTRY_SUBSPACE,
                        prefix.asBytes(),
                        key
                )
        );
    }

    byte[] packEntryKeyWithVersionstamp(Prefix prefix, int version) {
        return subspace.packWithVersionstamp(
                Tuple.from(
                        ENTRY_SUBSPACE,
                        prefix.asBytes(),
                        Versionstamp.incomplete(version)
                )
        );
    }

    byte[] packEntryMetadataKey(byte[] data) {
        return subspace.pack(
                Tuple.from(
                        ENTRY_METADATA_SUBSPACE,
                        data
                )
        );
    }

    byte[] packSegmentStatsKey(long segmentId, Prefix prefix, byte statType) {
        return subspace.pack(
                Tuple.from(
                        SEGMENT_STATS_SUBSPACE,
                        segmentId,
                        prefix.asBytes(),
                        statType
                )
        );
    }

    byte[] packSegmentStatsVolumePrefix() {
        return subspace.pack(
                Tuple.from(
                        SEGMENT_STATS_SUBSPACE
                )
        );
    }

    byte[] packSegmentStatsSegmentPrefix(long segmentId) {
        return subspace.pack(
                Tuple.from(
                        SEGMENT_STATS_SUBSPACE,
                        segmentId
                )
        );
    }

    byte[] packVacuumSegmentMetadataFieldKey(long segmentId, byte field) {
        return subspace.pack(
                Tuple.from(
                        VACUUM_METADATA_SUBSPACE,
                        VacuumMetadataSubspaces.SEGMENT,
                        segmentId,
                        field
                )
        );
    }

    byte[] packVacuumSegmentMetadataPrefixCardinalityKey(long segmentId, Prefix prefix) {
        return subspace.pack(
                Tuple.from(
                        VACUUM_METADATA_SUBSPACE,
                        VacuumMetadataSubspaces.SEGMENT,
                        segmentId,
                        VacuumMetadataField.PREFIX_CARDINALITY.getValue(),
                        prefix.asBytes()
                )
        );
    }

    Range packVacuumSegmentMetadataSegmentRange(long segmentId) {
        byte[] begin = subspace.pack(
                Tuple.from(
                        VACUUM_METADATA_SUBSPACE,
                        VacuumMetadataSubspaces.SEGMENT,
                        segmentId
                )
        );
        byte[] end = ByteArrayUtil.strinc(begin);
        return new Range(begin, end);
    }

    Range packVacuumSegmentMetadataPrefixRange(long segmentId) {
        byte[] begin = subspace.pack(
                Tuple.from(
                        VACUUM_METADATA_SUBSPACE,
                        VacuumMetadataSubspaces.SEGMENT,
                        segmentId,
                        VacuumMetadataField.PREFIX_CARDINALITY.getValue()
                )
        );
        byte[] end = ByteArrayUtil.strinc(begin);
        return new Range(begin, end);
    }

    Range packVacuumSegmentMetadataSegmentsRange() {
        byte[] begin = subspace.pack(Tuple.from(VACUUM_METADATA_SUBSPACE, VacuumMetadataSubspaces.SEGMENT));
        byte[] end = ByteArrayUtil.strinc(begin);
        return new Range(begin, end);
    }

    byte[] packVacuumStatsFieldKey(byte field) {
        return subspace.pack(
                Tuple.from(
                        VACUUM_METADATA_SUBSPACE,
                        VacuumMetadataSubspaces.VOLUME,
                        field
                )
        );
    }

    Range packVacuumMetadataRange() {
        byte[] begin = subspace.pack(Tuple.from(VACUUM_METADATA_SUBSPACE));
        byte[] end = ByteArrayUtil.strinc(begin);
        return new Range(begin, end);
    }

    byte[] packSegmentPositionPrefix(long segmentId) {
        return subspace.pack(
                Tuple.from(
                        SEGMENT_POSITION_SUBSPACE,
                        segmentId
                )
        );
    }

    byte[] packSegmentPositionKey(long segmentId, long position, long length) {
        return subspace.pack(
                Tuple.from(
                        SEGMENT_POSITION_SUBSPACE,
                        segmentId,
                        position,
                        length
                )
        );
    }

    byte[] packVolumeIdKey() {
        return subspace.pack(
                Tuple.from(
                        VOLUME_METADATA_SUBSPACE,
                        VolumeMetadataField.ID.getValue()
                )
        );
    }

    byte[] packVolumeStatusKey() {
        return subspace.pack(
                Tuple.from(
                        VOLUME_METADATA_SUBSPACE,
                        VolumeMetadataField.STATUS.getValue()
                )
        );
    }

    byte[] packVolumeSegmentIdKey(long segmentId) {
        return subspace.pack(
                Tuple.from(
                        VOLUME_METADATA_SUBSPACE,
                        VolumeMetadataField.SEGMENT_ID.getValue(),
                        segmentId
                )
        );
    }

    Range packVolumeSegmentsRange() {
        byte[] begin = subspace.pack(
                Tuple.from(
                        VOLUME_METADATA_SUBSPACE,
                        VolumeMetadataField.SEGMENT_ID.getValue()
                )
        );
        byte[] end = ByteArrayUtil.strinc(begin);
        return new Range(begin, end);
    }
}
