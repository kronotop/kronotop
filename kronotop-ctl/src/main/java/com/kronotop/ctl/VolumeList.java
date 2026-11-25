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

package com.kronotop.ctl;

import com.kronotop.resp.JsonResponseFormatter;
import com.kronotop.resp.RespConnection;
import com.kronotop.resp.RespValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.kronotop.ctl.ControlUtil.MEMBER_ID_LENGTH;
import static com.kronotop.ctl.ControlUtil.MEMBER_ID_PREFIX_LENGTH;
import static com.kronotop.ctl.ControlUtil.NONE;
import static com.kronotop.ctl.ControlUtil.prefix;
import static com.kronotop.ctl.ControlUtil.put;

/**
 * The volumes of a cluster, as each owner of a shard reports them with VOLUME.ADMIN DESCRIBE.
 */
public record VolumeList(List<MemberVolume> volumes) {
    private static final String VOLUME_ADMIN = "VOLUME.ADMIN";
    private static final String DESCRIBE = "DESCRIBE";
    private static final String COMMAND = VOLUME_ADMIN + " " + DESCRIBE;
    private static final String PRIMARY = "primary";
    private static final String STANDBY = "standby";
    private static final String[] UNITS = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    private static final RespValue NULL = new RespValue.Null();

    /**
     * Returns the volumes to read and the members to read them from, after the shard and member filters.
     *
     * @param description the cluster description
     * @param shardId     the only shard to include, or null for all shards
     * @param member      the only member to include, as a full ID or a four-character prefix, or null for all members
     * @return the locations sorted by shard ID, with the primary of each shard first
     * @throws IllegalArgumentException if the shard does not exist or the member cannot be resolved
     */
    public static List<VolumeLocation> select(ClusterDescription description, Integer shardId, String member) {
        String memberId = member == null ? null : resolveMember(description, member);
        boolean shardFound = false;
        List<VolumeLocation> locations = new ArrayList<>();
        for (ClusterDescription.Shard shard : description.shards()) {
            if (shardId != null && shard.id() != shardId) {
                continue;
            }
            shardFound = true;

            List<VolumeLocation> owners = new ArrayList<>();
            if (shard.primary() != null) {
                owners.add(new VolumeLocation(shard.id(), shard.linkedVolume(), shard.primary(), PRIMARY));
            }
            for (String standby : shard.standbys().stream().sorted().toList()) {
                owners.add(new VolumeLocation(shard.id(), shard.linkedVolume(), standby, STANDBY));
            }
            for (VolumeLocation owner : owners) {
                if (memberId == null || memberId.equals(owner.memberId())) {
                    locations.add(owner);
                }
            }
        }
        if (shardId != null && !shardFound) {
            throw new IllegalArgumentException("No such shard: '" + shardId + "'");
        }
        return locations;
    }

    private static String resolveMember(ClusterDescription description, String member) {
        if (member.length() == MEMBER_ID_LENGTH) {
            return member;
        }
        if (member.length() != MEMBER_ID_PREFIX_LENGTH) {
            throw new IllegalArgumentException("Invalid member id: '" + member + "'");
        }
        List<String> matches = new ArrayList<>();
        for (ClusterDescription.Member candidate : description.members()) {
            if (candidate.id().startsWith(member)) {
                matches.add(candidate.id());
            }
        }
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("No member found with prefix: '" + member + "'");
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException("More than one member found with prefix: '" + member + "'");
        }
        return matches.getFirst();
    }

    /**
     * Reads the volume at each location, with one connection per member and all members at the same time.
     *
     * @param description    the cluster description
     * @param locations      the locations to read
     * @param timeoutSeconds the longest time to wait for a member to connect and for each of its replies
     * @return one entry per location, in the same order, where a volume that could not be read carries the error
     */
    public static VolumeList fetch(ClusterDescription description, List<VolumeLocation> locations, int timeoutSeconds) {
        Map<String, List<VolumeLocation>> locationsByMember = new LinkedHashMap<>();
        for (VolumeLocation location : locations) {
            locationsByMember.computeIfAbsent(location.memberId(), ignored -> new ArrayList<>()).add(location);
        }

        Map<VolumeLocation, MemberVolume> results = new ConcurrentHashMap<>();
        // Closing the executor waits for every member.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (List<VolumeLocation> memberLocations : locationsByMember.values()) {
                executor.execute(() -> describe(description, memberLocations, timeoutSeconds, results));
            }
        }

        List<MemberVolume> volumes = new ArrayList<>();
        for (VolumeLocation location : locations) {
            volumes.add(results.get(location));
        }
        return new VolumeList(volumes);
    }

    // Reads the volumes on one member. Every location gets an entry in results, read or failed.
    private static void describe(
            ClusterDescription description, List<VolumeLocation> locations, int timeoutSeconds, Map<VolumeLocation, MemberVolume> results) {
        try (RespConnection conn = connect(description, locations.getFirst().memberId(), timeoutSeconds)) {
            for (VolumeLocation location : locations) {
                results.put(location, describe(conn, location));
            }
        } catch (Exception e) {
            // The connection is not usable anymore, so the locations without a result share the error.
            String error = Objects.requireNonNullElse(ControlUtil.errorMessage(e), e.toString());
            for (VolumeLocation location : locations) {
                results.putIfAbsent(location, MemberVolume.failed(location, error));
            }
        }
    }

    private static MemberVolume describe(RespConnection conn, VolumeLocation location) throws IOException {
        RespValue reply = conn.executeRaw(List.of(VOLUME_ADMIN, DESCRIBE, location.volume()));
        // A server error fails this volume only. The connection stays usable.
        return switch (reply) {
            case RespValue.SimpleError(String code, String message) -> MemberVolume.failed(location, code + " " + message);
            case RespValue.BlobError(String code, String message) -> MemberVolume.failed(location, code + " " + message);
            default -> MemberVolume.parse(location, reply);
        };
    }

    private static RespConnection connect(ClusterDescription description, String memberId, int timeoutSeconds)
            throws IOException {
        IOException failure = new IOException("No such member: '" + memberId + "'");
        for (ClusterDescription.Member member : description.members()) {
            if (!member.id().equals(memberId)) {
                continue;
            }
            failure = new IOException("No internal address for member: '" + memberId + "'");
            for (String address : member.internalAdvertise()) {
                try {
                    return ControlUtil.connect(address, timeoutSeconds);
                } catch (IOException e) {
                    failure = e;
                }
            }
        }
        throw failure;
    }

    private static String formatBytes(long bytes) {
        double value = bytes;
        int unit = 0;
        while (value >= 1024 && unit < UNITS.length - 1) {
            value /= 1024;
            unit++;
        }
        if (unit == 0) {
            return bytes + " " + UNITS[0];
        }
        return String.format(Locale.ROOT, "%.1f %s", value, UNITS[unit]);
    }

    /**
     * Returns whether any volume could not be read.
     *
     * @return true if at least one entry carries an error
     */
    public boolean hasErrors() {
        for (MemberVolume volume : volumes) {
            if (volume.error() != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the list as text with one row per volume and member.
     *
     * @return the text, ending with a newline
     */
    public String toTable() {
        TextTable table = new TextTable(
                "VOLUME", "MEMBER", "ROLE", "STATUS", "SEGMENTS", "SIZE", "USED", "FREE", "GARBAGE", "ENTRIES", "ERROR");
        for (MemberVolume volume : volumes) {
            VolumeLocation location = volume.location();
            Usage usage = volume.usage();
            if (usage == null) {
                table.addRow(location.volume(), prefix(location.memberId()), location.role(),
                        NONE, NONE, NONE, NONE, NONE, NONE, NONE, volume.error());
                continue;
            }
            table.addRow(
                    location.volume(),
                    prefix(location.memberId()),
                    location.role(),
                    volume.status(),
                    String.valueOf(usage.segments()),
                    formatBytes(usage.size()),
                    formatBytes(usage.usedBytes()),
                    formatBytes(usage.freeBytes()),
                    String.format(Locale.ROOT, "%.1f%%", usage.garbagePercentage()),
                    String.valueOf(usage.entries()),
                    NONE
            );
        }
        return table.render();
    }

    /**
     * Returns the list as indented JSON with the same fields as the table, and sizes in bytes.
     *
     * @return the JSON document
     */
    public String toJson() {
        List<RespValue> values = new ArrayList<>();
        for (MemberVolume volume : volumes) {
            VolumeLocation location = volume.location();
            Usage usage = volume.usage();
            Map<RespValue, RespValue> value = new LinkedHashMap<>();
            put(value, "volume", location.volume());
            put(value, "member_id", location.memberId());
            put(value, "role", location.role());
            put(value, "status", usage == null ? NULL : new RespValue.BlobString(volume.status()));
            put(value, "segments", usage == null ? NULL : new RespValue.Number(usage.segments()));
            put(value, "size", usage == null ? NULL : new RespValue.Number(usage.size()));
            put(value, "used_bytes", usage == null ? NULL : new RespValue.Number(usage.usedBytes()));
            put(value, "free_bytes", usage == null ? NULL : new RespValue.Number(usage.freeBytes()));
            put(value, "garbage_percentage", usage == null ? NULL : new RespValue.Double(usage.garbagePercentage()));
            put(value, "entries", usage == null ? NULL : new RespValue.Number(usage.entries()));
            put(value, "error", volume.error() == null ? NULL : new RespValue.BlobString(volume.error()));
            values.add(new RespValue.RespMap(value));
        }

        Map<RespValue, RespValue> root = new LinkedHashMap<>();
        put(root, "volumes", new RespValue.Array(values));
        return new JsonResponseFormatter().formatPretty(new RespValue.RespMap(root));
    }

    /**
     * A volume on one member: the shard, the volume name, the member ID and the role of the member.
     */
    public record VolumeLocation(int shardId, String volume, String memberId, String role) {
    }

    /**
     * The segment totals of a volume on one member.
     */
    public record Usage(int segments, long size, long usedBytes, long freeBytes, long entries) {

        double garbagePercentage() {
            return size == 0 ? 0 : (size - freeBytes - usedBytes) * 100.0 / size;
        }
    }

    /**
     * A volume as one member reports it. The status and the usage are null when the error is set.
     */
    public record MemberVolume(VolumeLocation location, String status, Usage usage, String error) {

        static MemberVolume failed(VolumeLocation location, String error) {
            return new MemberVolume(location, null, null, error);
        }

        static MemberVolume parse(VolumeLocation location, RespValue reply) throws IOException {
            Reply description = Reply.of(COMMAND, reply);
            int segments = 0;
            long size = 0;
            long usedBytes = 0;
            long freeBytes = 0;
            long entries = 0;
            for (RespValue value : description.reply("segments").fields().values()) {
                Reply segment = Reply.of(COMMAND, value);
                segments++;
                size += segment.number("size");
                usedBytes += segment.number("used_bytes");
                freeBytes += segment.number("free_bytes");
                entries += segment.number("cardinality");
            }
            return new MemberVolume(
                    location, description.text("status"), new Usage(segments, size, usedBytes, freeBytes, entries), null);
        }
    }
}
