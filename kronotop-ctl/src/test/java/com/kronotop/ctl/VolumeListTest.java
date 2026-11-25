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

import com.kronotop.resp.RespValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.kronotop.ctl.RespValues.map;
import static com.kronotop.ctl.RespValues.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VolumeListTest {
    private static final String PRIMARY_ID = "006cdc459c59e600c76494e8388857fc3cba2fa8";
    private static final String STANDBY_ID = "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149";
    private static final long MIB = 1024 * 1024;

    private static RespValue segment(long size, long freeBytes, long usedBytes, long cardinality) {
        return map(
                text("size"), new RespValue.Number(size),
                text("free_bytes"), new RespValue.Number(freeBytes),
                text("used_bytes"), new RespValue.Number(usedBytes),
                text("garbage_percentage"), new RespValue.Double(25.0),
                text("cardinality"), new RespValue.Number(cardinality)
        );
    }

    // Two segments of 1 GiB. Together they use 1 GiB, leave 512 MiB free and hold 150 entries.
    private static RespValue describeReply() {
        return map(
                text("name"), text("shard-0"),
                text("status"), text("READWRITE"),
                text("data_dir"), text("/var/lib/kronotop/shard-0"),
                text("segment_size"), new RespValue.Number(1024 * MIB),
                text("segments"), map(
                        new RespValue.Number(0), segment(1024 * MIB, 0, 768 * MIB, 100),
                        new RespValue.Number(1), segment(1024 * MIB, 512 * MIB, 256 * MIB, 50)
                )
        );
    }

    // The primary of shard-0 answered. The standby failed.
    private static VolumeList volumeList() throws IOException {
        VolumeList.VolumeLocation primary = new VolumeList.VolumeLocation(0, "shard-0", PRIMARY_ID, "primary");
        VolumeList.VolumeLocation standby = new VolumeList.VolumeLocation(0, "shard-0", STANDBY_ID, "standby");
        return new VolumeList(List.of(
                VolumeList.MemberVolume.parse(primary, describeReply()),
                VolumeList.MemberVolume.failed(standby, "Connection refused")
        ));
    }

    // Shards 0 and 1 have the same primary and the same standby.
    private static ClusterDescription description() {
        List<ClusterDescription.Shard> shards = List.of(
                new ClusterDescription.Shard(0, "READWRITE", "shard-0", PRIMARY_ID, List.of(STANDBY_ID)),
                new ClusterDescription.Shard(1, "READWRITE", "shard-1", PRIMARY_ID, List.of(STANDBY_ID))
        );
        List<ClusterDescription.Member> members = List.of(
                new ClusterDescription.Member(PRIMARY_ID, "RUNNING", false, List.of("10.0.0.1:3320"), List.of()),
                new ClusterDescription.Member(STANDBY_ID, "RUNNING", false, List.of("10.0.0.2:3320"), List.of())
        );
        return new ClusterDescription("my-cluster", "1.0.0", shards, members);
    }

    @Test
    void shouldRenderVolumeTable() throws IOException {
        // Behavior: on the primary, shard-0 has two segments of 1 GiB that use 1 GiB, leave 512 MiB free and hold
        // 150 entries. The standby failed with "Connection refused". toTable prints one row per member. The
        // primary row shows the segment count, the totals in binary units, 25.0% garbage and "-" as the error.
        // The standby row shows "-" in every value column and the error message at the end.
        String expected = """
                VOLUME   MEMBER  ROLE     STATUS     SEGMENTS  SIZE     USED     FREE       GARBAGE  ENTRIES  ERROR
                shard-0  006c    primary  READWRITE  2         2.0 GiB  1.0 GiB  512.0 MiB  25.0%    150      -
                shard-0  a3f1    standby  -          -         -        -        -          -        -        Connection refused
                """;
        assertEquals(expected, volumeList().toTable());
    }

    @Test
    void shouldRenderVolumeJson() throws IOException {
        // Behavior: on the primary, shard-0 has two segments of 1 GiB that use 1 GiB, leave 512 MiB free and hold
        // 150 entries. The standby failed with "Connection refused". toJson prints one object per member with the
        // full member ID. The primary object has the totals in bytes and a null error. The standby object has
        // null in every value field and the error message.
        String expected = """
                {
                  "volumes": [
                    {
                      "volume": "shard-0",
                      "member_id": "%s",
                      "role": "primary",
                      "status": "READWRITE",
                      "segments": 2,
                      "size": 2147483648,
                      "used_bytes": 1073741824,
                      "free_bytes": 536870912,
                      "garbage_percentage": 25.0,
                      "entries": 150,
                      "error": null
                    },
                    {
                      "volume": "shard-0",
                      "member_id": "%s",
                      "role": "standby",
                      "status": null,
                      "segments": null,
                      "size": null,
                      "used_bytes": null,
                      "free_bytes": null,
                      "garbage_percentage": null,
                      "entries": null,
                      "error": "Connection refused"
                    }
                  ]
                }""".formatted(PRIMARY_ID, STANDBY_ID);
        assertEquals(expected, volumeList().toJson());
    }

    @Test
    void shouldSelectLocationsByShardAndMember() {
        // Behavior: shards 0 and 1 have the same primary and the same standby. select with shard 1 and the
        // four-character prefix of the standby returns only shard-1 on the standby, with the full member ID.
        List<VolumeList.VolumeLocation> locations = VolumeList.select(description(), 1, "a3f1");

        assertEquals(List.of(new VolumeList.VolumeLocation(1, "shard-1", STANDBY_ID, "standby")), locations);
    }

    @Test
    void shouldRejectUnknownShard() {
        // Behavior: the cluster has shards 0 and 1. select with shard 7 throws an IllegalArgumentException with
        // the message "No such shard: '7'".
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> VolumeList.select(description(), 7, null));

        assertEquals("No such shard: '7'", exception.getMessage());
    }
}
