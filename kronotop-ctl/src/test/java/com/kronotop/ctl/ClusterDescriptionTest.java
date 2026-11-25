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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterDescriptionTest {
    private static final String PRIMARY_ID = "006cdc459c59e600c76494e8388857fc3cba2fa8";
    private static final String STANDBY_ID = "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149";
    private static final String SPARE_ID = "7be29f0c4d1a86e35b02c9d7f4a1e6083c5d2b9a";

    private static RespValue text(String value) {
        return new RespValue.BlobString(value);
    }

    private static RespValue texts(String... values) {
        return new RespValue.Array(Stream.of(values).map(ClusterDescriptionTest::text).toList());
    }

    // Builds a map reply from alternating keys and values, in the given order.
    private static RespValue map(RespValue... pairs) {
        Map<RespValue, RespValue> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put(pairs[i], pairs[i + 1]);
        }
        return new RespValue.RespMap(values);
    }

    private static RespValue shard(String primary, RespValue standbys, String status, String volume) {
        return map(
                text("primary"), text(primary),
                text("standbys"), standbys,
                text("status"), text(status),
                text("linked_volume"), text(volume)
        );
    }

    // Shard 0 has a primary and one standby. Shard 1 has no owner.
    private static RespValue describeClusterReply() {
        return map(
                text("metadata_version"), text("1.0.0"),
                text("cluster_name"), text("my-cluster"),
                text("shards"), map(
                        new RespValue.Number(0), shard(PRIMARY_ID, texts(STANDBY_ID), "READWRITE", "shard-0"),
                        new RespValue.Number(1), shard("", texts(), "INOPERABLE", "shard-1")
                )
        );
    }

    // The reply lists three members out of ID order. The spare member owns no shard.
    private static RespValue listMembersReply() {
        return map(
                text(STANDBY_ID), member("RUNNING", "10.0.0.2"),
                text(SPARE_ID), member("STOPPED", "10.0.0.3"),
                text(PRIMARY_ID), member("RUNNING", "10.0.0.1")
        );
    }

    // The standby member is silent although its status is RUNNING.
    private static RespValue listSilentMembersReply() {
        return texts(STANDBY_ID);
    }

    private static RespValue member(String status, String host) {
        return map(
                text("status"), text(status),
                text("external_advertise"), texts(host + ":5484"),
                text("internal_advertise"), texts(host + ":3320")
        );
    }

    @Test
    void shouldRenderShardAndMemberTables() throws IOException {
        // Behavior: shard 0 has a primary and one standby, shard 1 has no owner, and a third member owns no shard.
        // toTable prints the cluster name and the metadata version, then a shard table and a member table. The
        // shard table shows each owner as the first four characters of its member ID and a missing owner as "-".
        // The member table lists all three members sorted by member ID with their status and addresses, and SILENT
        // is "yes" only for the member named in the LIST-SILENT-MEMBERS reply.
        ClusterDescription description = ClusterDescription.parse(
                describeClusterReply(), listMembersReply(), listSilentMembersReply());

        String expected = """
                Cluster:           my-cluster
                Metadata version:  1.0.0

                SHARD  STATUS      VOLUME   PRIMARY  STANDBYS
                0      READWRITE   shard-0  006c     a3f1
                1      INOPERABLE  shard-1  -        -

                MEMBER  STATUS   SILENT  INTERNAL       EXTERNAL
                006c    RUNNING  no      10.0.0.1:3320  10.0.0.1:5484
                7be2    STOPPED  no      10.0.0.3:3320  10.0.0.3:5484
                a3f1    RUNNING  yes     10.0.0.2:3320  10.0.0.2:5484
                """;
        assertEquals(expected, description.toTable());
    }

    @Test
    void shouldRenderJsonWithShardsAndMembers() throws IOException {
        // Behavior: shard 0 has a primary and one standby, shard 1 has no owner, and a third member owns no shard.
        // toJson prints indented JSON with a shards list and a members list. A shard names its owners by full
        // member ID, and the shard with no owner has a null primary and an empty standbys list. The members list
        // has all three members sorted by member ID with their status and addresses, and silent is true only for
        // the member named in the LIST-SILENT-MEMBERS reply.
        ClusterDescription description = ClusterDescription.parse(
                describeClusterReply(), listMembersReply(), listSilentMembersReply());

        String expected = """
                {
                  "cluster_name": "my-cluster",
                  "metadata_version": "1.0.0",
                  "shards": [
                    {
                      "id": 0,
                      "status": "READWRITE",
                      "linked_volume": "shard-0",
                      "primary": "%1$s",
                      "standbys": [
                        "%3$s"
                      ]
                    },
                    {
                      "id": 1,
                      "status": "INOPERABLE",
                      "linked_volume": "shard-1",
                      "primary": null,
                      "standbys": []
                    }
                  ],
                  "members": [
                    {
                      "member_id": "%1$s",
                      "status": "RUNNING",
                      "silent": false,
                      "internal_advertise": [
                        "10.0.0.1:3320"
                      ],
                      "external_advertise": [
                        "10.0.0.1:5484"
                      ]
                    },
                    {
                      "member_id": "%2$s",
                      "status": "STOPPED",
                      "silent": false,
                      "internal_advertise": [
                        "10.0.0.3:3320"
                      ],
                      "external_advertise": [
                        "10.0.0.3:5484"
                      ]
                    },
                    {
                      "member_id": "%3$s",
                      "status": "RUNNING",
                      "silent": true,
                      "internal_advertise": [
                        "10.0.0.2:3320"
                      ],
                      "external_advertise": [
                        "10.0.0.2:5484"
                      ]
                    }
                  ]
                }""".formatted(PRIMARY_ID, SPARE_ID, STANDBY_ID);
        assertEquals(expected, description.toJson());
    }

    @Test
    void shouldRejectUnexpectedDescribeClusterReply() {
        // Behavior: parse gets an array as the DESCRIBE-CLUSTER reply, where a map is expected. It throws an
        // IOException whose message names DESCRIBE-CLUSTER and shows the reply.
        IOException exception = assertThrows(IOException.class,
                () -> ClusterDescription.parse(texts(), listMembersReply(), listSilentMembersReply()));

        assertEquals("Unexpected DESCRIBE-CLUSTER response: Array[values=[]]", exception.getMessage());
    }
}
