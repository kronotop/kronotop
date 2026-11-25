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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.ctl.ControlUtil.NONE;
import static com.kronotop.ctl.ControlUtil.prefix;
import static com.kronotop.ctl.ControlUtil.put;

/**
 * The shards and members of a cluster, built from the replies of KR.ADMIN DESCRIBE-CLUSTER, LIST-MEMBERS and
 * LIST-SILENT-MEMBERS.
 */
public record ClusterDescription(
        String clusterName, String metadataVersion, List<Shard> shards, List<Member> members) {
    private static final String KR_ADMIN = "KR.ADMIN";
    private static final String DESCRIBE_CLUSTER = "DESCRIBE-CLUSTER";
    private static final String LIST_MEMBERS = "LIST-MEMBERS";
    private static final String LIST_SILENT_MEMBERS = "LIST-SILENT-MEMBERS";

    /**
     * Reads the cluster description from a member.
     *
     * @param conn an open RESP3 connection
     * @return the cluster description
     * @throws IOException if a command fails or a reply is not in the expected form
     */
    public static ClusterDescription fetch(RespConnection conn) throws IOException {
        RespValue cluster = conn.execute(List.of(KR_ADMIN, DESCRIBE_CLUSTER));
        RespValue members = conn.execute(List.of(KR_ADMIN, LIST_MEMBERS));
        RespValue silentMembers = conn.execute(List.of(KR_ADMIN, LIST_SILENT_MEMBERS));
        return parse(cluster, members, silentMembers);
    }

    /**
     * Builds the cluster description from the replies of the three commands.
     *
     * @param cluster       the DESCRIBE-CLUSTER reply
     * @param members       the LIST-MEMBERS reply
     * @param silentMembers the LIST-SILENT-MEMBERS reply
     * @return the cluster description, with shards and members sorted by ID
     * @throws IOException if a reply is not in the expected form
     */
    public static ClusterDescription parse(RespValue cluster, RespValue members, RespValue silentMembers)
            throws IOException {
        List<String> silentIds = Reply.texts(LIST_SILENT_MEMBERS, silentMembers);
        List<Member> memberList = new ArrayList<>();
        for (Map.Entry<RespValue, RespValue> entry : Reply.of(LIST_MEMBERS, members).fields().entrySet()) {
            String memberId = Reply.text(LIST_MEMBERS, entry.getKey());
            Reply member = Reply.of(LIST_MEMBERS, entry.getValue());
            memberList.add(new Member(
                    memberId,
                    member.text("status"),
                    silentIds.contains(memberId),
                    member.texts("internal_advertise"),
                    member.texts("external_advertise")
            ));
        }
        memberList.sort(Comparator.comparing(Member::id));

        Reply description = Reply.of(DESCRIBE_CLUSTER, cluster);
        List<Shard> shards = new ArrayList<>();
        for (Map.Entry<RespValue, RespValue> entry : description.reply("shards").fields().entrySet()) {
            if (!(entry.getKey() instanceof RespValue.Number(long shardId))) {
                throw Reply.unexpected(DESCRIBE_CLUSTER, entry.getKey());
            }
            Reply shard = Reply.of(DESCRIBE_CLUSTER, entry.getValue());

            // The server sends an empty string when the shard has no primary.
            String primary = shard.text("primary");
            shards.add(new Shard(
                    (int) shardId,
                    shard.text("status"),
                    shard.text("linked_volume"),
                    primary.isEmpty() ? null : primary,
                    shard.texts("standbys")
            ));
        }
        shards.sort(Comparator.comparingInt(Shard::id));

        return new ClusterDescription(
                description.text("cluster_name"), description.text("metadata_version"), shards, memberList);
    }

    private static RespValue array(List<String> values) {
        List<RespValue> texts = new ArrayList<>();
        for (String value : values) {
            texts.add(new RespValue.BlobString(value));
        }
        return new RespValue.Array(texts);
    }

    private static String join(List<String> values) {
        return values.isEmpty() ? NONE : String.join(", ", values);
    }

    /**
     * Returns the description as text with one row per shard, followed by one row per member.
     *
     * @return the text, ending with a newline
     */
    public String toTable() {
        TextTable shardTable = new TextTable("SHARD", "STATUS", "VOLUME", "PRIMARY", "STANDBYS");
        for (Shard shard : shards) {
            List<String> standbys = new ArrayList<>();
            for (String standby : shard.standbys()) {
                standbys.add(prefix(standby));
            }
            shardTable.addRow(
                    String.valueOf(shard.id()),
                    shard.status(),
                    shard.linkedVolume(),
                    shard.primary() == null ? NONE : prefix(shard.primary()),
                    join(standbys)
            );
        }

        StringBuilder sb = new StringBuilder();
        sb.append("%-19s%s\n".formatted("Cluster:", clusterName));
        sb.append("%-19s%s\n".formatted("Metadata version:", metadataVersion));
        sb.append('\n').append(shardTable.render());
        if (!members.isEmpty()) {
            TextTable memberTable = new TextTable("MEMBER", "STATUS", "SILENT", "INTERNAL", "EXTERNAL");
            for (Member member : members) {
                memberTable.addRow(
                        prefix(member.id()),
                        member.status(),
                        member.silent() ? "yes" : "no",
                        join(member.internalAdvertise()),
                        join(member.externalAdvertise())
                );
            }
            sb.append('\n').append(memberTable.render());
        }
        return sb.toString();
    }

    /**
     * Returns the description as indented JSON with the same shards and members as the table.
     *
     * @return the JSON document
     */
    public String toJson() {
        List<RespValue> shardValues = new ArrayList<>();
        for (Shard shard : shards) {
            Map<RespValue, RespValue> value = new LinkedHashMap<>();
            put(value, "id", new RespValue.Number(shard.id()));
            put(value, "status", shard.status());
            put(value, "linked_volume", shard.linkedVolume());
            put(value, "primary",
                    shard.primary() == null ? new RespValue.Null() : new RespValue.BlobString(shard.primary()));
            put(value, "standbys", array(shard.standbys()));
            shardValues.add(new RespValue.RespMap(value));
        }

        List<RespValue> memberValues = new ArrayList<>();
        for (Member member : members) {
            Map<RespValue, RespValue> value = new LinkedHashMap<>();
            put(value, "member_id", member.id());
            put(value, "status", member.status());
            put(value, "silent", new RespValue.Boolean(member.silent()));
            put(value, "internal_advertise", array(member.internalAdvertise()));
            put(value, "external_advertise", array(member.externalAdvertise()));
            memberValues.add(new RespValue.RespMap(value));
        }

        Map<RespValue, RespValue> root = new LinkedHashMap<>();
        put(root, "cluster_name", clusterName);
        put(root, "metadata_version", metadataVersion);
        put(root, "shards", new RespValue.Array(shardValues));
        put(root, "members", new RespValue.Array(memberValues));
        return new JsonResponseFormatter().formatPretty(new RespValue.RespMap(root));
    }

    /**
     * A shard with its status, volume and owners. The owners are member IDs, and the primary is null when the shard
     * has none.
     */
    public record Shard(int id, String status, String linkedVolume, String primary, List<String> standbys) {
    }

    /**
     * A registered cluster member. It is silent when the member that answered LIST-SILENT-MEMBERS no longer sees its
     * heartbeats.
     */
    public record Member(
            String id, String status, boolean silent, List<String> internalAdvertise, List<String> externalAdvertise) {
    }
}
