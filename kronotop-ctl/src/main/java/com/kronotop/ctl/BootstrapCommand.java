/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.ctl;

import com.kronotop.resp.RespConnection;
import com.kronotop.resp.RespValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Command(
        name = "bootstrap",
        description = "Bootstrap a Kronotop cluster.",
        mixinStandardHelpOptions = true,
        separator = " "
)
public class BootstrapCommand implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapCommand.class);
    private static final Set<String> VALID_SHARD_KINDS = Set.of("BUCKET", "STASH");
    @Option(names = "--primary", arity = "2..*", paramLabel = "<shard-kind host:port=shard-ids>",
            description = "Assign primary shards (repeatable). Example: --primary BUCKET node1:3320=0,1,2")
    private List<String> primaries;
    @Option(names = "--standby", arity = "2..*", paramLabel = "<shard-kind host:port=shard-ids>",
            description = "Assign standby shards (repeatable). Example: --standby BUCKET node2:3320=0,1,2")
    private List<String> standbys;
    @Option(names = "--shard-discovery-timeout", paramLabel = "<seconds>", defaultValue = "30",
            description = "Max seconds to wait for shard discovery after cluster initialization. Default: ${DEFAULT-VALUE}")
    private int shardDiscoveryTimeout;

    @Override
    public Integer call() {
        if (primaries == null || primaries.isEmpty()) {
            logger.error("At least one --primary is required");
            return 1;
        }

        try {
            if (primaries.size() % 2 != 0) {
                throw new IllegalArgumentException(
                        "--primary requires pairs of <shard-kind> <host:port=shard-ids>, but got " + primaries.size() + " arguments");
            }
            List<RouteAssignment> assignments = new ArrayList<>();
            for (int i = 0; i < primaries.size(); i += 2) {
                assignments.add(parseAssignment("PRIMARY", primaries.get(i), primaries.get(i + 1)));
            }
            if (standbys != null) {
                if (standbys.size() % 2 != 0) {
                    throw new IllegalArgumentException(
                            "--standby requires pairs of <shard-kind> <host:port=shard-ids>, but got " + standbys.size() + " arguments");
                }
                for (int i = 0; i < standbys.size(); i += 2) {
                    assignments.add(parseAssignment("STANDBY", standbys.get(i), standbys.get(i + 1)));
                }
            }
            String firstPrimaryAddress = assignments.stream()
                    .filter(a -> a.routeKind().equals("PRIMARY"))
                    .findFirst()
                    .orElseThrow()
                    .address();

            String[] hostPort = parseHostPort(firstPrimaryAddress);
            try (RespConnection conn = new RespConnection(hostPort[0], Integer.parseInt(hostPort[1]))) {
                conn.hello3();

                if (initializeCluster(conn)) {
                    return 0;
                }

                waitForShards(conn, assignments);
                Map<String, String> addressToMemberId = resolveMemberIds(assignments);
                setRoutes(conn, assignments, addressToMemberId);
                setShardStatuses(conn, assignments);

                logger.info("Cluster bootstrapped successfully");
                return 0;
            }
        } catch (Exception e) {
            String message = e.getMessage();
            if (message != null && message.startsWith("Server error: ")) {
                message = message.substring("Server error: ".length());
            }
            logger.error("{}", message);
            return 1;
        }
    }

    private Map<String, String> resolveMemberIds(List<RouteAssignment> assignments) throws IOException {
        Set<String> uniqueAddresses = assignments.stream()
                .map(RouteAssignment::address)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        logger.info("Resolving member IDs");
        Map<String, String> addressToMemberId = new HashMap<>();
        for (String address : uniqueAddresses) {
            String[] hostPort = parseHostPort(address);
            try (RespConnection conn = new RespConnection(hostPort[0], Integer.parseInt(hostPort[1]))) {
                conn.hello3();
                RespValue response = conn.execute(List.of("KR.ADMIN", "DESCRIBE-MEMBER"));

                if (!(response instanceof RespValue.RespMap(Map<RespValue, RespValue> props))) {
                    throw new IOException("Unexpected DESCRIBE-MEMBER response from " + address);
                }

                String memberId = null;
                for (Map.Entry<RespValue, RespValue> prop : props.entrySet()) {
                    if ("member_id".equals(extractString(prop.getKey()))) {
                        memberId = extractString(prop.getValue());
                        break;
                    }
                }

                if (memberId == null) {
                    throw new IOException("DESCRIBE-MEMBER response missing member_id from " + address);
                }

                addressToMemberId.put(address, memberId);
                logger.info("Resolved member at {} -> {}", address, memberId);
            }
        }
        return addressToMemberId;
    }

    private boolean initializeCluster(RespConnection conn) throws IOException {
        logger.info("Initializing cluster");
        RespValue response = conn.executeRaw(List.of("KR.ADMIN", "INITIALIZE-CLUSTER"));
        String errorMessage = switch (response) {
            case RespValue.SimpleError(String code, String message) -> code + " " + message;
            case RespValue.BlobError(String code, String message) -> code + " " + message;
            default -> null;
        };
        if (errorMessage != null) {
            if (errorMessage.contains("cluster has already been initialized")) {
                logger.info("Cluster has already been initialized, skipping bootstrap");
                return true;
            }
            throw new IOException("Server error: " + errorMessage);
        }
        logger.info("Cluster initialized");
        return false;
    }

    private void waitForShards(RespConnection conn, List<RouteAssignment> assignments) throws IOException {
        Map<String, Set<Integer>> required = new HashMap<>();
        for (RouteAssignment a : assignments) {
            required.computeIfAbsent(a.shardKind(), k -> new HashSet<>()).addAll(a.shardIds());
        }

        logger.info("Waiting for shard discovery (timeout: {}s)", shardDiscoveryTimeout);
        for (int attempt = 1; attempt <= shardDiscoveryTimeout; attempt++) {
            Map<String, Set<Integer>> current = describeCluster(conn);

            boolean allDiscovered = true;
            for (String kind : required.keySet()) {
                Set<Integer> available = current.getOrDefault(kind, Set.of());
                if (available.isEmpty()) {
                    allDiscovered = false;
                    break;
                }
            }

            if (allDiscovered) {
                for (Map.Entry<String, Set<Integer>> entry : required.entrySet()) {
                    Set<Integer> available = current.get(entry.getKey());
                    Set<Integer> invalid = new TreeSet<>(entry.getValue());
                    invalid.removeAll(available);
                    if (!invalid.isEmpty()) {
                        throw new IOException("Invalid shard ids " + invalid + " for " + entry.getKey()
                                + ". Available: " + new TreeSet<>(available));
                    }
                }
                logger.info("All shards discovered");
                return;
            }

            logger.info("Waiting for shard discovery ({}/{}s)", attempt, shardDiscoveryTimeout);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for shard discovery");
            }
        }

        throw new IOException("Timed out waiting for shards after " + shardDiscoveryTimeout + "s");
    }

    private Map<String, Set<Integer>> describeCluster(RespConnection conn) throws IOException {
        RespValue response = conn.execute(List.of("KR.ADMIN", "DESCRIBE-CLUSTER"));

        if (!(response instanceof RespValue.RespMap(Map<RespValue, RespValue> cluster))) {
            throw new IOException("Unexpected DESCRIBE-CLUSTER response: " + response);
        }

        Map<String, Set<Integer>> shardsByKind = new HashMap<>();
        for (Map.Entry<RespValue, RespValue> entry : cluster.entrySet()) {
            String key = extractString(entry.getKey());
            if (!(entry.getValue() instanceof RespValue.RespMap(Map<RespValue, RespValue> shards))) {
                continue;
            }
            Set<Integer> shardIds = new HashSet<>();
            for (RespValue shardKey : shards.keySet()) {
                if (shardKey instanceof RespValue.Number(long id)) {
                    shardIds.add((int) id);
                }
            }
            shardsByKind.put(key.toUpperCase(), shardIds);
        }
        return shardsByKind;
    }

    private void setRoutes(RespConnection conn, List<RouteAssignment> assignments,
                           Map<String, String> addressToMemberId) throws IOException {
        logger.info("Setting routes");
        for (RouteAssignment a : assignments) {
            String memberId = addressToMemberId.get(a.address());
            for (int shardId : a.shardIds()) {
                logger.info("Route {} {} shard {} -> member {} ({})",
                        a.routeKind(), a.shardKind(), shardId, memberId, a.address());
                conn.execute(List.of(
                        "KR.ADMIN", "ROUTE", "SET",
                        a.routeKind(),
                        a.shardKind(),
                        String.valueOf(shardId),
                        memberId
                ));
            }
        }
    }

    private void setShardStatuses(RespConnection conn, List<RouteAssignment> assignments) throws IOException {
        for (RouteAssignment a : assignments) {
            for (int shardId : a.shardIds()) {
                logger.info("Setting shard status {} {} READWRITE", a.shardKind(), shardId);
                conn.execute(List.of("KR.ADMIN", "SET-SHARD-STATUS", a.shardKind(), String.valueOf(shardId), "READWRITE"));
            }
        }
    }

    private RouteAssignment parseAssignment(String routeKind, String shardKindRaw, String spec) {
        String shardKind = shardKindRaw.toUpperCase();
        if (!VALID_SHARD_KINDS.contains(shardKind)) {
            throw new IllegalArgumentException(
                    "Invalid shard kind: " + shardKind + ". Valid kinds: " + VALID_SHARD_KINDS);
        }

        int eqIndex = spec.indexOf('=');
        if (eqIndex < 0) {
            throw new IllegalArgumentException(
                    "Invalid format: " + spec + ". Expected host:port=shard1,shard2,...");
        }

        String address = spec.substring(0, eqIndex);
        String shardsPart = spec.substring(eqIndex + 1);

        List<Integer> shardIds = Arrays.stream(shardsPart.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .toList();

        return new RouteAssignment(routeKind, shardKind, address, shardIds);
    }

    private String[] parseHostPort(String address) {
        int lastColon = address.lastIndexOf(':');
        if (lastColon < 0) {
            throw new IllegalArgumentException("Invalid address (missing port): " + address);
        }
        return new String[]{address.substring(0, lastColon), address.substring(lastColon + 1)};
    }

    private String extractString(RespValue value) {
        return switch (value) {
            case RespValue.BlobString(String v) -> v;
            case RespValue.SimpleString(String v) -> v;
            default -> throw new IllegalArgumentException("Expected string, got: " + value);
        };
    }

    record RouteAssignment(String routeKind, String shardKind, String address, List<Integer> shardIds) {
    }
}
