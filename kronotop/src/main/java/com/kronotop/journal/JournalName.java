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

package com.kronotop.journal;

import com.kronotop.cluster.Member;

public class JournalName {
    private static final String SHARD_EVENTS_JOURNAL_NAME = ".shard-events";
    private static final String CLUSTER_EVENTS_JOURNAL_NAME = "cluster-events";
    private static final String COORDINATOR_EVENTS_JOURNAL_NAME = "coordinator-events";

    public static String shardEvents(Member member) {
        return String.format("%s%s", member.getAddress(), SHARD_EVENTS_JOURNAL_NAME);
    }

    public static String clusterEvents() {
        return CLUSTER_EVENTS_JOURNAL_NAME;
    }

    public static String coordinatorEvents() {
        return COORDINATOR_EVENTS_JOURNAL_NAME;
    }
}
