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

package com.kronotop.journal;

/**
 * The JournalName enum represents different predefined journal names used within the system.
 * Each enum constant corresponds to a specific journal and has an associated string value.
 */
public enum JournalName {
    BUCKET_EVENTS("bucket-events"),
    DISUSED_PREFIXES("disused-prefixes"),
    CLUSTER_EVENTS("cluster-events"),
    NAMESPACE_EVENTS("namespace-events");

    private final String value;

    JournalName(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
