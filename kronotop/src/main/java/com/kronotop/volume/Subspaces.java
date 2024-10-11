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

public class Subspaces {
    public static final byte ENTRY_SUBSPACE = 0x01;
    public static final byte ENTRY_METADATA_SUBSPACE = 0x02;
    public static final byte SEGMENT_CARDINALITY_SUBSPACE = 0x3;
    public static final byte SEGMENT_USED_BYTES_SUBSPACE = 0x4;
    public static final byte SEGMENT_LOG_SUBSPACE = 0x5;
    public static final byte SEGMENT_LOG_CARDINALITY_SUBSPACE = 0x6;
    public static final byte SEGMENT_REPLICATION_SLOT_SUBSPACE = 0x7;
    public static final byte VOLUME_STREAMING_SUBSCRIBERS_TRIGGER_SUBSPACE = 0x8;
}
