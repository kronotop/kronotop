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

class Prefixes {
    protected static final byte ENTRY_PREFIX = 0x01;
    protected static final byte ENTRY_METADATA_PREFIX = 0x02;
    protected static final byte SEGMENT_CARDINALITY_PREFIX = 0x3;
    protected static final byte SEGMENT_USED_BYTES_PREFIX = 0x4;
    protected static final byte SEGMENT_LOG_PREFIX = 0x5;
    protected static final byte SEGMENT_LOG_CARDINALITY_PREFIX = 0x6;
    protected static final byte SEGMENT_REPLICATION_PREFIX = 0x7;
    protected static final byte VOLUME_WATCH_CHANGES_TRIGGER_PREFIX = 0x8;
}
