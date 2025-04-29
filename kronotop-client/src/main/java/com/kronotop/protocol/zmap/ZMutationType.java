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

package com.kronotop.protocol.zmap;

public enum ZMutationType {
    ADD,
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    APPEND_IF_FITS,
    MAX,
    MIN,
    SET_VERSIONSTAMPED_KEY,
    SET_VERSIONSTAMPED_VALUE,
    BYTE_MIN,
    BYTE_MAX,
    COMPARE_AND_CLEAR;
}
