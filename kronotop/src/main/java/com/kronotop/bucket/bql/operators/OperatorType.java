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

package com.kronotop.bucket.bql.operators;

public enum OperatorType {
    EQ(0),
    OR(1),
    LT(2),
    GT(3),
    ALL(4),
    NIN(5),
    AND(6),
    NOT(7),
    GTE(8),
    LTE(9),
    NE(10),
    IN(11),
    NOR(12),
    SIZE(13),
    ELEM_MATCH(14),
    EXISTS(15);

    private final int value;

    OperatorType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
