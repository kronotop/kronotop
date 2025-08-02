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

import com.kronotop.bucket.bql.values.BqlValue;

import java.util.LinkedList;
import java.util.List;

public class BqlOperator {
    private final int level;
    private final OperatorType operatorType;
    private List<BqlValue<?>> values;

    public BqlOperator(int level, OperatorType operatorType) {
        this.level = level;
        this.operatorType = operatorType;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public int getLevel() {
        return level;
    }

    public void addValue(BqlValue<?> value) {
        if (values == null) {
            values = new LinkedList<>();
        }
        values.add(value);
    }

    public List<BqlValue<?>> getValues() {
        return values;
    }
}
