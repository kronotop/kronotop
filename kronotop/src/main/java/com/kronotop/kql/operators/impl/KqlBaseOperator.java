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

package com.kronotop.kql.operators.impl;

public class KqlBaseOperator {
    private final int level;
    private String field;

    public KqlBaseOperator(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getField() {
        return this.field;
    }

    protected String stringify(String name, Object value) {
        return String.format("KqlOperator { name=%s, level=%d, field=%s, value=%s }", name, getLevel(), getField(), value);
    }
}
