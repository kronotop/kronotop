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

package com.kronotop.kql.operators.impl.logical;

import com.kronotop.kql.KqlValue;
import com.kronotop.kql.operators.KqlOperator;
import com.kronotop.kql.operators.impl.KqlBaseOperator;

public class KqlOrOperator extends KqlBaseOperator implements KqlOperator {
    public static final String NAME = "$OR";
    public static final int IDENTIFIER = 0;
    private KqlValue value;

    public KqlOrOperator(int level) {
        super(level);
    }

    @Override
    public int getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public void setValue(KqlValue value) {
        this.value = value;
    }

    @Override
    public KqlValue getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("KqlOperator{name=%s, identifier=%d, level=%d, field=%s, value=%s}", NAME, IDENTIFIER, getLevel(), getField(), value);
    }
}
