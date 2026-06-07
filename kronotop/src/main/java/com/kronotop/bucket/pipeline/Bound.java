/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.planner.Operator;
import org.bson.types.ObjectId;

public class Bound {
    private final Operator operator;
    private final BqlValue value;
    private volatile ObjectId objectId;

    // Primary Index structure -> ObjectId
    // Single Field Index Key Structure -> BqlValue | ObjectId

    public Bound(Operator operator, BqlValue value) {
        this.operator = operator;
        this.value = value;
    }

    public Operator operator() {
        return operator;
    }

    public BqlValue value() {
        return value;
    }

    public void setObjectId(ObjectId objectId) {
        this.objectId = objectId;
    }

    public ObjectId objectId() {
        return objectId;
    }
}
