/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.plan;

import com.kronotop.core.Context;
import com.kronotop.sql.plan.visitors.EnumerableProjectVisitor;
import com.kronotop.sql.plan.visitors.EnumerableTableModifyVisitor;
import com.kronotop.sql.plan.visitors.EnumerableValuesVisitor;

public class Visitors {
    protected final EnumerableValuesVisitor enumerableValuesVisitor;
    protected final EnumerableTableModifyVisitor enumerableTableModifyVisitor;
    protected final EnumerableProjectVisitor enumerableProjectVisitor;

    public Visitors(Context context) {
        this.enumerableValuesVisitor = new EnumerableValuesVisitor(context);
        this.enumerableTableModifyVisitor =new EnumerableTableModifyVisitor(context);
        this.enumerableProjectVisitor =new EnumerableProjectVisitor(context);
    }
}
