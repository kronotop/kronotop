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

package com.kronotop.sql.executor;

import com.kronotop.core.Context;
import com.kronotop.sql.executor.visitors.*;

public class Visitors {
    protected final PhysicalValuesVisitor physicalValuesVisitor;
    protected final PhysicalTableModifyVisitor physicalTableModifyVisitor;
    protected final PhysicalProjectVisitor physicalProjectVisitor;
    protected final PhysicalTableScanVisitor physicalTableScanVisitor;
    protected final PhysicalCalcVisitor physicalCalcVisitor;

    public Visitors(Context context) {
        this.physicalValuesVisitor = new PhysicalValuesVisitor(context);
        this.physicalTableModifyVisitor = new PhysicalTableModifyVisitor(context);
        this.physicalProjectVisitor = new PhysicalProjectVisitor(context);
        this.physicalTableScanVisitor = new PhysicalTableScanVisitor(context);
        this.physicalCalcVisitor = new PhysicalCalcVisitor(context);
    }
}
