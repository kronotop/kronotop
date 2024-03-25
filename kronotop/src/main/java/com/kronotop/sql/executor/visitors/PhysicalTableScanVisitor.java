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

package com.kronotop.sql.executor.visitors;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.core.Context;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.optimizer.physical.PhysicalTableScan;
import com.kronotop.sql.executor.PlanContext;

public class PhysicalTableScanVisitor extends BaseVisitor {

    public PhysicalTableScanVisitor(Context context) {
        super(context);
    }

    public void visit(PlanContext planContext, PhysicalTableScan node) throws SqlExecutionException {
        Transaction tr = getTransaction(planContext);
        Namespace namespace = getNamespace(planContext);
        KronotopTable kronotopTable = getLatestKronotopTable(planContext, node);

        KronotopTable table = node.getTable().unwrap(KronotopTable.class);
        assert table != null;

        //System.out.println(table.getFilter());
        //System.out.println(table.getProjects());
        //System.out.println(node.getRowType().getFieldList().get(0).getIndex());

        // namespace | sql | table-prefix | RECORD_HEADER_PREFIX
        Subspace subspace = namespace.getSql().subspace(Tuple.fromBytes(kronotopTable.getPrefix()));

        // Casting 'RECORD_HEADER_PREFIX' to 'Object' is not redundant
        AsyncIterable<KeyValue> iterator = tr.getRange(subspace.range(Tuple.from((Object) RECORD_HEADER_PREFIX)));
        for (KeyValue keyValue : iterator) {
            System.out.println(keyValue.getValue());
            System.out.println(Tuple.fromBytes(keyValue.getKey()));
            //System.out.println(new String(keyValue.getKey()));
            //System.out.println(new String(keyValue.getValue()));
        }
    }
}
