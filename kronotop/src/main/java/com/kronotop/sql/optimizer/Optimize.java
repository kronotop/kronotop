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

package com.kronotop.sql.optimizer;

import com.kronotop.sql.KronotopSchema;
import com.kronotop.sql.Parser;
import com.kronotop.sql.optimizer.physical.PhysicalConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class Optimize {

    public static QueryOptimizationResult optimize(KronotopSchema schema, String query) throws SqlParseException {
        SqlNode sqlNode = Parser.parse(query);
        return optimize(schema, sqlNode);
    }

    public static QueryOptimizationResult optimize(KronotopSchema schema, SqlNode sqlNode) {
        Optimizer optimizer = new Optimizer(schema);
        SqlNode validatedSqlTree = optimizer.validate(sqlNode);
        QueryConvertResult queryConvertResult = optimizer.convert(validatedSqlTree);
        RelNode convertedRelTree = queryConvertResult.getRel();

        RelNode optimizedRelTree = optimizer.optimize(
                convertedRelTree,
                convertedRelTree.getTraitSet().plus(PhysicalConvention.INSTANCE),
                Rules.rules
        );
        return new QueryOptimizationResult(optimizedRelTree, queryConvertResult.getFieldNames());
    }
}
