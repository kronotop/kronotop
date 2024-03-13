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

package com.kronotop.sql;

import com.kronotop.common.resp.RESPError;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.optimizer.Optimize;
import com.kronotop.sql.plan.PlanContext;
import com.kronotop.sql.protocol.SqlMessage;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;

@Command(SqlMessage.COMMAND)
@MinimumParameterCount(SqlMessage.MINIMUM_PARAMETER_COUNT)
public class SqlHandler extends BaseSqlHandler implements Handler {

    public SqlHandler(SqlService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SQL).set(new SqlMessage(request));
    }

    private RedisMessage executeQuery(Request request, Response response, SqlMessage sqlMessage, String schema, String query) throws Exception {
        final PlanContext planContext = new PlanContext(request.getChannelContext(), sqlMessage);

        Plan plan = service.planCache.getPlan(schema, query);
        if (plan != null) {
            print("Retrieved a cached plan", plan.getRelNode());
            service.planExecutor.execute(planContext, plan);
            return planContext.getResponse();
        }

        try {
            SqlNode sqlTree = Parser.parse(query);
            if (service.statements.contains(sqlTree.getKind())) {
                KronotopSchema kronotopSchema = service.getMetadataService().findSchemaMetadata(schema).getKronotopSchema();
                RelNode optimizerRelTree = Optimize.optimize(kronotopSchema, sqlTree);

                print("After Optimization", optimizerRelTree);
                plan = new Plan(optimizerRelTree);
                service.planCache.putPlan(schema, query, plan);
                service.planExecutor.execute(planContext, plan);
                return planContext.getResponse();
            } else {
                // DDL Commands
                ExecutionContext executionContext = new ExecutionContext(request, response);
                executionContext.setSchema(schema);
                Executor<SqlNode> executor = service.executors.get(sqlTree.getKind());
                if (executor != null) {
                    return executor.execute(executionContext, sqlTree);
                }
                return new ErrorRedisMessage(RESPError.SQL, String.format("Unknown SQL command: %s", sqlTree.getKind()));
            }
        } catch (SqlParseException e) {
            // redis-cli has a problem with bulk errors:
            // Error: Protocol error, got "!" as reply type byte
            List<String> messages = List.of(e.getMessage().split("\n"));
            return new ErrorRedisMessage(RESPError.SQL, messages.get(0));
        } catch (SqlExecutionException e) {
            return new ErrorRedisMessage(RESPError.SQL, e.getMessage());
        } catch (SqlValidatorException e) {
            return new ErrorRedisMessage(RESPError.SQL, String.format("%s %s", e.getMessage(), e.getCause().getMessage()));
        }
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        SqlMessage sqlMessage = request.attr(MessageTypes.SQL).get();
        String schema = request.getChannelContext().channel().attr(ChannelAttributes.SCHEMA).get();
        List<RedisMessage> responses = new LinkedList<>();

        for (String query : sqlMessage.getQueries()) {
            try {
                RedisMessage result = executeQuery(request, response, sqlMessage, schema, query);
                responses.add(result);
            } catch (CalciteContextException e) {
                responses.add(new ErrorRedisMessage(RESPError.SQL, e.getCause().getMessage()));
            } catch (SqlValidatorException | SqlExecutionException e) {
                responses.add(new ErrorRedisMessage(RESPError.SQL, e.getMessage()));
            }
        }
        response.writeArray(responses);
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw);
    }
}
