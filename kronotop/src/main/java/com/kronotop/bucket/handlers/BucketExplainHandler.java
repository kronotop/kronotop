/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.QueryShape;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.handlers.protocol.BucketExplainMessage;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.BooleanRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketExplainMessage.COMMAND)
@MinimumParameterCount(BucketExplainMessage.MINIMUM_PARAMETER_COUNT)
public class BucketExplainHandler extends AbstractBucketHandler implements Handler {
    public BucketExplainHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETEXPLAIN).set(new BucketExplainMessage(request));
    }

    private BucketMetadata openBucketMetadata(Request request, String bucket) {
        try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
            return BucketMetadataUtil.open(context, tr, request.getSession(), bucket);
        }
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketExplainMessage message = request.attr(MessageTypes.BUCKETEXPLAIN).get();

            BucketMetadata metadata = openBucketMetadata(request, message.getBucket());
            BqlExpr bqlExpr = BqlParser.parse(message.getQuery());
            Collation queryCollation = message.getArguments().getCollation();
            long shapeHash = QueryShape.compute(bqlExpr, message.getArguments().getSortBy(), queryCollation);
            PlanCache.CachedPlan cachedPlan = service.getPlanCache().get(metadata.namespace(), metadata.uuid(), shapeHash);
            if (cachedPlan != null) {
                return new ExplainInput(cachedPlan.plan(), true, queryCollation);
            }

            QueryContext ctx = buildQueryContext(request, metadata, message.getQuery(), message.getArguments(), true);
            return new ExplainInput(ctx.plan(), false, queryCollation);
        }, (input) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
                result.put(bulkString("is_cached"), input.isCached() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
                if (input.queryCollation() != null) {
                    result.put(bulkString("query_collation"), PipelineExplainer.explainCollation(input.queryCollation()));
                }
                result.put(bulkString("plan"), PipelineExplainer.explainAsMapMessage(input.plan()));
                response.writeRedisMessage(new MapRedisMessage(result));
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                List<RedisMessage> result = new ArrayList<>();
                result.add(bulkString("is_cached"));
                result.add(input.isCached() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
                if (input.queryCollation() != null) {
                    result.add(bulkString("query_collation"));
                    result.add(PipelineExplainer.explainCollationAsArrayMessage(input.queryCollation()));
                }
                result.add(bulkString("plan"));
                result.add(PipelineExplainer.explainAsArrayMessage(input.plan()));
                response.writeRedisMessage(new ArrayRedisMessage(result));
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }

    record ExplainInput(PipelineNode plan, boolean isCached, Collation queryCollation) {
    }
}