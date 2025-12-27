/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.PipelineExplainer;
import com.kronotop.bucket.handlers.protocol.BucketExplainMessage;
import com.kronotop.bucket.handlers.protocol.BucketQueryMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

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

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketExplainMessage message = request.attr(MessageTypes.BUCKETEXPLAIN).get();
            QueryContext ctx = buildQueryContext(request, message.getBucket(), message.getQuery(), message.getArguments());
            return ctx.plan();
        }, (plan) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                response.writeRedisMessage(PipelineExplainer.explainAsMapMessage(plan));
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                response.writeRedisMessage(PipelineExplainer.explainAsArrayMessage(plan));
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}