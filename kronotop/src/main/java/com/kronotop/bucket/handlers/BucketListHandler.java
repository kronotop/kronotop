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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.DataStructureKind;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketListMessage;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.Handler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketListMessage.COMMAND)
@MinimumParameterCount(BucketListMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketListMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketListHandler extends AbstractBucketHandler implements Handler {

    public BucketListHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            List<RedisMessage> children = new ArrayList<>();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                DirectorySubspace subspace = NamespaceUtil.openDataStructureSubspace(
                        context, tr, request.getSession(), DataStructureKind.BUCKET
                );
                List<String> names = subspace.list(tr).join();
                for (String name : names) {
                    children.add(new FullBulkStringRedisMessage(
                            Unpooled.copiedBuffer(name, StandardCharsets.UTF_8)
                    ));
                }
            }
            return children;
        }, response::writeArray);
    }
}
