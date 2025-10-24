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

import com.apple.foundationdb.Transaction;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class BucketIndexListSubcommand implements SubcommandHandler {
    private final Context context;

    BucketIndexListSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        ListParameters parameters = new ListParameters(request.getParams());
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            Session session = request.getSession();
            List<RedisMessage> children = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, parameters.bucket);
                List<String> names = IndexUtil.list(tr, metadata.subspace());
                for (String name : names) {
                    Map<RedisMessage, RedisMessage> item = new LinkedHashMap<>();
                    item.put(new SimpleStringRedisMessage("name"), new SimpleStringRedisMessage(name));
                    children.add(new MapRedisMessage(item));
                }
            }
            return children;
        }, response::writeArray);
    }

    private static class ListParameters {
        private final String bucket;

        ListParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new KronotopException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
        }
    }
}
