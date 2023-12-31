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

package com.kronotop.foundationdb;

import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class NamespaceListOperand extends BaseNamespaceOperand implements CommandOperand {
    NamespaceListOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        super(service, namespaceMessage, response);
    }

    @Override
    public void execute() {
        NamespaceMessage.ListMessage listMessage = namespaceMessage.getListMessage();
        List<String> subpath = getBaseSubpath().addAll(listMessage.getSubpath()).asList();
        CompletableFuture<List<String>> future;
        if (subpath.size() == 0) {
            future = directoryLayer.list(transaction);
        } else {
            future = directoryLayer.list(transaction, subpath);
        }
        future.handle((result, ex) -> {
            if (ex != null) {
                if (ex.getCause() instanceof NoSuchDirectoryException) {
                    String message = NamespaceUtil.NoSuchNamespaceMessage(service.getContext(), ex.getCause());
                    response.writeError(RESPError.NOSUCHNAMESPACE, message);
                } else {
                    throw new KronotopException(ex);
                }
            }
            List<RedisMessage> children = new ArrayList<>();
            for (String namespace : result) {
                ByteBuf buf = response.getContext().alloc().buffer();
                children.add(new FullBulkStringRedisMessage(buf.writeBytes(namespace.getBytes())));
            }
            response.writeArray(children);
            return null;
        }).join();
    }
}
