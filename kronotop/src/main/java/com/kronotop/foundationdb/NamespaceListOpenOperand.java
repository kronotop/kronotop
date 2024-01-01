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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.foundationdb.protocol.NamespaceMessage;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class NamespaceListOpenOperand extends BaseNamespaceOperand implements CommandOperand {
    NamespaceListOpenOperand(FoundationDBService service, NamespaceMessage namespaceMessage, Response response) {
        super(service, namespaceMessage, response);
    }

    @Override
    public void execute() {
        Attribute<ConcurrentMap<String, DirectorySubspace>> openNamespacesAttr = response.
                getContext().
                channel().
                attr(ChannelAttributes.OPEN_NAMESPACES);
        List<RedisMessage> children = new ArrayList<>();
        for (String namespace : openNamespacesAttr.get().keySet()) {
            ByteBuf buf = response.getContext().alloc().buffer();
            children.add(new FullBulkStringRedisMessage(buf.writeBytes(namespace.getBytes())));
        }
        response.writeArray(children);
    }
}
