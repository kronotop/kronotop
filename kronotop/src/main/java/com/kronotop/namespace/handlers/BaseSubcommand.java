/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.namespace.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.handlers.protocol.NamespaceSubcommand;
import com.kronotop.server.Request;
import com.kronotop.server.WrongNumberOfArgumentsException;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class BaseSubcommand {
    final Context context;

    BaseSubcommand(Context context) {
        this.context = context;
    }

    String dottedNamespace(List<String> subpath) {
        return String.join(".", subpath);
    }

    /**
     * Reads a dotted namespace path from the buffer and splits it into segments.
     */
    List<String> readSubpath(ByteBuf buf) {
        String item = ProtocolMessageUtil.readAsString(buf);
        return new ArrayList<>(Arrays.asList(StringUtil.split(item)));
    }

    /**
     * Rejects paths that contain the reserved internal leaf.
     */
    void validateSubpath(List<String> subpath) {
        for (String item : subpath) {
            if (item.equals(Namespace.INTERNAL_LEAF)) {
                throw new KronotopException("Namespace '" + String.join(".", subpath) + "' is reserved for internal use");
            }
        }
    }

    WrongNumberOfArgumentsException wrongNumberOfArguments(Request request, NamespaceSubcommand subcommand) {
        return new WrongNumberOfArgumentsException(
                String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand)
        );
    }

    void checkNamespaceBeingRemoved(Transaction tr, List<String> subpath) {
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(tr, context, subpath);
        if (metadata.removed()) {
            throw new NamespaceBeingRemovedException(dottedNamespace(subpath));
        }
    }

    List<String> getNamespaceSubpath(List<String> subpath) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                namespaces().
                namespace(subpath).
                toList();
    }
}
