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

package com.kronotop.namespace.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BarrierNotSatisfiedException;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.namespace.NamespaceVersionBarrier;
import com.kronotop.namespace.handlers.protocol.NamespaceMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.time.Duration;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class PurgeSubcommand extends BaseSubcommand implements SubcommandExecutor {

    PurgeSubcommand(Context context) {
        super(context);
    }

    private void publishNamespaceRemovedEvent(NamespaceMessage.PurgeMessage message) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = NamespaceUtil.open(tr, context.getClusterName(), message.getSubpath());
            NamespaceMetadata metadata = NamespaceUtil.readMetadata(tr, subspace);
            NamespaceRemovedEvent event = new NamespaceRemovedEvent(metadata.id(), dottedNamespace(message.getSubpath()));
            context.getJournal().getPublisher().publish(tr, JournalName.NAMESPACE_EVENTS, event);
            tr.commit().join();
        }
    }

    @Override
    public void execute(Request request, Response response) {
        runAsync(context, response, () -> {
            NamespaceMessage message = request.attr(MessageTypes.NAMESPACE).get();
            NamespaceMessage.PurgeMessage purgeMessage = message.getPurgeMessage();

            String name = String.join(".", purgeMessage.getSubpath());
            if (context.getConfig().getString("default_namespace").equals(name)) {
                throw new KronotopException("Cannot purge the default namespace: '" + name + "'");
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = NamespaceUtil.open(tr, context.getClusterName(), purgeMessage.getSubpath());
                NamespaceMetadata metadata = NamespaceUtil.readMetadata(tr, subspace);
                if (!metadata.removed()) {
                    throw new KronotopException(
                            String.format("Namespace '%s' must be logically removed before purge", dottedNamespace(purgeMessage.getSubpath()))
                    );
                }
                NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, metadata);
                barrier.await(metadata.version(), 20, Duration.ofMillis(250)); // 5000 milliseconds
            } catch (BarrierNotSatisfiedException exp) {
                // try to reduce the entropy
                publishNamespaceRemovedEvent(purgeMessage);
                throw exp;
            }
            // Done. Remove the namespace.
            NamespaceUtil.remove(context, purgeMessage.getSubpath());
        }, response::writeOK);
    }
}
