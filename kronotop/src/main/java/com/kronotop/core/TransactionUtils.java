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

package com.kronotop.core;

import com.apple.foundationdb.Transaction;
import com.kronotop.server.ChannelAttributes;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;

public class TransactionUtils {

    public static Transaction getOrCreateTransaction(Context context, ChannelHandlerContext channelContext) {
        Channel channel = channelContext.channel();
        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);

        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            Attribute<Boolean> oneOffTransactionAttr = channel.attr(ChannelAttributes.ONE_OFF_TRANSACTION);
            Transaction tr = context.getFoundationDB().createTransaction();
            transactionAttr.set(tr);
            oneOffTransactionAttr.set(true);
            NamespaceUtils.clearOpenNamespaces(channelContext);
            return tr;
        }

        return transactionAttr.get();
    }

    public static void commitIfOneOff(Transaction tr, ChannelHandlerContext channelContext) {
        if (isOneOff(channelContext)) {
            tr.commit().join();
        }
    }

    public static Boolean isOneOff(ChannelHandlerContext channelContext) {
        Attribute<Boolean> oneOffTransactionAttr = channelContext.channel().attr(ChannelAttributes.ONE_OFF_TRANSACTION);
        return oneOffTransactionAttr.get() != null && !Boolean.FALSE.equals(oneOffTransactionAttr.get());
    }

    public static Boolean isSnapshotRead(ChannelHandlerContext channelContext) {
        Attribute<Boolean> snapshotReadAttr = channelContext.channel().attr(ChannelAttributes.SNAPSHOT_READ);
        return snapshotReadAttr.get() != null && !Boolean.FALSE.equals(snapshotReadAttr.get());
    }
}
