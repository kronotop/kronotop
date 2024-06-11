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

package com.kronotop;

import com.apple.foundationdb.Transaction;
import com.kronotop.server.ChannelAttributes;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class TransactionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionUtils.class);

    /**
     * Retrieves an existing transaction from the channel context if it exists, otherwise creates a new transaction and sets it in the channel context.
     *
     * @param context        the Context object representing the Kronotop instance
     * @param channelContext the ChannelHandlerContext object representing the channel context
     * @return the existing or newly created Transaction object
     */
    public static Transaction getOrCreateTransaction(Context context, ChannelHandlerContext channelContext) {
        Channel channel = channelContext.channel();
        Attribute<Transaction> transactionAttr = channel.attr(ChannelAttributes.TRANSACTION);
        Attribute<Boolean> beginAttr = channel.attr(ChannelAttributes.BEGIN);

        if (beginAttr.get() == null || Boolean.FALSE.equals(beginAttr.get())) {
            Transaction tr = context.getFoundationDB().createTransaction();
            transactionAttr.set(tr);
            channel.attr(ChannelAttributes.TRANSACTION_USER_VERSION).set(0);
            channel.attr(ChannelAttributes.AUTO_COMMIT).set(true);
            channel.attr(ChannelAttributes.POST_COMMIT_HOOKS).set(new LinkedList<>());
            NamespaceUtils.clearOpenNamespaces(channelContext);
            return tr;
        }

        return transactionAttr.get();
    }

    /**
     * Adds a post-commit hook to the specified channel context. The hook will be executed after a transaction is committed.
     *
     * @param hook           the commit hook to be added
     * @param channelContext the channel context to which the hook will be added
     */
    public static void addPostCommitHook(CommitHook hook, ChannelHandlerContext channelContext) {
        channelContext.channel().attr(ChannelAttributes.POST_COMMIT_HOOKS).get().add(hook);
    }

    /**
     * Commits the transaction and executes any post-commit hooks if the channel context represents a one-off transaction.
     *
     * @param tr             the transaction to commit
     * @param channelContext the channel context representing the transaction
     */
    public static void commitIfAutoCommitEnabled(Transaction tr, ChannelHandlerContext channelContext) {
        if (getAutoCommit(channelContext)) {
            tr.commit().join();
            runPostCommitHooks(channelContext);
        }
    }

    /**
     * Returns the value of the "auto_commit" attribute associated with the given ChannelHandlerContext.
     *
     * @param channelContext the ChannelHandlerContext object representing the channel context
     * @return the value of the "auto_commit" attribute, or false if it is null
     */
    public static Boolean getAutoCommit(ChannelHandlerContext channelContext) {
        Attribute<Boolean> autoCommitAttr = channelContext.channel().attr(ChannelAttributes.AUTO_COMMIT);
        if (autoCommitAttr.get() == null) {
            return false;
        }
        return autoCommitAttr.get();
    }

    /**
     * Determines if the channel context represents a snapshot read.
     *
     * @param channelContext the ChannelHandlerContext object representing the channel context
     * @return true if the channel context represents a snapshot read, false otherwise
     */
    public static Boolean isSnapshotRead(ChannelHandlerContext channelContext) {
        Attribute<Boolean> snapshotReadAttr = channelContext.channel().attr(ChannelAttributes.SNAPSHOT_READ);
        return snapshotReadAttr.get() != null && !Boolean.FALSE.equals(snapshotReadAttr.get());
    }

    /**
     * Runs the post-commit hooks associated with the given channel context.
     *
     * @param channelContext the channel context representing the transaction
     */
    public static void runPostCommitHooks(ChannelHandlerContext channelContext) {
        List<CommitHook> commitHooks = channelContext.channel().attr(ChannelAttributes.POST_COMMIT_HOOKS).get();
        for (CommitHook commitHook : commitHooks) {
            try {
                commitHook.run();
            } catch (Exception e) {
                LOGGER.error("Error while running a commit hook", e);
            }
        }
    }
}
