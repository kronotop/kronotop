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


package com.kronotop.sql.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.protocol.SqlMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * PlanContext class represents the context for executing a executor.
 */
public class PlanContext {
    private final ChannelHandlerContext channelContext;
    private final SqlMessage sqlMessage;

    // Plan execution state
    private final List<Row<RexLiteral>> rexLiterals = new ArrayList<>();
    private RelOptTable table;
    private Transaction transaction;
    private Namespace namespace;
    private Integer currentUserVersion;
    private String tableVersion;
    private KronotopTable kronotopTable;
    private CompletableFuture<byte[]> versionstamp;
    private Boolean mutated;

    public PlanContext(ChannelHandlerContext channelContext, SqlMessage sqlMessage) {
        this.channelContext = channelContext;
        this.sqlMessage = sqlMessage;
    }

    public ChannelHandlerContext getChannelContext() {
        return channelContext;
    }

    public SqlMessage getSqlMessage() {
        return sqlMessage;
    }

    public List<Row<RexLiteral>> getRexLiterals() {
        return rexLiterals;
    }

    public KronotopTable getKronotopTable() {
        return kronotopTable;
    }

    public void setKronotopTable(KronotopTable kronotopTable) {
        if (this.kronotopTable != null) {
            throw new IllegalArgumentException("kronotopTable has already been set");
        }
        this.kronotopTable = kronotopTable;
    }

    public String getTableVersion() {
        return tableVersion;
    }

    public void setTableVersion(String tableVersion) {
        if (this.tableVersion != null) {
            throw new IllegalArgumentException("tableVersion has already been set");
        }
        this.tableVersion = tableVersion;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public void setNamespace(Namespace namespace) {
        if (this.namespace != null) {
            throw new IllegalArgumentException("namespace has already been set");
        }
        this.namespace = namespace;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        if (this.transaction != null) {
            throw new IllegalArgumentException("transaction has already been set");
        }
        this.transaction = transaction;
    }

    public Integer getAndIncrementUserVersion() {
        currentUserVersion = channelContext.channel().attr(ChannelAttributes.TRANSACTION_USER_VERSION).get();
        channelContext.channel().attr(ChannelAttributes.TRANSACTION_USER_VERSION).set(currentUserVersion + 1);
        return currentUserVersion;
    }

    public Integer getUserVersion() {
        return currentUserVersion;
    }

    public LinkedList<Integer> getAsyncReturning() {
        LinkedList<Integer> asyncReturning = channelContext.channel().attr(ChannelAttributes.ASYNC_RETURNING).get();
        if (asyncReturning == null) {
            asyncReturning = new LinkedList<>();
            channelContext.channel().attr(ChannelAttributes.ASYNC_RETURNING).set(asyncReturning);
        }
        return asyncReturning;
    }

    public CompletableFuture<byte[]> getVersionstamp() {
        return versionstamp;
    }

    public void setVersionstamp(CompletableFuture<byte[]> versionstamp) {
        if (this.versionstamp != null) {
            throw new IllegalArgumentException("versionstamp has already been set");
        }
        this.versionstamp = versionstamp;
    }

    public Boolean getMutated() {
        return mutated;
    }

    public void setMutated(Boolean mutated) {
        if (this.mutated != null) {
            throw new IllegalArgumentException("mutated has already been set");
        }
        this.mutated = mutated;
    }

    public RelOptTable getTable() {
        return table;
    }

    public void setTable(RelOptTable table) {
        if (this.table != null) {
            throw new IllegalArgumentException("table has already been set");
        }
        this.table = table;
    }
}
