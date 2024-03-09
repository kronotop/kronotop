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


package com.kronotop.sql.plan;

import com.apple.foundationdb.Transaction;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.KronotopTable;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

public class PlanContext {
    private final ChannelHandlerContext channelContext;

    // Plan execution state
    private final List<Row> rows = new ArrayList<>();
    private RedisMessage response;
    private Transaction transaction;
    private Namespace namespace;
    private String tableVersion;
    private KronotopTable kronotopTable;

    public PlanContext(ChannelHandlerContext channelContext) {
        this.channelContext = channelContext;
    }

    public ChannelHandlerContext getChannelContext() {
        return channelContext;
    }

    public List<Row> getRows() {
        return rows;
    }

    public RedisMessage getResponse() {
        return response;
    }

    public void setResponse(RedisMessage response) {
        if (this.response != null) {
            throw new IllegalArgumentException("response has already been set");
        }
        this.response = response;
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

    public Integer incrementAndGetUserVersion() {
        Integer userVersion = channelContext.channel().attr(ChannelAttributes.TRANSACTION_USER_VERSION).get();
        channelContext.channel().attr(ChannelAttributes.TRANSACTION_USER_VERSION).set(userVersion + 1);
        return userVersion;
    }
}
