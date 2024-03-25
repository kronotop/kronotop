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


package com.kronotop.sql;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.ConfigTestUtil;
import com.kronotop.KronotopTestInstance;
import com.kronotop.common.KronotopException;
import com.kronotop.core.JSONUtils;
import com.kronotop.sql.ddl.model.TableModel;
import com.kronotop.sql.metadata.SqlMetadataService;
import com.kronotop.sql.metadata.TableWithVersion;
import com.typesafe.config.Config;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

public class BaseSqlTest {
    protected KronotopTestInstance kronotopInstance;
    protected EmbeddedChannel channel;

    protected EmbeddedChannel newChannel() {
        return kronotopInstance.newChannel();
    }

    /**
     * Retrieves the latest version of a table from the metadata store.
     *
     * @param tr     The FDB transaction.
     * @param schema The list of names representing the hierarchy of the schema.
     * @param table  The name of the table to retrieve the latest version for.
     * @return The TableWithVersion object representing the table with the latest version.
     * @throws KronotopException If the table exists but no version is found.
     */
    protected TableWithVersion getLatestTableVersion(Transaction tr, String schema, String table) {
        SqlMetadataService sqlMetadataService = kronotopInstance.getContext().getService(SqlMetadataService.NAME);
        DirectorySubspace subspace = sqlMetadataService.openTableSubspace(tr, schema, table);
        AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
        if (!iterator.hasNext()) {
            throw new KronotopException(String.format("Table '%s' exists but no version found", table));
        }

        KeyValue next = iterator.next();
        byte[] versionstamp = subspace.unpack(next.getKey()).getVersionstamp(0).getBytes();
        TableModel tableModel = JSONUtils.readValue(next.getValue(), TableModel.class);
        return new TableWithVersion(tableModel, versionstamp);
    }

    protected void setupCommon(Config config) throws UnknownHostException, InterruptedException {
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        channel = kronotopInstance.getChannel();
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("test.conf");
        setupCommon(config);
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }
}
