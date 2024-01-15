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

package com.kronotop.sql.backend.metadata;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.journal.Event;
import com.kronotop.core.journal.JournalName;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.backend.ddl.model.CreateTableModel;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.SchemaCreatedEvent;
import com.kronotop.sql.backend.metadata.events.TableCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class SqlMetadataService implements KronotopService {
    public static final String NAME = "SQL Metadata";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlMetadataService.class);
    private final Context context;
    private final AtomicReference<CompletableFuture<Void>> currentWatcher = new AtomicReference<>();
    private final AtomicReference<byte[]> lastSqlMetadataVersionstamp = new AtomicReference<>();
    private final ExecutorService executor;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final SchemaMetadata root = new SchemaMetadata();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean isShutdown;

    public SqlMetadataService(Context context) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("kr.sql-metadata-watcher-%d").build();
        this.executor = Executors.newSingleThreadExecutor(namedThreadFactory);
        this.context = context;
    }

    /**
     * Retrieves the name of the service.
     *
     * @return the name of the service
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Retrieves the context associated with the KronotopService.
     *
     * @return the context associated with the KronotopService
     */
    @Override
    public Context getContext() {
        return null;
    }

    public DirectoryLayout getSchemaLayout(List<String> schema) {
        return DirectoryLayout.
                Builder.
                clusterName(context.getClusterName()).
                internal().
                sql().
                metadata().
                schemas().
                addAll(schema);
    }

    /**
     * Initializes the default schema by creating the schema hierarchy in the metadata store.
     *
     * @param defaultSchemaHierarchy the list of names representing the hierarchy of the default schema
     */
    private void initializeDefaultSchema(List<String> defaultSchemaHierarchy) throws SchemaNotExistsException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> schema = getSchemaLayout(defaultSchemaHierarchy).asList();
            DirectoryLayer.getDefault().create(tr, schema).join();
            tr.commit().join();
            loadSchemaHierarchy(defaultSchemaHierarchy);
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                // Already exists
                return;
            } else if (e.getCause() instanceof FDBException) {
                if (((FDBException) e.getCause()).getCode() == 1020) {
                    // Highly likely created by a different Kronotop instance.
                    initializeDefaultSchema(defaultSchemaHierarchy);
                    return;
                }
            }
            throw new KronotopException(e);
        }
    }

    public void start() {
        List<String> defaultSchemaHierarchy = context.getConfig().getStringList("sql.default_schema");
        try {
            initializeDefaultSchema(defaultSchemaHierarchy);
        } catch (SchemaNotExistsException e) {
            // This should not be possible
            throw new KronotopException(e);
        }

        byte[] key = context.getFoundationDB().run(tr -> context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.sqlMetadataEvents()));
        this.lastSqlMetadataVersionstamp.set(key);

        executor.execute(new Watcher());
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                LOGGER.error("{} service has failed to start background threads", NAME);
            }
        } catch (InterruptedException e) {
            throw new KronotopException(e);
        }
        LOGGER.info("{} service has been started", NAME);
    }

    public void shutdown() {
        if (isShutdown) {
            return;
        }
        isShutdown = true;
        currentWatcher.get().cancel(true);
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Finds the SchemaMetadata object in the schema metadata store based on the given list of names.
     *
     * @param names the list of names representing the hierarchy to search for the schema
     * @return the SchemaMetadata object representing the found schema
     * @throws SchemaNotExistsException if the schema does not exist in the schema metadata store
     */
    public SchemaMetadata findSchema(List<String> names) throws SchemaNotExistsException {
        SchemaMetadata current = root;
        for (String name : names) {
            current = current.get(name);
        }
        return current;
    }

    /**
     * Finds the VersionedTableMetadata object for a given schema and table in the metadata store.
     *
     * @param schema The list of names representing the hierarchy to search for the schema.
     * @param table  The name of the table to search for.
     * @return The VersionedTableMetadata object representing the found table.
     * @throws SchemaNotExistsException If the schema does not exist in the metadata store.
     * @throws TableNotExistsException  If the table does not exist in the metadata store.
     */
    public VersionedTableMetadata findTable(List<String> schema, String table) throws SchemaNotExistsException, TableNotExistsException {
        SchemaMetadata current = root;
        for (String name : schema) {
            current = current.get(name);
        }

        TableMetadata tableMetadata = current.getTables();
        return tableMetadata.get(table);
    }

    private boolean isSchemaExistOnFDB(Transaction tr, List<String> names) {
        return DirectoryLayer.getDefault().exists(tr, getSchemaLayout(names).asList()).join();
    }

    /**
     * Checks if the schema exists in the schema metadata store, and loads the next schema metadata if it does not exist.
     *
     * @param current the current schema metadata
     * @param cursor  the cursor representing the hierarchy of schemas
     * @param name    the name of the schema to check and load
     * @return the SchemaMetadata object representing the loaded schema
     * @throws SchemaNotExistsException if the schema does not exist in the schema metadata store
     */
    private SchemaMetadata checkAndLoadSchema(SchemaMetadata current, List<String> cursor, String name) throws SchemaNotExistsException {
        // This method modifies the SQL metadata and should be called from a synchronized method. See:  loadSchemaHierarchy
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (!isSchemaExistOnFDB(tr, cursor)) {
                throw new SchemaNotExistsException(name);
            }
        }
        try {
            SchemaMetadata next = new SchemaMetadata();
            current.put(name, next);
            current = next;
        } catch (SchemaAlreadyExistsException e) {
            current = current.get(name);
        }

        return current;
    }

    /**
     * Loads the schema hierarchy based on the given list of names.
     *
     * @param names the list of names representing the hierarchy to load
     * @return the SchemaMetadata object representing the hierarchy
     * @throws SchemaNotExistsException if a schema does not exist in the metadata store
     */
    private synchronized SchemaMetadata loadSchemaHierarchy(List<String> names) throws SchemaNotExistsException {
        // This should be the ONLY way to load schema hierarchy in a Kronotop instance.
        SchemaMetadata current = root;
        List<String> cursor = new ArrayList<>();
        for (String name : names) {
            try {
                cursor.add(name);
                current = current.get(name);
            } catch (SchemaNotExistsException e) {
                current = checkAndLoadSchema(current, cursor, name);
            }
        }
        return current;
    }

    private DirectoryLayout getTableLayout(List<String> schema, String table) {
        return DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().sql().metadata().schemas().addAll(schema).tables().add(table);
    }

    /**
     * Loads the table metadata with the given versionstamp.
     *
     * @param tr           the FDB transaction
     * @param schema       the list of names representing the hierarchy to search for the schema
     * @param table        the name of the table to search for
     * @param versionBytes the versionstamp of the table metadata
     * @return the CreateTableModel object representing the table metadata
     * @throws IOException if there is an error reading the table metadata
     */
    private CreateTableModel loadTableMetadataWithVersionstamp(Transaction tr, List<String> schema, String table, byte[] versionBytes) throws IOException {
        List<String> subpath = getTableLayout(schema, table).asList();
        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
        Versionstamp versionstamp = Versionstamp.fromBytes(versionBytes);

        AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(Tuple.from(versionstamp)), 1).iterator();
        if (!iterator.hasNext()) {
            LOGGER.error("No {} event found with Versionstamp: {}", EventTypes.TABLE_CREATED, versionstamp);
        }

        KeyValue next = iterator.next();
        return objectMapper.readValue(next.getValue(), CreateTableModel.class);
    }

    /**
     * Adds a new version for a table in the schema metadata store.
     *
     * @param table        The table object to add a new version for.
     * @param versionstamp The versionstamp of the new table version.
     * @throws SchemaNotExistsException           If the schema does not exist in the schema metadata store.
     * @throws TableVersionAlreadyExistsException If the table version already exists in the metadata store.
     */
    private synchronized void addTableVersion(KronotopTable table, byte[] versionstamp) throws SchemaNotExistsException, TableVersionAlreadyExistsException {
        // This should be the ONLY way to create a new table version in a Kronotop instance.
        SchemaMetadata schemaMetadata = loadSchemaHierarchy(table.getSchema());
        VersionedTableMetadata versionedTableMetadata = schemaMetadata.getTables().getOrCreate(table.getName());
        String version = BaseEncoding.base64().encode(versionstamp);
        versionedTableMetadata.put(version, table);

        List<String> names = new ArrayList<>(table.getSchema());
        names.add(table.getName());
        LOGGER.info("New version: {} has been added for table: {}", version, String.join(".", names));
    }

    /**
     * Processes a TableCreatedEvent by loading the table metadata and adding a new version for the table.
     *
     * @param tr             the FDB transaction
     * @param broadcastEvent the broadcast event containing the TableCreatedEvent
     * @throws IOException                        if there is an error reading the table metadata or processing the event payload
     * @throws SchemaNotExistsException           if the schema does not exist in the schema metadata store
     * @throws TableVersionAlreadyExistsException if the table version already exists in the metadata store
     * @throws KronotopException                  if there is an error adding the new table version
     */
    private void processTableCreatedEvent(Transaction tr, BroadcastEvent broadcastEvent) throws IOException {
        TableCreatedEvent tableCreatedEvent = objectMapper.readValue(broadcastEvent.getPayload(), TableCreatedEvent.class);
        CreateTableModel createTableModel = loadTableMetadataWithVersionstamp(tr, tableCreatedEvent.getSchema(), tableCreatedEvent.getTable(), tableCreatedEvent.getVersionstamp());
        KronotopTable table = new KronotopTable(createTableModel);
        try {
            addTableVersion(table, tableCreatedEvent.getVersionstamp());
        } catch (SchemaNotExistsException | TableVersionAlreadyExistsException e) {
            LOGGER.error("Failed to add new table version: {}", e.getMessage());
            throw new KronotopException(e);
        }
    }

    /**
     * Processes a SchemaCreatedEvent by loading the schema hierarchy.
     *
     * @param broadcastEvent the broadcast event containing the SchemaCreatedEvent
     * @throws JsonProcessingException  if there is an error processing the event payload
     * @throws SchemaNotExistsException if a schema does not exist in the metadata store
     */
    private void processSchemaCreatedEvent(BroadcastEvent broadcastEvent) throws JsonProcessingException, SchemaNotExistsException {
        SchemaCreatedEvent schemaCreatedEvent = objectMapper.readValue(broadcastEvent.getPayload(), SchemaCreatedEvent.class);
        loadSchemaHierarchy(schemaCreatedEvent.getSchema());
    }

    /**
     * Fetches SQL metadata by consuming events from a journal and processing them.
     */
    private void fetchSqlMetadata() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                Event event = context.getJournal().getConsumer().consumeNext(tr, JournalName.sqlMetadataEvents(), lastSqlMetadataVersionstamp.get());
                if (event == null) return null;

                try {
                    BroadcastEvent broadcastEvent = objectMapper.readValue(event.getValue(), BroadcastEvent.class);
                    LOGGER.debug("{} event has been received", broadcastEvent.getType());
                    if (broadcastEvent.getType().equals(EventTypes.SCHEMA_CREATED)) {
                        processSchemaCreatedEvent(broadcastEvent);
                    } else if (broadcastEvent.getType().equals(EventTypes.TABLE_CREATED)) {
                        processTableCreatedEvent(tr, broadcastEvent);
                    } else {
                        LOGGER.error("Unknown {} event: {}, passing it", NAME, broadcastEvent.getType());
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to process a SQL metadata event, passing it", e);
                }
                lastSqlMetadataVersionstamp.set(event.getKey());
            }
        });
    }

    /**
     * This private inner class implements the Runnable interface and represents a watcher for the SQL metadata events journal.
     */
    private class Watcher implements Runnable {
        /**
         * Runs this operation.
         */
        @Override
        public void run() {
            if (isShutdown) return;

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = tr.watch(context.getJournal().getJournalMetadata(JournalName.sqlMetadataEvents()).getTrigger());
                tr.commit().join();
                currentWatcher.set(watcher);
                latch.countDown();
                try {
                    fetchSqlMetadata();
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.info("{} watcher has been cancelled", JournalName.sqlMetadataEvents());
                    return;
                }
                // A new event is ready to read
                fetchSqlMetadata();
            } catch (Exception e) {
                LOGGER.error("Error while watching journal: {}", JournalName.sqlMetadataEvents(), e);
            } finally {
                if (!isShutdown) {
                    executor.execute(this);
                }
            }
        }
    }
}
