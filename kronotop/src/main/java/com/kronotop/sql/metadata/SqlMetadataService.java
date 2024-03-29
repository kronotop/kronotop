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

package com.kronotop.sql.metadata;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.JSONUtils;
import com.kronotop.core.KeyWatcher;
import com.kronotop.core.KronotopService;
import com.kronotop.core.journal.Event;
import com.kronotop.core.journal.JournalName;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.ddl.model.TableModel;
import com.kronotop.sql.metadata.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SqlMetadataService is a class that extends KronotopService and provides methods to retrieve and manipulate
 * SQL metadata in the Kronotop database system.
 */
public class SqlMetadataService implements KronotopService {
    public static final String NAME = "SQL Metadata";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlMetadataService.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Context context;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final AtomicReference<byte[]> lastSqlMetadataVersionstamp = new AtomicReference<>();
    private final ExecutorService executor;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Metadata metadata = new Metadata();
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
        return context;
    }

    public DirectoryLayout getSchemaLayout(@Nonnull String schema) {
        if (schema.isEmpty()) {
            throw new IllegalArgumentException("schema cannot be empty");
        }
        return DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().sql().metadata().schemas().add(schema);
    }

    private boolean isSchemaExistOnFDB(Transaction tr, String schema) {
        return DirectoryLayer.getDefault().exists(tr, getSchemaLayout(schema).asList()).join();
    }

    private DirectoryLayout getTableLayout(String schema, String table) {
        return DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().sql().metadata().schemas().add(schema).tables().add(table);
    }

    public DirectorySubspace openTableSubspace(String schema, String table) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return openTableSubspace(tr, schema, table);
        }
    }

    public DirectorySubspace openTableSubspace(Transaction tr, String schema, String table) {
        List<String> tableLayout = getTableLayout(schema, table).asList();
        return DirectoryLayer.getDefault().open(tr, tableLayout).join();
    }

    /**
     * Initializes the default schema by creating the schema hierarchy in the metadata store.
     *
     * @param defaultSchema the list of names representing the hierarchy of the default schema
     */
    private void initializeDefaultSchema(String defaultSchema) throws SchemaNotExistsException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> schema = getSchemaLayout(defaultSchema).asList();
            DirectoryLayer.getDefault().create(tr, schema).join();
            tr.commit().join();
            findOrLoadSchemaMetadata(defaultSchema); // thread-safe
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                // Already exists
                return;
            } else if (e.getCause() instanceof FDBException) {
                if (((FDBException) e.getCause()).getCode() == 1020) {
                    // Highly likely created by a different Kronotop instance.
                    initializeDefaultSchema(defaultSchema);
                    return;
                }
            }
            throw new KronotopException(e);
        }
    }

    /**
     * Loads the table from FoundationDB with the given schema and table names.
     *
     * @param tr     The FDB transaction.
     * @param schema The name of the schema to search for.
     * @param table  The name of the table to search for.
     */
    private void loadTableFromFDB(Transaction tr, String schema, String table) {
        // This method is not thread-safe.
        DirectorySubspace subspace = openTableSubspace(tr, schema, table);
        for (KeyValue keyValue : tr.getRange(subspace.range())) {
            Versionstamp versionstamp = subspace.unpack(keyValue.getKey()).getVersionstamp(0);
            TableModel tableModel = JSONUtils.readValue(keyValue.getValue(), TableModel.class);
            try {
                addTableVersion(tableModel, versionstamp.getBytes());
            } catch (TableVersionAlreadyExistsException | SchemaNotExistsException e) {
                LOGGER.error("Error while loading table '{}' from FoundationDB",
                        String.join(".", List.of(tableModel.getTable(), tableModel.getTable())), e);
            }
        }
    }

    /**
     * Adds the provided schema to the metadata collection and initializes an associated KronotopSchema and Optimizer.
     * This method is not thread-safe.
     *
     * @param schema The schema to be added.
     * @return The SchemaMetadata object associated with the added schema.
     * @throws SchemaAlreadyExistsException If the provided schema already exists in the metadata collection.
     */
    private SchemaMetadata addSchemaMetadata(String schema) throws SchemaAlreadyExistsException {
        // This method is not thread-safe.
        SchemaMetadata schemaMetadata = new SchemaMetadata(schema);
        metadata.put(schema, schemaMetadata);
        return schemaMetadata;
    }

    /**
     * Loads the schema from the FoundationDB transaction and processes the specified schema.
     * This method is not thread-safe.
     *
     * @param tr     The FoundationDB transaction.
     * @param schema The schema to load.
     */
    private void loadSchemaFromFDB(Transaction tr, String schema) {
        // This method is not thread-safe.
        if (!metadata.has(schema)) {
            try {
                addSchemaMetadata(schema);
            } catch (SchemaAlreadyExistsException e) {
                throw new IllegalStateException(e);
            }
        }
        List<String> tableLayout = getSchemaLayout(schema).tables().asList();
        boolean hasTablesDir = DirectoryLayer.getDefault().exists(tr, tableLayout).join();
        if (!hasTablesDir) {
            // Nothing to do
            return;
        }
        List<String> tables = DirectoryLayer.getDefault().list(tr, tableLayout).join();
        for (String table : tables) {
            loadTableFromFDB(tr, schema, table);
        }
    }

    /**
     * Initializes the SqlMetadataService instance by loading the schemas and tables from FoundationDB.
     * This method creates the schema hierarchy in the metadata store and loads the tables associated with each schema.
     */
    private void initialize() {
        long start = System.currentTimeMillis();
        lock.writeLock().lock();
        List<String> schemaLayout = DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().sql().metadata().schemas().asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> schemas = DirectoryLayer.getDefault().list(tr, schemaLayout).join();
            for (String schema : schemas) {
                loadSchemaFromFDB(tr, schema);
            }
        } finally {
            lock.writeLock().unlock();
        }
        long end = System.currentTimeMillis();
        LOGGER.info("SQL metadata has been loaded from FoundationDB cluster in {} ms", end - start);
    }

    /**
     * Starts the SqlMetadataService by performing the necessary initialization tasks and starting background threads.
     * Note that this method blocks until all background threads are started or a timeout of 5 seconds occurs.
     *
     * @throws IllegalStateException        If an unexpected exception occurs during the initialization of the default schema.
     * @throws KronotopException             If the thread is interrupted while awaiting the start of the background threads.
     */
    public void start() {
        byte[] key = context.getFoundationDB().run(tr -> context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.sqlMetadataEvents()));
        this.lastSqlMetadataVersionstamp.set(key);

        String defaultSchemaHierarchy = context.getConfig().getString("sql.default_schema");
        try {
            initializeDefaultSchema(defaultSchemaHierarchy);
        } catch (SchemaNotExistsException e) {
            // This should not be possible
            throw new IllegalStateException(e);
        }

        initialize();

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
        keyWatcher.unwatch(context.getJournal().getJournalMetadata(JournalName.sqlMetadataEvents()).getTrigger());
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
     * Finds the metadata for a schema in the metadata store.
     *
     * @param schema The name of the schema to find the metadata for.
     * @return The SchemaMetadata object representing the metadata for the schema.
     * @throws SchemaNotExistsException If the specified schema does not exist in the metadata store.
     */
    public SchemaMetadata findSchemaMetadata(String schema) throws SchemaNotExistsException {
        lock.readLock().lock();
        try {
            return metadata.get(schema);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Finds and loads the schema metadata for the given schema name. This method is thread-safe.
     *
     * @param schema The name of the schema to find and load the metadata for.
     * @return The SchemaMetadata object representing the metadata for the schema.
     * @throws SchemaNotExistsException If the specified schema does not exist in the metadata store.
     */
    public SchemaMetadata findOrLoadSchemaMetadata(String schema) throws SchemaNotExistsException {
        lock.writeLock().lock();
        try {
            return getOrLoadSchema(schema);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finds the metadata of a versioned table in the schema.
     *
     * @param schema the name of the schema to search for
     * @param table  the name of the table to search for
     * @return the VersionedTableMetadata object representing the found table metadata
     * @throws SchemaNotExistsException if the schema does not exist in the metadata store
     * @throws TableNotExistsException  if the table does not exist in the metadata store
     */
    public VersionedTableMetadata findVersionedTableMetadata(String schema, String table) throws SchemaNotExistsException, TableNotExistsException {
        lock.readLock().lock();
        try {
            SchemaMetadata schemaMetadata = metadata.get(schema);
            TableMetadata tableMetadata = schemaMetadata.getTables();
            return tableMetadata.get(table);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Finds the latest version of a table in the specified schema and table name.
     *
     * @param schema The list of names representing the hierarchy of the schema to search in.
     * @param table  The name of the table to search for.
     * @return The KronotopTable object representing the found table metadata.
     * @throws SchemaNotExistsException If the specified schema does not exist.
     * @throws TableNotExistsException  If the specified table does not exist.
     */
    public KronotopTable findTable(String schema, String table) throws SchemaNotExistsException, TableNotExistsException {
        VersionedTableMetadata versionedTableMetadata = findVersionedTableMetadata(schema, table);
        return versionedTableMetadata.getLatest();
    }

    /**
     * Retrieves the schema metadata for the given schema name. If the metadata is already loaded in the cache,
     * it will return the cached metadata. Otherwise, it will load the schema metadata from FoundationDB and
     * add it to the cache before returning. Note that this method is not thread-safe.
     *
     * @param schema The name of the schema to retrieve the metadata for.
     * @return The SchemaMetadata object representing the metadata for the schema.
     * @throws SchemaNotExistsException If the specified schema does not exist in the metadata store.
     */
    private SchemaMetadata getOrLoadSchema(String schema) throws SchemaNotExistsException {
        // this method is not thread safe

        // Check the existing records first.
        if (metadata.has(schema)) {
            return metadata.get(schema);
        }

        // Check the schema name on FDB.
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (!isSchemaExistOnFDB(tr, schema)) {
                throw new SchemaNotExistsException(schema);
            }
        }

        // Create the record and initialize the optimizer for this root schema.
        try {
            return addSchemaMetadata(schema);
        } catch (SchemaAlreadyExistsException e) {
            // This should not be possible
            throw new IllegalStateException(e);
        }
    }

    /**
     * Renames a table in the specified schema.
     *
     * @param schema  The name of the schema containing the table.
     * @param oldName The current name of the table.
     * @param newName The new name for the table.
     * @throws SchemaNotExistsException    If the specified schema does not exist.
     * @throws TableNotExistsException     If the table with the old name does not exist.
     * @throws TableAlreadyExistsException If a table with the new name already exists.
     */
    private void renameTable(String schema, String oldName, String newName)
            throws SchemaNotExistsException, TableNotExistsException, TableAlreadyExistsException {
        lock.writeLock().lock();
        try {
            SchemaMetadata schemaMetadata = getOrLoadSchema(schema);
            TableMetadata tableMetadata = schemaMetadata.getTables();
            tableMetadata.rename(oldName, newName);

            // Update KronotopSchema
            VersionedTableMetadata versionedTableMetadata = tableMetadata.get(newName);
            schemaMetadata.getKronotopSchema().getTableMap().put(newName, versionedTableMetadata.getLatest());
            schemaMetadata.getKronotopSchema().getTableMap().remove(oldName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes the specified schema from the metadata store.
     *
     * @param schema the name of the schema to be removed
     * @throws SchemaNotExistsException if the specified schema does not exist in the metadata store
     */
    private void removeSchema(String schema) throws SchemaNotExistsException {
        lock.writeLock().lock();
        try {
            metadata.remove(schema);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a table from the schema hierarchy in the metadata store.
     *
     * @param schema the list of names representing the hierarchy of the schema
     * @param table  the name of the table to remove
     * @throws SchemaNotExistsException if the schema does not exist in the metadata store
     * @throws TableNotExistsException  if the table does not exist in the metadata store
     */
    private void removeTable(String schema, String table) throws SchemaNotExistsException, TableNotExistsException {
        lock.writeLock().lock();
        try {
            SchemaMetadata current = metadata.get(schema);
            current.getTables().remove(table);
            // Update KronotopSchema
            current.getKronotopSchema().getTableMap().remove(table);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Loads the table from FoundationDB with the given versionstamp.
     *
     * @param tr           the FDB transaction
     * @param schema       the list of names representing the hierarchy to search for the schema
     * @param table        the name of the table to search for
     * @param versionBytes the versionstamp of the table metadata
     * @return TableModel object representing the table metadata
     */
    private TableModel loadTableMetadataWithVersionstamp(Transaction tr, String schema, String table, byte[] versionBytes)
            throws TableNotExistsException {
        DirectorySubspace subspace = openTableSubspace(tr, schema, table);
        Versionstamp versionstamp = Versionstamp.fromBytes(versionBytes);

        byte[] key = subspace.pack(Tuple.from(versionstamp));
        byte[] value = tr.get(key).join();
        if (value == null) {
            throw new TableNotExistsException(table);
        }
        return JSONUtils.readValue(value, TableModel.class);
    }

    /**
     * Adds a version of the table to the schema metadata.
     *
     * @param tableModel   The table model.
     * @param versionstamp The version stamp.
     * @throws SchemaNotExistsException           If the schema does not exist in the metadata.
     * @throws TableVersionAlreadyExistsException If the table version already exists in the metadata.
     */
    private void addTableVersion(TableModel tableModel, byte[] versionstamp)
            throws SchemaNotExistsException, TableVersionAlreadyExistsException {
        // This method is not thread safe.
        DirectorySubspace subspace = openTableSubspace(tableModel.getSchema(), tableModel.getTable());
        KronotopTable table = new KronotopTable(tableModel, subspace.pack());
        SchemaMetadata schemaMetadata = getOrLoadSchema(table.getSchema());
        VersionedTableMetadata versionedTableMetadata = schemaMetadata.getTables().getOrCreate(table.getName());
        String version = BaseEncoding.base64().encode(versionstamp);
        versionedTableMetadata.put(version, table);

        // Update KronotopSchema
        schemaMetadata.getKronotopSchema().getTableMap().put(tableModel.getTable(), versionedTableMetadata.getLatest());
        LOGGER.debug("Table: {} version: {} has been loaded", String.join(".", List.of(table.getSchema(), table.getName())), version);
    }

    private void processTableCreatedEvent(Transaction tr, BroadcastEvent broadcastEvent) {
        TableCreatedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), TableCreatedEvent.class);
        try {
            TableModel tableModel = loadTableMetadataWithVersionstamp(tr, event.getSchema(), event.getTable(), event.getVersionstamp());
            lock.writeLock().lock();
            try {
                addTableVersion(tableModel, event.getVersionstamp());
            } finally {
                lock.writeLock().unlock();
            }
        } catch (TableNotExistsException | SchemaNotExistsException | TableVersionAlreadyExistsException e) {
            LOGGER.error("Error while processing {} event, schema: {}, table: {}", EventTypes.TABLE_CREATED, event.getSchema(), event.getTable(), e);
            throw new KronotopException(e);
        }
    }

    private void processSchemaCreatedEvent(BroadcastEvent broadcastEvent) throws SchemaNotExistsException {
        SchemaCreatedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), SchemaCreatedEvent.class);
        findOrLoadSchemaMetadata(event.getSchema());
    }

    private void processSchemaDroppedEvent(BroadcastEvent broadcastEvent) {
        SchemaDroppedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), SchemaDroppedEvent.class);
        try {
            removeSchema(event.getSchema());
        } catch (SchemaNotExistsException e) {
            LOGGER.debug("Failed to process {}", EventTypes.SCHEMA_DROPPED, e);
        }
    }

    private void processTableDroppedEvent(BroadcastEvent broadcastEvent) {
        TableDroppedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), TableDroppedEvent.class);
        try {
            removeTable(event.getSchema(), event.getTable());
        } catch (SchemaNotExistsException | TableNotExistsException e) {
            LOGGER.debug("Failed to process {}", EventTypes.TABLE_DROPPED, e);
        }
    }

    private void processTableRenamedEvent(BroadcastEvent broadcastEvent) {
        TableRenamedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), TableRenamedEvent.class);
        try {
            try {
                renameTable(event.getSchema(), event.getOldName(), event.getNewName());
            } catch (TableNotExistsException e) {
                // Edge case, but it happens during the tests. If the time gap between CREATE TABLE and ALTER TABLE events is too small,
                // the table may be renamed before adding the old one to in-memory metadata. In this case, we fetch the new table instead of renaming the existing one.
                TableModel tableModel = context.getFoundationDB().run(tr -> {
                    try {
                        return loadTableMetadataWithVersionstamp(tr, event.getSchema(), event.getNewName(), event.getVersionstamp());
                    } catch (TableNotExistsException ex) {
                        LOGGER.error("Failed to load table with versionstamp: {}, {}", event.getNewName(), BaseEncoding.base64().encode(event.getVersionstamp()));
                        throw new RuntimeException(ex);
                    }
                });
                lock.writeLock().lock();
                try {
                    addTableVersion(tableModel, event.getVersionstamp());
                } finally {
                    lock.writeLock().unlock();
                }
            }
        } catch (SchemaNotExistsException | TableAlreadyExistsException | TableVersionAlreadyExistsException e) {
            LOGGER.error("Error while processing {} event, schema: {}, table: {}", EventTypes.TABLE_RENAMED, event.getSchema(), event.getOldName(), e);
        }
    }

    private void processTableAlteredEvent(Transaction tr, BroadcastEvent broadcastEvent) {
        TableAlteredEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), TableAlteredEvent.class);
        try {
            TableModel tableModel = loadTableMetadataWithVersionstamp(tr, event.getSchema(), event.getTable(), event.getVersionstamp());
            lock.writeLock().lock();
            try {
                addTableVersion(tableModel, event.getVersionstamp());
            } finally {
                lock.writeLock().unlock();
            }
        } catch (SchemaNotExistsException | TableVersionAlreadyExistsException | TableNotExistsException e) {
            LOGGER.error("Error while processing {} event, schema: {}, table: {}", EventTypes.TABLE_ALTERED, event.getSchema(), event.getTable(), e);
        }
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
                    BroadcastEvent broadcastEvent = JSONUtils.readValue(event.getValue(), BroadcastEvent.class);
                    LOGGER.debug("{} event has been received", broadcastEvent.getType());
                    switch (broadcastEvent.getType()) {
                        case SCHEMA_CREATED:
                            processSchemaCreatedEvent(broadcastEvent);
                            break;
                        case TABLE_CREATED:
                            processTableCreatedEvent(tr, broadcastEvent);
                            break;
                        case SCHEMA_DROPPED:
                            processSchemaDroppedEvent(broadcastEvent);
                            break;
                        case TABLE_DROPPED:
                            processTableDroppedEvent(broadcastEvent);
                            break;
                        case TABLE_RENAMED:
                            processTableRenamedEvent(broadcastEvent);
                            break;
                        case TABLE_ALTERED:
                            processTableAlteredEvent(tr, broadcastEvent);
                            break;
                        default:
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
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, context.getJournal().getJournalMetadata(JournalName.sqlMetadataEvents()).getTrigger());
                tr.commit().join();
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
