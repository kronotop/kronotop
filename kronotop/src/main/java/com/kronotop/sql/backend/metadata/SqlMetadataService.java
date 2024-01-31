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
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.KeyWatcher;
import com.kronotop.core.KronotopService;
import com.kronotop.core.journal.Event;
import com.kronotop.core.journal.JournalName;
import com.kronotop.sql.JSONUtils;
import com.kronotop.sql.KronotopTable;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.backend.metadata.events.*;
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
    private final SchemaMetadata metadata = new SchemaMetadata();
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
            loadSchemaHierarchy(defaultSchema);
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

    public void start() {
        String defaultSchemaHierarchy = context.getConfig().getString("sql.default_schema");
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
     * Retrieves the latest version of a table from the metadata store.
     *
     * @param tr     The FDB transaction.
     * @param schema The list of names representing the hierarchy of the schema.
     * @param table  The name of the table to retrieve the latest version for.
     * @return The TableWithVersion object representing the table with the latest version.
     * @throws KronotopException If the table exists but no version is found.
     */
    public TableWithVersion getLatestTableVersion(Transaction tr, String schema, String table) {
        List<String> subpath = getTableLayout(schema, table).asList();
        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();

        AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
        if (!iterator.hasNext()) {
            throw new KronotopException(String.format("Table '%s' exists but no version found", table));
        }

        KeyValue next = iterator.next();
        byte[] versionstamp = subspace.unpack(next.getKey()).getVersionstamp(0).getBytes();
        TableModel tableModel = JSONUtils.readValue(next.getValue(), TableModel.class);
        return new TableWithVersion(tableModel, versionstamp);
    }

    /**
     * Finds the SchemaMetadata object representing the metadata for a schema in the metadata store.
     *
     * @param schema The name of the schema to search for.
     * @return The SchemaMetadata object representing the found schema metadata.
     * @throws SchemaNotExistsException If the schema does not exist in the metadata store.
     */
    public SchemaMetadata findSchema(String schema) throws SchemaNotExistsException {
        lock.readLock().lock();
        try {
            return metadata.get(schema);
        } finally {
            lock.readLock().unlock();
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
    private VersionedTableMetadata findVersionedTableMetadata(String schema, String table) throws SchemaNotExistsException, TableNotExistsException {
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
     * Finds the KronotopTable object representing the metadata for a table in the specified schema and table name.
     *
     * @param schema  The name of the schema to search in.
     * @param table   The name of the table to search for.
     * @param version The version of the table to retrieve.
     * @return The KronotopTable object representing the found table metadata.
     * @throws SchemaNotExistsException If the specified schema does not exist.
     * @throws TableNotExistsException  If the specified table does not exist.
     */
    public KronotopTable findTable(String schema, String table, String version) throws SchemaNotExistsException, TableNotExistsException {
        VersionedTableMetadata versionedTableMetadata = findVersionedTableMetadata(schema, table);
        return versionedTableMetadata.get(version);
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
     * Checks if a schema exists in the metadata store and loads its metadata if it does.
     *
     * @param schema The name of the schema to check and load.
     * @return The SchemaMetadata object representing the metadata for the schema.
     * @throws SchemaNotExistsException If the schema does not exist in the metadata store.
     */
    public SchemaMetadata checkAndLoadSchema(String schema) throws SchemaNotExistsException {
        // this method is not thread safe
        if (!metadata.has(schema)) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (!isSchemaExistOnFDB(tr, schema)) {
                    throw new SchemaNotExistsException(schema);
                }
            }
            SchemaMetadata current = new SchemaMetadata();
            try {
                metadata.put(schema, current);
            } catch (SchemaAlreadyExistsException e) {
                // Not possible
            }
            return current;
        }
        return metadata.get(schema);
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
    private void renameTable(String schema, String oldName, String newName) throws SchemaNotExistsException, TableNotExistsException, TableAlreadyExistsException {
        lock.writeLock().lock();
        try {
            SchemaMetadata schemaMetadata = checkAndLoadSchema(schema);
            TableMetadata tableMetadata = schemaMetadata.getTables();
            tableMetadata.rename(oldName, newName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void loadSchemaHierarchy(String schema) throws SchemaNotExistsException {
        lock.writeLock().lock();
        try {
            checkAndLoadSchema(schema);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes the schema hierarchy from the metadata store based on the given list of names.
     *
     * @param schema the list of names representing the hierarchy of the schema to remove
     * @throws SchemaNotExistsException if the schema does not exist in the metadata store
     */
    private void removeSchemaHierarchy(String schema) throws SchemaNotExistsException {
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
    private TableModel loadTableMetadataWithVersionstamp(Transaction tr, String schema, String table, byte[] versionBytes) throws TableNotExistsException {
        List<String> subpath = getTableLayout(schema, table).asList();
        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
        Versionstamp versionstamp = Versionstamp.fromBytes(versionBytes);

        AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(Tuple.from(versionstamp)), 1).iterator();
        if (!iterator.hasNext()) {
            throw new TableNotExistsException(table);
        }

        KeyValue next = iterator.next();
        return JSONUtils.readValue(next.getValue(), TableModel.class);
    }

    /**
     * Adds a new version for a table in the schema metadata store.
     *
     * @param tableModel   The table metadata for the new version.
     * @param versionstamp The versionstamp of the table metadata.
     * @throws SchemaNotExistsException           if the schema does not exist in the schema metadata store.
     * @throws TableVersionAlreadyExistsException if the table version already exists in the metadata store.
     */
    private void addTableVersion(TableModel tableModel, byte[] versionstamp) throws SchemaNotExistsException, TableVersionAlreadyExistsException {
        KronotopTable table = new KronotopTable(tableModel);
        lock.writeLock().lock();
        try {
            SchemaMetadata schemaMetadata = checkAndLoadSchema(table.getSchema());
            VersionedTableMetadata versionedTableMetadata = schemaMetadata.getTables().getOrCreate(table.getName());
            String version = BaseEncoding.base64().encode(versionstamp);
            versionedTableMetadata.put(version, table);
            LOGGER.info("New version: {} has been added for table: {}", version, table.getSchema());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void processTableCreatedEvent(Transaction tr, BroadcastEvent broadcastEvent) {
        TableCreatedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), TableCreatedEvent.class);
        try {
            TableModel tableModel = loadTableMetadataWithVersionstamp(tr, event.getSchema(), event.getTable(), event.getVersionstamp());
            addTableVersion(tableModel, event.getVersionstamp());
        } catch (TableNotExistsException | SchemaNotExistsException | TableVersionAlreadyExistsException e) {
            LOGGER.error("Failed to add new table version: {}", e.getMessage());
            throw new KronotopException(e);
        }
    }

    private void processSchemaCreatedEvent(BroadcastEvent broadcastEvent) throws SchemaNotExistsException {
        SchemaCreatedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), SchemaCreatedEvent.class);
        loadSchemaHierarchy(event.getSchema());
    }

    private void processSchemaDroppedEvent(BroadcastEvent broadcastEvent) {
        SchemaDroppedEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), SchemaDroppedEvent.class);
        try {
            removeSchemaHierarchy(event.getSchema());
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
                addTableVersion(tableModel, event.getVersionstamp());
            }
        } catch (SchemaNotExistsException | TableAlreadyExistsException | TableVersionAlreadyExistsException e) {
            LOGGER.debug("Failed to process {}", EventTypes.TABLE_RENAMED, e);
        }
    }

    private void processTableAlteredEvent(Transaction tr, BroadcastEvent broadcastEvent) {
        TableAlteredEvent event = JSONUtils.readValue(broadcastEvent.getPayload(), TableAlteredEvent.class);
        try {
            TableModel tableModel = loadTableMetadataWithVersionstamp(tr, event.getSchema(), event.getTable(), event.getVersionstamp());
            addTableVersion(tableModel, event.getVersionstamp());
        } catch (SchemaNotExistsException | TableVersionAlreadyExistsException | TableNotExistsException e) {
            LOGGER.debug("Failed to process {}", EventTypes.TABLE_ALTERED, e);
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
