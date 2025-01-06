/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.common.KronotopException;
import com.kronotop.server.CommandAlreadyRegisteredException;
import com.kronotop.server.ServerKind;
import com.kronotop.task.TaskService;
import com.kronotop.volume.handlers.SegmentInsertHandler;
import com.kronotop.volume.handlers.SegmentRangeHandler;
import com.kronotop.volume.handlers.VolumeAdminHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * VolumeService is a service class that handles the management of volumes within the Kronotop database system.
 * It extends CommandHandlerService and implements the KronotopService interface, providing methods to create, find,
 * close, and list volumes. The service ensures thread safety using a ReentrantReadWriteLock.
 */
public class VolumeService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Volume";
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeService.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String, Volume> volumes = new HashMap<>();

    public VolumeService(Context context) throws CommandAlreadyRegisteredException {
        super(context, NAME);

        handlerMethod(ServerKind.INTERNAL, new SegmentRangeHandler(this));
        handlerMethod(ServerKind.INTERNAL, new SegmentInsertHandler(this));
        handlerMethod(ServerKind.INTERNAL, new VolumeAdminHandler(this));
    }

    /**
     * Shuts down the VolumeService, ensuring that all managed volumes are properly closed and cleared.
     * This method acquires a write lock to ensure thread safety during the shutdown process.
     * It iterates through all entries in the volumes map, closes each Volume, and then clears the map.
     */
    @Override
    public void shutdown() {
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, Volume> entry : volumes.entrySet()) {
                entry.getValue().close();
            }
            volumes.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks the existence of a directory specified by the given path and creates it if it does not exist.
     * If the path exists but is not a directory, or if an I/O error occurs during creation, an exception is thrown.
     *
     * @param dataDir the path of the directory to check and create if necessary
     * @throws KronotopException if the path exists but is not a directory, or if an I/O error occurs
     */
    private void checkAndCreateDataDir(String dataDir) {
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (FileAlreadyExistsException e) {
            LOGGER.error("{} already exists but is not a directory", dataDir, e);
            throw new KronotopException(e);
        } catch (IOException e) {
            LOGGER.error("{} could not be created", dataDir, e);
            throw new KronotopException(e);
        }
    }

    /**
     * Submits a vacuum task for the specified volume if any pending vacuum task exists.
     * The method checks the metadata to determine if a vacuum operation is needed.
     * If the vacuum task is already completed, no further action is taken.
     *
     * @param volume the volume for which the vacuum task should be checked and submitted
     */
    private void submitVacuumTaskIfAny(Volume volume) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumMetadata vacuumMetadata = VacuumMetadata.load(tr, volume.getConfig().subspace());
            if (vacuumMetadata == null) {
                return;
            }
            TaskService taskService = context.getService(TaskService.NAME);
            VacuumTask task = new VacuumTask(context, volume, vacuumMetadata);
            LOGGER.debug("Submitting vacuum task {} for volume {}", task, volume.getConfig().name());
            taskService.execute(task);
        }
    }

    /**
     * Creates a new volume based on the provided configuration.
     * If a volume with the same name already exists and is not closed, it returns the existing volume.
     * Otherwise, it creates a new volume, ensuring the necessary directories are created.
     *
     * @param config the configuration for the volume to be created
     * @return the created volume
     * @throws IOException if an I/O error occurs during the creation of the directory or volume
     */
    public Volume newVolume(VolumeConfig config) throws IOException {
        lock.writeLock().lock();
        try {
            if (volumes.containsKey(config.name())) {
                Volume volume = volumes.get(config.name());
                if (!volume.isClosed()) {
                    // Already have the volume
                    return volume;
                }
            }
            // Create a new volume.
            checkAndCreateDataDir(config.dataDir());
            // TODO: Create folders for this specific volume in the constructor.
            Volume volume = new Volume(context, config);
            submitVacuumTaskIfAny(volume);
            volumes.put(config.name(), volume);
            return volume;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finds and returns a volume by name if it exists and is open.
     *
     * @param name the name of the volume to be found
     * @return the volume with the specified name
     * @throws ClosedVolumeException  if the volume is closed
     * @throws VolumeNotOpenException if the volume does not exist
     */
    public Volume findVolume(String name) {
        lock.readLock().lock();
        try {
            if (volumes.containsKey(name)) {
                Volume volume = volumes.get(name);
                if (volume.isClosed()) {
                    throw new ClosedVolumeException(name);
                }
                return volume;
            }
            throw new VolumeNotOpenException(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Closes the specified volume by name, removing it from the managed volumes.
     * If the volume does not exist or is already closed, a VolumeNotOpenException is thrown.
     *
     * @param name the name of the volume to be closed
     * @throws VolumeNotOpenException if the volume with the specified name does not exist or is already closed
     */
    public void closeVolume(String name) {
        lock.writeLock().lock();
        try {
            Volume volume = volumes.get(name);
            if (volume != null) {
                volume.close();
                volumes.remove(name);
                return;
            }
            throw new VolumeNotOpenException(name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves a list of all managed volumes.
     * <p>
     * This method returns an unmodifiable list of the volumes currently managed by the service.
     * The list will reflect any changes made to the volumes at the time of retrieval.
     *
     * @return a list of all managed volumes
     */
    public List<Volume> volumes() {
        lock.readLock().lock();
        try {
            // Returns an unmodifiable list
            return volumes.values().stream().toList();
        } finally {
            lock.readLock().unlock();
        }
    }
}
