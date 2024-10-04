/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.common.KronotopException;
import com.kronotop.server.CommandAlreadyRegisteredException;
import com.kronotop.server.ServerKind;
import com.kronotop.volume.handlers.SegmentRangeHandler;
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

public class VolumeService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Volume";
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeService.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String, Volume> volumes = new HashMap<>();

    public VolumeService(Context context) throws CommandAlreadyRegisteredException {
        super(context);

        handlerMethod(ServerKind.INTERNAL, new SegmentRangeHandler(this));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

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

    public Volume newVolume(VolumeConfig config) throws IOException {
        lock.writeLock().lock();
        try {
            if (volumes.containsKey(config.name())) {
                Volume volume = volumes.get(config.name());
                if (!volume.isClosed()) {
                    return volume;
                }
            }
            checkAndCreateDataDir(config.dataDir());
            // TODO: Create folders for this specific volume in the constructor.
            Volume volume = new Volume(context, config);
            volumes.put(config.name(), volume);
            return volume;
        } finally {
            lock.writeLock().unlock();
        }
    }

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
