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

import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.common.KronotopException;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class VolumeService implements KronotopService {
    public static final String NAME = "Volume";
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeService.class);
    private final Context context;
    private final Path rootPath;

    public VolumeService(Context context) {
        this.context = context;
        this.rootPath = createOrOpenRootPath();
    }

    private Path createOrOpenRootPath() {
        try {
            String rootPath = context.getConfig().getString("volumes.root_path");
            return Files.createDirectories(Paths.get(rootPath));
        } catch (ConfigException.Missing e) {
            LOGGER.error("volumes.root_path is missing");
            throw new KronotopException(e);
        } catch (FileAlreadyExistsException e) {
            LOGGER.error("volumes.root_path already exists but is not a directory", e);
            throw new KronotopException(e);
        } catch (IOException e) {
            LOGGER.error("volumes.root_path could not be created", e);
            throw new KronotopException(e);
        }
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
    }

    public Volume newVolume(VolumeConfig config) {
        return new Volume(context, rootPath, config);
    }
}
