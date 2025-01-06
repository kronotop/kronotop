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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.*;
import com.kronotop.volume.segment.Segment;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CleanupOrphanFilesSubcommand extends BaseHandler implements SubcommandHandler {

    public CleanupOrphanFilesSubcommand(VolumeService service) {
        super(service);
    }

    public Set<String> listFiles(String dir) throws IOException {
        try (Stream<Path> stream = Files.list(Paths.get(dir))) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public void execute(Request request, Response response) {
        CleanupOrphanFilesParameters parameters = new CleanupOrphanFilesParameters(request.getParams());

        List<RedisMessage> deletedFiles = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Volume volume = service.findVolume(parameters.volumeName);
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            Set<String> assumedFiles = new HashSet<>();
            volumeMetadata.getSegments().forEach(segmentId -> {
                String segmentName = Segment.generateName(segmentId);
                assumedFiles.add(segmentName);
                assumedFiles.add(segmentName + "." + Segment.SEGMENT_METADATA_FILE_EXTENSION);
            });

            Set<String> orphanFiles = new HashSet<>();
            Set<String> existingFiles = listFiles(Paths.get(volume.getConfig().dataDir(), Segment.SEGMENTS_DIRECTORY).toString());
            for (String existingFile : existingFiles) {
                if (!assumedFiles.contains(existingFile)) {
                    orphanFiles.add(existingFile);
                }
            }

            for (String orphanFile : orphanFiles) {
                Path path = Paths.get(volume.getConfig().dataDir(), Segment.SEGMENTS_DIRECTORY, orphanFile);
                if (Files.deleteIfExists(path)) {
                    deletedFiles.add(new SimpleStringRedisMessage(path.toString()));
                }
            }
        } catch (NoSuchFileException e) {
            throw new KronotopException("File not found: " + e.getMessage());
        } catch (IOException | ClosedVolumeException | VolumeNotOpenException e) {
            throw new KronotopException(e);
        }
        response.writeArray(deletedFiles);
    }

    private class CleanupOrphanFilesParameters {
        private final String volumeName;

        private CleanupOrphanFilesParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = readAsString(params.get(1));
        }
    }
}
