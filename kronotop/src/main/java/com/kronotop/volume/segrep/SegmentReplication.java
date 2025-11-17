/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.segrep;

import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MembershipService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SegmentReplication implements Runnable {
    private final Context context;
    private final Path destination;
    private final ReplicationClient client;

    private volatile boolean shutdown;

    public SegmentReplication(Context context, ReplicationClient client, String destination) {
        this.context = context;
        this.client = client;

        try {
            this.destination = Files.createDirectories(Path.of(destination));
        } catch (IOException exp) {
            throw new UncheckedIOException(exp);
        }
    }

    @Override
    public void run() {
        //List<Long> segmentIds = connection.sync().listSegments(volumeName);
        //for (Long segmentId : segmentIds) {
        //    System.out.println(segmentId + " " + connection.sync().findPosition(volumeName, segmentId));
        //}
    }

    public void shutdown() {
        shutdown = true;
    }
}
