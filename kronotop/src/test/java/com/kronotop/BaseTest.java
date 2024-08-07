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

package com.kronotop;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.network.Address;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class BaseTest {
    private final String clusterName = UUID.randomUUID().toString();
    private final MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
    @TempDir
    public File redisVolumeRootPathTempDir;

    protected String getEphemeralTCPPort() {
        // Ephemeral ports (49152 to 65535), as defined by the Internet Assigned Numbers Authority (IANA).
        return Integer.toString(ThreadLocalRandom.current().nextInt(49152, 65535));
    }

    protected Member createMember(String addressString) throws UnknownHostException {
        Versionstamp processId = processIdGenerator.getProcessID();
        Address address = Address.parseString(addressString);
        return new Member(address, processId);
    }

    protected Member createMemberWithEphemeralPort() throws UnknownHostException {
        String address = String.format("localhost:[%s]", getEphemeralTCPPort());
        return createMember(address);
    }

    protected Config loadConfig(String resourceName) {
        System.setProperty("cluster.name", clusterName);

        Path volumeRootPath = Paths.get(redisVolumeRootPathTempDir.getAbsolutePath(), UUID.randomUUID().toString());
        System.setProperty("volume_test.volume.root_path", volumeRootPath.toString());

        ConfigFactory.invalidateCaches();
        return ConfigFactory.load(resourceName);
    }
}
