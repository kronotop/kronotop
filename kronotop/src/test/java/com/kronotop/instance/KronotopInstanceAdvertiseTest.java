/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.instance;

import com.kronotop.BaseTest;
import com.kronotop.ConfigException;
import com.kronotop.KronotopTestInstance;
import com.kronotop.network.Address;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KronotopInstanceAdvertiseTest extends BaseTest {
    private KronotopTestInstance instance;

    @AfterEach
    void tearDown() {
        if (instance == null) {
            return;
        }
        instance.shutdown();
    }

    @Test
    void shouldThrowWhenExternalAdvertiseEmptyForNonLoopbackHost() {
        // Behavior: start fails with ConfigException when network.external.host is not a loopback
        // address and network.external.advertise is empty.
        Config config = loadConfig("test.conf").withValue(
                "network.external.host", ConfigValueFactory.fromAnyRef("10.0.0.1")
        );
        instance = new KronotopTestInstance(config, false, false);
        ConfigException exception = assertThrows(ConfigException.class, () -> instance.start());
        assertEquals("network.external.advertise is empty, " +
                "it is only optional when network.external.host is a loopback address", exception.getMessage());
    }

    @Test
    void shouldThrowWhenInternalAdvertiseEmptyForNonLoopbackHost() {
        // Behavior: start fails with ConfigException when network.internal.host is not a loopback
        // address and network.internal.advertise is empty.
        Config config = loadConfig("test.conf").withValue(
                "network.internal.host", ConfigValueFactory.fromAnyRef("10.0.0.1")
        );
        instance = new KronotopTestInstance(config, false, false);
        ConfigException exception = assertThrows(ConfigException.class, () -> instance.start());
        assertEquals("network.internal.advertise is empty, " +
                "it is only optional when network.internal.host is a loopback address", exception.getMessage());
    }

    @Test
    void shouldFallBackToBindPortWhenAdvertisePortMissing() throws Exception {
        // Behavior: an advertise address without a port section falls back to the resolved bind port
        // of the same network interface.
        Config config = loadConfig("test.conf").withValue(
                "network.external.advertise", ConfigValueFactory.fromIterable(List.of("10.0.0.1"))
        );
        instance = new KronotopTestInstance(config);
        instance.start();

        Address advertised = instance.getMember().getExternalAdvertise().getFirst();
        assertEquals("10.0.0.1", advertised.getHost());
        assertEquals(instance.getMember().getExternalAddress().getPort(), advertised.getPort());
    }

    @Test
    void shouldUseExplicitPortInAdvertiseAddress() throws Exception {
        // Behavior: an advertise address with an explicit port section keeps that port instead of
        // falling back to the bind port.
        Config config = loadConfig("test.conf").withValue(
                "network.external.advertise", ConfigValueFactory.fromIterable(List.of("10.0.0.1:6000"))
        );
        instance = new KronotopTestInstance(config);
        instance.start();

        Address advertised = instance.getMember().getExternalAdvertise().getFirst();
        assertEquals("10.0.0.1", advertised.getHost());
        assertEquals(6000, advertised.getPort());
    }
}
