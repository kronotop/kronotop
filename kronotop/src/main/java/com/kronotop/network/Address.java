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

package com.kronotop.network;

import com.google.common.net.HostAndPort;
import com.kronotop.KronotopException;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Represents an address of an instance in the cluster.
 */
public final class Address {
    private int port = -1;
    private String host;

    private transient int hashCode;

    public Address() {
    }

    public Address(String host, int port) {
        if (port == 0) {
            try {
                port = findAvailablePort();
            } catch (IOException e) {
                throw new KronotopException("failed to find an available port", e);
            }
        }
        this.host = host;
        this.port = port;
        this.hashCode = hashCodeInternal();
    }

    public static Address fromString(String hostPortString) {
        HostAndPort hostAndPort = HostAndPort.fromString(hostPortString);
        return new Address(hostAndPort.getHost(), hostAndPort.getPort());
    }

    private int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Address address)) {
            return false;
        }
        return hashCode == address.hashCode && port == address.port && this.host.equals(address.host);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hashCodeInternal() {
        int result = port;
        result = 31 * result + host.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return HostAndPort.fromParts(host, port).toString();
    }
}