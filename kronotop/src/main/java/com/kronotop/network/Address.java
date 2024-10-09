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

package com.kronotop.network;

import com.kronotop.common.KronotopException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import static com.kronotop.common.Preconditions.checkNotNull;

/**
 * Represents an address of an instance in the cluster.
 */
public final class Address {
    private int port = -1;
    private String host;

    private transient int hashCode;

    public Address() {
    }

    public Address(String host, int port) throws UnknownHostException {
        this(host, InetAddress.getByName(host), port);
    }

    public Address(InetAddress inetAddress, int port) {
        this(null, inetAddress, port);
    }

    /**
     * Creates a new Address
     *
     * @param inetSocketAddress the InetSocketAddress to use
     * @throws java.lang.NullPointerException     if inetSocketAddress is null
     * @throws java.lang.IllegalArgumentException if the address can't be resolved.
     */
    public Address(InetSocketAddress inetSocketAddress) {
        this(resolve(inetSocketAddress), inetSocketAddress.getPort());
    }

    public Address(String hostname, InetAddress inetAddress, int port) {
        checkNotNull(inetAddress, "inetAddress can't be null");

        if (port == 0) {
            try {
                port = findAvailablePort();
            } catch (IOException e) {
                throw new KronotopException("failed to find an available port", e);
            }
        }

        String[] addressArgs = inetAddress.getHostAddress().split("\\%");
        host = hostname != null ? hostname : addressArgs[0];
        this.port = port;
        this.hashCode = hashCodeInternal();
    }

    public Address(Address address) {
        this.host = address.host;
        this.port = address.port;
        this.hashCode = hashCodeInternal();
    }

    private static InetAddress resolve(InetSocketAddress inetSocketAddress) {
        checkNotNull(inetSocketAddress, "inetSocketAddress can't be null");

        InetAddress address = inetSocketAddress.getAddress();
        if (address == null) {
            throw new IllegalArgumentException("Can't resolve address: " + inetSocketAddress);
        }
        return address;
    }

    public static Address parseString(String hostPort) throws UnknownHostException {
        hostPort = hostPort.replace("[", "").replace("]", "");
        String[] parsed = hostPort.split(":");
        if (parsed.length != 2) {
            throw new IllegalArgumentException(String.format("Illegal Address format: %s", hostPort));
        }
        return new Address(parsed[0], Integer.parseInt(parsed[1]));
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
        return '[' + host + "]:" + port;
    }
}