/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.resp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * A minimal RESP3 connection wrapper around Socket, RespReader, and RespWriter.
 */
public class RespConnection implements AutoCloseable {

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

    private final Socket socket;
    private final RespReader reader;
    private final RespWriter writer;

    public RespConnection(String host, int port) throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), DEFAULT_CONNECT_TIMEOUT_MS);
        reader = new RespReader(socket.getInputStream());
        writer = new RespWriter(socket.getOutputStream());
    }

    /**
     * Switches the connection to RESP3 protocol mode.
     */
    public void hello3() throws IOException {
        RespValue response = executeRaw(List.of("HELLO", "3"));
        if (response instanceof RespValue.SimpleError(String code, String message)) {
            throw new IOException("HELLO 3 failed: " + code + " " + message);
        }
        if (response instanceof RespValue.BlobError(String code, String message)) {
            throw new IOException("HELLO 3 failed: " + code + " " + message);
        }
    }

    /**
     * Sends a command and returns the response. Throws on server error responses.
     */
    public RespValue execute(List<String> args) throws IOException {
        RespValue response = executeRaw(args);
        if (response instanceof RespValue.SimpleError(String code, String message)) {
            throw new IOException("Server error: " + code + " " + message);
        }
        if (response instanceof RespValue.BlobError(String code, String message)) {
            throw new IOException("Server error: " + code + " " + message);
        }
        return response;
    }

    /**
     * Sends a command and returns the raw response without throwing on errors.
     */
    public RespValue executeRaw(List<String> args) throws IOException {
        writer.writeCommand(args);
        return reader.read();
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
