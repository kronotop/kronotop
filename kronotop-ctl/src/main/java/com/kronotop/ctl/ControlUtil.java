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

package com.kronotop.ctl;

import com.kronotop.resp.RespConnection;

import java.io.IOException;

/**
 * Helpers shared by the kronotop-ctl commands.
 */
public final class ControlUtil {
    private static final String SERVER_ERROR_PREFIX = "Server error: ";

    private ControlUtil() {
    }

    /**
     * Opens a RESP3 connection to a cluster member.
     *
     * @param address the member address as host:port
     * @return the open connection
     * @throws IOException if the member cannot be reached or rejects HELLO 3
     */
    public static RespConnection connect(String address) throws IOException {
        int lastColon = address.lastIndexOf(':');
        if (lastColon < 0) {
            throw new IllegalArgumentException("Invalid address (missing port): " + address);
        }
        String host = address.substring(0, lastColon);
        int port = Integer.parseInt(address.substring(lastColon + 1));

        RespConnection conn;
        try {
            conn = new RespConnection(host, port);
        } catch (IOException e) {
            throw new IOException("Could not connect to '" + address + "': " + e.getMessage(), e);
        }
        try {
            conn.hello3();
        } catch (IOException e) {
            conn.close();
            throw e;
        }
        return conn;
    }

    /**
     * Returns the message of an exception without the server error prefix.
     *
     * @param e the exception
     * @return the message to print
     */
    public static String errorMessage(Exception e) {
        String message = e.getMessage();
        if (message != null && message.startsWith(SERVER_ERROR_PREFIX)) {
            return message.substring(SERVER_ERROR_PREFIX.length());
        }
        return message;
    }
}
