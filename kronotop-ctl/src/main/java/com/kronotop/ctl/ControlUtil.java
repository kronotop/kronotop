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
import com.kronotop.resp.RespValue;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Helpers shared by the kronotop-ctl commands.
 */
public final class ControlUtil {
    static final String NONE = "-";
    static final int MEMBER_ID_LENGTH = 40;
    static final int MEMBER_ID_PREFIX_LENGTH = 4;
    private static final String SERVER_ERROR_PREFIX = "Server error: ";

    private ControlUtil() {
    }

    /**
     * Opens a RESP3 connection to a cluster member with the default timeouts.
     *
     * @param address the member address as host:port
     * @return the open connection
     * @throws IOException if the member cannot be reached or rejects HELLO 3
     */
    public static RespConnection connect(String address) throws IOException {
        return connect(address, 0);
    }

    /**
     * Opens a RESP3 connection to a cluster member.
     *
     * @param address        the member address as host:port
     * @param timeoutSeconds the longest time to wait for the connection and for each reply, or 0 for the default
     *                       timeouts
     * @return the open connection
     * @throws IOException if the member cannot be reached or rejects HELLO 3
     */
    public static RespConnection connect(String address, int timeoutSeconds) throws IOException {
        int lastColon = address.lastIndexOf(':');
        if (lastColon < 0) {
            throw new IllegalArgumentException("Invalid address (missing port): " + address);
        }
        String host = address.substring(0, lastColon);
        int port = Integer.parseInt(address.substring(lastColon + 1));

        RespConnection conn;
        try {
            if (timeoutSeconds > 0) {
                int timeoutMillis = Math.toIntExact(TimeUnit.SECONDS.toMillis(timeoutSeconds));
                conn = new RespConnection(host, port, timeoutMillis, timeoutMillis);
            } else {
                conn = new RespConnection(host, port);
            }
        } catch (IOException e) {
            // For an unknown host, Java gives only the host name as the message, so we write the reason.
            String reason = e instanceof UnknownHostException ? "Unknown host" : e.getMessage();
            throw new IOException("Could not connect to '" + address + "': " + reason, e);
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

    // KR.ADMIN commands accept the first four characters of a member ID in place of the full ID.
    static String prefix(String memberId) {
        return memberId.length() > MEMBER_ID_PREFIX_LENGTH
                ? memberId.substring(0, MEMBER_ID_PREFIX_LENGTH)
                : memberId;
    }

    static void put(Map<RespValue, RespValue> object, String name, RespValue value) {
        object.put(new RespValue.BlobString(name), value);
    }

    static void put(Map<RespValue, RespValue> object, String name, String value) {
        put(object, name, new RespValue.BlobString(value));
    }
}
