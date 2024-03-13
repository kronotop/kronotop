/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SqlMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "SQL";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private final List<String> queries = new LinkedList<>();
    private final Set<String> returning = new HashSet<>();

    public SqlMessage(Request request) {
        this.request = request;
        parse();
    }

    /**
     * Checks if the given byte array represents a specific string pattern.
     *
     * @param item the byte array to check
     * @return true if the byte array represents the specific string pattern, false otherwise
     */
    private boolean isReturning(byte[] item) {
        if (item.length != 9) {
            return false;
        }
        for (int i = 0; i < item.length; i++) {
            switch (i) {
                case 0, 4:
                    if (item[i] != 'r' && item[i] != 'R') {
                        return false;
                    }
                    break;
                case 1:
                    if (item[i] != 'e' && item[i] != 'E') {
                        return false;
                    }
                    break;
                case 2:
                    if (item[i] != 't' && item[i] != 'T') {
                        return false;
                    }
                    break;
                case 3:
                    if (item[i] != 'u' && item[i] != 'U') {
                        return false;
                    }
                    break;
                case 5, 7:
                    if (item[i] != 'n' && item[i] != 'N') {
                        return false;
                    }
                    break;
                case 6:
                    if (item[i] != 'i' && item[i] != 'I') {
                        return false;
                    }
                    break;
                case 8:
                    if (item[i] != 'g' && item[i] != 'G') {
                        return false;
                    }
                    break;
            }
        }
        return true;
    }

    /**
     * Parses the parameters after a certain index and adds them to the 'returning' list.
     *
     * @param index the starting index to parse from
     */
    private void parseReturning(int index) {
        for (int i = index; i < request.getParams().size(); i++) {
            ByteBuf buf = request.getParams().get(i);

            byte[] item = new byte[buf.readableBytes()];
            buf.readBytes(item);
            String field = new String(item);
            returning.add(field);
        }
    }

    private void parse() {
        for (int i = 0; i < request.getParams().size(); i++) {
            ByteBuf buf = request.getParams().get(i);

            byte[] item = new byte[buf.readableBytes()];
            buf.readBytes(item);

            if (isReturning(item)) {
                parseReturning(i + 1);
                break;
            }

            String query = new String(item);
            queries.add(query);
        }
    }

    public List<String> getQueries() {
        return queries;
    }

    public Set<String> getReturning() {
        return returning;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }
}
