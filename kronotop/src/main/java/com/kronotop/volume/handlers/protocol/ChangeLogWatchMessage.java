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

package com.kronotop.volume.handlers.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class ChangeLogWatchMessage extends BaseMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "CHANGELOG.WATCH";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private String volume;
    private long logSequenceNumber;

    public ChangeLogWatchMessage(Request request) {
        super(request);
        parse();
    }

    private void parse() {
        // changelog.watch <volume-name> <log-sequence-number>
        volume = readString(0);
        logSequenceNumber = readLong(1);
    }

    public String getVolume() {
        return volume;
    }

    public long getSequenceNumber() {
        return logSequenceNumber;
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
