/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.handlers.protocol;

import com.kronotop.common.KronotopException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import com.kronotop.volume.SegmentRange;

import java.util.List;

public class SegmentRangeMessage extends BaseMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "SEGMENTRANGE";
    public static final int MINIMUM_PARAMETER_COUNT = 4;
    private String volume;
    private String segment;
    private SegmentRange[] segmentRanges;

    public SegmentRangeMessage(Request request) {
        super(request);
        parse();
    }


    private void parse() {
        if (request.getParams().size() % 2 > 0) {
            throw new KronotopException("Wrong number of parameters");
        }

        // segmentrange <volume-name> <segment-name> position length position length ...
        volume = readString(0);
        segment = readString(1);

        int index = 0;
        segmentRanges = new SegmentRange[(request.getParams().size() - 2) / 2];
        for (int i = 2; i < request.getParams().size(); i = i + 2) {
            long position = readLong(i);
            long length = readLong(i + 1);
            SegmentRange segmentRange = new SegmentRange(position, length);
            segmentRanges[index] = segmentRange;
            index++;
        }
    }

    public String getVolume() {
        return volume;
    }

    public String getSegment() {
        return segment;
    }

    public SegmentRange[] getSegmentRanges() {
        return segmentRanges;
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
