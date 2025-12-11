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

import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import com.kronotop.volume.ParentOperationKind;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the CHANGELOG.RANGE command with interval notation for range selection.
 * <p>
 * Format: CHANGELOG.RANGE &lt;volume-name&gt; LIFECYCLE|FINALIZATION|* &lt;start&gt; &lt;end&gt; [LIMIT &lt;number&gt;] [REVERSE]
 * <p>
 * LIMIT and REVERSE can appear in any order:
 * <ul>
 *   <li>CHANGELOG.RANGE volume * [0 100] LIMIT 10 REVERSE</li>
 *   <li>CHANGELOG.RANGE volume * [0 100] REVERSE LIMIT 10</li>
 *   <li>CHANGELOG.RANGE volume * [0 100] REVERSE</li>
 *   <li>CHANGELOG.RANGE volume * [0 100] LIMIT 10</li>
 * </ul>
 * <p>
 * Start and end use bracket notation:
 * <ul>
 *   <li>[100 200] - inclusive start (>=100), inclusive end (<=200)</li>
 *   <li>(100 200] - exclusive start (>100), inclusive end (<=200)</li>
 *   <li>[100 200) - inclusive start (>=100), exclusive end (<200)</li>
 *   <li>(100 200) - exclusive start (>100), exclusive end (<200)</li>
 * </ul>
 */
public class ChangeLogRangeMessage extends BaseMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "CHANGELOG.RANGE";
    public static final int MINIMUM_PARAMETER_COUNT = 4;
    public static final int MAXIMUM_PARAMETER_COUNT = 7;
    private static final String LIMIT = "LIMIT";
    private static final String REVERSE = "REVERSE";
    // Pattern for start: [100 or (100
    private static final Pattern START_PATTERN = Pattern.compile("^([(\\[])(\\d+)$");
    // Pattern for end: 200] or 200)
    private static final Pattern END_PATTERN = Pattern.compile("^(\\d+)([)\\]])$");

    private String volume;
    private ParentOperationKind parentOpKind;
    private long start;
    private long end;
    private boolean startInclusive;
    private boolean endInclusive;
    private int limit;
    private boolean reverse;

    public ChangeLogRangeMessage(Request request) {
        super(request);
        parse();
    }

    private void parseStart(String startStr) {
        Matcher matcher = START_PATTERN.matcher(startStr.trim());
        if (!matcher.matches()) {
            throw new IllegalCommandArgumentException(
                    String.format("Invalid start format '%s'. Expected format: [number or (number", startStr)
            );
        }

        String bracket = matcher.group(1);
        String value = matcher.group(2);

        startInclusive = bracket.equals("[");

        try {
            start = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalCommandArgumentException(String.format("Invalid start value '%s'", value));
        }
    }

    private void parseEnd(String endStr) {
        Matcher matcher = END_PATTERN.matcher(endStr.trim());
        if (!matcher.matches()) {
            throw new IllegalCommandArgumentException(
                    String.format("Invalid end format '%s'. Expected format: number] or number)", endStr)
            );
        }

        String value = matcher.group(1);
        String bracket = matcher.group(2);

        endInclusive = bracket.equals("]");

        try {
            end = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalCommandArgumentException(String.format("Invalid end value '%s'", value));
        }
    }

    private void parseOptionalParameters(int index) {
        while (index < request.getParams().size()) {
            String param = readString(index);
            if (param.equalsIgnoreCase(LIMIT)) {
                if (index + 1 >= request.getParams().size()) {
                    throw new IllegalCommandArgumentException("LIMIT requires a number argument");
                }
                limit = Math.toIntExact(readLong(index + 1));
                index += 2;
            } else if (param.equalsIgnoreCase(REVERSE)) {
                reverse = true;
                index++;
            } else {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", param));
            }
        }
    }

    private void parse() {
        // CHANGELOG.RANGE <volume-name> LIFECYCLE|FINALIZATION|* [start end] [LIMIT <number>] [REVERSE]
        volume = readString(0);
        String rawParentOpKind = readString(1);
        if (!Objects.equals(rawParentOpKind, "*")) {
            try {
                parentOpKind = ParentOperationKind.valueOf(rawParentOpKind.toUpperCase());
            } catch (IllegalArgumentException exp) {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", rawParentOpKind));
            }
        }

        parseStart(readString(2));
        parseEnd(readString(3));

        if (start > end) {
            throw new IllegalCommandArgumentException(
                    String.format("Start value %d cannot be greater than end value %d", start, end)
            );
        }

        if (request.getParams().size() > 4) {
            parseOptionalParameters(4);
        }
    }

    public String getVolume() {
        return volume;
    }

    public ParentOperationKind getParentOpKind() {
        return parentOpKind;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean isStartInclusive() {
        return startInclusive;
    }

    public boolean isEndInclusive() {
        return endInclusive;
    }

    public int getLimit() {
        return limit;
    }

    public boolean isReverse() {
        return reverse;
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
