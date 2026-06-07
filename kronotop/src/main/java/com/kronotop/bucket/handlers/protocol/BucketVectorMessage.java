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

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.bucket.Collation;
import com.kronotop.bucket.handlers.CollationHelper;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

public class BucketVectorMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.VECTOR";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private String bucket;
    private String selector;
    private byte[] vector;
    private byte[] filter;
    private Collation collation;
    private int topK;
    private float threshold = 0.0f;
    private int maxScanCandidates;
    private float overquery = -1.0f;
    private byte[] projection;

    public BucketVectorMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));
        selector = ProtocolMessageUtil.readAsString(request.getParams().get(1));
        vector = ProtocolMessageUtil.readAsByteArray(request.getParams().get(2));
        parseOptionalArguments();
    }

    private void parseOptionalArguments() {
        for (int i = 3; i < request.getParams().size(); i++) {
            String raw = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(request.getParams().get(i)));
            switch (raw) {
                case "FILTER" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("FILTER argument must be followed by a BQL expression");
                    }
                    filter = ProtocolMessageUtil.readAsByteArray(request.getParams().get(i + 1));
                    i++;
                }
                case "TOP" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("TOP argument must be followed by a positive integer");
                    }
                    topK = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (topK < 0) {
                        throw new IllegalCommandArgumentException("TOP argument must be a non-negative integer");
                    }
                    i++;
                }
                case "THRESHOLD" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("THRESHOLD argument must be followed by a number");
                    }
                    threshold = (float) ProtocolMessageUtil.readAsDouble(request.getParams().get(i + 1));
                    i++;
                }
                case "MAX-SCAN-CANDIDATES" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("MAX-SCAN-CANDIDATES argument must be followed by a positive integer");
                    }
                    maxScanCandidates = ProtocolMessageUtil.readAsInteger(request.getParams().get(i + 1));
                    if (maxScanCandidates <= 0) {
                        throw new IllegalCommandArgumentException("MAX-SCAN-CANDIDATES argument must be a positive integer");
                    }
                    i++;
                }
                case "OVERQUERY" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("OVERQUERY argument must be followed by a number >= 1.0");
                    }
                    overquery = (float) ProtocolMessageUtil.readAsDouble(request.getParams().get(i + 1));
                    if (overquery < 1.0f) {
                        throw new IllegalCommandArgumentException("OVERQUERY argument must be >= 1.0");
                    }
                    i++;
                }
                case "PROJECTION" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("PROJECTION argument must be followed by a projection specification");
                    }
                    projection = ProtocolMessageUtil.readAsByteArray(request.getParams().get(i + 1));
                    i++;
                }
                case "COLLATION" -> {
                    if (request.getParams().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("COLLATION argument must be followed by a collation specification");
                    }
                    byte[] data = ProtocolMessageUtil.readAsByteArray(request.getParams().get(i + 1));
                    collation = CollationHelper.deserializeAndValidate(data);
                    i++;
                }
                default -> throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
            }
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getSelector() {
        return selector;
    }

    public byte[] getVector() {
        return vector;
    }

    public byte[] getFilter() {
        return filter;
    }

    public int getTopK() {
        return topK;
    }

    public float getThreshold() {
        return threshold;
    }

    public int getMaxScanCandidates() {
        return maxScanCandidates;
    }

    public float getOverquery() {
        return overquery;
    }

    public byte[] getProjection() {
        return projection;
    }

    public Collation getCollation() {
        return collation;
    }
}
