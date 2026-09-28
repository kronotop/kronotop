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
import io.netty.buffer.ByteBuf;

public class BucketVectorMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.VECTOR";
    public static final int MINIMUM_ARGUMENT_COUNT = 3;
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
    private String namespace;

    public BucketVectorMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getArguments().get(0));
        selector = ProtocolMessageUtil.readAsString(request.getArguments().get(1));
        vector = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(2));
        parseOptionalArguments();
    }

    private void parseOptionalArguments() {
        long seen = 0;
        for (int i = 3; i < request.getArguments().size(); i++) {
            String raw = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(request.getArguments().get(i)));
            VectorArgumentKey key = VectorArgumentKey.findByValue(raw);
            if (key == null) {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
            }
            seen = ProtocolMessageUtil.markArgumentSeen(seen, key, key.getValue());
            switch (key) {
                case FILTER -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("FILTER argument must be followed by a BQL expression");
                    }
                    filter = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(i + 1));
                    i++;
                }
                case TOP -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("TOP argument must be followed by a positive integer");
                    }
                    topK = ProtocolMessageUtil.readAsInteger(request.getArguments().get(i + 1));
                    if (topK < 0) {
                        throw new IllegalCommandArgumentException("TOP argument must be a non-negative integer");
                    }
                    i++;
                }
                case THRESHOLD -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("THRESHOLD argument must be followed by a number");
                    }
                    threshold = (float) ProtocolMessageUtil.readAsDouble(request.getArguments().get(i + 1));
                    i++;
                }
                case MAX_SCAN_CANDIDATES -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("MAX-SCAN-CANDIDATES argument must be followed by a positive integer");
                    }
                    maxScanCandidates = ProtocolMessageUtil.readAsInteger(request.getArguments().get(i + 1));
                    if (maxScanCandidates <= 0) {
                        throw new IllegalCommandArgumentException("MAX-SCAN-CANDIDATES argument must be a positive integer");
                    }
                    i++;
                }
                case OVERQUERY -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("OVERQUERY argument must be followed by a number >= 1.0");
                    }
                    overquery = (float) ProtocolMessageUtil.readAsDouble(request.getArguments().get(i + 1));
                    if (overquery < 1.0f) {
                        throw new IllegalCommandArgumentException("OVERQUERY argument must be >= 1.0");
                    }
                    i++;
                }
                case PROJECTION -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("PROJECTION argument must be followed by a projection specification");
                    }
                    projection = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(i + 1));
                    i++;
                }
                case COLLATION -> {
                    if (request.getArguments().size() <= i + 1) {
                        throw new IllegalCommandArgumentException("COLLATION argument must be followed by a collation specification");
                    }
                    byte[] data = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(i + 1));
                    collation = CollationHelper.deserializeAndValidate(data);
                    i++;
                }
                case NAMESPACE -> {
                    ByteBuf value = ProtocolMessageUtil.requireValue(
                            request.getArguments(), i, key.getValue(), ProtocolMessageUtil.NAMESPACE_PATH);
                    namespace = ProtocolMessageUtil.readAsString(value);
                    i++;
                }
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

    /**
     * Returns the namespace given on the command, or null if not specified.
     */
    public String getNamespace() {
        return namespace;
    }
}
