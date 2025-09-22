package com.kronotop.bucket.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

public class BucketUpdateMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.UPDATE";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    public static final int MAXIMUM_PARAMETER_COUNT = 6;
    private final Request request;
    private String query;
    private String bucket;
    private QueryArguments arguments;

    public BucketUpdateMessage(Request request) {
        this.request = request;
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));
        query = ProtocolMessageUtil.readAsString(request.getParams().get(1));
        arguments = parseCommonQueryArguments(request, 2);
    }

    public QueryArguments getArguments() {
        return arguments;
    }

    public String getQuery() {
        return query;
    }

    public String getBucket() {
        return bucket;
    }
}
