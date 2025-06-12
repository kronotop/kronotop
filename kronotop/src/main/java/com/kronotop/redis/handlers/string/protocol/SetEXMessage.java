package com.kronotop.redis.handlers.string.protocol;

import com.kronotop.internal.ByteBufUtils;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class SetEXMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "SETEX";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private String key;
    private long seconds;
    private byte[] value;

    public SetEXMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ByteBufUtils.readAsString(request.getParams().getFirst());
        seconds = ByteBufUtils.readAsLong(request.getParams().get(1));
        value = new byte[request.getParams().get(2).readableBytes()];
        request.getParams().get(2).readBytes(value);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }

    public byte[] getValue() {
        return value;
    }

    public long getSeconds() {
        return seconds;
    }
}
