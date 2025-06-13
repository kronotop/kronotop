package com.kronotop.redis.handlers.string.protocol;

import com.kronotop.server.Request;

public class TTLMessage extends GetMessage {
    public static final String COMMAND = "TTL";

    public TTLMessage(Request request) {
        super(request);
    }
}
