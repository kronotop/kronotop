package com.kronotop.redis.server.protocol;

import com.kronotop.server.resp.Request;

public class FlushAllMessage extends FlushDBMessage {
    public static final String COMMAND = "FLUSHALL";

    public FlushAllMessage(Request request) {
        super(request);
    }
}
