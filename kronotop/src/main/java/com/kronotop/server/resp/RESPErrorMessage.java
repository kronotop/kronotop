package com.kronotop.server.resp;

import com.kronotop.common.resp.RESPError;

public class RESPErrorMessage {
    private final RESPError prefix;
    private final String message;


    public RESPErrorMessage(RESPError prefix, String message) {
        this.prefix = prefix;
        this.message = message;
    }

    public RESPError getPrefix() {
        return prefix;
    }

    public String getMessage() {
        return message;
    }
}
