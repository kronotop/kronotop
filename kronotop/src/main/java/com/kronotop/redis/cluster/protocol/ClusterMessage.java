package com.kronotop.redis.cluster.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;

import java.util.List;

public class ClusterMessage implements KronotopMessage<String> {
    public static final String COMMAND = "CLUSTER";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private String subcommand;

    public ClusterMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawSubcommand = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawSubcommand);
        subcommand = new String(rawSubcommand);
    }

    public String getSubcommand() {
        return subcommand;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }


}
