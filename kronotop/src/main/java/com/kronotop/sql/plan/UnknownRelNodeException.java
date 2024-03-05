package com.kronotop.sql.plan;

import com.kronotop.common.KronotopException;

public class UnknownRelNodeException extends KronotopException {
    public UnknownRelNodeException(String name) {
        super("Unknown RelNode: " + name);
    }
}
