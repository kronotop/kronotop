package com.kronotop.protocol;

public class RESPCommandBuilder {
    private String command = "";

    public RESPCommandBuilder append(String str) {
        command = command.concat(str).concat("\r\n");
        return this;
    }

    public String toString() {
        return command;
    }
}
