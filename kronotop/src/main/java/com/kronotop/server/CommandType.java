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

package com.kronotop.server;

/**
 * Enum representing all supported command types in Kronotop.
 * Uses JIT-optimized switch statement for O(1) string-to-enum lookup.
 */
public enum CommandType {
    // Connection commands
    PING,
    ECHO,
    SELECT,
    HELLO,
    AUTH,
    TIME,

    // String commands
    GET,
    SET,
    MGET,
    MSET,
    MSETNX,
    GETSET,
    APPEND,
    STRLEN,
    INCR,
    INCRBY,
    INCRBYFLOAT,
    DECR,
    DECRBY,
    GETRANGE,
    SETRANGE,
    SETEX,
    SETNX,
    GETDEL,
    SUBSTR,

    // Hash commands
    HGET,
    HSET,
    HMGET,
    HMSET,
    HDEL,
    HEXISTS,
    HLEN,
    HSTRLEN,
    HGETALL,
    HKEYS,
    HVALS,
    HINCRBY,
    HINCRBYFLOAT,
    HSETNX,
    HRANDFIELD,

    // Generic commands
    DEL,
    EXISTS,
    TYPE,
    RENAME,
    RENAMENX,
    EXPIRE,
    EXPIREAT,
    PEXPIRE,
    PEXPIREAT,
    TTL,
    PTTL,
    PERSIST,
    RANDOMKEY,
    SCAN,

    // Transaction commands
    MULTI,
    EXEC,
    DISCARD,
    WATCH,
    UNWATCH,

    // Sharded Pub/Sub commands
    SSUBSCRIBE,
    SUNSUBSCRIBE,
    SPUBLISH,

    // Server commands
    COMMAND,
    FLUSHDB,
    FLUSHALL,
    INFO,

    // Connection mode commands
    READONLY,
    READWRITE,

    // Cluster commands
    CLUSTER,
    KR_ADMIN("KR.ADMIN"),

    // Client commands
    CLIENT,

    // FoundationDB commands
    BEGIN,
    COMMIT,
    ROLLBACK,
    SNAPSHOTREAD,
    GETREADVERSION,
    GETAPPROXIMATESIZE,

    // Distsys primitives
    TICK,

    // ZMap commands
    ZGET,
    ZSET,
    ZDEL,
    ZDELRANGE,
    ZMUTATE,
    ZGETRANGE,
    ZGETRANGESIZE,
    ZGETKEY,
    ZWATCH,
    ZGET_I64("ZGET.I64"),
    ZGET_F64("ZGET.F64"),
    ZGET_D128("ZGET.D128"),
    ZINC_I64("ZINC.I64"),
    ZINC_F64("ZINC.F64"),
    ZINC_D128("ZINC.D128"),
    ZSET_I64("ZSET.I64"),
    ZSET_F64("ZSET.F64"),
    ZSET_D128("ZSET.D128"),

    // Bucket commands
    BUCKET_CREATE("BUCKET.CREATE"),
    BUCKET_INSERT("BUCKET.INSERT"),
    BUCKET_UPDATE("BUCKET.UPDATE"),
    BUCKET_DELETE("BUCKET.DELETE"),
    BUCKET_REMOVE("BUCKET.REMOVE"),
    BUCKET_QUERY("BUCKET.QUERY"),
    BUCKET_ADVANCE("BUCKET.ADVANCE"),
    BUCKET_CURSORS("BUCKET.CURSORS"),
    BUCKET_CLOSE("BUCKET.CLOSE"),
    BUCKET_PURGE("BUCKET.PURGE"),
    BUCKET_INDEX("BUCKET.INDEX"),
    BUCKET_EXPLAIN("BUCKET.EXPLAIN"),
    BUCKET_LOCATE("BUCKET.LOCATE"),
    BUCKET_LIST("BUCKET.LIST"),
    BUCKET_VECTOR("BUCKET.VECTOR"),

    // Volume commands
    VOLUME_ADMIN("VOLUME.ADMIN"),
    VOLUME_INSPECT("VOLUME.INSPECT"),
    VOLUME_STATS("VOLUME.STATS"),
    SEGMENT_INSERT("SEGMENT.INSERT"),
    SEGMENT_RANGE("SEGMENT.RANGE"),
    SEGMENT_TAILPOINTER("SEGMENT.TAILPOINTER"),
    CHANGELOG_RANGE("CHANGELOG.RANGE"),
    CHANGELOG_WATCH("CHANGELOG.WATCH"),

    // Task commands
    TASK_ADMIN("TASK.ADMIN"),

    // Namespace commands
    NAMESPACE,

    // Session commands
    SESSION_ATTRIBUTE("SESSION.ATTRIBUTE"),
    SESSION_CLOSE("SESSION.CLOSE"),

    // Query commands
    QUERY;

    private final String commandName;

    CommandType() {
        this.commandName = name();
    }

    CommandType(String commandName) {
        this.commandName = commandName;
    }

    /**
     * Parses a command string to its corresponding CommandType.
     * Uses JIT-optimized switch statement for O(1) lookup.
     *
     * @param command the command string to parse
     * @return the corresponding CommandType, or null if not found
     */
    public static CommandType parse(String command) {
        return switch (command) {
            // Connection commands
            case "PING" -> PING;
            case "ECHO" -> ECHO;
            case "SELECT" -> SELECT;
            case "HELLO" -> HELLO;
            case "AUTH" -> AUTH;
            case "TIME" -> TIME;

            // String commands
            case "GET" -> GET;
            case "SET" -> SET;
            case "MGET" -> MGET;
            case "MSET" -> MSET;
            case "MSETNX" -> MSETNX;
            case "GETSET" -> GETSET;
            case "APPEND" -> APPEND;
            case "STRLEN" -> STRLEN;
            case "INCR" -> INCR;
            case "INCRBY" -> INCRBY;
            case "INCRBYFLOAT" -> INCRBYFLOAT;
            case "DECR" -> DECR;
            case "DECRBY" -> DECRBY;
            case "GETRANGE" -> GETRANGE;
            case "SETRANGE" -> SETRANGE;
            case "SETEX" -> SETEX;
            case "SETNX" -> SETNX;
            case "GETDEL" -> GETDEL;
            case "SUBSTR" -> SUBSTR;

            // Hash commands
            case "HGET" -> HGET;
            case "HSET" -> HSET;
            case "HMGET" -> HMGET;
            case "HMSET" -> HMSET;
            case "HDEL" -> HDEL;
            case "HEXISTS" -> HEXISTS;
            case "HLEN" -> HLEN;
            case "HSTRLEN" -> HSTRLEN;
            case "HGETALL" -> HGETALL;
            case "HKEYS" -> HKEYS;
            case "HVALS" -> HVALS;
            case "HINCRBY" -> HINCRBY;
            case "HINCRBYFLOAT" -> HINCRBYFLOAT;
            case "HSETNX" -> HSETNX;
            case "HRANDFIELD" -> HRANDFIELD;

            // Generic commands
            case "DEL" -> DEL;
            case "EXISTS" -> EXISTS;
            case "TYPE" -> TYPE;
            case "RENAME" -> RENAME;
            case "RENAMENX" -> RENAMENX;
            case "EXPIRE" -> EXPIRE;
            case "EXPIREAT" -> EXPIREAT;
            case "PEXPIRE" -> PEXPIRE;
            case "PEXPIREAT" -> PEXPIREAT;
            case "TTL" -> TTL;
            case "PTTL" -> PTTL;
            case "PERSIST" -> PERSIST;
            case "RANDOMKEY" -> RANDOMKEY;
            case "SCAN" -> SCAN;

            // Transaction commands
            case "MULTI" -> MULTI;
            case "EXEC" -> EXEC;
            case "DISCARD" -> DISCARD;
            case "WATCH" -> WATCH;
            case "UNWATCH" -> UNWATCH;

            // Sharded Pub/Sub commands
            case "SSUBSCRIBE" -> SSUBSCRIBE;
            case "SUNSUBSCRIBE" -> SUNSUBSCRIBE;
            case "SPUBLISH" -> SPUBLISH;

            // Server commands
            case "COMMAND" -> COMMAND;
            case "FLUSHDB" -> FLUSHDB;
            case "FLUSHALL" -> FLUSHALL;
            case "INFO" -> INFO;

            // Connection mode commands
            case "READONLY" -> READONLY;
            case "READWRITE" -> READWRITE;

            // Cluster commands
            case "CLUSTER" -> CLUSTER;
            case "KR.ADMIN" -> KR_ADMIN;

            // Client commands
            case "CLIENT" -> CLIENT;

            // FoundationDB commands
            case "BEGIN" -> BEGIN;
            case "COMMIT" -> COMMIT;
            case "ROLLBACK" -> ROLLBACK;
            case "SNAPSHOTREAD" -> SNAPSHOTREAD;
            case "GETREADVERSION" -> GETREADVERSION;
            case "GETAPPROXIMATESIZE" -> GETAPPROXIMATESIZE;

            // Distsys primitives
            case "TICK" -> TICK;

            // ZMap commands
            case "ZGET" -> ZGET;
            case "ZSET" -> ZSET;
            case "ZDEL" -> ZDEL;
            case "ZDELRANGE" -> ZDELRANGE;
            case "ZMUTATE" -> ZMUTATE;
            case "ZGETRANGE" -> ZGETRANGE;
            case "ZGETRANGESIZE" -> ZGETRANGESIZE;
            case "ZGETKEY" -> ZGETKEY;
            case "ZWATCH" -> ZWATCH;
            case "ZGET.I64" -> ZGET_I64;
            case "ZGET.F64" -> ZGET_F64;
            case "ZGET.D128" -> ZGET_D128;
            case "ZINC.I64" -> ZINC_I64;
            case "ZINC.F64" -> ZINC_F64;
            case "ZINC.D128" -> ZINC_D128;
            case "ZSET.I64" -> ZSET_I64;
            case "ZSET.F64" -> ZSET_F64;
            case "ZSET.D128" -> ZSET_D128;

            // Bucket commands
            case "BUCKET.CREATE" -> BUCKET_CREATE;
            case "BUCKET.INSERT" -> BUCKET_INSERT;
            case "BUCKET.UPDATE" -> BUCKET_UPDATE;
            case "BUCKET.DELETE" -> BUCKET_DELETE;
            case "BUCKET.REMOVE" -> BUCKET_REMOVE;
            case "BUCKET.QUERY" -> BUCKET_QUERY;
            case "BUCKET.ADVANCE" -> BUCKET_ADVANCE;
            case "BUCKET.CURSORS" -> BUCKET_CURSORS;
            case "BUCKET.CLOSE" -> BUCKET_CLOSE;
            case "BUCKET.PURGE" -> BUCKET_PURGE;
            case "BUCKET.INDEX" -> BUCKET_INDEX;
            case "BUCKET.EXPLAIN" -> BUCKET_EXPLAIN;
            case "BUCKET.LOCATE" -> BUCKET_LOCATE;
            case "BUCKET.LIST" -> BUCKET_LIST;
            case "BUCKET.VECTOR" -> BUCKET_VECTOR;

            // Volume commands
            case "VOLUME.ADMIN" -> VOLUME_ADMIN;
            case "VOLUME.INSPECT" -> VOLUME_INSPECT;
            case "VOLUME.STATS" -> VOLUME_STATS;
            case "SEGMENT.INSERT" -> SEGMENT_INSERT;
            case "SEGMENT.RANGE" -> SEGMENT_RANGE;
            case "SEGMENT.TAILPOINTER" -> SEGMENT_TAILPOINTER;
            case "CHANGELOG.RANGE" -> CHANGELOG_RANGE;
            case "CHANGELOG.WATCH" -> CHANGELOG_WATCH;

            // Task commands
            case "TASK.ADMIN" -> TASK_ADMIN;

            // Namespace commands
            case "NAMESPACE" -> NAMESPACE;

            // Session commands
            case "SESSION.ATTRIBUTE" -> SESSION_ATTRIBUTE;
            case "SESSION.CLOSE" -> SESSION_CLOSE;

            // Query commands
            case "QUERY" -> QUERY;

            default -> null;
        };
    }

    public String getCommandName() {
        return commandName;
    }
}
