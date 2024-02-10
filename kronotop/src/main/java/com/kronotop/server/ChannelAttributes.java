/*
 * Copyright (c) 2023 Kronotop
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

import com.apple.foundationdb.Transaction;
import com.kronotop.foundationdb.namespace.Namespace;
import io.netty.util.AttributeKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ChannelAttributes class defines static final AttributeKey objects to be used as attribute keys in the ChannelHandlerContext.
 * These attributes represent different properties or values associated with a channel.
 */
public class ChannelAttributes {
    public static final AttributeKey<Boolean> AUTH = AttributeKey.valueOf("auth");

    public static final AttributeKey<Boolean> BEGIN = AttributeKey.valueOf("begin");

    public static final AttributeKey<Boolean> SNAPSHOT_READ = AttributeKey.valueOf("snapshot_read");

    public static final AttributeKey<Boolean> ONE_OFF_TRANSACTION = AttributeKey.valueOf("one_off_transaction");

    public static final AttributeKey<Transaction> TRANSACTION = AttributeKey.valueOf("transaction");

    public static final AttributeKey<String> CURRENT_NAMESPACE = AttributeKey.valueOf("current_namespace");

    public static final AttributeKey<Map<String, Namespace>> OPEN_NAMESPACES = AttributeKey.valueOf("open_namespaces");

    public static final AttributeKey<List<Request>> QUEUED_COMMANDS = AttributeKey.valueOf("queued_commands");

    public static final AttributeKey<Boolean> REDIS_MULTI = AttributeKey.valueOf("redis_multi");

    public static final AttributeKey<Boolean> REDIS_MULTI_DISCARDED = AttributeKey.valueOf("corrupt_redis_multi");

    public static final AttributeKey<HashMap<String, Long>> WATCHED_KEYS = AttributeKey.valueOf("watched_keys");

    public static final AttributeKey<Long> CLIENT_ID = AttributeKey.valueOf("client_id");

    public static final AttributeKey<String> SCHEMA = AttributeKey.valueOf("schema");
}
