/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.CommitHook;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.foundationdb.namespace.Namespace;
import io.netty.util.AttributeKey;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The ChannelAttributes class defines static final AttributeKey objects to be used as attribute keys in the ChannelHandlerContext.
 * These attributes represent different properties or values associated with a channel.
 */
public class SessionAttributes {
    public static final AttributeKey<Boolean> AUTH = AttributeKey.valueOf("auth");

    public static final AttributeKey<Boolean> BEGIN = AttributeKey.valueOf("begin");

    public static final AttributeKey<Boolean> SNAPSHOT_READ = AttributeKey.valueOf("snapshot_read");

    public static final AttributeKey<Boolean> AUTO_COMMIT = AttributeKey.valueOf("auto_commit");

    public static final AttributeKey<Transaction> TRANSACTION = AttributeKey.valueOf("transaction");

    public static final AttributeKey<List<CommitHook>> POST_COMMIT_HOOKS = AttributeKey.valueOf("post_commit_hooks");

    public static final AttributeKey<String> CURRENT_NAMESPACE = AttributeKey.valueOf("current_namespace");

    public static final AttributeKey<Map<String, Namespace>> OPEN_NAMESPACES = AttributeKey.valueOf("open_namespaces");

    public static final AttributeKey<LinkedList<Integer>> ASYNC_RETURNING = AttributeKey.valueOf("async_returning");

    public static final AttributeKey<List<Request>> QUEUED_COMMANDS = AttributeKey.valueOf("queued_commands");

    public static final AttributeKey<Boolean> MULTI = AttributeKey.valueOf("multi");

    public static final AttributeKey<Boolean> MULTI_DISCARDED = AttributeKey.valueOf("multi_discarded");

    public static final AttributeKey<HashMap<String, Long>> WATCHED_KEYS = AttributeKey.valueOf("watched_keys");

    public static final AttributeKey<Long> CLIENT_ID = AttributeKey.valueOf("client_id");

    public static final AttributeKey<Session> SESSION = AttributeKey.valueOf("session");

    public static final AttributeKey<HashMap<String, Object>> CLIENT_ATTRIBUTES = AttributeKey.valueOf("client_attributes");

    public static final AttributeKey<Boolean> READONLY = AttributeKey.valueOf("readonly");

    public static final AttributeKey<AtomicInteger> USER_VERSION_COUNTER = AttributeKey.valueOf("user_version_counter");

    public static final AttributeKey<InputType> INPUT_TYPE = AttributeKey.valueOf("input_type");

    public static final AttributeKey<ReplyType> REPLY_TYPE = AttributeKey.valueOf("reply_type");

    public static final AttributeKey<Integer> LIMIT = AttributeKey.valueOf("limit");

    public static final AttributeKey<Boolean> PIN_READ_VERSION = AttributeKey.valueOf("pin_read_version");

    public static final AttributeKey<Map<Integer, QueryContext>> BUCKET_READ_QUERY_CONTEXTS = AttributeKey.valueOf("bucket_read_query_contexts");

    public static final AttributeKey<Map<Integer, QueryContext>> BUCKET_DELETE_QUERY_CONTEXTS = AttributeKey.valueOf("bucket_delete_query_contexts");

    public static final AttributeKey<Map<Integer, QueryContext>> BUCKET_UPDATE_QUERY_CONTEXTS = AttributeKey.valueOf("bucket_update_query_contexts");


}
