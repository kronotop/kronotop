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

import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.foundationdb.namespace.protocol.NamespaceMessage;
import com.kronotop.foundationdb.protocol.*;
import com.kronotop.foundationdb.zmap.protocol.*;
import com.kronotop.redis.handlers.client.protocol.ClientMessage;
import com.kronotop.redis.handlers.cluster.protocol.ClusterMessage;
import com.kronotop.redis.handlers.connection.protocol.AuthMessage;
import com.kronotop.redis.handlers.connection.protocol.HelloMessage;
import com.kronotop.redis.handlers.connection.protocol.PingMessage;
import com.kronotop.redis.handlers.connection.protocol.SelectMessage;
import com.kronotop.redis.handlers.generic.protocol.*;
import com.kronotop.redis.handlers.hash.protocol.*;
import com.kronotop.redis.handlers.protocol.InfoMessage;
import com.kronotop.redis.handlers.string.protocol.*;
import com.kronotop.redis.handlers.transactions.protocol.*;
import com.kronotop.redis.server.protocol.CommandMessage;
import com.kronotop.redis.server.protocol.FlushAllMessage;
import com.kronotop.redis.server.protocol.FlushDBMessage;
import com.kronotop.task.handlers.protocol.TaskAdminMessage;
import com.kronotop.volume.handlers.protocol.SegmentInsertMessage;
import com.kronotop.volume.handlers.protocol.SegmentRangeMessage;
import com.kronotop.volume.handlers.protocol.VolumeAdminMessage;
import io.netty.util.AttributeKey;

/**
 * The MessageTypes class contains constants representing various message types used in a system.
 * Each constant represents a specific message type and has an associated AttributeKey.
 */
public class MessageTypes {
    // External commands
    public static final AttributeKey<SetMessage> SET = AttributeKey.valueOf(SetMessage.COMMAND);
    public static final AttributeKey<SetNXMessage> SETNX = AttributeKey.valueOf(SetNXMessage.COMMAND);
    public static final AttributeKey<GetMessage> GET = AttributeKey.valueOf(GetMessage.COMMAND);
    public static final AttributeKey<AppendMessage> APPEND = AttributeKey.valueOf(AppendMessage.COMMAND);
    public static final AttributeKey<DecrByMessage> DECRBY = AttributeKey.valueOf(DecrByMessage.COMMAND);
    public static final AttributeKey<DecrMessage> DECR = AttributeKey.valueOf(DecrMessage.COMMAND);
    public static final AttributeKey<GetDelMessage> GETDEL = AttributeKey.valueOf(GetDelMessage.COMMAND);
    public static final AttributeKey<GetRangeMessage> GETRANGE = AttributeKey.valueOf(GetRangeMessage.COMMAND);
    public static final AttributeKey<GetSetMessage> GETSET = AttributeKey.valueOf(GetSetMessage.COMMAND);
    public static final AttributeKey<IncrByFloatMessage> INCRBYFLOAT = AttributeKey.valueOf(IncrByFloatMessage.COMMAND);
    public static final AttributeKey<IncrByMessage> INCRBY = AttributeKey.valueOf(IncrByMessage.COMMAND);
    public static final AttributeKey<IncrMessage> INCR = AttributeKey.valueOf(IncrMessage.COMMAND);
    public static final AttributeKey<SetRangeMessage> SETRANGE = AttributeKey.valueOf(SetRangeMessage.COMMAND);
    public static final AttributeKey<StrlenMessage> STRLEN = AttributeKey.valueOf(StrlenMessage.COMMAND);
    public static final AttributeKey<MGetMessage> MGET = AttributeKey.valueOf(MGetMessage.COMMAND);
    public static final AttributeKey<MSetMessage> MSET = AttributeKey.valueOf(MSetMessage.COMMAND);
    public static final AttributeKey<MSetNXMessage> MSETNX = AttributeKey.valueOf(MSetNXMessage.COMMAND);
    public static final AttributeKey<DelMessage> DEL = AttributeKey.valueOf(DelMessage.COMMAND);
    public static final AttributeKey<ExistsMessage> EXISTS = AttributeKey.valueOf(ExistsMessage.COMMAND);
    public static final AttributeKey<ReadonlyMessage> READONLY = AttributeKey.valueOf(ReadonlyMessage.COMMAND);
    public static final AttributeKey<ReadWriteMessage> READWRITE = AttributeKey.valueOf(ReadWriteMessage.COMMAND);
    public static final AttributeKey<RandomKeyMessage> RANDOMKEY = AttributeKey.valueOf(RandomKeyMessage.COMMAND);
    public static final AttributeKey<RenameMessage> RENAME = AttributeKey.valueOf(RenameMessage.COMMAND);
    public static final AttributeKey<RenameNXMessage> RENAMENX = AttributeKey.valueOf(RenameNXMessage.COMMAND);
    public static final AttributeKey<ScanMessage> SCAN = AttributeKey.valueOf(ScanMessage.COMMAND);
    public static final AttributeKey<TypeMessage> TYPE = AttributeKey.valueOf(TypeMessage.COMMAND);
    public static final AttributeKey<ClusterMessage> CLUSTER = AttributeKey.valueOf(ClusterMessage.COMMAND);
    public static final AttributeKey<ClientMessage> CLIENT = AttributeKey.valueOf(ClientMessage.COMMAND);
    public static final AttributeKey<AuthMessage> AUTH = AttributeKey.valueOf(AuthMessage.COMMAND);
    public static final AttributeKey<HelloMessage> HELLO = AttributeKey.valueOf(HelloMessage.COMMAND);
    public static final AttributeKey<PingMessage> PING = AttributeKey.valueOf(PingMessage.COMMAND);
    public static final AttributeKey<SelectMessage> SELECT = AttributeKey.valueOf(SelectMessage.COMMAND);
    public static final AttributeKey<CommandMessage> COMMAND = AttributeKey.valueOf(CommandMessage.COMMAND);
    public static final AttributeKey<FlushAllMessage> FLUSHALL = AttributeKey.valueOf(FlushAllMessage.COMMAND);
    public static final AttributeKey<FlushDBMessage> FLUSHDB = AttributeKey.valueOf(FlushDBMessage.COMMAND);
    public static final AttributeKey<DiscardMessage> DISCARD = AttributeKey.valueOf(DiscardMessage.COMMAND);
    public static final AttributeKey<ExecMessage> EXEC = AttributeKey.valueOf(ExecMessage.COMMAND);
    public static final AttributeKey<MultiMessage> MULTI = AttributeKey.valueOf(MultiMessage.COMMAND);
    public static final AttributeKey<UnwatchMessage> UNWATCH = AttributeKey.valueOf(UnwatchMessage.COMMAND);
    public static final AttributeKey<WatchMessage> WATCH = AttributeKey.valueOf(WatchMessage.COMMAND);
    public static final AttributeKey<InfoMessage> INFO = AttributeKey.valueOf(InfoMessage.COMMAND);
    public static final AttributeKey<BeginMessage> BEGIN = AttributeKey.valueOf(BeginMessage.COMMAND);
    public static final AttributeKey<CommitMessage> COMMIT = AttributeKey.valueOf(CommitMessage.COMMAND);
    public static final AttributeKey<GetApproximateSizeMessage> GETAPPROXIMATESIZE = AttributeKey.valueOf(GetApproximateSizeMessage.COMMAND);
    public static final AttributeKey<GetReadVersionMessage> GETREADVERSION = AttributeKey.valueOf(GetReadVersionMessage.COMMAND);
    public static final AttributeKey<NamespaceMessage> NAMESPACE = AttributeKey.valueOf(NamespaceMessage.COMMAND);
    public static final AttributeKey<RollbackMessage> ROLLBACK = AttributeKey.valueOf(RollbackMessage.COMMAND);
    public static final AttributeKey<SnapshotReadMessage> SNAPSHOTREAD = AttributeKey.valueOf(SnapshotReadMessage.COMMAND);
    public static final AttributeKey<ZDelMessage> ZDEL = AttributeKey.valueOf(ZDelMessage.COMMAND);
    public static final AttributeKey<ZDelPrefixMessage> ZDELPREFIX = AttributeKey.valueOf(ZDelPrefixMessage.COMMAND);
    public static final AttributeKey<ZDelRangeMessage> ZDELRANGE = AttributeKey.valueOf(ZDelRangeMessage.COMMAND);
    public static final AttributeKey<ZGetMessage> ZGET = AttributeKey.valueOf(ZGetMessage.COMMAND);
    public static final AttributeKey<ZGetKeyMessage> ZGETKEY = AttributeKey.valueOf(ZGetKeyMessage.COMMAND);
    public static final AttributeKey<ZGetRangeMessage> ZGETRANGE = AttributeKey.valueOf(ZGetRangeMessage.COMMAND);
    public static final AttributeKey<ZGetRangeSizeMessage> ZGETRANGESIZE = AttributeKey.valueOf(ZGetRangeSizeMessage.COMMAND);
    public static final AttributeKey<ZMutateMessage> ZMUTATE = AttributeKey.valueOf(ZMutateMessage.COMMAND);
    public static final AttributeKey<ZSetMessage> ZSET = AttributeKey.valueOf(ZSetMessage.COMMAND);
    public static final AttributeKey<HDelMessage> HDEL = AttributeKey.valueOf(HDelMessage.COMMAND);
    public static final AttributeKey<HExistsMessage> HEXISTS = AttributeKey.valueOf(HExistsMessage.COMMAND);
    public static final AttributeKey<HGetAllMessage> HGETALL = AttributeKey.valueOf(HGetAllMessage.COMMAND);
    public static final AttributeKey<HGetMessage> HGET = AttributeKey.valueOf(HGetMessage.COMMAND);
    public static final AttributeKey<HIncrByFloatMessage> HINCRBYFLOAT = AttributeKey.valueOf(HIncrByFloatMessage.COMMAND);
    public static final AttributeKey<HIncrByMessage> HINCRBY = AttributeKey.valueOf(HIncrByMessage.COMMAND);
    public static final AttributeKey<HKeysMessage> HKEYS = AttributeKey.valueOf(HKeysMessage.COMMAND);
    public static final AttributeKey<HSetMessage> HSET = AttributeKey.valueOf(HSetMessage.COMMAND);
    public static final AttributeKey<HSetNXMessage> HSETNX = AttributeKey.valueOf(HSetNXMessage.COMMAND);
    public static final AttributeKey<HLenMessage> HLEN = AttributeKey.valueOf(HLenMessage.COMMAND);
    public static final AttributeKey<HStrlenMessage> HSTRLEN = AttributeKey.valueOf(HStrlenMessage.COMMAND);
    public static final AttributeKey<HValsMessage> HVALS = AttributeKey.valueOf(HValsMessage.COMMAND);
    public static final AttributeKey<HRandFieldMessage> HRANDFIELD = AttributeKey.valueOf(HRandFieldMessage.COMMAND);
    public static final AttributeKey<HMGetMessage> HMGET = AttributeKey.valueOf(HMGetMessage.COMMAND);

    // Internal commands
    public static final AttributeKey<SegmentRangeMessage> SEGMENTRANGE = AttributeKey.valueOf(SegmentRangeMessage.COMMAND);
    public static final AttributeKey<KrAdminMessage> KRADMIN = AttributeKey.valueOf(KrAdminMessage.COMMAND);
    public static final AttributeKey<SegmentInsertMessage> SEGMENTINSERT = AttributeKey.valueOf(SegmentInsertMessage.COMMAND);
    public static final AttributeKey<VolumeAdminMessage> VOLUMEADMIN = AttributeKey.valueOf(VolumeAdminMessage.COMMAND);
    public static final AttributeKey<TaskAdminMessage> TASKADMIN = AttributeKey.valueOf(TaskAdminMessage.COMMAND);
}
