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

import com.kronotop.bucket.handlers.protocol.*;
import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.core.handlers.client.protocol.ClientMessage;
import com.kronotop.core.handlers.connection.protocol.AuthMessage;
import com.kronotop.core.handlers.connection.protocol.EchoMessage;
import com.kronotop.core.handlers.connection.protocol.HelloMessage;
import com.kronotop.core.handlers.connection.protocol.PingMessage;
import com.kronotop.core.handlers.protocol.InfoMessage;
import com.kronotop.core.handlers.server.protocol.CommandMessage;
import com.kronotop.core.handlers.server.protocol.TimeMessage;
import com.kronotop.core.handlers.session.protocol.SessionAttributeMessage;
import com.kronotop.core.handlers.session.protocol.SessionCloseMessage;
import com.kronotop.core.handlers.transaction.protocol.*;
import com.kronotop.namespace.handlers.protocol.NamespaceMessage;
import com.kronotop.task.handlers.protocol.TaskAdminMessage;
import com.kronotop.volume.handlers.protocol.*;
import com.kronotop.zmap.handlers.protocol.*;
import io.netty.util.AttributeKey;

/**
 * The MessageTypes class contains constants representing various message types used in a system.
 * Each constant represents a specific message type and has an associated AttributeKey.
 */
public class MessageTypes {
    // External commands
    public static final AttributeKey<ClientMessage> CLIENT = AttributeKey.valueOf(ClientMessage.COMMAND);
    public static final AttributeKey<AuthMessage> AUTH = AttributeKey.valueOf(AuthMessage.COMMAND);
    public static final AttributeKey<HelloMessage> HELLO = AttributeKey.valueOf(HelloMessage.COMMAND);
    public static final AttributeKey<PingMessage> PING = AttributeKey.valueOf(PingMessage.COMMAND);
    public static final AttributeKey<EchoMessage> ECHO = AttributeKey.valueOf(EchoMessage.COMMAND);
    public static final AttributeKey<CommandMessage> COMMAND = AttributeKey.valueOf(CommandMessage.COMMAND);
    public static final AttributeKey<TimeMessage> TIME = AttributeKey.valueOf(TimeMessage.COMMAND);
    public static final AttributeKey<InfoMessage> INFO = AttributeKey.valueOf(InfoMessage.COMMAND);

    // Transaction management commands
    public static final AttributeKey<BeginMessage> BEGIN = AttributeKey.valueOf(BeginMessage.COMMAND);
    public static final AttributeKey<CommitMessage> COMMIT = AttributeKey.valueOf(CommitMessage.COMMAND);
    public static final AttributeKey<GetApproximateSizeMessage> GETAPPROXIMATESIZE = AttributeKey.valueOf(GetApproximateSizeMessage.COMMAND);
    public static final AttributeKey<GetReadVersionMessage> GETREADVERSION = AttributeKey.valueOf(GetReadVersionMessage.COMMAND);
    public static final AttributeKey<RollbackMessage> ROLLBACK = AttributeKey.valueOf(RollbackMessage.COMMAND);
    public static final AttributeKey<SnapshotReadMessage> SNAPSHOTREAD = AttributeKey.valueOf(SnapshotReadMessage.COMMAND);
    public static final AttributeKey<TickMessage> TICK = AttributeKey.valueOf(TickMessage.COMMAND);
    public static final AttributeKey<TransactionMessage> TRANSACTION = AttributeKey.valueOf(TransactionMessage.COMMAND);

    // Namespace commands
    public static final AttributeKey<NamespaceMessage> NAMESPACE = AttributeKey.valueOf(NamespaceMessage.COMMAND);

    // ZMap commands
    public static final AttributeKey<ZDelMessage> ZDEL = AttributeKey.valueOf(ZDelMessage.COMMAND);
    public static final AttributeKey<ZDelRangeMessage> ZDELRANGE = AttributeKey.valueOf(ZDelRangeMessage.COMMAND);
    public static final AttributeKey<ZGetMessage> ZGET = AttributeKey.valueOf(ZGetMessage.COMMAND);
    public static final AttributeKey<ZGetKeyMessage> ZGETKEY = AttributeKey.valueOf(ZGetKeyMessage.COMMAND);
    public static final AttributeKey<ZWatchMessage> ZWATCH = AttributeKey.valueOf(ZWatchMessage.COMMAND);
    public static final AttributeKey<ZGetRangeMessage> ZGETRANGE = AttributeKey.valueOf(ZGetRangeMessage.COMMAND);
    public static final AttributeKey<ZGetRangeSizeMessage> ZGETRANGESIZE = AttributeKey.valueOf(ZGetRangeSizeMessage.COMMAND);
    public static final AttributeKey<ZMutateMessage> ZMUTATE = AttributeKey.valueOf(ZMutateMessage.COMMAND);
    public static final AttributeKey<ZSetMessage> ZSET = AttributeKey.valueOf(ZSetMessage.COMMAND);
    public static final AttributeKey<ZIncI64Message> ZINCI64 = AttributeKey.valueOf(ZIncI64Message.COMMAND);
    public static final AttributeKey<ZGetI64Message> ZGETI64 = AttributeKey.valueOf(ZGetI64Message.COMMAND);
    public static final AttributeKey<ZIncF64Message> ZINCF64 = AttributeKey.valueOf(ZIncF64Message.COMMAND);
    public static final AttributeKey<ZGetF64Message> ZGETF64 = AttributeKey.valueOf(ZGetF64Message.COMMAND);
    public static final AttributeKey<ZIncD128Message> ZINCD128 = AttributeKey.valueOf(ZIncD128Message.COMMAND);
    public static final AttributeKey<ZGetD128Message> ZGETD128 = AttributeKey.valueOf(ZGetD128Message.COMMAND);
    public static final AttributeKey<ZSetI64Message> ZSETI64 = AttributeKey.valueOf(ZSetI64Message.COMMAND);
    public static final AttributeKey<ZSetF64Message> ZSETF64 = AttributeKey.valueOf(ZSetF64Message.COMMAND);
    public static final AttributeKey<ZSetD128Message> ZSETD128 = AttributeKey.valueOf(ZSetD128Message.COMMAND);

    // Internal commands
    public static final AttributeKey<SegmentInsertMessage> SEGMENTINSERT = AttributeKey.valueOf(SegmentInsertMessage.COMMAND);
    public static final AttributeKey<SegmentRangeMessage> SEGMENTRANGE = AttributeKey.valueOf(SegmentRangeMessage.COMMAND);
    public static final AttributeKey<SegmentTailPointerMessage> SEGMENTTAILPOINTER = AttributeKey.valueOf(SegmentTailPointerMessage.COMMAND);
    public static final AttributeKey<ChangeLogWatchMessage> CHANGELOGWATCH = AttributeKey.valueOf(ChangeLogWatchMessage.COMMAND);
    public static final AttributeKey<ChangeLogRangeMessage> CHANGELOGRANGE = AttributeKey.valueOf(ChangeLogRangeMessage.COMMAND);
    public static final AttributeKey<KrAdminMessage> KRADMIN = AttributeKey.valueOf(KrAdminMessage.COMMAND);
    public static final AttributeKey<VolumeAdminMessage> VOLUMEADMIN = AttributeKey.valueOf(VolumeAdminMessage.COMMAND);
    public static final AttributeKey<VolumeInspectMessage> VOLUMEINSPECT = AttributeKey.valueOf(VolumeInspectMessage.COMMAND);
    public static final AttributeKey<VolumeStatsMessage> VOLUMESTATS = AttributeKey.valueOf(VolumeStatsMessage.COMMAND);
    public static final AttributeKey<TaskAdminMessage> TASKADMIN = AttributeKey.valueOf(TaskAdminMessage.COMMAND);

    // Buckets
    public static final AttributeKey<BucketCreateMessage> BUCKETCREATE = AttributeKey.valueOf(BucketCreateMessage.COMMAND);
    public static final AttributeKey<BucketInsertMessage> BUCKETINSERT = AttributeKey.valueOf(BucketInsertMessage.COMMAND);
    public static final AttributeKey<BucketQueryMessage> BUCKETQUERY = AttributeKey.valueOf(BucketQueryMessage.COMMAND);
    public static final AttributeKey<BucketExplainMessage> BUCKETEXPLAIN = AttributeKey.valueOf(BucketExplainMessage.COMMAND);
    public static final AttributeKey<BucketAdvanceMessage> BUCKETADVANCE = AttributeKey.valueOf(BucketAdvanceMessage.COMMAND);
    public static final AttributeKey<BucketDeleteMessage> BUCKETDELETE = AttributeKey.valueOf(BucketDeleteMessage.COMMAND);
    public static final AttributeKey<BucketUpdateMessage> BUCKETUPDATE = AttributeKey.valueOf(BucketUpdateMessage.COMMAND);
    public static final AttributeKey<BucketCloseMessage> BUCKETCLOSE = AttributeKey.valueOf(BucketCloseMessage.COMMAND);
    public static final AttributeKey<BucketCursorsMessage> BUCKETCURSORS = AttributeKey.valueOf(BucketCursorsMessage.COMMAND);
    public static final AttributeKey<BucketIndexMessage> BUCKETINDEX = AttributeKey.valueOf(BucketIndexMessage.COMMAND);
    public static final AttributeKey<BucketRemoveMessage> BUCKETREMOVE = AttributeKey.valueOf(BucketRemoveMessage.COMMAND);
    public static final AttributeKey<BucketPurgeMessage> BUCKETPURGE = AttributeKey.valueOf(BucketPurgeMessage.COMMAND);
    public static final AttributeKey<BucketLocateMessage> BUCKETLOCATE = AttributeKey.valueOf(BucketLocateMessage.COMMAND);
    public static final AttributeKey<BucketListMessage> BUCKETLIST = AttributeKey.valueOf(BucketListMessage.COMMAND);
    public static final AttributeKey<BucketVectorMessage> BUCKETVECTOR = AttributeKey.valueOf(BucketVectorMessage.COMMAND);

    // Session management
    public static final AttributeKey<SessionAttributeMessage> SESSIONATTRIBUTE = AttributeKey.valueOf(SessionAttributeMessage.COMMAND);
    public static final AttributeKey<SessionCloseMessage> SESSIONCLOSE = AttributeKey.valueOf(SessionCloseMessage.COMMAND);
}
