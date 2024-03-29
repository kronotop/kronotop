/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.kronotop.server.resp3;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.UnstableApi;

import java.util.*;

/**
 * Aggregates {@link RedisMessage} parts into {@link ArrayRedisMessage} or {@link SetRedisMessage}.
 * This decoder should be used together with {@link RedisDecoder}.
 */
@UnstableApi
public final class RedisArrayAggregator extends MessageToMessageDecoder<RedisMessage> {

    private final Deque<AggregateState> depths = new ArrayDeque<AggregateState>(4);

    @Override
    protected void decode(ChannelHandlerContext ctx, RedisMessage msg, List<Object> out) throws Exception {
        // only decode Array and Set types
        if (msg instanceof ArrayHeaderRedisMessage
                || msg instanceof SetHeaderRedisMessage
                || msg instanceof PushHeaderRedisMessage) {
            msg = decodeRedisCollectionHeader((AggregatedHeaderRedisMessage) msg);
            if (msg == null) {
                return;
            }
        } else {
            ReferenceCountUtil.retain(msg);
        }

        while (!depths.isEmpty()) {
            AggregateState current = depths.peek();
            current.children.add(msg);

            // if current aggregation completed, go to parent aggregation.
            if (current.children.size() == current.length) {
                if (RedisMessageType.ARRAY_HEADER.equals(current.aggregateType)) {
                    msg = new ArrayRedisMessage((List<RedisMessage>) current.children);
                } else if (RedisMessageType.SET_HEADER.equals(current.aggregateType)) {
                    msg = new SetRedisMessage((Set<RedisMessage>) current.children);
                } else if (RedisMessageType.PUSH.equals(current.aggregateType)) {
                    msg = new PushRedisMessage((List<RedisMessage>) current.children);
                }
                depths.pop();
            } else {
                // not aggregated yet. try next time.
                return;
            }
        }

        out.add(msg);
    }

    private RedisMessage decodeRedisCollectionHeader(AggregatedHeaderRedisMessage header) {
        // todo use NullRedisMessage replacing *-1 and $-1 ?
        if (header.isNull()) {
            return ArrayRedisMessage.NULL_INSTANCE;
        } else if (header.length() == 0L) {
            return (header instanceof SetHeaderRedisMessage) ?
                    SetRedisMessage.EMPTY_INSTANCE : ArrayRedisMessage.EMPTY_INSTANCE;
        } else if (header.length() > 0L) {
            // Currently, this codec doesn't support `long` length for arrays because Java's List.size() is int.
            if (header.length() > Integer.MAX_VALUE) {
                throw new CodecException("this codec doesn't support longer length than " + Integer.MAX_VALUE);
            }

            // start aggregating array or set according header type
            depths.push(new AggregateState(header, (int) header.length()));
            return null;
        } else {
            throw new CodecException("bad length: " + header.length());
        }
    }

    private static final class AggregateState {
        private final int length;
        private final Collection<RedisMessage> children;
        private final RedisMessageType aggregateType;

        AggregateState(AggregatedHeaderRedisMessage headerType, int length) {
            this.length = length;
            if (headerType instanceof ArrayHeaderRedisMessage) {
                this.children = new ArrayList<RedisMessage>(length);
                this.aggregateType = RedisMessageType.ARRAY_HEADER;
            } else if (headerType instanceof SetHeaderRedisMessage) {
                this.children = new HashSet<RedisMessage>(length);
                this.aggregateType = RedisMessageType.SET_HEADER;
            } else if (headerType instanceof PushHeaderRedisMessage) {
                this.children = new ArrayList<RedisMessage>(length);
                this.aggregateType = RedisMessageType.PUSH;
            } else {
                // never going to run here
                throw new CodecException("bad header type: " + headerType);
            }
        }
    }
}
