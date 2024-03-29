/*
 * Copyright 2021 The Netty Project
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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.UnstableApi;

import java.util.Collection;

/**
 * Abstract class for Aggregate data types message.
 */
@UnstableApi
public abstract class AbstractCollectionRedisMessage extends AbstractReferenceCounted
        implements AggregatedRedisMessage {

    protected final Collection<RedisMessage> children;

    protected AbstractCollectionRedisMessage(Collection<RedisMessage> children) {
        this.children = ObjectUtil.checkNotNull(children, "children");
    }

    protected abstract Collection<RedisMessage> children();

    @Override
    protected void deallocate() {
        for (RedisMessage msg : children) {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public AbstractCollectionRedisMessage touch(Object hint) {
        for (RedisMessage msg : children) {
            ReferenceCountUtil.touch(msg);
        }
        return this;
    }

    /**
     * Returns whether the content of this message is {@code null}.
     *
     * @return indicates whether the content of this message is {@code null}.
     */
    public boolean isNull() {
        return false;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) +
                '[' +
                "children=" +
                children.size() +
                ']';
    }
}
