/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.redis.storage;

import com.kronotop.redis.handlers.string.StringValue;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ShardTest extends BaseStorageTest {
    protected RedisShard shard;

    @BeforeEach
    public void beforeEach() {
        shard = new OnHeapRedisShardImpl(context, 0);
    }

    @Test
    public void test_getIndex() {
        assertNotNull(shard.index());
    }

    @Test
    public void test_setReadOnly() {
        assertDoesNotThrow(() -> shard.setReadOnly(true));
    }

    @Test
    public void test_isReadOnly() {
        assertFalse(shard.isReadOnly());
        shard.setReadOnly(true);
        assertTrue(shard.isReadOnly());
    }

    @Test
    public void test_put() {
        shard.storage().put("foo", new RedisValueContainer(new StringValue("bar".getBytes())));
        shard.setReadOnly(true);
        ShardReadOnlyException expected = assertThrows(ShardReadOnlyException.class, () -> shard.storage().put("boo", new RedisValueContainer(new StringValue("foo".getBytes()))));
        assertNotNull(expected);
    }

    @Test
    public void test_remove() {
        shard.setReadOnly(true);
        ShardReadOnlyException expected = assertThrows(ShardReadOnlyException.class, () -> shard.storage().remove("boo"));
        assertNotNull(expected);
    }

    @Test
    public void test_remove_with_value() {
        shard.setReadOnly(true);
        ShardReadOnlyException expected = assertThrows(ShardReadOnlyException.class, () -> shard.storage().remove("boo", new StringValue("foo".getBytes())));
        assertNotNull(expected);
    }

    @Test
    public void test_compute() {
        shard.setReadOnly(true);
        ShardReadOnlyException expected = assertThrows(
                ShardReadOnlyException.class,
                () -> shard.storage().compute("boo", (key, value) -> value)
        );
        assertNotNull(expected);
    }

    @Test
    public void test_computeIfAbsent() {
        shard.setReadOnly(true);
        ShardReadOnlyException expected = assertThrows(
                ShardReadOnlyException.class,
                () -> shard.storage().computeIfAbsent("boo", (key) -> new RedisValueContainer(new StringValue("foo".getBytes())))
        );
        assertNotNull(expected);
    }
}
