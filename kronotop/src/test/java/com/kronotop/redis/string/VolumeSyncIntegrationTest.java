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

package com.kronotop.redis.string;

import com.apple.foundationdb.Transaction;
import com.kronotop.redis.BaseVolumeSyncIntegrationTest;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.StringPack;
import com.kronotop.redistest.RedisCommandBuilder;
import com.kronotop.volume.KeyEntry;
import com.kronotop.volume.Session;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class VolumeSyncIntegrationTest extends BaseVolumeSyncIntegrationTest {

    @Test
    public void test_SET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.set(key, value).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                return syncedValue.equals(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_APPEND() {
        {
            RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
            {
                ByteBuf buf = Unpooled.buffer();
                cmd.append(key, "one").encode(buf);
                channel.writeInbound(buf);
            }
            {
                ByteBuf buf = Unpooled.buffer();
                cmd.append(key, "two").encode(buf);
                channel.writeInbound(buf);
            }
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                return syncedValue.equals("onetwo");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_DECRBY() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.decrby(key, 10).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                Integer result = Integer.parseInt(syncedValue);
                return result.equals(-10);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_DECR() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.decr(key).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                Integer result = Integer.parseInt(syncedValue);
                return result.equals(-1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_INCRBY() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.incrby(key, 10).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                Integer result = Integer.parseInt(syncedValue);
                return result.equals(10);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_INCR() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.incr(key).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                Integer result = Integer.parseInt(syncedValue);
                return result.equals(1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_INCRBYFLOAT() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.incrbyfloat(key, 1.1).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                Double result = Double.parseDouble(syncedValue);
                return result.equals(1.1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_GETDEL() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set(key, value);
            channel.writeInbound(buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.getdel(key).encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> !volumeContainsStringKey(key));
    }

    @Test
    public void test_GETSET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set(key, value).encode(buf);
            channel.writeInbound(buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.getset(key, "value-after-getset").encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                return syncedValue.equals("value-after-getset");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_MSET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        HashMap<String, String> pairs = getKeyValuePairs();
        cmd.mset(pairs).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            RedisService service = kronotopInstance.getContext().getService(RedisService.NAME);
            RedisShard shard = service.findShard(key);
            try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
                Session session = new Session(tr);
                Iterable<KeyEntry> iterable = shard.volume().getRange(session);

                HashMap<String, String> result = new HashMap<>();
                for (KeyEntry entry : iterable) {
                    StringPack pack = StringPack.unpack(entry.entry());
                    result.put(pack.key(), new String(pack.stringValue().value()));
                }

                return pairs.entrySet().stream().allMatch(e -> e.getValue().equals(result.get(e.getKey())));
            }
        });
    }

    @Test
    public void test_MSETNX() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        HashMap<String, String> pairs = getKeyValuePairs();
        cmd.msetnx(pairs).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            RedisService service = kronotopInstance.getContext().getService(RedisService.NAME);
            RedisShard shard = service.findShard(key);
            try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
                Session session = new Session(tr);
                Iterable<KeyEntry> iterable = shard.volume().getRange(session);

                HashMap<String, String> result = new HashMap<>();
                for (KeyEntry entry : iterable) {
                    StringPack pack = StringPack.unpack(entry.entry());
                    result.put(pack.key(), new String(pack.stringValue().value()));
                }

                return pairs.entrySet().stream().allMatch(e -> e.getValue().equals(result.get(e.getKey())));
            }
        });
    }


    @Test
    public void test_SETNX() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();

        cmd.setnx(key, value).encode(buf);
        channel.writeInbound(buf);

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                return syncedValue.equals(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    @Test
    public void test_SETRANGE() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.set(key, "Hello World!").encode(buf);
            channel.writeInbound(buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.setrange(key, 6, "Kronotop").encode(buf);
            channel.writeInbound(buf);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> checkOnVolume(key, keyEntry -> {
            try {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                if (!pack.key().equals(key)) {
                    return false;
                }
                String syncedValue = new String(pack.stringValue().value());
                return syncedValue.equals("Hello Kronotop");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }
}
