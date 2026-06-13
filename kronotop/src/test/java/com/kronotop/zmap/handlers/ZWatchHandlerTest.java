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

package com.kronotop.zmap.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.ZWatcher;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ZWatchHandlerTest extends BaseHandlerTest {

    private ZWatcher zwatcher() {
        ZMapService service = instance.getContext().getService(ZMapService.NAME);
        return service.getZWatcher();
    }

    private void assertOK(Object response) {
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private void zset(EmbeddedChannel channel, String key, String value) {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zset(key, value).encode(buf);
        assertOK(runCommand(channel, buf));
    }

    private ByteBuf zwatchBuf(String key) {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zwatch(key).encode(buf);
        return buf;
    }

    @Test
    void shouldFireWhenKeyMutated() throws Exception {
        // Behavior: ZWATCH blocks until the watched key changes, then returns OK. The mutation
        // happens on a separate connection.
        zset(channel, "watched", "v1");

        EmbeddedChannel watcherChannel = newChannel();
        CompletableFuture<Object> watchFuture = CompletableFuture.supplyAsync(
                () -> runCommand(watcherChannel, zwatchBuf("watched")));

        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 1);

        // Mutate the key on the original connection.
        zset(channel, "watched", "v2");

        assertOK(watchFuture.get(5, TimeUnit.SECONDS));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 0);
    }

    @Test
    void shouldRejectZwatchInsideBegin() {
        // Behavior: ZWATCH must run on its own snapshot, so it cannot participate in an open user
        // transaction and is rejected inside BEGIN.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf begin = Unpooled.buffer();
        cmd.begin().encode(begin);
        assertOK(runCommand(channel, begin));

        Object response = runCommand(channel, zwatchBuf("k"));
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("ERR ZWATCH is not allowed within a transaction", ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldNotFireAcrossNamespaces() throws Exception {
        // Behavior: a watch only observes keys in the calling session's namespace. A mutation in
        // namespace A does not wake a ZWATCH in namespace B, even on the same raw key.
        String otherNamespace = UUID.randomUUID().toString();
        EmbeddedChannel watcherChannel = newChannel();

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf create = Unpooled.buffer();
        cmd.namespaceCreate(otherNamespace).encode(create);
        assertOK(runCommand(watcherChannel, create));
        ByteBuf use = Unpooled.buffer();
        cmd.namespaceUse(otherNamespace).encode(use);
        assertOK(runCommand(watcherChannel, use));

        CompletableFuture<Object> watchFuture = CompletableFuture.supplyAsync(
                () -> runCommand(watcherChannel, zwatchBuf("shared")));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 1);

        // Mutate the same raw key in the default namespace.
        zset(channel, "shared", "from-a");

        // The watch in namespace B must stay blocked.
        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse(watchFuture.isDone());

        // Mutating the key in namespace B wakes it, proving the watch was alive and namespace-scoped.
        // The mutation runs on a fresh connection that also uses namespace B.
        EmbeddedChannel mutator = newChannel();
        ByteBuf useB = Unpooled.buffer();
        cmd.namespaceUse(otherNamespace).encode(useB);
        assertOK(runCommand(mutator, useB));
        zset(mutator, "shared", "from-b");

        assertOK(watchFuture.get(5, TimeUnit.SECONDS));
    }

    @Test
    void shouldKeepOtherWaiterWhenOneDisconnects() throws Exception {
        // Behavior: two waiters share one underlying watch. One disconnecting removes only its own
        // demand and leaves the other waiter's watch intact.
        EmbeddedChannel firstWaiter = newChannel();
        EmbeddedChannel secondWaiter = newChannel();

        // First waiter registers the shared watch.
        firstWaiter.writeInbound(zwatchBuf("shared"));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().totalWaiters() == 1);

        // Second waiter joins the same key.
        CompletableFuture<Object> secondFuture = CompletableFuture.supplyAsync(
                () -> runCommand(secondWaiter, zwatchBuf("shared")));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().totalWaiters() == 2);
        assertEquals(1, zwatcher().watcherCount());

        // First waiter disconnects.
        firstWaiter.close();
        firstWaiter.runPendingTasks();
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().totalWaiters() == 1);
        assertEquals(1, zwatcher().watcherCount(), "underlying watch must remain for the other waiter");

        // Mutate the key on a fresh connection: the surviving waiter must fire.
        zset(channel, "shared", "v1");
        assertOK(secondFuture.get(5, TimeUnit.SECONDS));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 0);
    }

    @Test
    void shouldWakeAllWaitersAndRearmAfterFire() throws Exception {
        // Behavior: multiple waiters sharing one underlying watch all wake on a single mutation. After
        // the watch fires and drains, a fresh registration on the same key arms a new watch and fires
        // again, proving no stale state is left behind.
        zset(channel, "k", "v0");

        EmbeddedChannel firstWaiter = newChannel();
        EmbeddedChannel secondWaiter = newChannel();
        CompletableFuture<Object> firstFuture = CompletableFuture.supplyAsync(
                () -> runCommand(firstWaiter, zwatchBuf("k")));
        CompletableFuture<Object> secondFuture = CompletableFuture.supplyAsync(
                () -> runCommand(secondWaiter, zwatchBuf("k")));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().totalWaiters() == 2);
        assertEquals(1, zwatcher().watcherCount());

        zset(channel, "k", "v1");
        assertOK(firstFuture.get(5, TimeUnit.SECONDS));
        assertOK(secondFuture.get(5, TimeUnit.SECONDS));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 0);

        // Re-register on the same key: a fresh watch is installed and fires on the next mutation.
        EmbeddedChannel thirdWaiter = newChannel();
        CompletableFuture<Object> thirdFuture = CompletableFuture.supplyAsync(
                () -> runCommand(thirdWaiter, zwatchBuf("k")));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 1);
        zset(channel, "k", "v2");
        assertOK(thirdFuture.get(5, TimeUnit.SECONDS));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 0);
    }

    @Test
    void shouldShareSingleWatchAcrossConcurrentRegistrations() throws Exception {
        // Behavior: many clients registering the same new key concurrently share exactly one underlying
        // watch; the spare watches armed during the race are discarded. A single mutation wakes all.
        zset(channel, "k", "v0");

        int waiterCount = 5;
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (int i = 0; i < waiterCount; i++) {
            EmbeddedChannel waiter = newChannel();
            futures.add(CompletableFuture.supplyAsync(() -> runCommand(waiter, zwatchBuf("k"))));
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().totalWaiters() == waiterCount);
        assertEquals(1, zwatcher().watcherCount(), "concurrent registrations must share one watch");

        zset(channel, "k", "v1");
        for (CompletableFuture<Object> future : futures) {
            assertOK(future.get(5, TimeUnit.SECONDS));
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 0);
    }

    @Test
    void shouldReleaseWatchedKeyWhenConnectionDisconnects() {
        // Behavior: ZWATCH blocks the connection, so a session holds at most one watched key. When the
        // connection disconnects while blocked, that key is released: the underlying watch is cancelled
        // and the waiter removed.
        EmbeddedChannel waiter = newChannel();
        waiter.writeInbound(zwatchBuf("k1"));

        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 1);
        assertEquals(1, zwatcher().totalWaiters());

        waiter.close();
        waiter.runPendingTasks();

        // The underlying watch must be cancelled and the waiter released.
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().watcherCount() == 0);
        assertEquals(0, zwatcher().totalWaiters());
    }

    @Test
    void shouldUnblockWaitersOnShutdown() {
        // Behavior: shutting the watcher down cancels every underlying watch and unblocks every waiter,
        // draining the watch and waiter counts to zero and releasing the blocked client.
        EmbeddedChannel waiter = newChannel();
        waiter.writeInbound(zwatchBuf("k"));
        await().atMost(Duration.ofSeconds(5)).until(() -> zwatcher().totalWaiters() == 1);

        zwatcher().shutdown();

        assertEquals(0, zwatcher().watcherCount());
        assertEquals(0, zwatcher().totalWaiters());

        // The blocked client is released with an error reply.
        Object response = await().atMost(Duration.ofSeconds(5)).until(() -> {
            waiter.runPendingTasks();
            return waiter.readOutbound();
        }, Objects::nonNull);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }
}
