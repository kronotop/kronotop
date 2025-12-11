/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.replication;

import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationWatchDogTest extends BaseNetworkedVolumeIntegrationTest {

    private void setStandbyMember(KronotopInstance standby) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", "STANDBY", SHARD_KIND.name(), SHARD_ID, standby.getMember().getId()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());

        RoutingService routing = standby.getContext().getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Route route = routing.findRoute(SHARD_KIND, SHARD_ID);
            if (route == null) {
                return false;
            }
            return route.standbys().contains(standby.getMember());
        });
    }

    @Test
    void shouldStopAfterMaxRetries() throws InterruptedException {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Stop the automatically started replication first
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);

        MockReplicationTask mockTask = new MockReplicationTask();
        mockTask.setAlwaysFail(true, new RuntimeException("Simulated failure"));

        ReplicationService.ReplicationWatchDog watchDog = replicationService.new ReplicationWatchDog(
                SHARD_KIND,
                SHARD_ID,
                mockTask
        );
        replicationService.registerReplicationWatchDog(SHARD_KIND, SHARD_ID, watchDog);

        CountDownLatch latch = new CountDownLatch(1);
        Thread watchDogThread = new Thread(() -> {
            watchDog.run();
            latch.countDown();
        });
        watchDogThread.start();

        // Wait for watchdog to finish (should stop after MAX_RETRIES=3)
        boolean finished = latch.await(30, TimeUnit.SECONDS);
        assertTrue(finished, "WatchDog should have stopped after max retries");

        // Verify start() was called exactly 4 times (retry counter reaches MAX_RETRIES on 3rd failure)
        assertEquals(4, mockTask.getStartCallCount());

        // Verify shutdown() was called via stopReplication -> watchdog.stop()
        assertEquals(1, mockTask.getShutdownCallCount());

        // Verify watchdog is not running
        assertFalse(watchDog.isRunning());
    }

    @Test
    void shouldResetRetryCounterAfterLongRunningAttempt() throws InterruptedException {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Stop the automatically started replication first
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);

        AtomicInteger callCount = new AtomicInteger(0);

        ReplicationTask mockTask = new ReplicationTask() {
            @Override
            public void start() {
                int call = callCount.incrementAndGet();
                // On call 3, sleep longer than reset_threshold (1 second) to reset retry counter
                if (call == 3) {
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                throw new RuntimeException("Simulated failure");
            }

            @Override
            public void reconnect() {
            }

            @Override
            public void shutdown() {
            }
        };

        ReplicationService.ReplicationWatchDog watchDog = replicationService.new ReplicationWatchDog(
                SHARD_KIND,
                SHARD_ID,
                mockTask
        );
        replicationService.registerReplicationWatchDog(SHARD_KIND, SHARD_ID, watchDog);

        CountDownLatch latch = new CountDownLatch(1);
        Thread watchDogThread = new Thread(() -> {
            watchDog.run();
            latch.countDown();
        });
        watchDogThread.start();

        // Wait for watchdog to finish
        boolean finished = latch.await(60, TimeUnit.SECONDS);
        assertTrue(finished, "WatchDog should have stopped after max retries");

        // With MAX_RETRIES=3:
        // Calls 1,2: rapid failures, retry=1,2
        // Call 3: long running (>1s), retry resets to 0
        // Calls 4,5,6,7: rapid failures, retry=1,2,3,4 -> stops when retry > 3
        // Total: 7 calls
        assertEquals(7, callCount.get());
    }

    @Test
    void shouldExitSuccessfullyWhenStartCompletes() throws InterruptedException {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Stop the automatically started replication first
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);

        AtomicInteger callCount = new AtomicInteger(0);

        ReplicationTask mockTask = new ReplicationTask() {
            @Override
            public void start() {
                callCount.incrementAndGet();
                // Completes successfully without throwing
            }

            @Override
            public void reconnect() {
            }

            @Override
            public void shutdown() {
            }
        };

        ReplicationService.ReplicationWatchDog watchDog = replicationService.new ReplicationWatchDog(
                SHARD_KIND,
                SHARD_ID,
                mockTask
        );
        replicationService.registerReplicationWatchDog(SHARD_KIND, SHARD_ID, watchDog);

        CountDownLatch latch = new CountDownLatch(1);
        Thread watchDogThread = new Thread(() -> {
            watchDog.run();
            latch.countDown();
        });
        watchDogThread.start();

        // Wait for watchdog to finish
        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "WatchDog should have exited after successful start");

        // Verify start() was called exactly once
        assertEquals(1, callCount.get());

        // Verify watchdog is not running after successful completion
        assertFalse(watchDog.isRunning());
    }

    @Test
    void shouldStopImmediatelyWhenStoppedFlagSet() throws InterruptedException {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Stop the automatically started replication first
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);

        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch startedLatch = new CountDownLatch(1);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        ReplicationTask mockTask = new ReplicationTask() {
            @Override
            public void start() {
                callCount.incrementAndGet();
                startedLatch.countDown();
                try {
                    proceedLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException("Simulated failure");
            }

            @Override
            public void reconnect() {
            }

            @Override
            public void shutdown() {
            }
        };

        ReplicationService.ReplicationWatchDog watchDog = replicationService.new ReplicationWatchDog(
                SHARD_KIND,
                SHARD_ID,
                mockTask
        );
        replicationService.registerReplicationWatchDog(SHARD_KIND, SHARD_ID, watchDog);

        CountDownLatch latch = new CountDownLatch(1);
        Thread watchDogThread = new Thread(() -> {
            watchDog.run();
            latch.countDown();
        });
        watchDogThread.start();

        // Wait for start() to be called
        assertTrue(startedLatch.await(5, TimeUnit.SECONDS));

        // Stop the watchdog while it's running
        watchDog.stop();

        // Allow start() to complete
        proceedLatch.countDown();

        // Wait for watchdog to finish
        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "WatchDog should have stopped immediately");

        // Verify start() was called only once (no retries after stop)
        assertEquals(1, callCount.get());
    }

    @Test
    void shouldCallShutdownWhenStoppedAfterMaxRetries() throws InterruptedException {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Stop the automatically started replication first
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);

        AtomicInteger shutdownCount = new AtomicInteger(0);

        ReplicationTask mockTask = new ReplicationTask() {
            @Override
            public void start() {
                throw new RuntimeException("Simulated failure");
            }

            @Override
            public void reconnect() {
            }

            @Override
            public void shutdown() {
                shutdownCount.incrementAndGet();
            }
        };

        ReplicationService.ReplicationWatchDog watchDog = replicationService.new ReplicationWatchDog(
                SHARD_KIND,
                SHARD_ID,
                mockTask
        );
        replicationService.registerReplicationWatchDog(SHARD_KIND, SHARD_ID, watchDog);

        CountDownLatch latch = new CountDownLatch(1);
        Thread watchDogThread = new Thread(() -> {
            watchDog.run();
            latch.countDown();
        });
        watchDogThread.start();

        // Wait for watchdog to finish after max retries
        boolean finished = latch.await(30, TimeUnit.SECONDS);
        assertTrue(finished, "WatchDog should have stopped after max retries");

        // Verify shutdown() was called exactly once
        assertEquals(1, shutdownCount.get());
    }

    @Test
    void shouldNotIncrementRetryOnSuccessfulStart() throws InterruptedException {
        KronotopInstance standby = addNewInstance();
        setStandbyMember(standby);

        ReplicationService replicationService = standby.getContext().getService(ReplicationService.NAME);

        // Stop the automatically started replication first
        replicationService.stopReplication(SHARD_KIND, SHARD_ID, false);

        AtomicInteger callCount = new AtomicInteger(0);
        AtomicInteger shutdownCount = new AtomicInteger(0);

        ReplicationTask mockTask = new ReplicationTask() {
            @Override
            public void start() {
                callCount.incrementAndGet();
                // Completes successfully without throwing
            }

            @Override
            public void reconnect() {
            }

            @Override
            public void shutdown() {
                shutdownCount.incrementAndGet();
            }
        };

        ReplicationService.ReplicationWatchDog watchDog = replicationService.new ReplicationWatchDog(
                SHARD_KIND,
                SHARD_ID,
                mockTask
        );
        replicationService.registerReplicationWatchDog(SHARD_KIND, SHARD_ID, watchDog);

        CountDownLatch latch = new CountDownLatch(1);
        Thread watchDogThread = new Thread(() -> {
            watchDog.run();
            latch.countDown();
        });
        watchDogThread.start();

        // Wait for watchdog to finish
        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertTrue(finished, "WatchDog should have exited after successful start");

        // Verify start() was called exactly once (no retry logic triggered)
        assertEquals(1, callCount.get());

        // Verify shutdown() was never called (clean exit, not failure)
        assertEquals(0, shutdownCount.get());
    }
}
