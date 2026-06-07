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

package com.kronotop.recipes.lock;

import com.kronotop.recipes.lock.DistributedLock.LockHandle;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

/**
 * Runs the distributed lock against a live Kronotop server and shows two things the recipe
 * cares about: waiting is the caller's choice (poll = loop {@code tryAcquire}), and the
 * fencing token is what keeps a paused holder from acting on the protected resource.
 *
 * <p>Usage: {@code Demo [host] [port]} (defaults to localhost:5484). Start a Kronotop server
 * and initialize the cluster first.
 */
public final class Demo {

    private static final String LOCK_NAME = "orders";

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : envOr("KRONOTOP_HOST", "localhost");
        int port = args.length > 1 ? Integer.parseInt(args[1]) : Integer.parseInt(envOr("KRONOTOP_PORT", "5484"));

        FencedResource resource = new FencedResource();
        contentionAndPolling(host, port, resource);
        fencingProtectsAPausedHolder(host, port, resource);
        renewHoldsLockAcrossLongTask(host, port);
    }

    /**
     * Client A holds the lock for a moment; client B's {@code tryAcquire} returns null while it is
     * held, so B polls (loops tryAcquire with a short randomized delay) until A releases. This is the
     * whole "no watch primitive" story: the wait lives at the call site, not inside the lock.
     */
    private static void contentionAndPolling(String host, int port, FencedResource resource)
            throws InterruptedException {
        System.out.println("== Part 1: contention and caller-side polling ==");

        try (ZMap connA = new ZMap(host, port); ZMap connB = new ZMap(host, port)) {
            DistributedLock lockA = new DistributedLock(connA, LOCK_NAME);
            DistributedLock lockB = new DistributedLock(connB, LOCK_NAME);

            LockHandle a = lockA.tryAcquire(5_000);
            assert a != null;
            System.out.println("A: acquired " + describe(a));
            resource.write(a.fencingToken(), "A writes while holding the lock");

            Thread b = Thread.ofPlatform().start(() -> {
                Random random = new Random();
                LockHandle held = lockB.tryAcquire(5_000);
                int attempts = 1;
                while (held == null) {
                    // Polling is just looping the single-shot primitive. A real client would also
                    // give up once a wait window elapses; here we poll until A releases.
                    sleep(50 + random.nextInt(50));
                    held = lockB.tryAcquire(5_000);
                    attempts++;
                }
                System.out.println("B: acquired after " + attempts + " attempts " + describe(held));
                resource.write(held.fencingToken(), "B writes after acquiring");
                lockB.release(held);
                System.out.println("B: released");
            });

            sleep(300); // let B poll and observe the held lock a few times
            lockA.release(a);
            System.out.println("A: released");
            b.join();
        }
        System.out.println();
    }

    /**
     * Client A takes a short lease and then pauses past it without releasing. Client B sees the lease
     * expired, acquires, and writes with a higher fencing token. When A wakes up still believing it
     * holds the lock and writes, the resource rejects it because A's token is now lower than the
     * highest it has seen. Without the fencing check both A and B would act as the holder.
     */
    private static void fencingProtectsAPausedHolder(String host, int port, FencedResource resource) {
        System.out.println("== Part 2: fencing token protects a paused holder ==");

        try (ZMap connA = new ZMap(host, port); ZMap connB = new ZMap(host, port)) {
            DistributedLock lockA = new DistributedLock(connA, LOCK_NAME);
            DistributedLock lockB = new DistributedLock(connB, LOCK_NAME);

            LockHandle a = lockA.tryAcquire(500); // short lease
            assert a != null;
            System.out.println("A: acquired with a 500ms lease " + describe(a));

            sleep(700); // A pauses past its lease without releasing

            LockHandle b = lockB.tryAcquire(5_000);
            if (b == null) {
                throw new IllegalStateException("expected the expired lease to be acquirable");
            }
            System.out.println("B: acquired the expired lock " + describe(b));
            resource.write(b.fencingToken(), "B writes as the new holder");

            // A wakes up, still believing it holds the lock.
            System.out.println("A: wakes up and tries to write with its stale fencing token");
            resource.write(a.fencingToken(), "A writes as a stale holder");

            lockB.release(b);
        }
        System.out.println();
    }

    /**
     * Client A holds the lock across a task longer than its lease by renewing on a short interval.
     * While A keeps renewing, client B stays locked out even past the original lease deadline: without
     * the renew, B would have acquired once the first lease expired. Renew never touches the identity
     * key, so A's token and its release stay valid across every extension.
     */
    private static void renewHoldsLockAcrossLongTask(String host, int port) {
        System.out.println("== Part 3: renew holds the lock across a long task ==");

        try (ZMap connA = new ZMap(host, port); ZMap connB = new ZMap(host, port)) {
            DistributedLock lockA = new DistributedLock(connA, LOCK_NAME);
            DistributedLock lockB = new DistributedLock(connB, LOCK_NAME);

            long lease = 500;
            LockHandle a = lockA.tryAcquire(lease);
            assert a != null;
            System.out.println("A: acquired with a " + lease + "ms lease " + describe(a));
            System.out.println("A: running a task longer than the lease, renewing on a shorter interval");

            int renewals = 0;
            for (int step = 0; step < 3; step++) {
                sleep(300); // renew interval, shorter than the lease
                boolean renewed = lockA.renew(a, lease);
                renewals++;
                System.out.println("A: renew -> " + renewed + ", lease extended");

                // We are now past the original 500ms lease. Without renew this acquire would succeed.
                LockHandle blocked = lockB.tryAcquire(lease);
                System.out.println("B: tryAcquire -> " + (blocked == null ? "null, still locked out" : "ACQUIRED, unexpected"));
            }

            System.out.println("A: task done after " + renewals + " renewals, releasing");
            lockA.release(a);

            LockHandle b = lockB.tryAcquire(lease);
            assert b != null;
            System.out.println("B: acquired now that A is done " + describe(b));
            lockB.release(b);
        }
        System.out.println();
    }

    private static String describe(LockHandle handle) {
        return "[token=" + hex(handle.token()) + " fencing=" + hex(handle.fencingToken()) + "]";
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value != null ? value : fallback;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * An in-memory stand-in for the external resource the lock guards. It enforces the recipe's
     * fencing rule: reject a write whose token is lower than the highest token seen, compared as an
     * unsigned big-endian byte string, and update the high-water mark atomically. Equal is accepted,
     * since one holder reuses its fencing token across every write in a single acquisition.
     */
    private static final class FencedResource {
        private byte[] highestTokenSeen = new byte[0];

        synchronized void write(byte[] fencingToken, String payload) {
            if (Arrays.compareUnsigned(fencingToken, highestTokenSeen) < 0) {
                System.out.println("  resource: REJECTED (" + payload + ") token "
                        + hex(fencingToken) + " < high-water " + hex(highestTokenSeen));
                return;
            }
            highestTokenSeen = fencingToken;
            System.out.println("  resource: accepted (" + payload + ") token " + hex(fencingToken));
        }
    }
}
