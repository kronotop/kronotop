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

import io.lettuce.core.RedisException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * A lease-based distributed lock built on Kronotop's ZMap, following
 * recipes/distributed-lock-on-zmap.md.
 *
 * <p>The single primitive is {@link #tryAcquire(long)}: one acquire attempt that returns
 * immediately, a {@link LockHandle} when it took the lock or {@code null} when the lock is
 * held by someone else. ZMap has no watch primitive, so there is nothing to block on. Waiting
 * is the caller's choice: call {@code tryAcquire} once, or loop it (poll) with a short
 * randomized delay. The busy-wait stays visible at the call site instead of hiding inside the
 * lock.
 *
 * <p>One {@link ZMap} (one connection, one session) backs one lock, so a BEGIN..COMMIT block
 * stays on a single transaction. Mutual exclusion comes from FoundationDB's serializable reads,
 * which are on by default; do not turn SNAPSHOTREAD on for this connection.
 *
 * <p>An instance is not thread-safe: all calls share the one session, so they must come from a
 * single thread. To renew in the background while another thread runs the protected work, guard
 * {@link #renew} and {@link #release} with your own mutual exclusion. Otherwise a release issued
 * while a renew's transaction is open joins that transaction and only takes effect at its commit,
 * which the lock cannot prevent.
 */
public final class DistributedLock {

    private static final SecureRandom RANDOM = new SecureRandom();
    private final ZMap zmap;
    private final byte[] identityKey;
    private final byte[] leaseKey;
    /**
     * @param zmap a connection dedicated to this lock
     * @param name the lock's own name, the resource being guarded; a lock named {@code orders}
     *             uses keys {@code lock:orders} and {@code lock:orders:lease}
     */
    public DistributedLock(ZMap zmap, String name) {
        this.zmap = zmap;
        this.identityKey = ("lock:" + name).getBytes(StandardCharsets.UTF_8);
        this.leaseKey = ("lock:" + name + ":lease").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] newToken() {
        byte[] token = new byte[16];
        RANDOM.nextBytes(token);
        return token;
    }

    private static byte[] encodeDeadline(long epochMillis) {
        return ByteBuffer.allocate(Long.BYTES).putLong(epochMillis).array();
    }

    private static long decodeDeadline(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    private static boolean isNotCommitted(RedisException e) {
        String message = e.getMessage();
        return message != null && message.contains("NOT_COMMITTED");
    }

    /**
     * One acquire attempt. Returns a handle if the lock was free, and we took it, or {@code null}
     * if it is currently held. Never blocks and never loops.
     *
     * @param leaseMillis how long the lease should last from now
     */
    public LockHandle tryAcquire(long leaseMillis) {
        zmap.begin();
        // The two serializable reads add the read conflict range that gives us mutual exclusion:
        // if another client also reads the keys as free and writes, one commit wins and the other
        // fails with NOT_COMMITTED at COMMIT.
        byte[] identity = zmap.zget(identityKey);
        byte[] leaseBytes = zmap.zget(leaseKey);

        if (!isFree(identity, leaseBytes)) {
            zmap.rollback();
            return null;
        }

        byte[] token = newToken();
        long expiresAt = System.currentTimeMillis() + leaseMillis;
        zmap.zset(identityKey, token);
        zmap.zset(leaseKey, encodeDeadline(expiresAt));
        try {
            byte[] fencingToken = zmap.commitReturningVersionstamp();
            return new LockHandle(token, fencingToken, expiresAt);
        } catch (RedisException e) {
            // A conflicting write landed on the lock keys, either another client's acquire or a
            // release. A failed commit already ended the transaction, so do not ROLLBACK here;
            // just report that the lock was not taken.
            if (isNotCommitted(e)) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Release the lock. A single COMPARE_AND_CLEAR against our token, with no transaction open. It
     * clears the identity key only if its value still equals our token, so a holder whose lease has
     * already expired and been re-acquired by someone else clears nothing. The lease key is left
     * behind and is harmlessly overwritten by the next acquire.
     */
    public void release(LockHandle handle) {
        zmap.compareAndClear(identityKey, handle.token());
    }

    /**
     * Extend a live lease. Reads the identity and lease keys, confirms the token is still ours and
     * the lease has not expired, then rewrites only the lease deadline. Returns {@code true} if the
     * lease was extended, {@code false} if the lock was lost (token changed or lease expired). On
     * {@code false} the caller must stop the protected work and not retry. Renew on an interval
     * shorter than the lease to hold the lock across a long task.
     *
     * <p>A NOT_COMMITTED at the commit is retried internally rather than reported as lost: the
     * conflicting write may be another client's acquire, but it may also be a stale ex-holder's
     * no-op release, which conflicts without clearing anything and leaves the lock ours. The retry
     * re-reads with a fresh transaction and lets the token and lease checks decide.
     */
    public boolean renew(LockHandle handle, long leaseMillis) {
        for (; ; ) {
            zmap.begin();
            byte[] identity = zmap.zget(identityKey);
            byte[] leaseBytes = zmap.zget(leaseKey);
            long now = System.currentTimeMillis();
            if (!Arrays.equals(identity, handle.token()) || leaseBytes == null || decodeDeadline(leaseBytes) <= now) {
                zmap.rollback();
                return false;
            }
            zmap.zset(leaseKey, encodeDeadline(now + leaseMillis));
            try {
                zmap.commit();
                return true;
            } catch (RedisException e) {
                if (isNotCommitted(e)) {
                    // Not a verdict: re-read and let the checks above decide whether the lock
                    // is genuinely lost. A failed commit already ended the transaction.
                    continue;
                }
                throw e;
            }
        }
    }

    /**
     * The lock is free when the identity key is missing or the lease has expired. A present identity
     * key always has a lease key beside it, since acquire writes both and nothing clears the lease on
     * its own; a present identity with a missing lease should never occur, but treat it as free as a
     * defensive guard.
     */
    private boolean isFree(byte[] identity, byte[] leaseBytes) {
        if (identity == null) {
            return true;
        }
        if (leaseBytes == null) {
            return true;
        }
        return decodeDeadline(leaseBytes) <= System.currentTimeMillis();
    }

    /**
     * Proof of one acquisition.
     *
     * @param token        the 16-byte identity stored under {@code lock:NAME}. Release and renew
     *                     compare against it, so a stale holder cannot clear or extend a lock that
     *                     has since been re-acquired under a different token.
     * @param fencingToken the versionstamp returned by the acquire commit. Pass it to the protected
     *                     resource, which must reject any write carrying a lower token than the
     *                     highest it has seen. This is what makes the lock safe across a paused holder.
     * @param expiresAt    the lease deadline this acquisition was granted, epoch milliseconds.
     */
    public record LockHandle(byte[] token, byte[] fencingToken, long expiresAt) {
    }
}
