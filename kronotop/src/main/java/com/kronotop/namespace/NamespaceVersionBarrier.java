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

package com.kronotop.namespace;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BarrierNotSatisfiedException;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberView;
import com.kronotop.cluster.MembershipService;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import io.github.resilience4j.core.functions.CheckedRunnable;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A barrier that waits until all alive cluster members have observed a specific namespace version.
 * Used during namespace removal to ensure all members have invalidated their caches before
 * proceeding with the actual deletion.
 */
public class NamespaceVersionBarrier {
    private final Context context;
    private final NamespaceMetadata metadata;
    private final MembershipService membership;

    /**
     * Creates a new NamespaceVersionBarrier.
     *
     * @param context  the context providing access to cluster services
     * @param metadata the namespace metadata containing the namespace ID
     */
    public NamespaceVersionBarrier(Context context, NamespaceMetadata metadata) {
        this.context = context;
        this.metadata = metadata;
        this.membership = context.getService(MembershipService.NAME);
    }

    private DirectorySubspace openMemberSubspace(ReadTransaction tr, Member member) {
        List<String> subpath = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                members().
                member(member.getId()).toList();
        return DirectoryLayer.getDefault().open(tr, subpath).join();
    }

    /**
     * Blocks until all alive cluster members have observed the target namespace version.
     *
     * @param targetVersion the minimum version all members must have observed
     * @param maxAttempts   the maximum number of retry attempts
     * @param waitDuration  the duration to wait between retry attempts
     * @throws BarrierNotSatisfiedException if not all members observed the version within max attempts
     */
    public void await(long targetVersion, int maxAttempts, Duration waitDuration) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .waitDuration(waitDuration)
                .retryExceptions(BarrierNotSatisfiedException.class)
                .build();
        final Retry retry = Retry.of("versionBarrier", config);

        Map<Member, MemberView> knownMembers = membership.getKnownMembers();
        AtomicInteger attempts = new AtomicInteger();
        CheckedRunnable runnable = Retry.decorateCheckedRunnable(retry, () -> {
            attempts.getAndIncrement();
            int satisfies = 0;
            int aliveMembers = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReadTransaction readTr = tr.snapshot();

                for (Map.Entry<Member, MemberView> entry : knownMembers.entrySet()) {
                    MemberView view = entry.getValue();
                    if (!view.isAlive()) {
                        continue;
                    }
                    aliveMembers++;
                    Member member = entry.getKey();
                    DirectorySubspace memberSubspace = openMemberSubspace(readTr, member);
                    Long version = NamespaceUtil.readLastSeenNamespaceVersion(readTr, memberSubspace, metadata.id());
                    if (version != null && version >= targetVersion) {
                        satisfies++;
                    }
                }
            }
            if (satisfies != aliveMembers) {
                throw new BarrierNotSatisfiedException("Barrier not satisfied: not all members observed version "
                        + targetVersion + " within " + attempts.get() + " attempts");
            }
        });

        try {
            runnable.run();
        } catch (Throwable th) {
            if (th instanceof BarrierNotSatisfiedException exp) {
                throw exp;
            }
            throw new KronotopException(th);
        }
    }
}
