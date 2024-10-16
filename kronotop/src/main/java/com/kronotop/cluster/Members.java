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

package com.kronotop.cluster;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.JSONUtils;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;

import java.util.concurrent.CompletionException;

/**
 * Manages the registration and status of members within the Kronotop Cluster.
 */
public class Members {
    private static final String MEMBER_KEY = "member";
    private final Context context;

    public Members(Context context) {
        // [kronotop, development, metadata, members, <UUID>]
        this.context = context;
    }

    /**
     * Retrieves a KronotopDirectoryNode object for the specified member.
     *
     * @param memberId the unique identifier of the member whose directory node is to be retrieved
     * @return the KronotopDirectoryNode associated with the specified member ID
     */
    private KronotopDirectoryNode getDirectoryNode(String memberId) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                members().
                member(memberId);
    }

    /**
     * Checks if a member is registered in the system by verifying the existence of its directory.
     *
     * @param tr the transaction to be used for the database operation
     * @param memberId the unique identifier of the member to be checked
     * @return true if the member is registered, false otherwise
     */
    public boolean isRegistered(Transaction tr, String memberId) {
        KronotopDirectoryNode directory = getDirectoryNode(memberId);
        return DirectoryLayer.getDefault().exists(tr, directory.toList()).join();
    }

    /**
     * Checks if a member is registered in the system by verifying the existence of its directory.
     *
     * @param memberId the unique identifier of the member to be checked
     * @return true if the member is registered, false otherwise
     */
    public boolean isRegistered(String memberId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return isRegistered(tr, memberId);
        }
    }

    /**
     * Registers a new member in the system by creating its directory subspace and storing its details in the database.
     *
     * @param member the member object to be registered
     * @return the DirectorySubspace created for the registered member
     * @throws MemberAlreadyRegisteredException if the member is already registered or not gracefully stopped
     */
    public DirectorySubspace register(Member member) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (isRegistered(tr, member.getId())) {
                throw new MemberAlreadyRegisteredException(
                        String.format("Member: %s already registered or not gracefully stopped", member.getId())
                );
            }

            KronotopDirectoryNode directory = getDirectoryNode(member.getId());
            DirectorySubspace subspace = DirectoryLayer.getDefault().create(tr, directory.toList()).join();
            tr.set(subspace.pack(Tuple.from(MEMBER_KEY)), JSONUtils.writeValueAsBytes(member));
            tr.commit().join();
            return subspace;
        }
    }

    /**
     * Unregisters a member identified by the given member ID from the system.
     * Removes the associated directory from the FoundationDB.
     *
     * @param memberId the unique identifier of the member to be unregistered.
     * @throws MemberNotRegisteredException if the member with the specified ID is not registered.
     */
    public void unregister(String memberId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopDirectoryNode directory = getDirectoryNode(memberId);
            DirectoryLayer.getDefault().remove(tr, directory.toList()).join();
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new MemberNotRegisteredException(String.format("Member: %s not registered", memberId));
            }
        }
    }

    /**
     * Sets the status of a member identified by the provided memberId.
     *
     * @param memberId the unique identifier of the member
     * @param status the new status to be assigned to the member
     * @return the updated Member object with the new status
     * @throws KronotopException if the member is not registered properly
     * @throws MemberNotRegisteredException if the specified member is not registered
     * @throws CompletionException if there is an exception during the transaction completion
     */
    public Member setStatus(String memberId, MemberStatus status) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopDirectoryNode directory = getDirectoryNode(memberId);

            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
            byte[] key = subspace.pack(Tuple.from(MEMBER_KEY));
            byte[] data = tr.get(key).join();
            if (data == null) {
                throw new KronotopException(String.format("Member: %s not registered properly", memberId));
            }

            Member member = JSONUtils.readValue(data, Member.class);
            member.setStatus(status);

            tr.set(key, JSONUtils.writeValueAsBytes(member));
            tr.commit().join();

            return member;
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new MemberNotRegisteredException(String.format("Member: %s not registered", memberId));
            }
            throw e;
        }
    }
}
