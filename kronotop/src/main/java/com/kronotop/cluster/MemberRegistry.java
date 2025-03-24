/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.JSONUtils;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;

/**
 * The MemberRegistry class provides methods to manage member registrations and their associated metadata.
 * This includes registering, unregistering, checking registration status, updating member status,
 * and retrieving member details from a FoundationDB backend.
 */
class MemberRegistry {
    private static final String MEMBER_KEY = "member";
    private final Context context;

    public MemberRegistry(Context context) {
        // [kronotop, development, metadata, members, <UUID>]
        this.context = context;
    }

    /**
     * Retrieves a KronotopDirectoryNode object for the specified member.
     *
     * @param memberId the unique identifier of the member whose directory node is to be retrieved
     * @return the KronotopDirectoryNode associated with the specified member ID
     */
    protected KronotopDirectoryNode getDirectoryNode(String memberId) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                members().
                member(memberId);
    }

    /**
     * Determines if a member is registered in the system by checking the existence of
     * its directory within the database.
     *
     * @param tr       the transaction object used for database operations
     * @param memberId the unique identifier of the member to be checked
     * @return true if the member's directory exists, false otherwise
     */
    boolean isAdded(Transaction tr, String memberId) {
        KronotopDirectoryNode directory = getDirectoryNode(memberId);
        return DirectoryLayer.getDefault().exists(tr, directory.toList()).join();
    }

    /**
     * Checks whether a member is registered in the system by verifying the existence
     * of its directory within the database.
     *
     * @param memberId the unique identifier of the member to be checked
     * @return true if the member's directory exists, false otherwise
     */
    boolean isAdded(String memberId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return isAdded(tr, memberId);
        }
    }

    /**
     * Adds a new member to the registry by saving their information in the database.
     *
     * @param member the member to be added to the registry
     * @return the DirectorySubspace associated with the added member
     * @throws MemberAlreadyRegisteredException if the member is already registered
     */
    DirectorySubspace add(Member member) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (isAdded(tr, member.getId())) {
                throw new MemberAlreadyRegisteredException(
                        String.format("Member: %s already registered", member.getId())
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
     * Updates the member information in the database. If the member is not registered,
     * a MemberNotRegisteredException is thrown.
     *
     * @param member the member whose information is to be updated
     * @throws MemberNotRegisteredException if the member is not registered
     */
    void update(Member member) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            update(tr, member);
            tr.commit().join();
        }
    }

    /**
     * Updates the member information in the database. If the member is not registered,
     * a MemberNotRegisteredException is thrown.
     *
     * @param tr     the transaction object used for database operations
     * @param member the member whose information is to be updated
     * @throws MemberNotRegisteredException if the member is not registered
     */
    void update(Transaction tr, Member member) {
        if (!isAdded(tr, member.getId())) {
            throw new MemberNotRegisteredException(String.format("Member: %s not registered", member.getId()));
        }

        KronotopDirectoryNode directory = getDirectoryNode(member.getId());
        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
        tr.set(subspace.pack(Tuple.from(MEMBER_KEY)), JSONUtils.writeValueAsBytes(member));
    }

    /**
     * Removes the member identified by the provided memberId from the system by deleting their directory entry
     * in the database.
     *
     * @param memberId the unique identifier of the member to be removed
     * @throws MemberNotRegisteredException if the specified member is not registered
     */
    void remove(String memberId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            remove(tr, memberId);
            tr.commit().join();
        }
    }

    /**
     * Removes the member identified by the provided memberId from the system by deleting their directory entry
     * in the database within the provided transaction context.
     *
     * @param tr       the transaction object used for database operations
     * @param memberId the unique identifier of the member to be removed
     * @throws MemberNotRegisteredException if the specified member is not registered
     */
    void remove(Transaction tr, String memberId) {
        try {
            KronotopDirectoryNode directory = getDirectoryNode(memberId);
            DirectoryLayer.getDefault().remove(tr, directory.toList()).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new MemberNotRegisteredException(String.format("Member: %s not registered", memberId));
            }
            throw e;
        }
    }

    /**
     * Sets the status of a member identified by the provided memberId.
     *
     * @param memberId the unique identifier of the member
     * @param status   the new status to be assigned to the member
     * @return the updated Member object with the new status
     * @throws KronotopException            if the member is not registered properly
     * @throws MemberNotRegisteredException if the specified member is not registered
     * @throws CompletionException          if there is an exception during the transaction completion
     */
    Member setStatus(String memberId, MemberStatus status) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Member member = findMember(tr, memberId);
            member.setStatus(status);

            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, getDirectoryNode(memberId).toList()).join();
            byte[] key = subspace.pack(Tuple.from(MEMBER_KEY));
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

    /**
     * Retrieves the Member object associated with the specified member ID.
     *
     * @param memberId the unique identifier of the member to be retrieved
     * @return the Member object associated with the specified member ID
     * @throws KronotopException if the member is not registered properly
     */
    Member findMember(ReadTransaction tr, String memberId) {
        KronotopDirectoryNode directory = getDirectoryNode(memberId);

        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
            byte[] key = subspace.pack(Tuple.from(MEMBER_KEY));
            byte[] data = tr.get(key).join();
            if (data == null) {
                throw new KronotopException(String.format("Member: %s not registered properly", memberId));
            }
            return JSONUtils.readValue(data, Member.class);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new MemberNotRegisteredException(String.format("Member: %s not registered", memberId));
            }
            throw e;
        }
    }

    /**
     * Retrieves the Member object associated with the specified member ID.
     *
     * @param memberId the unique identifier of the member to be retrieved
     * @return the Member object associated with the specified member ID
     * @throws MemberNotRegisteredException if the member is not registered
     * @throws CompletionException          if an error occurs during transaction completion
     */
    Member findMember(String memberId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return findMember(tr, memberId);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new MemberNotRegisteredException(String.format("Member: %s not registered", memberId));
            }
            throw e;
        }
    }

    /**
     * Retrieves a sorted set of members from the database.
     *
     * @return a TreeSet containing Member objects sorted by their process IDs.
     */
    TreeSet<Member> listMembers() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return listMembers(tr);
        }
    }

    TreeSet<Member> listMembers(Transaction tr) {
        TreeSet<Member> members = new TreeSet<>(Comparator.comparing(Member::getProcessId));

        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                members();
        List<String> memberIds = DirectoryLayer.getDefault().list(tr, directory.toList()).join();
        for (String memberId : memberIds) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, getDirectoryNode(memberId).toList()).join();
            byte[] key = subspace.pack(Tuple.from(MEMBER_KEY));
            byte[] value = tr.get(key).join();
            Member member = JSONUtils.readValue(value, Member.class);
            members.add(member);
        }
        return members;
    }
}
