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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.JSONUtils;
import com.kronotop.cluster.membership.MembershipUtils;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;

/**
 * The MemberRegistry class provides methods to manage member registrations and their associated metadata.
 * This includes registering, unregistering, checking registration status, updating member status,
 * and retrieving member details from a FoundationDB backend.
 */
public class MemberRegistry {
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
    private KronotopDirectoryNode getDirectoryNode(String memberId) {
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
    public boolean isAdded(Transaction tr, String memberId) {
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
    public boolean isAdded(String memberId) {
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
    public DirectorySubspace add(Member member) {
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
            member.setSubspace(subspace);
            return subspace;
        }
    }

    /**
     * Removes the member identified by the provided memberId from the system by deleting their directory entry
     * in the database.
     *
     * @param memberId the unique identifier of the member to be removed
     * @throws MemberNotRegisteredException if the specified member is not registered
     */
    public void remove(String memberId) {
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
     * @param status   the new status to be assigned to the member
     * @return the updated Member object with the new status
     * @throws KronotopException            if the member is not registered properly
     * @throws MemberNotRegisteredException if the specified member is not registered
     * @throws CompletionException          if there is an exception during the transaction completion
     */
    public Member setStatus(String memberId, MemberStatus status) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Member member = findMember(tr, memberId);
            if (member.getSubspace() == null) {
                throw new KronotopException(String.format("Member: %s has no subspace", memberId));
            }
            member.setStatus(status);

            byte[] key = member.getSubspace().pack(Tuple.from(MEMBER_KEY));
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
    private Member findMember(Transaction tr, String memberId) {
        KronotopDirectoryNode directory = getDirectoryNode(memberId);

        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
        byte[] key = subspace.pack(Tuple.from(MEMBER_KEY));
        byte[] data = tr.get(key).join();
        if (data == null) {
            throw new KronotopException(String.format("Member: %s not registered properly", memberId));
        }
        Member member = JSONUtils.readValue(data, Member.class);
        member.setSubspace(subspace);
        return member;
    }

    /**
     * Retrieves the Member object associated with the specified member ID.
     *
     * @param memberId the unique identifier of the member to be retrieved
     * @return the Member object associated with the specified member ID
     * @throws KronotopException if the member is not registered properly
     */
    public Member findMember(String memberId) {
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
    public TreeSet<Member> listMembers() {
        DirectorySubspace subspace = MembershipUtils.createOrOpenClusterMetadataSubspace(context);
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                members();
        TreeSet<Member> members = new TreeSet<>(Comparator.comparing(Member::getProcessId));
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace membersSubspace = subspace.open(tr, directory.excludeSubspace(subspace)).join();
            AsyncIterable<KeyValue> iterable = tr.getRange(membersSubspace.pack(), ByteArrayUtil.strinc(membersSubspace.pack()));
            for (KeyValue keyValue : iterable) {
                Member member = JSONUtils.readValue(keyValue.getValue(), Member.class);
                members.add(member);
            }
        }
        return members;
    }
}
