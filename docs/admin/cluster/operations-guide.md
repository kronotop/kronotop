---
title: "Cluster Operations Guide"
description: "This guide covers the cluster lifecycle: bootstrapping a new cluster, inspecting its state, watching member health, decommissioning members, and dropping a cluster."
---

This guide covers the cluster lifecycle: bootstrapping a new cluster, inspecting its state, watching
member health, decommissioning members, and dropping a cluster. It is intended for operators.

All `KR.ADMIN` commands run on the management interface (default port 3320). The examples below use
RESP3 output.

---

## Concepts

**Shard status** controls whether a shard accepts traffic:

- `READWRITE`: accepts reads and writes.
- `READONLY`: accepts reads, rejects writes.
- `INOPERABLE`: rejects all access.

**Member status** reflects a node's lifecycle state:

- `RUNNING`: the member is active.
- `UNAVAILABLE`: the member is registered but not reachable.
- `STOPPED`: the member has been shut down or marked for removal.
- `UNKNOWN`: the member's state has not been determined.

**Roles** describe a member's relationship to a shard. The `primary` owns the shard and serves writes.
A `standby` replicates from the primary and can be promoted to primary.

---

## Bringing Up a New Cluster

A freshly initialized cluster cannot serve traffic. Every shard starts as `INOPERABLE` with no routes
assigned. The steps below must run in order: a shard must have a primary before it can be opened for
writes.

1. **Initialize the cluster.** Run this exactly once. It is the only `KR.ADMIN` subcommand that does
   not require a pre-initialized cluster.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
   OK
   ```

2. **List the members** to get their IDs.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN LIST-MEMBERS
   1# "006cdc459c59e600c76494e8388857fc3cba2fa8" =>
      1# "status" => "RUNNING"
      2# "process_id" => "A1B2C3D4E5F6G7H8I9J0"
      3# "external_host" => "10.0.0.1"
      4# "external_port" => (integer) 5484
      5# "internal_host" => "10.0.0.1"
      6# "internal_port" => (integer) 3320
      7# "latest_heartbeat" => (integer) 31404
   ```

3. **Assign a primary** to each shard. A four-character member ID prefix is accepted in place of the
   full forty-character ID.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY BUCKET 0 006cdc459c59e600c76494e8388857fc3cba2fa8
   OK
   ```

4. **Add standbys** (optional). A primary must already be assigned to the shard.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN ROUTE SET STANDBY BUCKET 0 a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
   OK
   ```

5. **Open the shards** for traffic. Passing `*` as the shard ID applies the status to every bucket
   shard in a single transaction.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET * READWRITE
   OK
   ```

6. **Verify the topology.**
   ```kronotop
   127.0.0.1:3320> KR.ADMIN DESCRIBE-CLUSTER
   1# "metadata_version" => "1.0.0"
   2# "cluster_name" => "my-cluster"
   3# "bucket" =>
      1# (integer) 0 =>
         1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
         2# "standbys" =>
            1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
         3# "status" => "READWRITE"
         4# "linked_volumes" =>
            1) "bucket-shard-0"
   ```

`ROUTE SET PRIMARY` also accepts `*` for the shard ID to assign the same member as primary across all
bucket shards atomically. If any shard fails a pre-condition, the whole operation rolls back.

For full parameter and error details, see
[KR.ADMIN INITIALIZE-CLUSTER](commands/kr-admin-initialize-cluster.md),
[KR.ADMIN ROUTE](commands/kr-admin-route.md), and
[KR.ADMIN SET-SHARD-STATUS](commands/kr-admin-set-shard-status.md).

---

## Inspecting Cluster State

Five read-only commands answer different questions:

- [`DESCRIBE-CLUSTER`](commands/kr-admin-describe-cluster.md): full topology, every shard's primary,
  standbys, status, and linked volumes.
- [`DESCRIBE-SHARD`](commands/kr-admin-describe-shard.md): the same per-shard detail for a single shard.
- [`LIST-MEMBERS`](commands/kr-admin-list-members.md): every registered member with its host, ports,
  status, and last heartbeat.
- [`FIND-MEMBER`](commands/kr-admin-find-member.md): metadata for one member, looked up by full ID or
  four-character prefix.
- [`DESCRIBE-MEMBER`](commands/kr-admin-describe-member.md): metadata for the local member, including its
  own ID.

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD BUCKET 0
1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
2# "standbys" =>
   1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
3# "status" => "READWRITE"
4# "linked_volumes" =>
   1) "bucket-shard-0"
```

---

## Health Monitoring

Each member increments its own heartbeat counter every `cluster.heartbeat.interval` seconds. Other
members read that counter and expect it to keep advancing. When a member's counter stops moving for
longer than `cluster.heartbeat.maximum_silent_period` intervals, it is suspected dead.

[`LIST-SILENT-MEMBERS`](commands/kr-admin-list-silent-members.md) returns the IDs of the members currently
suspected:

```kronotop
127.0.0.1:3320> KR.ADMIN LIST-SILENT-MEMBERS
1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
```

This reflects the local failure-detection state of the responding node, not a cluster-wide consensus.
A different node may report a different set. If a silent member resumes sending heartbeats, it drops
off the list automatically.

[`LIST-MEMBERS`](commands/kr-admin-list-members.md) and [`FIND-MEMBER`](commands/kr-admin-find-member.md) expose
the raw `latest_heartbeat` counter, useful for a manual check or a monitoring script. The value is not
a timestamp: it is meaningful only against an earlier reading of the same member. Read it twice, a few
intervals apart. If a member's counter has not advanced, that member has gone silent.

```kronotop
127.0.0.1:3320> KR.ADMIN FIND-MEMBER ad14d838a2fa6bf2b87bf7872dbeb63bec03898b
1# "status" => "RUNNING"
2# "process_id" => "0000085BAE3Q20000000xxxx"
3# "external_host" => "172.20.0.4"
4# "external_port" => (integer) 5484
5# "internal_host" => "172.20.0.4"
6# "internal_port" => (integer) 3320
7# "latest_heartbeat" => (integer) 31396
```

---

## Decommissioning a Member

A member in `RUNNING` status cannot be removed. This guard prevents removing an active node by
accident. Stop it first, then remove it:

1. **Mark the member as stopped.**
   ```kronotop
   127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS 006cdc459c59e600c76494e8388857fc3cba2fa8 STOPPED
   OK
   ```

2. **Remove it.**
   ```kronotop
   127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER 006cdc459c59e600c76494e8388857fc3cba2fa8
   OK
   ```

Both commands accept a four-character ID prefix instead of the full ID. The change propagates to the
other members promptly.

Before removing a member, reassign any shards it owns as primary. See
[KR.ADMIN SET-MEMBER-STATUS](commands/kr-admin-set-member-status.md) and
[KR.ADMIN REMOVE-MEMBER](commands/kr-admin-remove-member.md).

---

## Dropping a Cluster

[`DROP-CLUSTER`](commands/kr-admin-drop-cluster.md) removes a cluster's entire metadata tree from
FoundationDB. It is a two-phase command to prevent accidental deletion.

1. **Request a token.** Pass only the cluster name. The name must match the running node's configured
   cluster name. The returned token is valid for 60 seconds.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN DROP-CLUSTER my-cluster
   "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
   ```

2. **Confirm the deletion.** Pass the cluster name and the token.
   ```kronotop
   127.0.0.1:3320> KR.ADMIN DROP-CLUSTER my-cluster a1b2c3d4-e5f6-7890-abcd-ef1234567890
   OK
   ```

Notes:

- This removes metadata from FoundationDB only. Volume segment files on local disk are left in place
  and must be cleaned up by hand.
- Multiple Kronotop clusters can share one FoundationDB instance. The command drops only the named
  cluster; the others are untouched.
- After a drop, nodes that belonged to the cluster are in an inconsistent state. Stop them.
