# Cluster Administration

`KR.ADMIN` command set provides administrative control over the Kronotop cluster itself—covering member discovery, 
cluster management, sharding, runtime diagnostics, and internal state inspection. These commands are issued through the management 
interface and are primarily intended for operators, infrastructure automation, and advanced debugging workflows.

* [Commands](#commands)
  * [KR.ADMIN DESCRIBE-MEMBER](#kradmin-describe-member)
  * [KR.ADMIN FIND-MEMBER](#kradmin-find-member)
  * [KR.ADMIN LIST-MEMBERS](#kradmin-list-members)
  * [KR.ADMIN INITIALIZE-CLUSTER](#kradmin-initialize-cluster)
  * [KR.ADMIN REMOVE-MEMBER](#kradmin-remove-member)
  * [KR.ADMIN DESCRIBE-CLUSTER](#kradmin-describe-cluster)
  * [KR.ADMIN DESCRIBE-SHARD](#kradmin-describe-shard)
  * [KR.ADMIN SET-MEMBER-STATUS](#kradmin-set-member-status)
  * [KR.ADMIN LIST-SILENT-MEMBERS](#kradmin-list-silent-members)
  * [KR.ADMIN SET-SHARD-STATUS](#kradmin-set-shard-status)
  * [KR.ADMIN ROUTE](#kradmin-route)

## Commands

Cluster administration commands are exposed through the **management interface**, which listens on a dedicated TCP port.
By default, this port is `3320`.

To interact with the cluster using admin commands, you can use the standard `redis-cli` tool:

```
redis-cli -3 -p 3320
```

### KR.ADMIN DESCRIBE-MEMBER

`KR.ADMIN DESCRIBE-MEMBER` command returns detailed information about the current cluster member (the node you are 
connected to). It provides metadata such as the member’s unique ID, process status, network bindings, and the timestamp 
of the latest heartbeat.

This command is useful for debugging, monitoring, and verifying the node’s runtime state within the cluster.

**Syntax**

```
KR.ADMIN DESCRIBE-MEMBER
```

**Example**

```
127.0.0.1:3320> KR.ADMIN DESCRIBE-MEMBER
1# member_id => 99f14bd9e6f9e95953c2f0740846b08508eb97b4
2# status => RUNNING
3# process_id => 000034D0P50600000000xxxx
4# external_host => 127.0.0.1
5# external_port => (integer) 5484
6# internal_host => 127.0.0.1
7# internal_port => (integer) 3320
8# latest_heartbeat => (integer) 71172
```

**Output Fields**

* `member_id`: Unique identifier of the current cluster member.
* `status`: Runtime status of the process (e.g., RUNNING, STOPPED, etc.).
* `process_id`: Logical process identifier assigned to this member.
* `external_host` & `external_port`: Network address and port used for client traffic.
* `internal_host` & `internal_port`: Network address and port used for internal coordination and management commands.
* `latest_heartbeat`: Timestamp or logical clock value of the most recent heartbeat sent by this node.

**Member Statuses**

* RUNNING
* STOPPED
* UNAVAILABLE
* UNKNOWN

### KR.ADMIN FIND-MEMBER

`KR.ADMIN FIND-MEMBER` command retrieves metadata for a specific cluster member, identified by its `member_id`. This is particularly 
useful for querying the state of a remote node from any point in the cluster, without having to connect to that node directly.

**Syntax**

```
KR.ADMIN FIND-MEMBER <member_id>
```

**Example**

```
127.0.0.1:3320> kr.admin find-member 99f14bd9e6f9e95953c2f0740846b08508eb97b4
1# status => RUNNING
2# process_id => 000034D0P50600000000xxxx
3# external_host => 127.0.0.1
4# external_port => (integer) 5484
5# internal_host => 127.0.0.1
6# internal_port => (integer) 3320
7# latest_heartbeat => (integer) 71219
```

**Error Cases**

The `member_id` must be a valid UUIDv4. If the format is invalid, the command returns an error.

```
127.0.0.1:3320> KR.ADMIN FIND-MEMBER invalid-member-id
(error) ERR Invalid memberId: invalid-member-id
```

If the specified `member_id` is syntactically valid (a valid UUIDv4) but does not correspond to any known or registered member 
in the cluster, the command returns an error indicating that the member is not found.

```
127.0.0.1:3320> KR.ADMIN FIND-MEMBER 89f14bd9e6f9e95953c2f0740846b08508eb97b4
(error) ERR Member: 89f14bd9e6f9e95953c2f0740846b08508eb97b4 not registered
```

### KR.ADMIN LIST-MEMBERS

`KR.ADMIN LIST-MEMBERS` command returns a list of all cluster members currently known to the local node. For each member, 
it provides runtime status, network information, and the latest heartbeat timestamp. This command is useful for monitoring 
cluster topology, verifying member availability, and debugging coordination issues.


**Syntax**

```
KR.ADMIN LIST-MEMBERS
```

**Example**

```
127.0.0.1:3320> KR.ADMIN LIST-MEMBERS
1# 99f14bd9e6f9e95953c2f0740846b08508eb97b4 =>
   1# status => RUNNING
   2# process_id => 000034D0P50600000000xxxx
   3# external_host => 127.0.0.1
   4# external_port => (integer) 5484
   5# internal_host => 127.0.0.1
   6# internal_port => (integer) 3320
   7# latest_heartbeat => (integer) 71285
2# b558c6eec79c646928e6678e06b5c67479809663 =>
   1# status => RUNNING
   2# process_id => 000035Q4A9OVA0000000xxxx
   3# external_host => 127.0.0.1
   4# external_port => (integer) 5585
   5# internal_host => 127.0.0.1
   6# internal_port => (integer) 3421
   7# latest_heartbeat => (integer) 11058
```

**Output Structure**

Each top-level entry represents a cluster member, keyed by its `member_id`. For each member, the following fields are shown:

* `status`: Current runtime status of the member.
* `process_id`: Logical process identifier for the member.
* `external_host` & `external_port`: Network endpoint used by clients to communicate with this member.
* `internal_host` & `internal_port`: Network endpoint used for internode communication and admin commands.
* `latest_heartbeat`: Most recent heartbeat timestamp or tick observed by this node for the given member.

**Notes**

This command reflects the local node’s view of the cluster. It is possible for nodes to have temporary inconsistencies in
membership metadata. Use in combination with `KR.ADMIN FIND-MEMBER <member_id>` for more targeted inspection.

If a member has failed or is unresponsive, its status may show a different state or the entry may be missing entirely.

### KR.ADMIN INITIALIZE-CLUSTER

`KR.ADMIN INITIALIZE-CLUSTER` command bootstraps a new Kronotop cluster from the current node. It registers the local member 
as the first and only member in the cluster and sets up the necessary metadata structures for coordination and replication.

This command must be executed **exactly once** before the cluster can accept any other members or serve data.

**Syntax**

```
KR.ADMIN INITIALIZE-CLUSTER
```

**Example**

```
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
OK
```

**Error Cases**

If the cluster has already been initialized, attempting to run `KR.ADMIN INITIALIZE-CLUSTER` again will result in an error:

```
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
(error) ERR cluster has already been initialized
```

**Note**

* Initializing the cluster is required before any operations can be performed.

### KR.ADMIN REMOVE-MEMBER

`KR.ADMIN REMOVE-MEMBER` command forcefully removes a member from the cluster by its member-id. This is typically used to 
clean up metadata for members that have **permanently failed, been decommissioned**, or are otherwise no longer part of the cluster.

**Syntax**

```
KR.ADMIN REMOVE-MEMBER <member_id>
```

**Example**

```
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER b558c6eec79c646928e6678e06b5c67479809663
OK
```

**Error Cases**

If the specified `member_id` is syntactically valid (a valid UUIDv4) but does not correspond to any known or registered member
in the cluster, the command returns an error indicating that the member is not found.

```
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER 89f14bd9e6f9e95953c2f0740846b08508eb97b4
(error) ERR Member: 89f14bd9e6f9e95953c2f0740846b08508eb97b4 not registered
```

A member in `RUNNING` status cannot be removed. The member must be in one of the following states to be eligible for removal:

* UNAVAILABLE,
* STOPPED,
* UNKNOWN

```
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER 99f14bd9e6f9e95953c2f0740846b08508eb97b4
(error) ERR Member in RUNNING status cannot be removed
```

**Behavior**

* Deletes the specified member from the internal cluster metadata.
* Does not attempt to contact or shut down the target node.
* Safe to use only if the member is known to be permanently offline or irrecoverable.
* Removed members will no longer appear in `KR.ADMIN LIST-MEMBERS`.

**Notes**

* The command requires a valid `member_id` (UUIDv4).
* Removing an active or recoverable member may lead to inconsistent state or data loss.
* Always confirm the member’s status using `KR.ADMIN FIND-MEMBER` or `KR.ADMIN LIST-MEMBERS` before removal.

### KR.ADMIN DESCRIBE-CLUSTER

`KR.ADMIN DESCRIBE-CLUSTER` command returns a complete snapshot of the current cluster topology, including information 
about all shard groups, their replication layout, and associated volumes. This command is essential for understanding 
how data is distributed across the cluster, which nodes are acting as primaries or standbys, and the operational status of each shard.

**Syntax**

```
KR.ADMIN DESCRIBE-CLUSTER
```

**Example**

```
127.0.0.1:3320> KR.ADMIN DESCRIBE-CLUSTER
1# redis =>
   1# (integer) 0 =>
      1# primary => 99f14bd9e6f9e95953c2f0740846b08508eb97b4
      2# standbys => (empty array)
      3# status => READWRITE
      4# linked_volumes => 1) redis-shard-0
   2# (integer) 1 =>
      1# primary => 99f14bd9e6f9e95953c2f0740846b08508eb97b4
      2# standbys => 1) b558c6eec79c646928e6678e06b5c67479809663
      3# status => READWRITE
      4# linked_volumes => 1) redis-shard-1
2# bucket =>
   1# (integer) 0 =>
      1# primary => 99f14bd9e6f9e95953c2f0740846b08508eb97b4
      2# standbys => (empty array)
      3# status => READWRITE
      4# linked_volumes => 1) bucket-shard-0
   2# (integer) 1 =>
      1# primary => 99f14bd9e6f9e95953c2f0740846b08508eb97b4
      2# standbys => (empty array)
      3# status => READWRITE
      4# linked_volumes => 1) bucket-shard-1
```

**Output**

The response is grouped first by shard kind (e.g., REDIS, BUCKET), then by shard ID. Each shard contains the following fields:

* `primary`: The member ID currently acting as the primary for this shard.
* `standbys`: An array of member IDs acting as standbys (replication targets).
* `status`: Operational state of the shard (READWRITE, READONLY, or INOPERABLE).
* `linked_volumes`: A list of volume names associated with the shard. These represent the physical or logical storage for the shard's data.

**Notes**

* A shard with no standbys is considered *under-replicated*, which may impact fault tolerance.
* The command reflects the global cluster view as seen by the node handling the request.
* Useful for debugging replication lag, topology drift, and verifying HA configurations.

### KR.ADMIN DESCRIBE-SHARD

`KR.ADMIN DESCRIBE-SHARD` command returns detailed metadata for a specific shard within the cluster. It provides information 
about the primary and standby members, current replication state, and associated volumes.

This command is useful for inspecting the health and role assignments of an individual shard, particularly in debugging failovers, 
replication consistency, and shard placement.

**Syntax**

```
KR.ADMIN DESCRIBE-SHARD
```

**Example**

```
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD BUCKET 1
1# primary => 99f14bd9e6f9e95953c2f0740846b08508eb97b4
2# standbys => (empty array)
3# status => READWRITE
4# linked_volumes => 1) bucket-shard-1
```

**Output**

* `primary`: The member currently acting as the primary node for the shard.
* `standbys`: A list of standby members.
* `status`: Current operational state of the shard (READWRITE, READONLY, INOPERABLE).
* `linked_volumes`: Names of volumes physically associated with the shard.

**Notes**

* This command only inspects a single shard. To view the full cluster layout, use `KR.ADMIN DESCRIBE-CLUSTER`.
* Use `VOLUME.ADMIN REPLICATIONS` command when diagnosing replication lag, unresponsive primaries, or verifying volume assignments at the shard level.

### KR.ADMIN SET-MEMBER-STATUS

`KR.ADMIN SET-MEMBER-STATUS` command manually overrides the status of a specific cluster member.


**Syntax**

```
KR.ADMIN SET-MEMBER-STATUS member-id member-status
```

**Arguments**

* `member-id`: The UUIDv4 identifier of the member whose status should be updated.
* `member-status`: The desired status value. Valid options:
  * RUNNING
  * UNAVAILABLE
  * STOPPED
  * UNKNOWN

**Example**

```
127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS 99f14bd9e6f9e95953c2f0740846b08508eb97b4 RUNNING
OK
```

**Use Cases**

* Force removal eligibility by marking a member UNAVAILABLE or STOPPED,
* Reset inconsistent state during manual recovery,
* System administration and maintenance tasks.

**Notes**

* This command does not validate the actual health of the target process.
* Use with caution in production environments, as incorrect status settings can disrupt replication and routing logic.
* To view a member’s current status, use `KR.ADMIN FIND-MEMBER` or `KR.ADMIN LIST-MEMBERS`.

### KR.ADMIN LIST-SILENT-MEMBERS

`KR.ADMIN LIST-SILENT-MEMBERS` command returns a list of silent cluster members—nodes that are **expected to send heartbeats 
but have not been heard from** within a configured timeout window. 

These members may be offline, crashed, misconfigured, or experiencing network partitioning. This command is useful for 
identifying potentially failed or unreachable nodes in the cluster.

**Syntax**

```
KR.ADMIN LIST-SILENT-MEMBERS
```

**Example**

```
127.0.0.1:3320> KR.ADMIN LIST-SILENT-MEMBERS
1) 99f14bd9e6f9e95953c2f0740846b08508eb97b4
```

**Output**

* A flat list of member-ids (UUIDv4) representing silent nodes.
* If no members are considered silent, the result is an empty array.

**Notes**

* Silent members are not automatically removed from the cluster. Use `KR.ADMIN REMOVE-MEMBER` if manual cleanup is required.
* This status is determined based on heartbeat intervals tracked internally by each node.
* You can use `KR.ADMIN FIND-MEMBER` to inspect the last known state of each silent member.

### KR.ADMIN SET-SHARD-STATUS

`KR.ADMIN SET-SHARD-STATUS` command manually updates the **operational status** of a specific shard—or all shards—within a given shard kind. 
This is primarily used to control read/write availability, disable problematic shards, or temporarily enforce read-only access.

**Syntax**

```
KR.ADMIN SET-SHARD-STATUS shard-kind shard-id|* shard-status
```

**Arguments**

* `shard-kind`: The type of shard group. Valid values: `REDIS`, `BUCKET`.
* `shard-id`: The numeric ID of the target shard. You can use `*` (asterisk) to apply the status to all shards of the specified kind.
* `shard-status`: Desired operational status. Valid values:
  * `READWRITE`: Shard accepts both reads and writes.
  * `READONLY`: Shard accepts reads but rejects writes. 
  * `INOPERABLE`: Shard is considered unavailable; all access is rejected.
  
**Example**

Set status for all `REDIS` shards:

```
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS REDIS * READWRITE
OK
```

Set status for shard id 1 for `BUCKET` shard kind:

```
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET 1 READWRITE
OK
```

**Notes**

* This command only affects the **logical access control** layer. It does *not* alter replication roles or volume state.
* Setting a shard to `INOPERABLE` disables it entirely, even for internal use.
* Useful for:
  * Maintenance operations
  * Incident response (e.g., isolating a corrupted shard)
  * Load shedding and testing fallback mechanisms

### KR.ADMIN ROUTE

`KR.ADMIN ROUTE` command is used to **manually assign routing roles**—such as `PRIMARY` or `STANDBY`—to specific cluster 
members for given shards. It enables precise control over shard placement and replication topology. This command is useful 
in scenarios such as custom rebalancing, controlled failover, or restoring specific members to their roles after recovery.

**Syntax**

```
KR.ADMIN ROUTE operation route-kind shard-kind shard-id|* member-id
```

**Parameters**
* `operation`: The type of routing action. It can be `SET` or `UNSET`.
* `route-kind`: The role to assign. Valid values:
  * `PRIMARY`: Assigns the specified member as the primary for the shard.
  * `STANDBY`: Assigns the member as a standby (replication follower).
* `shard-kind`: The type of shard group. Valid values: `REDIS`, `BUCKET`.
* `shard-id`: The target shard ID, or asterisk(`*`) to apply the operation to all shards of the specified kind.
* `member-id`: UUIDv4 identifier of the member to assign to the specified role.

**Example**

Set a member as primary for all `REDIS` shards:

```
127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY REDIS * a0dc14d811a285834c187ddc20549de7c1c1a381
OK
```

Assign a member as primary for a specific `BUCKET` shard:

```
127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY BUCKET 1 a0dc14d811a285834c187ddc20549de7c1c1a381
OK
```

Assign a member as a standby for `BUCKET` shard 2:

```
127.0.0.1:3320> KR.ADMIN ROUTE SET STANDBY BUCKET 2 99f14bd9e6f9e95953c2f0740846b08508eb97b4
OK
```

**Notes**

* Manual routing changes take effect immediately and override any automatic placement or failover decisions.
* The target member must be healthy and already registered in the cluster.
* Use `KR.ADMIN DESCRIBE-CLUSTER` to verify the resulting topology.
* This command does **not** automatically synchronize data between members. Make sure replication state is consistent before routing traffic to newly assigned primaries.