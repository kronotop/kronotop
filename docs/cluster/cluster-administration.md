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