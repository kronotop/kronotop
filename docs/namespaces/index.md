---
title: "Namespaces"
sidebar:
  label: "Overview"
description: "Namespaces are lightweight logical databases built on FoundationDB'sdirectory layer."
---

## Overview

Namespaces are lightweight logical databases built on
FoundationDB's [directory layer](https://apple.github.io/foundationdb/developer-guide.html#directories). They provide
complete data
isolation between tenants, applications, or environments with zero runtime overhead. The directory layer maps
hierarchical
paths to short binary prefixes at open time, so namespace resolution adds no cost to subsequent operations.

Every data structure created within a namespace (Buckets and ZMaps) is fully isolated from data in other
namespaces.

## Default Namespace

Every session starts in the `global` namespace. This is the default namespace configured at the cluster level and cannot
be removed or purged. If a client never issues a `NAMESPACE USE` command, all operations execute within `global`.

## Hierarchical Organization

Namespaces are identified by dot-separated hierarchical paths, analogous to directories in a filesystem. Creating a
namespace like `production.users.api` automatically creates the intermediate directories `production` and
`production.users` in FoundationDB's directory layer.

This hierarchy is useful for organizing data by environment, team, or service boundary. For example, a set of
microservices might use:

* `production.users`
* `production.orders`
* `production.products`

Each namespace is fully isolated: data stored in `production.users` is invisible to queries running in
`production.orders`.

## Session Scoping

A client session is always bound to exactly one namespace at a time. The `NAMESPACE USE` command switches the active
namespace for the current session. All subsequent commands (queries, inserts, index operations) operate within the
selected namespace until the session ends or another `NAMESPACE USE` is issued.

```kronotop
> NAMESPACE USE production.orders
OK

> NAMESPACE CURRENT
production.orders
```

Different sessions connected to the same cluster can operate in different namespaces concurrently.

## Cross-Namespace Transactions

`NAMESPACE USE` can be called inside an active transaction. The underlying FoundationDB transaction spans every
namespace
touched during the session, so `COMMIT` atomically applies all changes across namespaces and `ROLLBACK` discards them
all.

This works because `BEGIN` binds a single FoundationDB transaction to the session, while `NAMESPACE USE` only updates
which namespace subsequent commands target. It does not create a new transaction. Each command reads the current
namespace at execution time, so switching namespaces mid-transaction simply routes the next operations to a different
namespace within the same transaction.

```kronotop
> NAMESPACE USE production.sales
OK

> BEGIN
OK

> BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2}'
...

> NAMESPACE USE production.inventory
OK

> BUCKET.INSERT stock DOCS '{"item": "keyboard", "delta": -2}'
...

> COMMIT
OK
```

In this example, the insert into `production.sales` and the insert into `production.inventory` are committed as a
single atomic operation. If either fails, neither write is applied.

## Two-Phase Removal

Deleting a namespace in a distributed system requires coordination. A naive single-step delete could destroy data while
background workers (index maintenance, replication) or other cluster members still hold cached references to the
namespace. Kronotop therefore splits deletion into two phases:

1. **`NAMESPACE REMOVE`**: Marks the namespace as logically removed. A `NamespaceRemovedEvent` is published to the
   cluster journal. Each member that consumes this event invalidates caches, closes sessions bound to the namespace,
   and shuts down related workers.

2. **`NAMESPACE PURGE`**: Permanently deletes the FoundationDB directory. Before proceeding, the command enforces a
   **distributed sync barrier** that verifies every alive cluster member has observed the removal event. If the barrier
   is not satisfied, the command returns `BARRIERNOTSATISFIED` and should be retried. Typically a single retry is
   sufficient.

This two-phase approach guarantees that no cluster member references a namespace that has been physically deleted.

## Renaming

`NAMESPACE MOVE` renames a namespace by relocating its FoundationDB directory from the old path to a new path. After the
move, a **tombstone** is written under the old name. The tombstone acts as a barrier: `NAMESPACE CREATE` on the old name
is blocked until every alive cluster member has observed the move event. This prevents a member that still caches the
old namespace from serving stale data under a newly created namespace with the same name.

Once all members have observed the tombstone, it is automatically cleaned up.

## Reserved Names

The name `__internal__` is reserved at any level of the namespace hierarchy. Commands that accept a namespace path will
reject any path containing `__internal__` as a segment.

## Commands

| Command                                            | Description                            |
|----------------------------------------------------|----------------------------------------|
| [NAMESPACE CREATE](commands/namespace-create.md)   | Create a new namespace                 |
| [NAMESPACE REMOVE](commands/namespace-remove.md)   | Mark a namespace for logical removal   |
| [NAMESPACE PURGE](commands/namespace-purge.md)     | Permanently delete a removed namespace |
| [NAMESPACE MOVE](commands/namespace-move.md)       | Rename a namespace                     |
| [NAMESPACE USE](commands/namespace-use.md)         | Switch the session to a namespace      |
| [NAMESPACE CURRENT](commands/namespace-current.md) | Show the session's active namespace    |
| [NAMESPACE EXISTS](commands/namespace-exists.md)   | Check whether a namespace exists       |
| [NAMESPACE LIST](commands/namespace-list.md)       | List child namespaces under a path     |
