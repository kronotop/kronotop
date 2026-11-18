---
title: "Transactions"
sidebar:
  label: "Overview"
description: "Kronotop transactions are thin wrappers around FoundationDB transactions."
---

## Overview

Kronotop transactions are thin wrappers around FoundationDB transactions. Every session (one TCP connection = one
session) can have at most one active transaction at a time. Transaction state (the FoundationDB transaction handle,
post-commit hooks, the user version counter, and the snapshot read flag) lives on the session object.

If a session disconnects or the client issues `SESSION.CLOSE` while a transaction is in progress, the transaction is
automatically rolled back.

## Auto-Commit

By default, sessions operate in auto-commit mode. Each command that touches FoundationDB creates its own transaction,
executes, and commits immediately. The client does not need to issue `BEGIN` or `COMMIT`.

```kronotop
> ZSET mykey 42
OK
```

In this mode every command is atomic in isolation, but consecutive commands are not grouped into a single atomic unit.

## Explicit Transactions

To group multiple commands into a single atomic unit, wrap them in a `BEGIN` / `COMMIT` block. While a transaction is
open, auto-commit is disabled: all commands share the same underlying FoundationDB transaction.

```kronotop
> BEGIN
OK

> ZSET key1 100
OK

> ZSET key2 200
OK

> COMMIT
OK
```

`ROLLBACK` discards all uncommitted changes and returns the session to auto-commit mode. After either `COMMIT` or
`ROLLBACK`, the session is back in auto-commit and ready for the next transaction.

`COMMIT` accepts an optional `RETURNING` clause to retrieve metadata from the committed transaction. Two arguments
are supported:

* `committed-version` returns the committed version as an integer.
* `versionstamp` returns the transaction's versionstamp as a bulk string.

```kronotop
> BEGIN
OK

> ZSET key1 42
OK

> COMMIT RETURNING committed-version
(integer) 1234567890
```

After a successful commit, any registered post-commit hooks are executed before the session returns to auto-commit.

### Transaction Inspection

Two commands provide visibility into an active transaction:

* `GETREADVERSION`: Returns the read version assigned to the transaction, useful for reasoning about causal ordering.
* `GETAPPROXIMATESIZE`: Returns the approximate byte size of mutations performed so far, useful for staying within
  FoundationDB's 10 MB transaction size limit.

```kronotop
> BEGIN
OK

> ZSET key1 42
OK

> GETREADVERSION
(integer) 1391961467874

> GETAPPROXIMATESIZE
(integer) 156

> COMMIT
OK
```

## Snapshot Reads

`SNAPSHOTREAD ON` switches the session to snapshot isolation for reads. Snapshot reads do not create read conflict
ranges, so they will not cause transactions to conflict with concurrent writes to the same keys. This is useful for
long-running or read-heavy workloads where strict serializability is not required.

```kronotop
> SNAPSHOTREAD ON
OK

> BEGIN
OK

> ZGET mykey
42

> COMMIT
OK
```

The setting applies to ZMap read commands (`ZGET`, `ZGETI64`, `ZGETF64`, `ZGETD128`, `ZGETRANGE`, `ZGETKEY`,
`ZGETRANGESIZE`) and `BUCKET.QUERY`. Mutation commands (`BUCKET.INSERT`, `BUCKET.DELETE`, `BUCKET.UPDATE`)
always use serializable reads regardless of the setting.

The setting is session-scoped and persists until explicitly changed with `SNAPSHOTREAD OFF`, until `SESSION.CLOSE`, or
until the session ends. It can be toggled at any time, regardless of whether a transaction is currently active.

## Cross-Namespace Transactions

`BEGIN` binds a single FoundationDB transaction to the session. `NAMESPACE USE` only changes which namespace subsequent
commands target. It does not create a new transaction. This means a single transaction can atomically span multiple
namespaces.

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

## FoundationDB Constraints

Kronotop transactions inherit the constraints of the underlying FoundationDB transactions. Kronotop stores metadata,
indexes, and cluster state in FoundationDB, while document bodies are stored in the Volume storage engine on the local
filesystem. The constraints below apply to the metadata stored in FoundationDB. Document body writes to Volume are not
subject to these limits.

| Constraint           | Limit                                                             |
|----------------------|-------------------------------------------------------------------|
| Transaction size     | 10 MB total (keys + values of all mutations)                      |
| Transaction duration | 5 seconds for reads and for commits with writes (see below)       |
| Default isolation    | Serializable (snapshot isolation available via `SNAPSHOTREAD ON`) |

Use `GETAPPROXIMATESIZE` to monitor transaction size during large writes. If a workload exceeds these limits, split the
work across multiple transactions.

### Transaction Time Window

Every FoundationDB transaction operates against a single point-in-time snapshot of the database, identified by a version
number called the **read version**. The read version is obtained once and remains fixed for the entire lifetime of the
transaction. All reads within the transaction see the database as it was at that version.

A transaction has two time limits, and both are 5 seconds:

* **Read window**: starts when the read version is obtained. This limit comes from FoundationDB.
* **Write limit**: starts at `BEGIN`. It applies only to a transaction that contains at least one write. This limit
  comes from Kronotop.

#### When is the read version obtained?

`BEGIN` creates a transaction object but does not obtain a read version, so the read window has not started yet. The
write limit starts counting at this point. The read version is obtained lazily:

* **First read operation**: When the transaction performs its first read (e.g., `ZGET`, `BUCKET.QUERY`), FoundationDB
  obtains the read version automatically. This is the most common case. All subsequent reads within the same transaction
  use the same read version; they do not obtain a new one.

* **Explicit `GETREADVERSION`**: Calling `GETREADVERSION` forces the read version to be obtained immediately, even if
  no read has been performed yet. This starts the read window.

* **Commit without prior reads**: If a transaction performs only writes (blind writes) and never reads, the read
  version is obtained at commit time. The read window cannot expire for such a transaction, but the write limit still
  applies. See [Transactions that write](#transactions-that-write).

#### What does the read window mean in practice?

Once the read version is obtained, you have approximately 5 seconds to complete and commit the transaction. If more than
5 seconds pass between obtaining the read version and committing, FoundationDB rejects the commit with a
`TRANSACTION_TOO_OLD` error.

Consider this sequence:

```kronotop
> BEGIN
OK                   -- transaction object created, no read version yet

> ZGET key1          -- read version obtained HERE, read window starts
"value1"

> ZGET key2          -- same read version, no new window
"value2"

-- 20 seconds pass --

> ZSET key3 100      -- mutation is buffered locally
OK

> COMMIT
(error) TRANSACTION_TOO_OLD Transaction is too old to perform reads or be committed
```

The commit fails because 20 seconds elapsed since the first `ZGET` obtained the read version. The later `ZGET` commands
did not reset the window. They reused the same read version.

#### Transactions that write

A transaction that contains at least one write must be committed within 5 seconds of `BEGIN`. The time counts from
`BEGIN`, even if the first write comes later. If the limit has passed, `COMMIT` fails with `TRANSACTION_TOO_OLD`, no
change is applied, and the transaction is closed. Run `BEGIN` again and repeat the work.

```kronotop
> BEGIN
OK                   -- write limit starts HERE

> ZSET key1 100      -- mutation is buffered locally
OK

-- 8 seconds pass --

> COMMIT
(error) TRANSACTION_TOO_OLD Transaction is too old to perform reads or be committed
```

FoundationDB does not set this limit for a transaction that only writes. Such a transaction obtains its read version at
commit time, so the read window cannot expire. Kronotop adds the limit so that every transaction with writes has a
limited lifetime. Replication relies on this to follow the writes of the primary in order.

A transaction with no writes has nothing to commit, so its `COMMIT` returns `OK` at any age. In such a transaction,
`TRANSACTION_TOO_OLD` comes from a read command that runs after the read window has closed.

#### Auto-commit mode

In auto-commit mode, each command gets its own transaction that is created, executed, and committed within a single
round-trip. Both limits start inside the command, so they matter only if a single command runs for more than 5 seconds.

## Commands

| Command                                              | Description                                      |
|------------------------------------------------------|--------------------------------------------------|
| [BEGIN](commands/begin.md)                           | Start a new transaction                          |
| [COMMIT](commands/commit.md)                         | Commit the current transaction                   |
| [ROLLBACK](commands/rollback.md)                     | Abort the current transaction                    |
| [GETREADVERSION](commands/getreadversion.md)         | Get the read version of the current transaction  |
| [GETAPPROXIMATESIZE](commands/getapproximatesize.md) | Get the approximate byte size of the transaction |
| [SNAPSHOTREAD](commands/snapshotread.md)             | Enable or disable snapshot read mode             |
| [TICK](commands/tick.md)                             | Get a monotonically increasing 64-bit integer    |
| [TRANSACTION LIST](commands/transaction-list.md)     | List the open transactions of this member        |
| [TRANSACTION KILL](commands/transaction-kill.md)     | Cancel an open transaction of this member        |
| [TRANSACTION SLOW](commands/transaction-slow.md)     | List the slow transactions of this member        |
