---
title: "ZWATCH"
description: "Blocks the connection until the value at a key changes, then wakes the caller."
---

Blocks the connection until the value at a key changes, then wakes the caller.

## Syntax

```kronotop
ZWATCH <key>
```

## Parameters

| Parameter | Type  | Required | Description        |
|-----------|-------|----------|--------------------|
| `key`     | bytes | Yes      | The key to watch.  |

## Return Value

Simple string: `OK` when the watch fires. The signal carries no value.

On failure, the command returns a RESP error. There is no timeout and no `nil` reply: the command ends either by firing
or by error.

## Behavior

`ZWATCH` registers a watch on the key and blocks the connection until the value differs from what it was when the watch
was registered, then returns `OK`. The signal does not include the new value. Read the current value with `ZGET` after the
signal. Deleting a key that holds a value wakes the watch, since the value goes from present to absent. Deleting a key
that is already absent does not, since `ZDEL` returns `OK` but the value has not changed.

A watch is registered on its own snapshot, and that snapshot is committed before the command starts waiting. For this reason
`ZWATCH` cannot take part in an explicit transaction. Issued after `BEGIN`, it is rejected with an error.

When the connection closes while the command is blocked, the client stops waiting and its hold on the key is released.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries,
so two sessions in different namespaces that watch the same key name watch different values.

`ZWATCH` is an edge-triggered wakeup, not a change feed. It reports that the value differs from when the watch was registered,
not the sequence of changes that produced the difference. The usual pattern is a loop: issue `ZWATCH`, and on each signal
read the value with `ZGET` and re-issue `ZWATCH`.

## Guarantees

What `ZWATCH` guarantees:

- It wakes when the value at the key differs from what it was when the watch was registered.
- A single `ZWATCH` call returns at most once. It either fires once or fails.
- A change in another namespace never wakes it.
- When the connection closes while blocked, the watch stops consuming resources on the server.

What `ZWATCH` does not guarantee:

- **It tracks the value, not writes.** The watch reacts to a difference between the current value and the value when it
  was registered, not to the act of writing. Writing a key's current content back to it might not wake the watch, since the
  value has not changed. Deleting a key that is already absent is the same case: `ZDEL` returns `OK`, but with no change in
  value the watch does not wake. Deleting a key that holds a value does wake it, since the value goes from present to absent.
- **No intermediate values.** Several changes committed before the signal produce a single wake. If a key is set to
  `A`, then `B`, then `C` before the watch fires, the client receives one signal and `B` is never observed.
- **No coverage of the re-registration gap.** Changes committed between a signal and the next `ZWATCH` are not reported. A client
  that must observe every transition cannot get it from `ZWATCH`.
- **Change-and-revert may be silent.** A value that changes and reverts to its previous content before the signal may
  wake the watch or stay silent.
- **No value in the signal.** The client must read the current value with `ZGET` after each signal.
- **No timeout.** The command blocks until it fires or fails. It never returns `nil`.
- **It can fail instead of firing.** A watch may end with an error rather than a signal, for example when the
  per-database limit on outstanding watches is reached, on storage server failure, on network loss, or on cancellation.
  The client re-issues `ZWATCH` after an error.

## Shared watches and the watch limit

Any number of clients waiting on the same key shares a single underlying FoundationDB watch. This bounds the number of
underlying watches to the number of distinct watched keys, not the number of waiting clients, so a key consumed by many
clients does not exhaust the watch budget.

A client that joins a key already being watched shares the baseline of the first waiter rather than its own. It may
therefore wake on a change that was committed before it joined. This wake is harmless, since the client reads the
current value with `ZGET` regardless.

FoundationDB caps the number of outstanding watches per database. Sharing removes exhaustion by many clients on one key,
but not exhaustion by many distinct keys, since a watch is still spent per key. When the database is already at its
limit, the next client to register a previously unwatched key receives a RESP error. Clients already waiting on registered keys
are unaffected.

## Errors

| Error Code | Description                                                              |
|------------|--------------------------------------------------------------------------|
| `ERR`      | Wrong number of arguments, watch failure, or internal failure.           |
| `ERR`      | `ZWATCH is not allowed within a transaction` when issued inside `BEGIN`. |

## Examples

**Block on a key and wake on a change from another connection.**

Connection A blocks on the key:

```kronotop
> ZWATCH mykey
```

Connection B changes the key:

```kronotop
> ZSET mykey "new value"
OK
```

Connection A wakes and reads the current value:

```kronotop
OK

> ZGET mykey
"new value"
```

**Reject inside an explicit transaction.**

```kronotop
> BEGIN
OK

> ZWATCH mykey
(error) ERR ZWATCH is not allowed within a transaction
```
