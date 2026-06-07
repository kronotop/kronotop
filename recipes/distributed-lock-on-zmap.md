# Distributed Lock on ZMap

A lease-based distributed lock built on Kronotop's ZMap. The design is explained below,
with the RESP commands each operation issues, so you can implement it in any language with
a RESP client.

> **Draft.** This recipe is a work in progress and has not been widely reviewed. Distributed
> locking is easy to get subtly wrong, so read the reasoning critically and do not treat it as
> settled before using it in production. More eyes on the design and the demo are welcome.

## Why ZMap fits

ZMap is a RESP-compatible proxy over FoundationDB's ordered key-value API. This recipe
needs three building blocks: atomic acquire, safe release, and a fencing token. ZMap
supplies each directly.

- **Atomic acquire.** The acquire path reads the lock keys, checks whether they are
  free, and writes a new holder inside one transaction. Two blind writes to the same key
  would not conflict on their own, so the mutual exclusion comes from the read: a
  serializable `ZGET` adds a read conflict range on the lock keys. If two clients both
  read the keys as free and write, the winner commits, and the loser's read range now
  overlaps the winner's write, so FoundationDB rejects the loser at `COMMIT` with
  `NOT_COMMITTED`. Snapshot reads break this, since they add no read conflict range. The
  loser never sees a held read; it reads the keys as free and only learns it lost at
  commit, so it falls back into the same retry loop as a held attempt.
- **Safe release.** Release uses `ZMUTATE lock:NAME <token> COMPARE_AND_CLEAR`, which
  clears the identity key only when its stored value still equals our token. The token
  is a fixed 16-byte value that never changes for one acquisition, so the comparison is
  an exact byte match against an immutable value. Once the lease expires and another
  owner takes the key, the token differs and release does nothing, so a slow client
  cannot clear a lock it no longer holds.
- **Fencing token.** Acquire commits with `COMMIT RETURNING versionstamp`. The
  versionstamp increases monotonically across the cluster, so it works as a fencing
  token: pass it to the protected resource and reject any write carrying a lower token
  than the highest the resource has seen.

Acquire and release on their own do not give mutual exclusion. A client that pauses long
enough for its lease to expire can wake up still believing it holds the lock while a second
client has already acquired it. In that window both clients act as the holder. The fencing
token does not prevent the overlap; it lets the resource reject the stale holder's writes
once it has seen a higher token, which keeps the overlap harmless. Only the protected
resource can enforce this, by rejecting any write carrying a token lower than the highest it
has seen. Without that check this is a lease-based advisory lock, fine for coordinating
cooperating clients but not a guarantee that one client acts at a time. See Correctness and
limits for the exact comparison rule.

ZMap has no watch primitive, so acquire polls with a short randomized delay until the
lock is free or the wait window ends.

## Keys and values

Each lock uses two keys under a `lock:NAME` prefix, holding raw bytes rather than a
structured value. `NAME` is a placeholder for the lock's own name, the resource you are
guarding: a lock named `orders` uses `lock:orders` and `lock:orders:lease`. Splitting
the two keys keeps the immutable identity apart from the mutable lease, so release
compares only a value that never changes.

- `lock:NAME` stores the **token**: a 16-byte random value that identifies one
  acquisition and never changes while that acquisition holds the lock. It is what release
  and renew compare against, so it acts as proof of ownership: a unique per-acquisition
  identity that stops a stale holder from clearing or renewing a lock that has since been
  re-acquired under a different token. Pick 16 bytes from a good random source so two
  acquisitions never collide. Store nothing but the token bytes in this key. Release
  compares the whole stored value against the token byte for byte, so appending an owner
  label or any extra bytes makes the comparison fail and release silently does nothing,
  leaving the lock to expire only on its lease.
- `lock:NAME:lease` stores the **expiresAt** deadline as 8 bytes, big-endian epoch
  milliseconds. This is the only value renew rewrites.

`COMPARE_AND_CLEAR` matches the stored value byte for byte, so the value must have a
single byte representation. Raw bytes do. A serialized structure may not, because field
order or number formatting can shift between encodings and break the match.

The fencing token is returned to the caller rather than stored, since it is only known
after the commit.

## Operations

**acquire** is a single attempt, `tryAcquire`: it returns as soon as it has taken the lock
or found it held, and never loops on its own. A blocking acquire is just `tryAcquire` looped
with a short randomized delay until the lock is free or a wait window ends. One attempt:
`BEGIN`, then `ZGET lock:NAME` and `ZGET lock:NAME:lease`. Treat the lock
as free when the identity key is missing or `expiresAt` is in the past. Acquire writes
both keys in one transaction and no operation clears the lease key on its own, so a
present identity key always has a lease key beside it; a present identity key with a
missing lease key should never occur, but treat it as free too as a defensive guard.
When free, `ZSET` both keys with a fresh token and a new deadline, then `COMMIT
RETURNING versionstamp`, and return the holder state and the fencing token. When held,
`ROLLBACK` and retry after a short delay until the wait window ends. If the `COMMIT`
fails with `NOT_COMMITTED`, a conflicting write landed on the lock keys, either another
client's acquire or a release; a failed commit already ends the transaction, so skip the
`ROLLBACK` and retry from a fresh `BEGIN` after a short delay, otherwise as in the held
case.

Run acquire with serializable reads, which is the default. If the session has
`SNAPSHOTREAD ON`, the reads create no conflict range and two clients can both commit,
which breaks mutual exclusion. Keep snapshot reads off on the lock connection.

**release**: a single `ZMUTATE lock:NAME <token> COMPARE_AND_CLEAR` against our token,
issued with no transaction open on the connection. As an FDB atomic mutation it adds a
write conflict range on the identity key but no read conflict range, so the release
itself can never fail with `NOT_COMMITTED`. That write is not invisible to conflict
detection: the mutation declares its write conflict range whether or not the stored value
matches, so even a release that clears nothing aborts an in-flight serializable acquire
or renew that already read the identity key. Run release outside
`BEGIN`/`COMMIT`. If a transaction is open on the session, the `ZMUTATE` joins it and
does nothing until that transaction commits, so a release issued before a pending renew
has committed would not take effect. It clears only the identity key. The lease key is
left behind and is harmlessly overwritten by the next acquire, which treats a missing
identity key as free, so clearing it is unnecessary and would only force a transaction.

**renew**: extend the lease. `BEGIN`, `ZGET lock:NAME` and `ZGET lock:NAME:lease`, confirm
the token is still ours and the lease key is present with an `expiresAt` still in the
future, `ZSET lock:NAME:lease` with a new `expiresAt`, then `COMMIT`. Renew never touches
the identity key, so release stays valid across renewals. A matching token is not proof
the lease is still live: expiry never clears the identity key, so a holder whose lease has
already lapsed still finds its own token there. Extending on a matching token alone would
resurrect a lease the holder already forfeited, since during the expiry gap the lock was
free for another client to take. Treat a missing lease key or an `expiresAt` in the past as
lost even when the token still matches. If the token is no longer ours or the lease has
already expired, the lock was lost: stop the protected work and do not retry the renew.

A `NOT_COMMITTED` at the renew's `COMMIT` is not a verdict. It means a conflicting write
landed on the lock keys, and there are two writers that can produce it: another client's
acquire, which did take the lock, or a stale ex-holder's release whose
`COMPARE_AND_CLEAR` did not match and cleared nothing, which left the lock ours. A no-op
release still conflicts because the mutation declares its write conflict range whether or
not the value matches. The failed commit cannot tell the two apart, so do not treat it as
lost: retry the renew from a fresh `BEGIN` and let the re-read decide. If the token is
still ours and the lease is still in the future, extend it; otherwise the lock was
genuinely lost. Renew on an interval shorter than the lease to hold the lock across a
long task.

Renew depends on the same serializable read as acquire, so it must also run with snapshot
reads off. The token check alone is not enough: with `SNAPSHOTREAD ON` the `ZGET` adds no
conflict range, so a renew that reads a stale token as still ours can extend the lease and
commit even after another client has re-acquired the lock, leaving both clients believing
they hold it. With serializable reads, that re-acquire either makes the renew read the new
token (so the check fails) or forces `NOT_COMMITTED` at commit.

## RESP commands

- `BEGIN`, `COMMIT`, `COMMIT RETURNING versionstamp`, `ROLLBACK`
- `ZGET lock:NAME`, `ZGET lock:NAME:lease`
- `ZSET lock:NAME <token>`, `ZSET lock:NAME:lease <expiresAt>`
- `ZMUTATE lock:NAME <token> COMPARE_AND_CLEAR`

Hold one connection per lock. A connection is one session, so `BEGIN` through `COMMIT`
stays on a single transaction.

One session is not safe to share across threads. Every command on a lock runs on its
single connection, so all calls must come from one thread. The long-task pattern, where
renew runs on a background timer while another thread runs the protected work and later
calls release, breaks this: two threads issuing commands on one session interleave their
bytes and corrupt the open transaction. A release sent while a renew's transaction is
still open also joins that transaction and only takes effect at its commit, so it does not
release when you think it does. Either drive renew and release from the same thread, or
guard them with your own mutual exclusion so only one command is in flight on the
connection at a time.

## Correctness and limits

- The lease deadline uses client wall-clock time, and acquire compares it against the
  acquirer's own clock. A contender whose clock runs ahead can read a live lease as
  expired and take the lock while the holder, renewing on time, still owns it by its own
  clock. The holder only learns of the loss at its next renew, so both act as the holder
  for up to a renew interval. No pause is needed for this overlap; clock skew alone
  produces it. The fencing token does not prevent it, but the resource rejects the stale
  holder's writes once it has seen a higher token, so use it for any operation a stale
  holder must not perform.
- The fencing token is the 10-byte versionstamp returned by the acquire commit as a bulk
  string. The resource must compare it as an unsigned big-endian byte string, where a
  lower token is the lexicographically smaller one. Comparing it as a signed integer
  breaks fencing. The resource must also apply its highest-token-seen check and update
  atomically. If two stale writers read the same high-water mark before either updates
  it, both pass the check and fencing fails.
- Fencing rejects a stale write only after the resource has seen a higher token. Until the
  new holder's first fenced write advances the high-water mark, a paused old holder whose
  token still equals the current mark can still write, so for a short window both holders
  can write. Have the new holder advance the mark as its first action after acquiring to
  keep that window small. For a resource that lives outside Kronotop, the window between
  acquire and the first fenced write cannot be closed completely.
- The lock operates in the session's active namespace. The default is `global`; switch
  with `NAMESPACE USE <namespace>` before acquiring if needed.

## When to use it

This is a lease-based advisory lock for coarse-grained coordination of a resource that
lives outside Kronotop. If the resource you are guarding lives inside Kronotop or
FoundationDB, do not use a lock at all: a single serializable transaction gives you real
mutual exclusion with no lease and no clock involved.

### Good fits:

- A singleton background job across a fleet: only one instance runs the nightly
  aggregation, the cleanup pass, or the periodic sync. Occasional double runs during a
  handoff are acceptable, or the job is idempotent.
- Holding a long task to one runner at a time, with renew keeping the lease alive across
  the task.
- One owner per partition, queue, or external consumer slot.
- Writing to an external store that can enforce the fencing token, such as a conditional
  write with a version check. Here the lock is more than advice: the store stays
  consistent because it rejects a stale holder's write once it has seen the newer holder's
  token, though that takes effect only after the newer holder's first fenced write.

### Poor fits:

- Exactly-once or money-correct work where two actors must never both act and the
  resource cannot enforce the fencing token. The lock alone does not guarantee this.
  Pair it with an idempotency key, or choose a different design.
- Low-latency, high-contention mutual exclusion. Acquire polls rather than blocking, so
  it is meant for coordination, not a hot-path mutex.
- Work that needs fairness or ordered handoff. There is no queue, so a waiter can starve.

In short: good for loosely timed coordination, deciding which single client runs or owns
something. Strict safety comes only when the protected resource enforces the fencing token.

## Further reading

- Martin
  Kleppmann, [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- Martin Kleppmann, *Designing Data-Intensive Applications* (O'Reilly, 2017)
