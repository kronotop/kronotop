# Distributed Lock on ZMap

A lease-based distributed lock built on Kronotop's ZMap. The design is explained below,
with the RESP commands each operation issues, so you can implement it in any language with
a RESP client.

> **Draft.** This recipe is a work in progress and has not been widely reviewed. Distributed
> locking is easy to get subtly wrong. Read the reasoning critically, and do not treat it as
> settled before you use it in production. More eyes on the design are welcome.

> **Disclosure**: Claude Opus 5 was used to harden the algorithm, find edge cases, and proofread.

## Why ZMap fits

ZMap is a RESP-compatible proxy over FoundationDB's ordered key-value API. This recipe
needs four building blocks: atomic acquire, safe release, a fencing token, and a way to wait
without polling. ZMap supplies each directly.

- **Atomic acquire.** The acquire path reads the lock keys, checks whether they are free,
  and writes a new holder inside one transaction. Two blind writes to the same key would
  not conflict on their own. The mutual exclusion comes from the read: a serializable
  `ZGET` adds a read conflict range on the lock keys. Say two clients both read the keys as
  free and write. The winner commits. The loser's read range now overlaps the winner's
  write, so FoundationDB rejects the loser at `COMMIT` with `NOT_COMMITTED`. Snapshot reads
  break this, because they add no read conflict range. The loser never reads the lock as
  held. It reads the keys as free and only learns it lost at commit, so it retries like any
  attempt that found the lock held.
- **Safe release.** Release uses `ZMUTATE lock:NAME <token> COMPARE_AND_CLEAR`. It clears
  the identity key only when the stored value still equals our token. The token is a fixed
  16-byte value that never changes for one acquisition. The comparison is therefore an
  exact byte match against a value that cannot move. Once the lease expires and another
  owner takes the key, the token differs and release does nothing. A slow client cannot
  clear a lock it no longer holds.
- **Fencing token.** Acquire commits with `COMMIT RETURNING versionstamp`. The versionstamp
  increases monotonically across the cluster, so it works as a fencing token. Pass it to
  the protected resource, and reject any write that carries a lower token than the highest
  the resource has seen.
- **Blocking wait.** `ZWATCH` blocks the connection until the value at a key changes. A
  waiter blocks on the identity key instead of asking for it on a timer. A matching release
  is itself the change that wakes it. The wait still needs a limit, because some ways of
  losing a lock write nothing at all. See Waiting for the lock.

Acquire and release give mutual exclusion only while the holder stays inside its lease. A
client can pause long enough for its lease to expire. It then wakes up still believing it
holds the lock, while a second client has already acquired it. In that window both act as
the holder. The fencing token does not prevent the overlap. It lets the resource reject the
stale holder's writes once it has seen a higher token, which keeps the overlap harmless.
Only the protected resource can enforce this. Without that check you have a lease-based
advisory lock. That is fine for coordinating cooperating clients, but it is not a guarantee
that one client acts at a time. See Correctness and limits for the exact comparison rule.

## Keys and values

Each lock uses two keys under a `lock:NAME` prefix. They hold raw bytes rather than a
structured value. `NAME` is a placeholder for the lock's own name, the resource you are
guarding: a lock named `orders` uses `lock:orders` and `lock:orders:lease`. Splitting the
two keys keeps the immutable identity apart from the mutable lease, so release compares
only a value that never changes.

- `lock:NAME` stores the **token**: a 16-byte random value that identifies one acquisition.
  It never changes while that acquisition holds the lock. Release and renew compare against
  it, so it acts as proof of ownership. It stops a stale holder from clearing or renewing a
  lock that has since been re-acquired under a different token. Pick 16 bytes from a good
  random source so two acquisitions never collide. Store nothing but the token bytes in
  this key. Release compares the whole stored value against the token byte for byte. An
  owner label or any extra byte makes the comparison fail, and release then does nothing.
  The lock would go away only when its lease runs out.
- `lock:NAME:lease` stores the **expiresAt** deadline as 8 bytes, big-endian epoch
  milliseconds. This is the only value renew rewrites.

`COMPARE_AND_CLEAR` matches the stored value byte for byte, so the value must have a single
byte representation. Raw bytes do. A serialized structure may not, because field order or
number formatting can shift between encodings and break the match.

The fencing token is returned to the caller rather than stored, because it is only known
after the commit.

## Operations

**acquire** is a single attempt, `tryAcquire`. It returns as soon as it has taken the lock
or found it held, and it never loops on its own. A blocking acquire runs `tryAcquire` in a
loop until the lock is free or a wait window ends. Waiting for the lock describes the wait
between attempts.

One attempt: `BEGIN`, then `ZGET lock:NAME` and `ZGET lock:NAME:lease`. Treat the lock as
free when the identity key is missing, or when `expiresAt` is in the past. Acquire writes
both keys in one transaction, and no operation clears the lease key on its own. So an
identity key always has a lease key beside it. The other case should never happen. If you
do see it, treat the lock as free.

When the lock is free, `ZSET` both keys with a fresh token and a new deadline. Then `COMMIT
RETURNING versionstamp`. Return the token, the deadline you wrote, and the fencing token.
When the lock is held, send `ROLLBACK`, wait, and retry until the wait window ends.

**When the commit conflicts.** A `COMMIT` that fails with `NOT_COMMITTED` means another
write reached the lock keys first. It was another client's acquire, or a release. The
failed commit already ends the transaction, so do not send `ROLLBACK`. Wait a short moment,
then start again from a fresh `BEGIN`.

**An unknown commit result.** A `COMMIT` can also fail with `COMMIT_UNKNOWN_RESULT`. The
transaction may or may not have committed. Release and renew survive this by retrying:
`COMPARE_AND_CLEAR` on the same token is idempotent, and a renew retry re-reads the lock
and decides again. Acquire is the case that needs care. The lock may now be held under the
token you just wrote, but the commit returned no versionstamp, so you have no fencing
token. Do not retry acquire blindly. The retry would read your own token as a live holder
and wait out the whole lease for nothing. Read `lock:NAME` instead. If it holds your token,
the acquire committed. Release it with `COMPARE_AND_CLEAR` and acquire again to get a
fencing token. If you do not use the token, keep the lock as it is. If it holds anything
else, or nothing, the acquire did not commit and you can retry.

**release**: a single `ZMUTATE lock:NAME <token> COMPARE_AND_CLEAR` against our token,
issued with no transaction open on the connection.

**A release cannot lose a conflict.** As a FoundationDB atomic mutation, it adds a write
conflict range on the identity key but reads nothing. It has no read that can go stale, so
it cannot lose a conflict on the lock keys. Conflict detection still sees the write: the
mutation declares its write conflict range whether or not the stored value matches. So even
a release that clears nothing aborts an acquire or a renew that is in flight and has
already read the identity key.

**Run it outside a transaction.** If a transaction is open on the session, the `ZMUTATE`
joins it. It then does nothing until that transaction commits. A release sent before a
pending renew has committed would not take effect when you think it does.

**It clears only the identity key.** The lease key is left behind. The next acquire
overwrites it harmlessly, because a missing identity key already means the lock is free.
Clearing it is unnecessary, and it would only force a transaction.

**renew**: extend the lease. `BEGIN`, then `ZGET lock:NAME` and `ZGET lock:NAME:lease`.
Confirm the identity key is present and still holds our token, and that the lease key is
present with an `expiresAt` still in the future. Then `ZSET lock:NAME:lease` with a new
`expiresAt`, and `COMMIT`. Renew never touches the identity key, so release stays valid
across renewals.

**Check the token and the lease together.** A matching token is not proof the lease is
still live. Expiry never clears the identity key, so a holder whose lease has already
lapsed still finds its own token there. Extending on a matching token alone would bring
back a lease the holder already lost. During the expiry gap the lock was free for another
client to take. Treat a missing lease key, or an `expiresAt` in the past, as a lost lock,
even when the token still matches. When the lock is lost, stop the protected work and do
not retry the renew.

**Stop at your own deadline.** The lease can also end without a failed renew. Check your
own deadline before each step of the protected work, and stop when your clock passes
`expiresAt`. A process that is paused long enough never gets to run its renew at all, so
your own clock is the only signal left.

**Read again before you give up the lock.** A `NOT_COMMITTED` at the renew's `COMMIT` does
not tell you the lock is lost. It means a conflicting write reached the lock keys, and two
writers can produce it. One is another client's acquire, which did take the lock. The other
is a stale ex-holder's release whose `COMPARE_AND_CLEAR` matched nothing and cleared
nothing, which leaves the lock ours. A release that clears nothing still conflicts, as
described under release above. The failed commit cannot tell the two apart, so retry the
renew from a fresh `BEGIN` and let the re-read decide. If the token is still ours and the
lease is still in the future, extend it. Otherwise the lock was genuinely lost.

**Sizing the lease.** Renew on an interval shorter than the lease to hold the lock across a
long task. Size the lease so it covers a renew that is slow but still succeeds, plus the
clock skew you expect between clients. A lease of a few times the renew interval leaves
room for one or two lost round trips. A short lease frees a dead holder's lock sooner, but
it makes a live holder lose the lock after one slow round trip.

**Keep snapshot reads off.** Acquire and renew both need serializable reads, which is the
default. With `SNAPSHOTREAD ON` the reads create no conflict range. Two clients can then
both acquire and both commit, which breaks mutual exclusion. Renew loses the same
protection: a renew that reads a stale token as still ours can extend the lease and commit,
even after another client has re-acquired the lock. Both clients then believe they hold the
lock. With serializable reads, that re-acquire either makes the renew read the new token, so
the check fails, or it forces `NOT_COMMITTED` at commit. Keep snapshot reads off on the
lock connection.

## Waiting for the lock

`ZWATCH` blocks the connection until the value at a key changes, then returns. A waiter
uses it to sleep until the lock moves, instead of asking again on a timer. It removes the
polling delay from a normal handoff, but a waiter still needs a bounded wait. `ZWATCH` is
rejected inside a transaction, so the connection it runs on never issues `BEGIN`.

**Watch `lock:NAME`, not `lock:NAME:lease`.** The identity key changes only when the lock
changes hands: a matching release clears it, and the next acquire writes a new token. Every
renew rewrites the lease key, so watching that one wakes the waiter again and again while
the holder is still working.

**The wait needs a limit.** The wait limit is the longest a waiter stays blocked before it
gives up and tries again. Two changes of ownership cannot reach the waiter through a watch,
and either one on its own is enough to make the limit necessary:

- A lease running out is not a write. If the holder dies without releasing, nothing touches
  the identity key. The watch stays silent while the lock is in fact free.
- Nothing tells the client when the server has registered the watch. A release can land
  between the `ZGET` that read the lock as held and that registration. The watch then
  starts from a value that is already cleared, and it waits for a change that already
  happened.

Set the wait limit to whichever comes first: the end of the wait window, or the `expiresAt`
read during the attempt. The two are different things. The wait window is how long the
caller keeps trying at all, while the wait limit bounds one block on `ZWATCH`. A normal
handoff fires the watch well before either one, so the limit only matters in the two cases
above. When it is reached, the waiter simply attempts again.

**Reaching the wait limit means closing a connection.** `ZWATCH` takes no timeout, and
there is no command to cancel it. Dropping the connection is the only way to stop waiting.
A waiter holds no lock state yet, so its blocked connection is disposable. Give the watch
its own connection and leave the lock connection alone. That way a closed connection never
costs the lock connection its `NAMESPACE USE`. Both connections must be in the same
namespace, because keys are scoped to it. A timer on another thread closes the watch
connection once the limit passes. A watch that fires normally leaves its connection idle
and clean, so it is reused for the next wait and rebuilt only after a limit. A rebuilt
connection starts fresh, so it needs its `NAMESPACE USE` again before the next `ZWATCH`.

**Keep a short randomized delay before the next attempt.** Everyone waiting on the key
wakes from the same signal and races for the lock. One wins, and the rest fail at `COMMIT`
with `NOT_COMMITTED`. Spreading the retries keeps that race small.

A `NOT_COMMITTED` at the acquire's own `COMMIT` is a different case. That attempt read the
lock as free, so it has no `expiresAt` to build a limit from. Retry after a short delay
rather than watching.

**A watch can fail instead of firing.** Reaching the FoundationDB limit on outstanding
watches is one such failure. Treat the error as a wake with no change to read: sleep the
rest of the wait limit and attempt again. The lock stays correct. It only loses the fast
handoff until a watch is available again.

## RESP commands

- `BEGIN`, `COMMIT`, `COMMIT RETURNING versionstamp`, `ROLLBACK`
- `ZGET lock:NAME`, `ZGET lock:NAME:lease`
- `ZSET lock:NAME <token>`, `ZSET lock:NAME:lease <expiresAt>`
- `ZMUTATE lock:NAME <token> COMPARE_AND_CLEAR`
- `ZWATCH lock:NAME`

Hold one connection per lock. A connection is one session, so `BEGIN` through `COMMIT`
stays on a single transaction. A waiting acquire holds a second, disposable connection for
its `ZWATCH`, in the same namespace as the first.

One session is not safe to share across threads. Every command on a lock runs on its single
connection, so all calls must come from one thread. The long-task pattern breaks this rule:
renew runs on a background timer while another thread runs the protected work and later
calls release. Two threads issuing commands on one session interleave their bytes and
corrupt the open transaction. A release sent while a renew's transaction is still open also
joins that transaction, and it then takes effect only at that commit. Either drive renew
and release from the same thread, or guard them with your own mutual exclusion. Only one
command may be in flight on the connection at a time. Closing the watch connection at the
wait limit is the one thing another thread does. It sends no command, and it touches only
the watch connection.

## Correctness and limits

- The lease deadline uses client wall-clock time, and acquire compares it against the
  acquirer's own clock. A contender whose clock runs ahead can read a live lease as expired
  and take the lock. The holder renews on time and still owns the lock by its own clock.
  The holder only learns of the loss at its next renew, so both act as the holder for up to
  a renew interval. Clock skew alone produces this overlap, with no pause involved. The
  fencing token does not prevent it. The resource rejects the stale holder's writes once it
  has seen a higher token. Use the token for any operation a stale holder must not perform.
- The fencing token is the 10-byte versionstamp returned by the acquire commit as a bulk
  string. The resource must compare it as a big-endian byte string, where a lower token is
  the lexicographically smaller one. Compare the whole 10 bytes. A comparison that reads
  only part of the token, or that reorders its bytes, breaks fencing. The check against the
  highest token seen, and the update of that value, must also be atomic. If two stale
  writers read the same highest token seen before either updates it, both pass the check
  and fencing fails.
- Fencing rejects a stale write only after the resource has seen a higher token. Until the
  new holder's first fenced write raises the highest token seen, a paused old holder whose
  token still equals that value can still write. For a short window both holders can write.
  Have the new holder raise the value as its first action after acquiring, to keep that
  window small. For a resource that lives outside Kronotop, the window between acquire and
  the first fenced write cannot be closed completely.
- Each distinct key costs one watch. Many clients waiting on one lock share a single watch
  and cost one between them. A client waiting on many different lock names spends one watch
  each, and it can reach the FoundationDB limit on outstanding watches. Past the limit
  `ZWATCH` returns an error and the waiter falls back to sleeping.
- A waiter can wake for something that is not a handoff, such as another client's acquire
  writing a new token over an expired lease. The wake costs nothing, because the waiter
  re-reads the lock on its next attempt and finds it held.
- The lock operates in the session's active namespace. The default is `global`. Switch with
  `NAMESPACE USE <namespace>` before acquiring if you need another one.

## When to use it

This is a lease-based advisory lock for coarse-grained coordination of a resource that
lives outside Kronotop. If the resource you are guarding lives inside Kronotop or
FoundationDB, do not use a lock at all. A single serializable transaction gives you real
mutual exclusion, with no lease and no clock involved.

### Good fits

- A singleton background job across a fleet: only one instance runs the nightly
  aggregation, the cleanup pass, or the periodic sync. Occasional double runs during a
  handoff are acceptable, or the job is idempotent.
- Holding a long task to one runner at a time, with renew keeping the lease alive across
  the task.
- One owner per partition, queue, or external consumer slot.
- Writing to an external store that can enforce the fencing token, such as a conditional
  write with a version check. Here the lock is more than advice. The store stays consistent,
  because it rejects a stale holder's write once it has seen the newer holder's token. That
  protection starts only after the newer holder's first fenced write.

### Poor fits

- Work that moves money, or any exactly-once work where two actors must never both act and
  the resource cannot enforce the fencing token. The lock alone does not guarantee this.
  Pair it with an idempotency key, or choose a different design.
- Low-latency, high-contention mutual exclusion. Acquire blocks on a watch rather than
  polling. But every waiter wakes on the same signal and races, and the losers retry after a
  failed commit. It is built for coordination, not for a hot-path mutex.
- Work that needs fairness or ordered handoff. There is no queue, so a waiter can starve.

Use it for coordination that does not need exact timing, to decide which single client runs
or owns something. Strict safety comes only when the protected resource enforces the
fencing token.

## Further reading

- Martin Kleppmann, [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- Martin Kleppmann, *Designing Data-Intensive Applications* (O'Reilly, 2017)
