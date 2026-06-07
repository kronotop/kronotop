# Distributed Lock Demo

A runnable Java version of [recipes/distributed-lock-on-zmap.md](../../recipes/distributed-lock-on-zmap.md). It
connects to a live Kronotop server over RESP with Lettuce and exercises the lock end to end.

The lock exposes one primitive, `tryAcquire`, which returns immediately: a handle when it took the
lock, or `null` when the lock is held. ZMap has no watch primitive, so there is nothing to block on.
Waiting is the caller's choice: call `tryAcquire` once, or loop it (poll) with a short randomized
delay. The demo shows the poll loop at the call site, not hidden inside the lock.

## Run

Start a Kronotop server and initialize the cluster, then:

```
cd recipes/demo
mvn -q compile exec:java
```

Point it elsewhere with `mvn -q compile exec:java -Dexec.args="<host> <port>"` (default
`localhost 5484`).

Output:

```
== Part 1: contention and caller-side polling ==
A: acquired [token=1c3389a5582d29ab3ee2aa52db9f56d4 fencing=000000342d9c624f0000]
  resource: accepted (A writes while holding the lock) token 000000342d9c624f0000
A: released
B: acquired after 5 attempts [token=e5fb51a88fff356ec1aa7fd66772102c fencing=000000342da207ea0000]
  resource: accepted (B writes after acquiring) token 000000342da207ea0000
B: released

== Part 2: fencing token protects a paused holder ==
A: acquired with a 500ms lease [token=be86e27ffe425c5c82c6a76661afa646 fencing=000000342da2a3770000]
B: acquired the expired lock [token=dca2787dc27f6d5b6ca3b214e67616f9 fencing=000000342dadcec40000]
  resource: accepted (B writes as the new holder) token 000000342dadcec40000
A: wakes up and tries to write with its stale fencing token
  resource: REJECTED (A writes as a stale holder) token 000000342da2a3770000 <= high-water 000000342dadcec40000

== Part 3: renew holds the lock across a long task ==
A: acquired with a 500ms lease [token=5ce35323fa3e9c27e1b51143f7e6060f fencing=000000342dae6e400000]
A: running a task longer than the lease, renewing on a shorter interval
A: renew -> true, lease extended
B: tryAcquire -> null, still locked out
A: renew -> true, lease extended
B: tryAcquire -> null, still locked out
A: renew -> true, lease extended
B: tryAcquire -> null, still locked out
A: task done after 3 renewals, releasing
B: acquired now that A is done [token=994e6024f8b987038630f5856f9fd986 fencing=000000342dbdc4c30000]
```

## What the output shows

- **Contention and polling.** Client A holds the lock; client B's `tryAcquire` returns `null` while
  it is held, so B polls until A releases, then acquires and writes. The wait lives at B's call site.
- **Fencing protects a paused holder.** Client A takes a short lease and pauses past it without
  releasing. Client B sees the lease expired and acquires with a higher fencing token. When A wakes
  up still believing it holds the lock and writes, the resource rejects it because A's token is now
  lower than the highest it has seen. The fencing token, enforced by the protected resource, is what
  keeps a stale holder from acting.
- **Renew holds the lock across a long task.** Client A runs a task longer than its lease and renews
  on a shorter interval. Client B stays locked out even past the original lease deadline, because
  each renew extends it. Once A finishes and releases, B acquires.

