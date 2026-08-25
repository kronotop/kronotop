---
title: "Protocol Versions"
description: "Kronotop speaks RESP2 and RESP3. This page explains what a RESP2 client receives when a reply holds a type that only RESP3 has."
---

Kronotop speaks two protocol versions on the same port. A new connection starts in RESP2, and
[`HELLO`](commands/hello.md) switches it for the rest of the connection.

RESP3 is the better choice for new applications, and every example in this documentation uses
RESP3 output. This page is for clients that stay on RESP2.

## Type Mapping

RESP3 has types that RESP2 does not have. When a reply holds one of them, the server rewrites it
before sending, so a RESP2 client never receives a type it cannot parse. A reply that holds no
such type is sent as it is.

| RESP3 type      | What a RESP2 client receives                       |
|-----------------|----------------------------------------------------|
| map             | flat array of keys and values, field order is kept |
| set             | array                                              |
| boolean         | integer `1` for true, `0` for false                |
| double          | bulk string that holds 8 raw bytes, see below      |
| null            | bulk string with a length of -1                    |
| big number      | bulk string                                        |
| verbatim string | bulk string without the format prefix              |

The rewrite covers the whole reply, not only the outer level. A map nested three levels deep is
flattened the same way.

`TASK.ADMIN LIST` shows the difference. On a RESP3 connection:

```kronotop
127.0.0.1:3320> TASK.ADMIN LIST
1# journal:cleanup-task =>
   1# running => (false)
   2# finished => (false)
   3# started_at => (integer) 1752582119
   4# last_run => (integer) 0
```

The same reply on a RESP2 connection:

```kronotop
127.0.0.1:3320> TASK.ADMIN LIST
1) "journal:cleanup-task"
2) 1) "running"
   2) (integer) 0
   3) "finished"
   4) (integer) 0
   5) "started_at"
   6) (integer) 1752582119
   7) "last_run"
   8) (integer) 0
```

## Doubles Are Sent As Binary

This is the one case where a RESP2 client can go wrong without seeing an error.

RESP2 has no double type. The value is sent as a bulk string that holds the 8 byte IEEE 754
big-endian form of the number. It is not the number written as text, so printing it gives you a
few unreadable characters. Read the 8 bytes as a big-endian double instead. 

Doubles show up in the `BUCKET.VECTOR` score and in the `garbage_percentage` and `fill_ratio`
fields of `VOLUME.STATS`.

## Replies That Change Shape

For most commands the RESP2 reply is the RESP3 reply with the types swapped. A few commands use
a different shape instead: the field names are dropped and the position carries the meaning.

| Command                                                                                                    | RESP2 reply                 |
|------------------------------------------------------------------------------------------------------------|-----------------------------|
| [BUCKET.QUERY](../bucket/commands/bucket-query.md), [BUCKET.ADVANCE](../bucket/commands/bucket-advance.md) | `[cursor_id, [documents]]`  |
| [BUCKET.DELETE](../bucket/commands/bucket-delete.md), [BUCKET.UPDATE](../bucket/commands/bucket-update.md) | `[cursor_id, [object_ids]]` |
| [BUCKET.VECTOR](../bucket/commands/bucket-vector.md)                                                       | `[[score, document], ...]`  |

Each of those command pages shows both versions side by side.

## Which Version To Use

Use RESP3 unless your client library cannot. Maps arrive with their field names, so a new field
in a future release does not shift the position of the fields you already read. On RESP2 the
same reply is a flat array, and your parser depends on the order.
