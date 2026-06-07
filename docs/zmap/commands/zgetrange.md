---
title: "ZGETRANGE"
description: "Retrieves an ordered range of key-value pairs from the ZMap ordered key-value store."
---

Retrieves an ordered range of key-value pairs from the ZMap ordered key-value store.

## Syntax

```kronotop
ZGETRANGE <begin> <end> [LIMIT count] [REVERSE] [BEGIN_KEY_SELECTOR selector] [END_KEY_SELECTOR selector]
```

## Parameters

| Parameter            | Type    | Required | Description                                                                                   |
|----------------------|---------|----------|-----------------------------------------------------------------------------------------------|
| `begin`              | bytes   | Yes      | The start key of the range. Use `*` for unbounded start (from the beginning of the subspace). |
| `end`                | bytes   | Yes      | The end key of the range. Use `*` for unbounded end (to the end of the subspace).             |
| `LIMIT`              | integer | No       | Maximum number of key-value pairs to return. Default is `100`.                                |
| `REVERSE`            | flag    | No       | When present, reverses the scan direction so results are returned in descending key order.    |
| `BEGIN_KEY_SELECTOR` | string  | No       | Controls how the begin boundary is resolved. Default is `first_greater_or_equal`.             |
| `END_KEY_SELECTOR`   | string  | No       | Controls how the end boundary is resolved. Default is `first_greater_than`.                   |

## Key Selectors

Key selectors control exactly which keys are included at the range boundaries.

| Selector                 | Description                                                                                             |
|--------------------------|---------------------------------------------------------------------------------------------------------|
| `first_greater_or_equal` | The first key greater than or equal to the specified key. This is the default for `BEGIN_KEY_SELECTOR`. |
| `first_greater_than`     | The first key strictly greater than the specified key. This is the default for `END_KEY_SELECTOR`.      |
| `last_less_than`         | The last key strictly less than the specified key.                                                      |
| `last_less_or_equal`     | The last key less than or equal to the specified key.                                                   |

With the default selectors, the begin key is **inclusive** and the end key is also **inclusive**. This is because
`first_greater_than` on the end key resolves to the first key *after* the specified end key, and FoundationDB uses a
half-open interval on resolved keys, so the specified end key itself is included.

## Return Value

Array: a list of `[key, value]` pairs. Each pair is a two-element array containing the key and its associated value as
bulk strings. Returns an empty array if no keys match the range.

## Behavior

`ZGETRANGE` reads an ordered range of key-value pairs from the ZMap subspace of the session's current namespace, backed
by FoundationDB.

With the default key selectors, the range is **both endpoints inclusive**: both the begin and end keys are included in
the results. FoundationDB natively uses half-open intervals; the default `END_KEY_SELECTOR first_greater_than` resolves
the end boundary to the key *after* the specified end key, which makes the specified end key inclusive within that
half-open interval.

The special value `*` can be used as a wildcard to represent an unbounded boundary:

- `*` as `begin`: starts the range from the very first key in the subspace.
- `*` as `end`: extends the range to the very last key in the subspace.

The `LIMIT` parameter caps the number of returned pairs. Combined with `REVERSE`, you can retrieve the last N entries in
a range.

Key selectors allow fine-tuning of the range boundaries. For example, using `BEGIN_KEY_SELECTOR first_greater_than`
excludes the begin key from the results.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the range
  read, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the range read is performed within the current transaction.

`ZGETRANGE` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not conflict
with concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same keys in different namespaces refer to different entries.

## Errors

| Error Code | Description                                                                                |
|------------|--------------------------------------------------------------------------------------------|
| `ERR`      | Wrong number of arguments, invalid LIMIT value, invalid key selector, or internal failure. |

## Examples

**Get an explicit key range (both-inclusive by default):**

```kronotop
> ZSET key-0 "a"
OK
> ZSET key-1 "b"
OK
> ZSET key-2 "c"
OK
> ZSET key-3 "d"
OK
> ZSET key-4 "e"
OK
> ZSET key-5 "f"
OK

> ZGETRANGE key-0 key-5
1) 1) "key-0"
   2) "a"
2) 1) "key-1"
   2) "b"
3) 1) "key-2"
   2) "c"
4) 1) "key-3"
   2) "d"
5) 1) "key-4"
   2) "e"
6) 1) "key-5"
   2) "f"
```

**Limit results:**

```kronotop
> ZGETRANGE key-0 key-5 LIMIT 3
1) 1) "key-0"
   2) "a"
2) 1) "key-1"
   2) "b"
3) 1) "key-2"
   2) "c"
```

**Reverse with limit (last 3 entries):**

```kronotop
> ZGETRANGE key-0 key-5 LIMIT 3 REVERSE
1) 1) "key-5"
   2) "f"
2) 1) "key-4"
   2) "e"
3) 1) "key-3"
   2) "d"
```

**Full subspace scan with `* *`:**

```kronotop
> ZGETRANGE * *
1) 1) "key-0"
   2) "a"
2) 1) "key-1"
   2) "b"
...
6) 1) "key-5"
   2) "f"
```

**Partial wildcard (from a specific key to the end):**

```kronotop
> ZGETRANGE key-2 *
1) 1) "key-2"
   2) "c"
2) 1) "key-3"
   2) "d"
3) 1) "key-4"
   2) "e"
4) 1) "key-5"
   2) "f"
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET mykey-a "alpha"
OK

> ZSET mykey-b "bravo"
OK

> COMMIT
OK

> BEGIN
OK

> ZGETRANGE mykey-a mykey-b
1) 1) "mykey-a"
   2) "alpha"
2) 1) "mykey-b"
   2) "bravo"

> COMMIT
OK
```
