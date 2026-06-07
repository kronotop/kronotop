---
title: "ZGETKEY"
description: "Resolves and returns the key name that matches a key selector relative to a reference key in the ZMap ordered key-value store."
---

Resolves and returns the key name that matches a key selector relative to a reference key in the ZMap ordered key-value
store.

## Syntax

```kronotop
ZGETKEY <key> [KEY_SELECTOR selector]
```

## Parameters

| Parameter      | Type   | Required | Description                                                      |
|----------------|--------|----------|------------------------------------------------------------------|
| `key`          | bytes  | Yes      | The reference key for the selector lookup.                       |
| `KEY_SELECTOR` | string | No       | The key selector strategy. Defaults to `first_greater_or_equal`. |

## Key Selectors

| Selector                 | Description                                                                            |
|--------------------------|----------------------------------------------------------------------------------------|
| `first_greater_or_equal` | Returns the first key greater than or equal to the reference key. This is the default. |
| `first_greater_than`     | Returns the first key strictly greater than the reference key.                         |
| `last_less_than`         | Returns the last key strictly less than the reference key.                             |
| `last_less_or_equal`     | Returns the last key less than or equal to the reference key.                          |

## Return Value

Bulk string: the resolved key name, or `nil` if no key matches the selector.

## Behavior

`ZGETKEY` resolves a key using FoundationDB's KeySelector mechanism. Instead of returning a value, it returns the *key
name* that satisfies the given selector relative to the reference key. This enables cursor-like navigation over the
ordered keyspace: finding the next key, previous key, or nearest match without knowing exact key names ahead of time.

The default selector is `first_greater_or_equal`, which returns the reference key itself if it exists, or the next key
in order if it does not.

If no key in the keyspace satisfies the selector, the command returns `nil`.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the read,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the read is performed within the current transaction.

`ZGETKEY` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not conflict
with concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code | Description                                                           |
|------------|-----------------------------------------------------------------------|
| `ERR`      | Wrong number of arguments, invalid key selector, or internal failure. |

## Examples

**Get an exact key (default selector):**

```kronotop
> ZSET mykey "Hello"
OK

> ZGETKEY mykey
"mykey"
```

**Get the next key after a reference key:**

```kronotop
> ZSET key-0 "value-0"
OK

> ZSET key-1 "value-1"
OK

> ZGETKEY key-0 KEY_SELECTOR first_greater_than
"key-1"
```

**Get the previous key before a reference key:**

```kronotop
> ZSET key-0 "value-0"
OK

> ZSET key-1 "value-1"
OK

> ZGETKEY key-1 KEY_SELECTOR last_less_than
"key-0"
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET key-0 "value-0"
OK

> ZSET key-1 "value-1"
OK

> ZGETKEY key-0 KEY_SELECTOR first_greater_than
"key-1"

> COMMIT
OK
```
