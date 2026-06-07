---
title: "BUCKET.CLOSE"
sidebar:
  order: 8
description: "Closes a cursor and releases its associated query context from the session."
---

Closes a cursor and releases its associated query context from the session.

## Syntax

```kronotop
BUCKET.CLOSE <operation> <cursor-id>
```

## Parameters

| Parameter   | Type    | Required | Description                                                                                          |
|-------------|---------|----------|------------------------------------------------------------------------------------------------------|
| `operation` | string  | Yes      | The operation type. Must be `QUERY`, `DELETE`, or `UPDATE`.                                          |
| `cursor-id` | integer | Yes      | The cursor ID returned by the initial command (`BUCKET.QUERY`, `BUCKET.DELETE`, or `BUCKET.UPDATE`). |

## Return Value

Returns `OK` on success.

## Errors

| Error Code | Description                                                                                 |
|------------|---------------------------------------------------------------------------------------------|
| `ERR`      | `no cursor found` if the cursor does not exist or was already closed.                       |
| `ERR`      | `Unknown '<operation>' action` if the operation type is not `QUERY`, `DELETE`, or `UPDATE`. |

## Examples

**Close a query cursor:**

```kronotop
> BUCKET.QUERY users '{}' LIMIT 100
cursor_id -> (integer) 1
entries -> [...] (first 100 documents)

> BUCKET.CLOSE QUERY 1
OK
```

**Double-close returns an error:**

```kronotop
> BUCKET.QUERY users '{}' LIMIT 100
cursor_id -> (integer) 1
entries -> [...]

> BUCKET.CLOSE QUERY 1
OK

> BUCKET.CLOSE QUERY 1
(error) ERR no cursor found
```

**Close after pagination:**

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' LIMIT 50
cursor_id -> (integer) 1
object_ids -> [...] (first 50 deleted)

> BUCKET.ADVANCE DELETE 1
cursor_id -> (integer) 1
object_ids -> [...] (next 50 deleted)

> BUCKET.CLOSE DELETE 1
OK
```

**Closing one cursor does not affect others:**

```kronotop
> BUCKET.QUERY users '{}' LIMIT 10
cursor_id -> (integer) 1
entries -> [...]

> BUCKET.DELETE users '{"status": "inactive"}' LIMIT 10
cursor_id -> (integer) 2
object_ids -> [...]

> BUCKET.CLOSE QUERY 1
OK

> BUCKET.ADVANCE DELETE 2
cursor_id -> (integer) 2
object_ids -> [...] (still works)
```
