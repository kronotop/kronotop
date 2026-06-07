---
title: "BUCKET.CURSORS"
sidebar:
  order: 9
description: "Lists all active cursors for the current session."
---

Lists all active cursors for the current session.

## Syntax

```kronotop
BUCKET.CURSORS [operation]
```

## Parameters

| Parameter   | Type   | Required | Description                                                                                                                                       |
|-------------|--------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `operation` | string | No       | Filter cursors by operation type. Must be `QUERY`, `DELETE`, or `UPDATE` (case-insensitive). If omitted, returns cursors for all operation types. |

## Return Value

Returns a mapping of cursor IDs to their query filters for each operation type. Query filters are serialized as JSON
regardless of the original format used when creating the cursor.

**RESP3 (map format):**

Without operation filter:

```kronotop
> BUCKET.CURSORS
1# "QUERY" =>
   1# (integer) 2 => {}
2# "UPDATE" =>
   1# (integer) 1 => {"name": "Henry"}
3# "DELETE" => (empty map)
```

With operation filter (e.g., `BUCKET.CURSORS UPDATE`):

```kronotop
> BUCKET.CURSORS UPDATE
1# "UPDATE" =>
   1# (integer) 1 => {"name": "Henry"}
```

**RESP2 (array format):**

Without operation filter:

```kronotop
> BUCKET.CURSORS
1) "QUERY"
2) 1) (integer) 2
   2) {}
3) "UPDATE"
4) 1) (integer) 1
   2) {"name": "Henry"}
5) "DELETE"
6) (empty array)
```

With operation filter:

```kronotop
> BUCKET.CURSORS UPDATE
1) "UPDATE"
2) 1) (integer) 1
   2) {"name": "Henry"}
```

When no cursors exist for an operation type, the corresponding map or array is empty.

## Errors

| Error Code | Description                                                                         |
|------------|-------------------------------------------------------------------------------------|
| `ERR`      | Unknown operation type. The error message format is `Unknown '<operation>' action`. |

## Examples

**List all cursors:**

```kronotop
> BUCKET.QUERY users '{"age": {"$gt": 20}}' LIMIT 10
1# "cursor_id" => (integer) 1
2# "entries" => ... (first 10 documents)

> BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' LIMIT 5
1# "cursor_id" => (integer) 2
2# "entries" => ... (first 5 object_ids)

> BUCKET.CURSORS
1# "QUERY" =>
   1# (integer) 1 => "{"age": {"$gt": 20}}"
2# "UPDATE" =>
   1# (integer) 2 => "{"status": "pending"}"
3# "DELETE" => (empty map)
```

**List only query cursors:**

```kronotop
> BUCKET.CURSORS QUERY
1# "QUERY" =>
   1# (integer) 2 "{"age": {"$gt": 20}}"
```

**List cursors when none exist:**

```kronotop
> BUCKET.CURSORS
1) QUERY -> (empty map)
2) UPDATE -> (empty map)
3) DELETE -> (empty map)
```

**Verify cursor removal after close:**

```kronotop
> BUCKET.QUERY users '{}' LIMIT 10
1# "cursor_id" => (integer) 1
2# "entries" => ... (documents)

> BUCKET.CURSORS QUERY
1# "QUERY" =>
   1# (integer) 2 => {}

> BUCKET.CLOSE QUERY 1
OK

> BUCKET.CURSORS QUERY
1# "QUERY" => (empty map)
```
