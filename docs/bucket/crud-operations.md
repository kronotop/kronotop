---
title: "CRUD Operations"
description: "A quick-reference guide to basic document operations (insert, query, update, delete) with a simple product catalog."
---

A quick-reference guide to basic document operations (insert, query, update, delete) with a simple product catalog.
This guide assumes you have a RESP-compatible CLI client (`kronotop-cli`, `valkey-cli` or similar) installed
and connected to a running Kronotop instance.

## Session Setup

Kronotop supports both RESP2 and RESP3 wire protocols. Switch to RESP3. Its map-based responses are
more readable for this tutorial:

```kronotop
127.0.0.1:5484> HELLO 3
1# "server" => "Kronotop"
2# "version" => "2026.06-1"
3# "proto" => (integer) 3
4# "id" => (integer) 0
5# "mode" => "cluster"
6# "role" => "master"
7# "modules" => (empty array)
```

Configure the session to use JSON for readability:

```kronotop
127.0.0.1:5484> SESSION.ATTRIBUTE SET input_type json
OK
127.0.0.1:5484> SESSION.ATTRIBUTE SET reply_type json
OK
127.0.0.1:5484> SESSION.ATTRIBUTE SET object_id_format hex
OK
```

All examples in this guide run in **auto-commit mode**. Each command is executed as a one-off transaction
that commits immediately. See [Transactions](../transactions/index.md) for explicit transaction control.

## Create a Bucket

Create a bucket named `products`:

```kronotop
127.0.0.1:5484> BUCKET.CREATE products
OK
```

## Insert Documents

### Single insert

```kronotop
127.0.0.1:5484> BUCKET.INSERT products DOCS '{
  "category": "books",
  "price": 19.99,
  "name": "The Disconnected"
}'
1) "69dbdc95690a394e625a82c0"
```

Kronotop generates an `_id` (ObjectId) for each document and returns it.

### Batch insert

Pass multiple documents after the `DOCS` keyword. All documents are inserted atomically in a single
transaction, either all succeed or none do.

```kronotop
127.0.0.1:5484> BUCKET.INSERT products DOCS '{
  "category": "books",
  "price": 24.99,
  "name": "The Black Book"
}' '{
  "category": "electronics",
  "price": 499.99,
  "name": "Wireless Headphones"
}'
1) "69dbdccc690a394e625a82c1"
2) "69dbdccc690a394e625a82c2"
```

See [BUCKET.INSERT](commands/bucket-insert.md) for user-provided `_id` values and document format details.

## Query Documents

### All documents

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}'
1# "cursor_id" => (integer) 2
2# "entries" =>
   1) {"_id": "69dbdc95690a394e625a82c0", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69dbdccc690a394e625a82c1", "category": "books", "price": 24.99, "name": "The Black Book"}
   3) {"_id": "69dbdccc690a394e625a82c2", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

### Filter by field

Find all books:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{"category": "books"}'
1# "cursor_id" => (integer) 3
2# "entries" =>
   1) {"_id": "69dbdc95690a394e625a82c0", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69dbdccc690a394e625a82c1", "category": "books", "price": 24.99, "name": "The Black Book"}
```

### Filter with comparison operators

Find products cheaper than 25:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{"price": {"$lt": 25.0}}'
1# "cursor_id" => (integer) 5
2# "entries" =>
   1) {"_id": "69dbdc95690a394e625a82c0", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69dbdccc690a394e625a82c1", "category": "books", "price": 24.99, "name": "The Black Book"}
```

### Combine multiple filters

Find electronics cheaper than 100, no match in our data:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{
  "$and": [
    {"category": "electronics"},
    {"price": {"$lt": 100.0}}
  ]
}'
1# "cursor_id" => (integer) 6
2# "entries" => (empty array)
```

### Sort and limit

`SORTBY` requires an index on the sort field. Create one on `price` first:

```kronotop
127.0.0.1:5484> BUCKET.INDEX CREATE products '{
  "price": {"bson_type": "double"}
}'
OK
```

Now find the cheapest product:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' SORTBY price ASC LIMIT 1
1# "cursor_id" => (integer) 7
2# "entries" => 1) {"_id": "69dbdc95690a394e625a82c0", "category": "books", "price": 19.99, "name": "The Disconnected"}
```

See [BUCKET.QUERY](commands/bucket-query.md) for the full query syntax and filter operators.
See [SORTBY](sortby.md) for sorting details and compound index support.

## Update Documents

### Set a field

Increase the price of "The Disconnected":

```kronotop
127.0.0.1:5484> BUCKET.UPDATE products '{"name": "The Disconnected"}' '{
  "$set": {"price": 22.99}
}'
1# "cursor_id" => (integer) 8
2# "object_ids" => 1) "69dbdc95690a394e625a82c0"
```

### Unset a field

Remove the `category` field from all electronics:

```kronotop
127.0.0.1:5484> BUCKET.UPDATE products '{"category": "electronics"}' '{
  "$unset": ["category"]
}'
1# "cursor_id" => (integer) 9
2# "object_ids" => 1) "69dbdccc690a394e625a82c2"
```

See [BUCKET.UPDATE](commands/bucket-update.md) for `array_filters`, `upsert`, and other update operators.

## Delete Documents

Delete all books:

```kronotop
127.0.0.1:5484> BUCKET.DELETE products '{"category": "books"}'
1# "cursor_id" => (integer) 10
2# "object_ids" =>
   1) "69dbdc95690a394e625a82c0"
   2) "69dbdccc690a394e625a82c1"
```

See [BUCKET.DELETE](commands/bucket-delete.md) for batch deletion and filter options.

## Pagination with BUCKET.ADVANCE

When a query matches more documents than the batch size, use `BUCKET.ADVANCE` to fetch subsequent pages.

First, insert a few more products:

```kronotop
127.0.0.1:5484> BUCKET.INSERT products DOCS '{
  "category": "books",
  "price": 12.99,
  "name": "The Disconnected"
}' '{
  "category": "electronics",
  "price": 79.99,
  "name": "USB-C Hub"
}' '{
  "category": "electronics",
  "price": 149.99,
  "name": "Mechanical Keyboard"
}'
1) "69dbddc3690a394e625a82c3"
2) "69dbddc3690a394e625a82c4"
3) "69dbddc3690a394e625a82c5"
```

Query with a limit of 2:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' LIMIT 2
1# "cursor_id" => (integer) 12
2# "entries" =>
   1) {"_id": "69dbdccc690a394e625a82c2", "price": 499.99, "name": "Wireless Headphones"}
   2) {"_id": "69dbddc3690a394e625a82c3", "category": "books", "price": 12.99, "name": "The Disconnected"}
```

Fetch the next page using the cursor ID:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 12
1# "cursor_id" => (integer) 12
2# "entries" =>
   1) {"_id": "69dbddc3690a394e625a82c4", "category": "electronics", "price": 79.99, "name": "USB-C Hub"}
   2) {"_id": "69dbddc3690a394e625a82c5", "category": "electronics", "price": 149.99, "name": "Mechanical Keyboard"}
```

Continue until the entries array is empty:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 12
1# "cursor_id" => (integer) 12
2# "entries" => (empty array)
```

`BUCKET.ADVANCE` also works with `DELETE` and `UPDATE` operations. See [BUCKET.ADVANCE](commands/bucket-advance.md) for
details.

## Closing Cursors

Every `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` command creates a cursor that holds state in
the session. Each cursor holds the filter, sort configuration, and current position. These cursors stay open until
explicitly
closed. When you are done paginating, close the cursor to release its resources:

```kronotop
127.0.0.1:5484> BUCKET.CLOSE QUERY 12
OK
```

A closed cursor cannot be advanced:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 12
(error) ERR No previous query context found for 'query' operation with the given cursor id
```

See [BUCKET.CLOSE](commands/bucket-close.md) and [BUCKET.CURSORS](commands/bucket-cursors.md) for details.
