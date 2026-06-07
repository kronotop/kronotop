---
title: "BUCKET.INSERT"
sidebar:
  order: 3
description: "Inserts one or more documents into a bucket."
---

Inserts one or more documents into a bucket.

## Syntax

```kronotop
BUCKET.INSERT <bucket> DOCS <document> [document ...]
```

## Parameters

| Parameter | Type         | Required | Description                                                                                                             |
|-----------|--------------|----------|-------------------------------------------------------------------------------------------------------------------------|
| `bucket`  | string       | Yes      | Name of the target bucket. The bucket must already exist (see `BUCKET.CREATE`).                                         |
| `DOCS`    | JSON or BSON | Yes      | One or more documents to insert. Documents can be in JSON or BSON format depending on the session's input type setting. |

## Return Value

Returns an array of ObjectIds, one for each inserted document. ObjectIds are returned in both auto-commit mode and
within explicit transactions.

An ObjectId is a 12-byte unique identifier. The encoding format of returned ObjectIds depends on the session's
`object_id_format` setting:

| Format  | Response Type | Description                               |
|---------|---------------|-------------------------------------------|
| `hex`   | String        | 24-character hex-encoded string (default) |
| `bytes` | Binary        | Raw 12-byte array                         |

To change the format:

```kronotop
SESSION.ATTRIBUTE SET object_id_format hex
SESSION.ATTRIBUTE SET object_id_format bytes
```

Example response with `hex` format:

```kronotop
1) "6835a1c0e4b0f72a3c000001"
2) "6835a1c0e4b0f72a3c000002"
```

Example response with `bytes` format:

```kronotop
1) "\x68\x35\xa1\xc0\xe4\xb0\xf7\x2a\x3c\x00\x00\x01"
2) "\x68\x35\xa1\xc0\xe4\xb0\xf7\x2a\x3c\x00\x00\x02"
```

## The `_id` Field

Each document stored in a bucket has an `_id` field that serves as its primary key.

- **Auto-generated**: If a document does not contain an `_id` field, Kronotop automatically generates an ObjectId and
  injects it into the document before storage.
- **User-provided**: If a document already contains an `_id` field, it must be of type ObjectId. Any other type causes
  an error.
- **Duplicate detection**: When user-provided `_id` values are used, Kronotop checks the primary index for duplicates.
  If an `_id` already exists in the bucket, a `DUPLICATEKEY` error is returned.

## Document Format

Documents must be valid JSON or BSON objects. The input format is determined by the session's `input_type` setting,
which can be configured via `SESSION.ATTRIBUTE SET input_type <format>`.

| Format   | Description                                                          |
|----------|----------------------------------------------------------------------|
| **JSON** | Standard JSON format. Useful for debugging and human-readable input. |
| **BSON** | Binary JSON format. Recommended for production use.                  |

**Internal storage:** Kronotop always stores documents in BSON format regardless of the input format. JSON documents are
automatically converted to BSON before storage.

**Production recommendation:** Use BSON as the input format in production environments. BSON avoids the conversion
overhead on every insert, reducing CPU usage and latency. BSON also supports richer data types (such as `DateTime`,
`Int64`, `Decimal128`) that cannot be natively represented in JSON.

## Routing

The command must be sent to a node that owns at least one shard assigned to the bucket. If the bucket's shards are all
hosted on other nodes, the server rejects the request with a redirect to the appropriate node.

## Errors

| Error Code              | Description                                                                                                              |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `REJECT`                | The bucket's shards are hosted on another node. The error includes the target address: `REJECT <shardId> <host>:<port>`. |
| `DUPLICATEKEY`          | A document with the same `_id` already exists in the bucket.                                                             |
| `ERR`                   | `_id field must be of type ObjectId`: the `_id` field in a document is not an ObjectId.                                  |
| `INDEXTYPE_MISMATCH`    | The value type does not match the expected index type (when `strict_types` is enabled).                                  |
| `BUCKETBEINGREMOVED`    | The target bucket is being removed.                                                                                      |
| `NAMESPACEBEINGREMOVED` | The target namespace is being removed.                                                                                   |
| `NOSUCHNAMESPACE`       | The specified namespace does not exist.                                                                                  |
| `VECTORINDEXNOTREADY`   | A vector index on the bucket is still bootstrapping. Retry after a short delay.                                          |

## Examples

The following examples assume `input_type` is set to `json` and `object_id_format` is set to `hex`.

**Insert a single document:**

```kronotop
BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30}'
```

Response:

```kronotop
1) "6835a1c0e4b0f72a3c000001"
```

**Insert multiple documents:**

```kronotop
BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30}' '{"name": "Bob", "age": 25}'
```

Response:

```kronotop
1) "6835a1c0e4b0f72a3c000001"
2) "6835a1c0e4b0f72a3c000002"
```

**Insert within a transaction:**

```kronotop
BEGIN
BUCKET.INSERT users DOCS '{"name": "Alice"}' '{"name": "Bob"}'
COMMIT
```

Response from `BUCKET.INSERT`:

```kronotop
1) "6835a1c0e4b0f72a3c000001"
2) "6835a1c0e4b0f72a3c000002"
```

**Insert with a user-provided `_id`:**

```kronotop
BUCKET.INSERT users DOCS '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "Alice"}'
```

Response:

```kronotop
1) "507f1f77bcf86cd799439011"
```
