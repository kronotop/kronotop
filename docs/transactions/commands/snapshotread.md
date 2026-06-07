---
title: "SNAPSHOTREAD"
description: "Enables or disables snapshot read mode for the current session."
---

Enables or disables snapshot read mode for the current session.

## Syntax

```kronotop
SNAPSHOTREAD <ON | OFF>
```

## Parameters

| Parameter | Required     | Description                                 |
|-----------|--------------|---------------------------------------------|
| `ON`      | Yes (one of) | Enables snapshot read mode on the session.  |
| `OFF`     | Yes (one of) | Disables snapshot read mode on the session. |

Exactly one of `ON` or `OFF` must be provided. The argument is case-insensitive.

## Return Value

Simple string: `OK` on success.

## Behavior

The command sets or clears snapshot read mode on the current session. When enabled, subsequent FoundationDB reads
performed by the session use snapshot isolation instead of the default serializable isolation.

Snapshot reads do not create read conflict ranges, which means they will not cause transactions to conflict with
concurrent writes to the same keys. This is useful for long-running reads or read-heavy workloads where strict
serializability is not required, and reducing transaction conflicts is more important than guaranteeing a perfectly
consistent view.

The setting is session-scoped. It persists until explicitly changed or the session ends. It can be toggled at any time,
regardless of whether a transaction is currently active.

### Supported Commands

The following commands honor the `SNAPSHOTREAD` setting:

| Command         | Honors SNAPSHOTREAD                |
|-----------------|------------------------------------|
| `ZGET`          | Yes                                |
| `ZGETI64`       | Yes                                |
| `ZGETF64`       | Yes                                |
| `ZGETD128`      | Yes                                |
| `ZGETRANGE`     | Yes                                |
| `ZGETKEY`       | Yes                                |
| `ZGETRANGESIZE` | Yes                                |
| `BUCKET.QUERY`  | Yes                                |
| `BUCKET.INSERT` | No, always uses serializable reads |
| `BUCKET.DELETE` | No, always uses serializable reads |
| `BUCKET.UPDATE` | No, always uses serializable reads |

Mutation commands (`BUCKET.INSERT`, `BUCKET.DELETE`, `BUCKET.UPDATE`) always use serializable reads regardless of the
`SNAPSHOTREAD` setting.

## Errors

| Error Code | Description                                                                             |
|------------|-----------------------------------------------------------------------------------------|
| `ERR`      | `illegal argument for SNAPSHOTREAD: '<value>'`: The argument is neither `ON` nor `OFF`. |

## Examples

**Enable snapshot read mode:**

```kronotop
> SNAPSHOTREAD ON
OK
```

**Disable snapshot read mode:**

```kronotop
> SNAPSHOTREAD OFF
OK
```

**Invalid argument:**

```kronotop
> SNAPSHOTREAD MAYBE
(error) ERR illegal argument for SNAPSHOTREAD: 'MAYBE'
```
