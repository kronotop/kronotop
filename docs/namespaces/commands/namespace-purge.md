---
title: "NAMESPACE PURGE"
description: "Permanently deletes a namespace and its FoundationDB directory (hard delete)."
---

Permanently deletes a namespace and its FoundationDB directory (hard delete). This is the second phase of namespace
deletion, following `NAMESPACE REMOVE`.

## Syntax

```kronotop
NAMESPACE PURGE <namespace>
```

## Parameters

| Parameter   | Type   | Required | Description                                                                                                                                                 |
|-------------|--------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `namespace` | string | Yes      | Dot-separated hierarchical path of the namespace to purge (e.g. `staging.orders`). The namespace must be marked for removal first using `NAMESPACE REMOVE`. |

## Return Value

Returns `OK` on success.

## Behavior

The command enforces a **distributed sync barrier** before physically deleting the namespace. This barrier waits for all
alive cluster members to confirm they have observed the namespace's "removed" status (set by `NAMESPACE REMOVE`).

The barrier polls up to 20 times with 250ms intervals (5 seconds total). If all members have observed the removal within
that window, the namespace's FoundationDB directory is permanently deleted.

The barrier mechanism prevents data races in a distributed environment:

- Background workers (index maintenance, replication) may still be processing the namespace
- Other cluster nodes may have pending operations or cached references
- Without coordination, purging could cause errors or inconsistent state

If any member has not yet observed the removal, the barrier fails with `BARRIERNOTSATISFIED`. When this happens, the
command automatically publishes a namespace-removed event to accelerate propagation, and you should retry the purge. In
most cases, a single retry is sufficient.

The default namespace (configured via `default_namespace`) cannot be purged.

### Hierarchical deletion

Purging a parent namespace (e.g. `a.b`) permanently deletes all child namespaces (e.g. `a.b.c`, `a.b.c.d`) along with
it.

### Volume data cleanup

`NAMESPACE PURGE` deletes the namespace's FoundationDB directory but does **not** clean up volume data. The bytes on
the disk and any orphaned prefix references remain until explicitly reclaimed. After purging, run
`VOLUME.ADMIN MARK-STALE-PREFIXES START` on the affected volumes to clean up orphaned references.
When possible, prefer deleting buckets individually with `BUCKET.REMOVE` + `BUCKET.PURGE` before dropping
the namespace. `BUCKET.PURGE` handles prefix cleanup inline, avoiding the need for a separate stale prefix scan. See
the [Volume Operations Guide](../../volume/operations-guide.md) for the full procedure.

### Transaction conflict handling

If the underlying FoundationDB transaction fails due to a conflict (error code 1020), the operation is automatically
retried.

## Errors

| Error Code            | Description                                                                                                                                          |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `NOSUCHNAMESPACE`     | The namespace does not exist.                                                                                                                        |
| `ERR`                 | The namespace is not marked for removal, attempting to purge the default namespace, or the namespace path contains the reserved `__internal__` name. |
| `BARRIERNOTSATISFIED` | Not all cluster members have observed the removal. Retry the command.                                                                                |

## Examples

**Permanently delete a removed namespace:**

```kronotop
> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE PURGE staging.orders
OK
```

**Attempting to purge without removing first:**

```kronotop
> NAMESPACE PURGE staging.orders
(error) ERR Namespace 'staging.orders' must be logically removed before purge
```

**Default namespace:**

```kronotop
> NAMESPACE PURGE global
(error) ERR Cannot purge the default namespace: 'global'
```

**Handling barrier not satisfied:**

```kronotop
> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE PURGE staging.orders
(error) BARRIERNOTSATISFIED Barrier not satisfied: not all members observed version ...

> NAMESPACE PURGE staging.orders
OK
```
