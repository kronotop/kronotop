---
title: "NAMESPACE MOVE"
description: "Renames a namespace by moving its FoundationDB directory from the old path to the new path."
---

Renames a namespace by moving its FoundationDB directory from the old path to the new path.

## Syntax

```kronotop
NAMESPACE MOVE <old-namespace> <new-namespace>
```

## Parameters

| Parameter       | Type   | Required | Description                                                                               |
|-----------------|--------|----------|-------------------------------------------------------------------------------------------|
| `old-namespace` | string | Yes      | Dot-separated hierarchical path of the source namespace (e.g. `staging.orders`).          |
| `new-namespace` | string | Yes      | Dot-separated hierarchical path for the destination namespace (e.g. `production.orders`). |

## Return Value

Returns `OK` on success.

## Behavior

The command moves a namespace from `old-namespace` to `new-namespace` within the FoundationDB directory layer. The
operation uses an isolated one-off transaction to prevent consistency issues.

Before performing the move, the command checks whether the old namespace is marked for removal. If it is, the operation
is rejected.

The `__internal__` name is reserved at any level of the hierarchy and cannot be used in either the old or new path.

### Tombstone and barrier

After moving the directory, a **tombstone** with a unique token is written under the old namespace name. This marks the
old path as "recently moved."

A cluster-wide event is published to the journal. Every cluster member that consumes this event:

1. Invalidates its bucket metadata cache entries keyed under the old namespace
2. Invalidates Bucket plan cache entries keyed under the old namespace
3. Removes the old namespace from all sessions' open-namespaces set
4. Acknowledges the tombstone

The tombstone acts as a **barrier**: `NAMESPACE CREATE` on the old name is blocked until every alive cluster member has
observed the tombstone. This prevents a member that still caches the old namespace from serving stale data under a newly
created namespace with the same name.

Once all alive members have observed, the tombstone is automatically cleaned up on the next barrier check. If any alive
member has not yet observed, the barrier check re-publishes the event to nudge lagging members.

## Errors

| Error Code               | Description                                                                                    |
|--------------------------|------------------------------------------------------------------------------------------------|
| `NOSUCHNAMESPACE`        | The source namespace does not exist.                                                           |
| `NAMESPACEALREADYEXISTS` | The destination namespace already exists.                                                      |
| `NAMESPACEBEINGREMOVED`  | The source namespace is marked for removal via `NAMESPACE REMOVE` and has not yet been purged. |
| `ERR`                    | The namespace path contains the reserved `__internal__` name.                                  |

## Examples

**Move a namespace:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE MOVE staging.orders production.orders
OK
```

**Non-existent source:**

```kronotop
> NAMESPACE MOVE non.existent production.orders
(error) NOSUCHNAMESPACE No such namespace: 'non.existent'
```

**Destination already exists:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE CREATE production.orders
OK

> NAMESPACE MOVE staging.orders production.orders
(error) NAMESPACEALREADYEXISTS Namespace already exists: production.orders
```

**Namespace being removed:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE MOVE staging.orders production.orders
(error) NAMESPACEBEINGREMOVED Namespace 'staging.orders' is being removed
```

**Reserved name:**

```kronotop
> NAMESPACE MOVE name.__internal__ production.data
(error) ERR Namespace 'name.__internal__' is reserved for internal use
```
