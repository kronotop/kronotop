---
title: "NAMESPACE REMOVE"
description: "Marks a namespace for logical removal without deleting its FoundationDB directory."
---

Marks a namespace for logical removal without deleting its FoundationDB directory. Physical deletion requires a
subsequent `NAMESPACE PURGE`.

## Syntax

```kronotop
NAMESPACE REMOVE <namespace>
```

## Parameters

| Parameter   | Type   | Required | Description                                                                         |
|-------------|--------|----------|-------------------------------------------------------------------------------------|
| `namespace` | string | Yes      | Dot-separated hierarchical path of the namespace to remove (e.g. `staging.orders`). |

## Return Value

Returns `OK` on success.

## Behavior

The command sets a `removed` flag on the namespace metadata. The FoundationDB directory is **not** deleted. This is a
logical removal only. To physically delete the directory, run `NAMESPACE PURGE` after the removal has been observed by
all cluster members.

The command uses an isolated one-off transaction to prevent consistency issues. If the transaction fails due to a
conflict (error code 1020), it is automatically retried.

The default namespace (configured via `default_namespace`) cannot be removed.

The `__internal__` name is reserved at any level of the hierarchy and cannot be used.

### Hierarchical removal

Removing a parent namespace (e.g. `a.b`) affects all child namespaces (e.g. `a.b.c`, `a.b.c.d`). The children inherit
the removed state from their parent.

### Cluster-wide side effects

A cluster-wide event is published to the journal. Every cluster member that consumes this event:

1. Invalidates its bucket metadata cache entries keyed under the namespace
2. Invalidates plan cache entries keyed under the namespace
3. Removes the namespace from all sessions' open-namespaces set
4. Shuts down workers registered for the namespace
5. Records the observed namespace version once workers terminate (barrier for `NAMESPACE PURGE`)

## Errors

Argument errors:

| Error Code | Error message                                     | Cause                                                         |
|------------|---------------------------------------------------|---------------------------------------------------------------|
| `ERR`      | `Namespace '<path>' is reserved for internal use` | The namespace path contains the reserved `__internal__` name. |

Namespace errors:

| Error Code              | Error message                                   | Cause |
|-------------------------|-------------------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`                   | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed`           | -     |
| `ERR`                   | `Cannot remove the default namespace: '<path>'` | -     |

## Examples

**Remove a namespace:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE REMOVE staging.orders
OK
```

**Non-existent namespace:**

```kronotop
> NAMESPACE REMOVE non.existent
(error) NOSUCHNAMESPACE No such namespace: 'non.existent'
```

**Already marked for removal:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE REMOVE staging.orders
(error) NAMESPACEBEINGREMOVED Namespace 'staging.orders' is being removed
```

**Default namespace:**

```kronotop
> NAMESPACE REMOVE global
(error) ERR Cannot remove the default namespace: 'global'
```

**Reserved name:**

```kronotop
> NAMESPACE REMOVE name.__internal__
(error) ERR Namespace 'name.__internal__' is reserved for internal use
```
