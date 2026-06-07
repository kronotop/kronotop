---
title: "NAMESPACE CREATE"
description: "Creates a new namespace with the given hierarchical path."
---

Creates a new namespace with the given hierarchical path.

## Syntax

```kronotop
NAMESPACE CREATE <namespace>
```

## Parameters

| Parameter   | Type   | Required | Description                                                                  |
|-------------|--------|----------|------------------------------------------------------------------------------|
| `namespace` | string | Yes      | Dot-separated hierarchical path for the namespace (e.g. `production.users`). |

## Return Value

Returns `OK` on success.

## Behavior

Namespaces are hierarchical paths separated by dots (`.`). Creating a namespace like `production.users.api`
automatically creates intermediate directories (`production` and `production.users`) in the FoundationDB directory
layer.

The command uses an isolated one-off transaction to prevent consistency issues.

Before creating the namespace, a tombstone barrier check is performed. If the namespace was previously moved via
`NAMESPACE MOVE` and not all cluster members have observed the tombstone, the creation is rejected. This prevents stale
reads on members that still reference the old namespace path.

The maximum namespace depth is 10. For example, `a.b.c.d.e.f.g.h.i.j` is the deepest allowed path.

The `__internal__` name is reserved at any level of the hierarchy and cannot be used.

## Errors

| Error Code               | Description                                                                                                                                                                                          |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `NAMESPACEALREADYEXISTS` | A namespace with the same path already exists.                                                                                                                                                       |
| `NAMESPACEBEINGREMOVED`  | The namespace was previously removed via `NAMESPACE REMOVE` but has not yet been purged via `NAMESPACE PURGE`.                                                                                       |
| `ERR`                    | The namespace path contains the reserved `__internal__` leaf, the namespace depth exceeds the maximum allowed depth of 10, or the tombstone barrier is not satisfied after a prior `NAMESPACE MOVE`. |

## Examples

**Create a namespace:**

```kronotop
> NAMESPACE CREATE production.users
OK
```

**Duplicate namespace:**

```kronotop
> NAMESPACE CREATE production.users
OK

> NAMESPACE CREATE production.users
(error) NAMESPACEALREADYEXISTS Namespace already exists: production.users
```

**Namespace being removed:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE CREATE staging.orders
(error) NAMESPACEBEINGREMOVED Namespace 'staging.orders' is being removed
```

**Reserved name:**

```kronotop
> NAMESPACE CREATE name.__internal__
(error) ERR Namespace 'name.__internal__' is reserved for internal use
```

**Tombstone barrier isn’t satisfied:**

```kronotop
> NAMESPACE CREATE old-namespace
(error) ERR Not all cluster members have observed the tombstone for namespace 'old-namespace'
```
