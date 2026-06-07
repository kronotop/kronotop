---
title: "NAMESPACE EXISTS"
description: "Checks whether a namespace exists."
---

Checks whether a namespace exists.

## Syntax

```kronotop
NAMESPACE EXISTS <namespace>
```

## Parameters

| Parameter   | Type   | Required | Description                                                                  |
|-------------|--------|----------|------------------------------------------------------------------------------|
| `namespace` | string | Yes      | Dot-separated hierarchical path for the namespace (e.g. `production.users`). |

## Return Value

Integer: `1` if the namespace exists, `0` if it does not.

## Behavior

The command checks the FoundationDB directory layer for the given namespace path. It uses an isolated one-off
transaction.

If the directory entry exists but the namespace is marked for removal (`NAMESPACE REMOVE`), the command raises a
`NAMESPACEBEINGREMOVED` error rather than returning `1`. A namespace pending removal is not considered to exist.

The `__internal__` reserved name is rejected at parse time.

## Errors

| Error Code              | Description                                                                                                    |
|-------------------------|----------------------------------------------------------------------------------------------------------------|
| `NAMESPACEBEINGREMOVED` | The namespace was previously removed via `NAMESPACE REMOVE` but has not yet been purged via `NAMESPACE PURGE`. |
| `ERR`                   | The namespace path contains the reserved `__internal__` leaf.                                                  |

## Examples

**Namespace exists:**

```kronotop
> NAMESPACE CREATE production.users
OK

> NAMESPACE EXISTS production.users
(integer) 1
```

**Namespace does not exist:**

```kronotop
> NAMESPACE EXISTS production.orders
(integer) 0
```

**Reserved name:**

```kronotop
> NAMESPACE EXISTS name.__internal__
(error) ERR Namespace 'name.__internal__' is reserved for internal use
```

**Namespace being removed:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE EXISTS staging.orders
(error) NAMESPACEBEINGREMOVED Namespace 'staging.orders' is being removed
```
