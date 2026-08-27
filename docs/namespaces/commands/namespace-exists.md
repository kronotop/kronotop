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

Boolean: `true` if the namespace exists, `false` if it does not.

RESP2 has no boolean type. On a RESP2 connection you get the integer `1` for true and `0` for false. See
[Protocol Versions](../../connection/protocol-versions.md).

## Behavior

The command checks the FoundationDB directory layer for the given namespace path. It uses an isolated one-off
transaction.

If the directory entry exists but the namespace is marked for removal (`NAMESPACE REMOVE`), the command raises a
`NAMESPACEBEINGREMOVED` error rather than returning `true`. A namespace pending removal is not considered to exist.

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
(true)
```

**Namespace does not exist:**

```kronotop
> NAMESPACE EXISTS production.orders
(false)
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
