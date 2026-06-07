---
title: "NAMESPACE LIST"
description: "Lists the child namespaces under a given path or lists root-level namespaces when no path is provided."
---

Lists the child namespaces under a given path or lists root-level namespaces when no path is provided.

## Syntax

```kronotop
NAMESPACE LIST [namespace]
```

## Parameters

| Parameter   | Type   | Required | Description                                                                                                                    |
|-------------|--------|----------|--------------------------------------------------------------------------------------------------------------------------------|
| `namespace` | string | No       | Dot-separated hierarchical path to list children of (e.g. `production.users`). When omitted, root-level namespaces are listed. |

## Return Value

Array of bulk strings: each element is the name of a child namespace. Returns an empty array when no children exist.

## Behavior

The command opens an isolated one-off transaction against the FoundationDB directory layer.

When called without arguments, it lists all root-level namespaces. When called with a namespace path, it lists the
immediate children of that path.

The reserved `__internal__` namespace is automatically filtered from the results and never appears in the output.

If the cluster has not been initialized yet and no path is provided, an empty array is returned.

## Errors

| Error Code        | Description                                                   |
|-------------------|---------------------------------------------------------------|
| `NOSUCHNAMESPACE` | The given namespace path does not exist.                      |
| `ERR`             | The namespace path contains the reserved `__internal__` leaf. |

## Examples

**List root-level namespaces:**

```kronotop
> NAMESPACE LIST
1) "global"
```

**List children of a namespace:**

```kronotop
> NAMESPACE CREATE production.users
OK

> NAMESPACE CREATE production.orders
OK

> NAMESPACE LIST production
1) "users"
2) "orders"
```

**List children of a leaf namespace (no children):**

```kronotop
> NAMESPACE CREATE production.users
OK

> NAMESPACE LIST production.users
(empty array)
```

**Non-existent namespace:**

```kronotop
> NAMESPACE LIST nonexistent
(error) NOSUCHNAMESPACE No such namespace: 'nonexistent'
```

**Reserved name:**

```kronotop
> NAMESPACE LIST name.__internal__
(error) ERR Namespace 'name.__internal__' is reserved for internal use
```
