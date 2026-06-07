---
title: "NAMESPACE USE"
description: "Switches the current session to a namespace."
---

Switches the current session to a namespace.

## Syntax

```kronotop
NAMESPACE USE <namespace>
```

## Parameters

| Parameter   | Type   | Required | Description                                                                  |
|-------------|--------|----------|------------------------------------------------------------------------------|
| `namespace` | string | Yes      | Dot-separated hierarchical path for the namespace (e.g. `production.users`). |

## Return Value

Simple string: `OK` on success.

## Behavior

The command checks whether the given namespace exists in the FoundationDB directory layer using an isolated one-off
transaction. If the namespace exists and is not marked for removal, the session's active namespace is updated to the
given path. All subsequent commands in the session will operate in that namespace until changed again.

Every new session starts with the default namespace configured via `default_namespace` in the cluster configuration.

The `__internal__` reserved name is rejected at parse time.

## Errors

| Error Code              | Description                                                                                                                              |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `NOSUCHNAMESPACE`       | The namespace does not exist.                                                                                                            |
| `NAMESPACEBEINGREMOVED` | The namespace (or one of its ancestors) was previously removed via `NAMESPACE REMOVE` but has not yet been purged via `NAMESPACE PURGE`. |
| `ERR`                   | The namespace path contains the reserved `__internal__` leaf.                                                                            |

## Examples

**Switch to a namespace:**

```kronotop
> NAMESPACE CREATE production.users
OK

> NAMESPACE USE production.users
OK
```

**Non-existent namespace:**

```kronotop
> NAMESPACE USE non.existing.namespace
(error) NOSUCHNAMESPACE No such namespace: 'non.existing.namespace'
```

**Reserved name:**

```kronotop
> NAMESPACE USE name.__internal__
(error) ERR Namespace 'name.__internal__' is reserved for internal use
```

**Namespace being removed:**

```kronotop
> NAMESPACE CREATE staging.orders
OK

> NAMESPACE REMOVE staging.orders
OK

> NAMESPACE USE staging.orders
(error) NAMESPACEBEINGREMOVED Namespace 'staging.orders' is being removed
```
