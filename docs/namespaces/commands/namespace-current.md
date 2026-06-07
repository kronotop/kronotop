---
title: "NAMESPACE CURRENT"
description: "Returns the active namespace for the current session."
---

Returns the active namespace for the current session.

## Syntax

```kronotop
NAMESPACE CURRENT
```

## Parameters

None.

## Return Value

Bulk string: the dot-separated namespace path currently active in the session.

## Behavior

Every new session starts with the default namespace configured via `default_namespace` in the cluster configuration. The
active namespace can be changed with `NAMESPACE USE`.

`NAMESPACE CURRENT` reads the active namespace from the session attributes and returns it as a bulk string.

## Errors

| Error Code | Description                                                                                                                                             |
|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ERR`      | The current namespace is null, empty, or blank. This should not occur under normal operation since sessions are initialized with the default namespace. |

## Examples

**Return the default namespace:**

```kronotop
> NAMESPACE CURRENT
global
```

**Return the namespace after switching:**

```kronotop
> NAMESPACE USE production.users
OK

> NAMESPACE CURRENT
production.users
```
