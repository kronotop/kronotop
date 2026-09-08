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

Namespace errors:

| Error Code | Error message                               | Cause                                                                              |
|------------|---------------------------------------------|------------------------------------------------------------------------------------|
| `ERR`      | `current namespace is empty, blank or null` | The session has no current namespace. This does not happen under normal operation. |

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
