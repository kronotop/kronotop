---
title: "AUTH"
description: "Authenticates the current connection."
---

Authenticates the current connection.

## Syntax

```kronotop
AUTH [username] password
```

## Parameters

| Parameter  | Type   | Required | Description                            |
|------------|--------|----------|----------------------------------------|
| `username` | string | No       | Username for named-user authentication |
| `password` | string | Yes      | Password to authenticate with          |

## Return Value

Simple string `OK` on successful authentication.

## Behavior

Supports two authentication modes:

- **Default user mode (1 parameter):** Checks the provided password against the `auth.requirepass` configuration value.
- **Named user mode (2 parameters):** Checks the provided username and password against the `auth.users.<username>`
  configuration.

On successful authentication, the session is marked as authenticated.

This command does not require the cluster to be initialized.

## Errors

| Error       | Condition                                                             |
|-------------|-----------------------------------------------------------------------|
| `WRONGPASS` | Invalid username or password                                          |
| `ERR`       | No password is configured but AUTH was called with a single parameter |

## Examples

**Default user authentication:**

```kronotop
127.0.0.1:5484> AUTH mysecretpassword
OK
```

**Named user authentication:**

```kronotop
127.0.0.1:5484> AUTH admin mysecretpassword
OK
```

**Wrong password:**

```kronotop
127.0.0.1:5484> AUTH wrongpassword
(error) WRONGPASS invalid username-password pair or user is disabled.
```
