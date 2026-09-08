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

On successful authentication, the connection is marked as authenticated and stays that way until it is closed.
`SESSION.CLOSE` does not clear it.

This command does not require the cluster to be initialized.

## Errors

Argument errors:

| Error Code  | Error message                                                                                                              | Cause |
|-------------|----------------------------------------------------------------------------------------------------------------------------|-------|
| `WRONGPASS` | `invalid username-password pair or user is disabled.`                                                                      | -     |
| `ERR`       | `AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?` | -     |

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
