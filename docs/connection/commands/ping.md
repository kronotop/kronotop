---
title: "PING"
description: "Returns PONG or echoes back the given message."
---

Returns PONG or echoes back the given message.

## Syntax

```kronotop
PING [message]
```

## Parameters

| Parameter | Type   | Required | Description                   |
|-----------|--------|----------|-------------------------------|
| `message` | string | No       | Optional message to echo back |

## Return Value

- **Without message:** Simple string `PONG`.
- **With message:** Bulk string containing the provided message.

## Behavior

If a non-empty message is provided, returns it as a bulk string. Otherwise, returns the simple string `PONG`.

This command does not require the cluster to be initialized.

## Errors

No command-specific errors.

## Examples

**Without message:**

```kronotop
127.0.0.1:5484> PING
PONG
```

**With message:**

```kronotop
127.0.0.1:5484> PING "hello world"
"hello world"
```
