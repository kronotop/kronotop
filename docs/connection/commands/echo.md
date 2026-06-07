---
title: "ECHO"
description: "Echoes back the given message."
---

Echoes back the given message.

## Syntax

```kronotop
ECHO message
```

## Parameters

| Parameter | Type   | Required | Description              |
|-----------|--------|----------|--------------------------|
| `message` | string | Yes      | The message to echo back |

## Return Value

Bulk string containing the provided message.

## Behavior

Returns the given message as a bulk string. The message is echoed back exactly as provided.

This command does not require the cluster to be initialized.

## Errors

No command-specific errors.

## Examples

```kronotop
127.0.0.1:5484> ECHO "Hello Kronotop"
"Hello Kronotop"
```
