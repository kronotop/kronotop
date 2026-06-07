---
title: "SESSION.CLOSE"
description: "Closes the current session and resets all session state while keeping the connection open."
---

Closes the current session and resets all session state while keeping the connection open.

## Syntax

```kronotop
SESSION.CLOSE
```

This command takes no parameters.

## Return Value

Simple string: `OK` on success.

## Behavior

The command performs a full session reset without closing the underlying network connection:

1. **Cursors**: All active cursors (read, delete, update query contexts) are cleared
2. **FDB Transaction**: Any active FoundationDB transaction is rolled back and closed
3. **MULTI State**: `MULTI` transaction state (queued commands, the MULTI flag) is reset
4. **Watched Keys**: All keys being watched via `WATCH` are unwatched
5. **Cursor ID Counter**: Reset to 1
6. **Session Attributes**: All attributes (`reply_type`, `input_type`, `limit`, `object_id_format`) are reset to their
   defaults

## Examples

**Basic usage:**

```kronotop
> SESSION.CLOSE
OK
```

**After starting a transaction:**

```kronotop
> BEGIN
OK

> SESSION.CLOSE
OK

> ROLLBACK
(error) TRANSACTION there is no transaction in progress.
```

The transaction is rolled back; no explicit `ROLLBACK` is needed.

**Resetting modified session attributes:**

```kronotop
> SESSION.ATTRIBUTE SET limit 50
OK

> SESSION.CLOSE
OK

> SESSION.ATTRIBUTE LIST
1# reply_type => bson
2# input_type => bson
3# limit => (integer) 100
4# object_id_format => bytes
```

The `limit` attribute is reset to its default value (100).
