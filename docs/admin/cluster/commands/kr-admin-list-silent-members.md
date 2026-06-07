---
title: "KR.ADMIN LIST-SILENT-MEMBERS"
description: "Lists cluster members suspected to be unresponsive."
---

Lists cluster members suspected to be unresponsive.

## Syntax

```kronotop
KR.ADMIN LIST-SILENT-MEMBERS
```

## Parameters

None.

## Return Value

RESP3 array of bulk strings. Each element is the member ID of a member that has been detected as silent. Returns an
empty array when all known members are alive.

## Behavior

Returns the IDs of cluster members whose heartbeats have not been observed for longer than the configured
`maximum_silent_period`. This reflects the local failure-detection state of the responding node, not a cluster-wide
consensus.

Only other RUNNING members are tracked. The local member is excluded from the known-members set and therefore never
appears in the result.

A member is marked as silent when its heartbeat has been missing for longer than `maximum_silent_period`. If a
previously silent member resumes sending heartbeats, it is automatically removed from this list.

Requires cluster initialization.

## Errors

| Error                                      | Condition                              |
|--------------------------------------------|----------------------------------------|
| `ERR cluster has not been initialized yet` | The cluster must be initialized first. |

## Examples

**All members healthy:**

```kronotop
127.0.0.1:3320> KR.ADMIN LIST-SILENT-MEMBERS
(empty array)
```

**One silent member:**

```kronotop
127.0.0.1:3320> KR.ADMIN LIST-SILENT-MEMBERS
1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
```
