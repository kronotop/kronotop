---
title: "Volume Operations Guide"
description: "This guide covers routine storage maintenance tasks that reclaim disk space after data deletion."
---

This guide covers routine storage maintenance tasks that reclaim disk space after data is deleted. It is for cluster
administrators.

## How storage cleanup works

When documents are deleted or updated, Kronotop removes or replaces their metadata in FoundationDB right away. Internal
prefix references can still become orphaned over time.

The collect stale prefixes admin task scans all prefix references in FoundationDB, finds the ones that no longer point
to valid data, and clears them. Until they are cleared, stale references skew the garbage percentage.

The task does not run automatically. An administrator has to start it.

## Stale prefix cleanup

A *prefix* is the internal grouping key that ties stored entries to their logical owner, a bucket. When a bucket or
namespace is deleted, the data and metadata it owned become orphaned, and the prefix references in FoundationDB are not
always cleaned up automatically.

The `COLLECT-STALE-PREFIXES` task scans all prefix references, finds the ones whose targets are missing or invalid, and
clears them. The associated data then becomes eligible for garbage collection.

Start the task:

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES START
OK
```

The task runs in the background with batch-priority transactions, so it does not compete with user traffic for
FoundationDB resources. It processes prefixes in batches of 10,000 and records its progress, so it can resume where it
left off after a restart.

The task completes on its own after it scans all prefixes, and under normal conditions you do not need to stop it. To
stop it early:

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES STOP
OK
```

Only one instance of the task can run at a time. For argument details and error conditions, see
[KR.ADMIN COLLECT-STALE-PREFIXES](../admin/cluster/commands/kr-admin-collect-stale-prefixes.md).

## Namespace purge considerations

`NAMESPACE PURGE` permanently deletes a namespace's FoundationDB directory. It does not clean up volume data, so the
bytes on disk and any orphaned prefix references stay until you reclaim them.

After you purge a namespace, run the stale prefix scan to clean up orphaned references:

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES START
OK
# Wait for the task to complete
```

When you delete a single bucket, `BUCKET.PURGE` cleans up its prefix immediately, so you do not need
`COLLECT-STALE-PREFIXES`.

Prefer deleting buckets one by one over a namespace purge. When possible, delete each bucket with `BUCKET.REMOVE` and
`BUCKET.PURGE` before you drop the namespace. `BUCKET.PURGE` cleans up prefixes inline, so you skip the separate
`COLLECT-STALE-PREFIXES` pass. Use `NAMESPACE PURGE` when the namespace has too many buckets to delete one by one, or
when you must remove it urgently whatever the cleanup cost.

## Vacuuming segments

Delete and update operations leave unreachable data in segments over time. Collecting stale prefixes makes this garbage
visible but does not free disk space. Vacuum frees it: it moves the live entries out of high-garbage segments into the
current writable segment and deletes the empty segment files.

Run vacuum after `COLLECT-STALE-PREFIXES` has completed and `VOLUME.ADMIN DESCRIBE` shows high `garbage_percentage`
values on one or more segments.

A typical run has three steps:

1. Start vacuum on a volume with a garbage threshold. Only segments whose garbage percentage is above this value are
   processed:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM START shard-0 30
   OK
   ```

2. Monitor progress. Vacuum completes on its own after it processes all eligible segments:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS shard-0
   ```

3. Drop the metadata after the run finishes. You must do this before you start a new vacuum on the same volume:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP shard-0
   OK
   ```

To stop vacuum early:

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STOP shard-0
OK
```

After you stop it, run `DROP` to clear the metadata before you start a new vacuum.

Only one vacuum can run on a volume at a time. Vacuum runs in the background and does not compete with user traffic.

For argument details, error conditions, and the status output format, see
[VOLUME.ADMIN VACUUM](commands/volume-admin-vacuum.md).

## Recommended maintenance procedure

Follow these steps after a namespace purge or during periodic maintenance:

1. Run stale prefix cleanup:
   ```kronotop
   127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES START
   OK
   ```
   Wait for the task to complete. It runs in the background and needs no monitoring.

2. List the volumes on each member:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN LIST
   1) shard-0
   2) shard-1
   ...
   ```
   The list shows only the volumes open on the connected member. Run it on every member, or use
   `KR.ADMIN DESCRIBE-CLUSTER` to see which member owns each volume.

3. Inspect each volume:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN DESCRIBE shard-0
   ```
   Check the `garbage_percentage` value of each segment.

4. Vacuum the volumes with high garbage:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM START shard-0 30
   OK
   ```
   Run this for each volume whose `garbage_percentage` is above your threshold. Wait for it to complete, then drop the
   metadata:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP shard-0
   OK
   ```

5. Inspect the volumes again:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN DESCRIBE shard-0
   ```
   Confirm that vacuum evacuated and deleted the stale segment.

6. Optionally, clean up orphan files:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES shard-0
   ```
   Run this if you suspect files left behind by crashes or interrupted operations.

7. Repeat steps 3 to 6 for each volume on each cluster member as needed.
