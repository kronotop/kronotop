---
title: "Volume Operations Guide"
description: "This guide covers routine storage maintenance tasks that reclaim disk space after data deletion."
---

This guide covers routine storage maintenance tasks that reclaim disk space after data deletion. It is intended for
cluster administrators.

---

## How Storage Cleanup Works

When documents are deleted or updated, the metadata in FoundationDB is removed or replaced immediately. However,
internal prefix references may become orphaned over time.

The **Mark stale prefixes** admin task scans all prefix references in FoundationDB, identifies those that no longer
point to valid data, and clears them. Stale references skew garbage percentage calculations until cleared.

This task does not run automatically. It must be triggered explicitly by an administrator.

---

## Stale Prefix Cleanup

A *prefix* is the internal grouping key that associates stored entries with their logical owner (a bucket). When a
bucket or namespace is deleted, the data and metadata it owned become orphaned, but the prefix references in
FoundationDB are not automatically cleaned up in every case.

The `MARK-STALE-PREFIXES` task scans all prefix references, detects those whose targets are missing or invalid, and
clears them. This makes the associated data eligible for garbage collection.

**Starting the task:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES START
OK
```

The task runs in the background using batch-priority transactions, so it does not compete with user-facing traffic for
FoundationDB resources. It processes prefixes in batches of 10,000 and tracks its progress internally, allowing it to
resume from where it left off if restarted.

The task auto-completes when all prefixes have been scanned. No manual stop is required under normal circumstances.

**Stopping the task (if needed):**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES STOP
OK
```

Only one instance of the task can run at a time.

---

## Namespace Purge Considerations

`NAMESPACE PURGE` permanently deletes a namespace's FoundationDB directory, but it does **not** clean up volume data.
The bytes on the disk and any orphaned prefix references remain until explicitly reclaimed.

After purging a namespace, run the stale prefix scan to clean up orphaned references:

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES START
OK
# Wait for the task to complete
```

For single-bucket deletion, `BUCKET.PURGE` handles its own prefix cleanup immediately, so `MARK-STALE-PREFIXES` is not
required.

**Prefer per-bucket deletion over namespace purge.** When possible, delete buckets individually with
`BUCKET.REMOVE` + `BUCKET.PURGE` before dropping the namespace. This approach is cleaner because `BUCKET.PURGE`
handles prefix cleanup inline. No separate `MARK-STALE-PREFIXES` pass is needed. Reserve `NAMESPACE PURGE` for cases
where the namespace contains too many buckets to delete one by one, or when the namespace must be removed urgently
regardless of cleanup cost.

---

## Vacuuming Segments

Over time, delete and update operations leave unreachable data in segments. Marking stale prefixes makes this garbage
visible, but does not free disk space. Vacuum is the step that actually reclaims it: live entries are evacuated from
high-garbage segments into the current writable segment, and the emptied segment files are destroyed.

**When to run:** After `MARK-STALE-PREFIXES` has completed and `VOLUME.ADMIN DESCRIBE` shows elevated
`garbage_percentage` values on one or more segments.

**Typical workflow:**

1. **Start vacuum** on a volume, specifying a garbage threshold. Only segments whose garbage percentage exceeds this
   value will be processed:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM START bucket-shard-0 30
   OK
   ```

2. **Monitor progress.** Vacuum auto-completes when all eligible segments are processed:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS bucket-shard-0
   ```

3. **Drop metadata** after the run finishes. This is required before starting a new vacuum on the same volume:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP bucket-shard-0
   OK
   ```

**Stopping early (if needed):**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STOP bucket-shard-0
OK
```

After stopping, run `DROP` to clear the metadata before starting a new vacuum.

Only one vacuum can run on a given volume at a time. Vacuum runs in the background and does not compete with user-facing
traffic.

For full parameter details, error conditions, and status output format, see
[VOLUME.ADMIN VACUUM](commands/volume-admin-vacuum.md).

---

## Recommended Maintenance Procedure

Follow this checklist after namespace purges or periodic maintenance:

1. **Run stale prefix cleanup**
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES START
   OK
   ```
   Wait for the task to auto-complete. It processes in the background and does not require monitoring.

2. **List all volumes**
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN LIST
   1) bucket-shard-0
   2) bucket-shard-1
   ...
   ```

3. **Inspect each volume**
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-0
   ```
   Check `garbage_percentage` values per segment.

4. **Vacuum high-garbage volumes**
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM START bucket-shard-0 30
   OK
   ```
   Run this for each volume where `garbage_percentage` exceeds your chosen threshold. Wait for completion, then drop
   the metadata:
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP bucket-shard-0
   OK
   ```

5. **Re-inspect volumes**
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-0
   ```
   Confirm that the stale segment has been evacuated and deleted.

6. **Clean up orphan files (optional)**
   ```kronotop
   127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES bucket-shard-0
   ```
   Run this if you suspect leftover files from crashes or interrupted operations.

7. **Repeat steps 3–6 for each volume** on each cluster member as needed.
