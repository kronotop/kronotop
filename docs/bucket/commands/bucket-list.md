---
title: "BUCKET.LIST"
sidebar:
  order: 2
description: "Returns the names of all buckets in a namespace."
---

Returns the names of all buckets in a namespace. Without `NAMESPACE`, it lists the session's current namespace.

## Syntax

```kronotop
BUCKET.LIST [NAMESPACE <path>]
```

## Arguments

| Argument    | Type   | Required | Description                                                                                                                                              |
|-------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `NAMESPACE` | string | No       | Run this command in the given namespace instead of the session's current one. The namespace must exist. The session's current namespace does not change. |

## Return Value

Returns an array of bulk strings, one per bucket. Each element is the bucket name. If the namespace contains no buckets,
an empty array is returned.

Buckets that have been marked for removal with `BUCKET.REMOVE` but not yet purged with `BUCKET.PURGE` are still included
in the result. Only after a successful `BUCKET.PURGE` does the bucket disappear from the list.

## Errors

Argument errors:

| Error Code | Error message                                        | Cause |
|------------|------------------------------------------------------|-------|
| `ERR`      | `Unknown '<keyword>' argument`                       | -     |
| `ERR`      | `NAMESPACE argument must be followed by a namespace` | -     |

Namespace errors:

| Error Code              | Error message                         | Cause |
|-------------------------|---------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`         | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed` | -     |

## Examples

**List buckets on a fresh namespace (no buckets exist):**

```kronotop
> BUCKET.LIST
(empty array)
```

**List a single bucket:**

```kronotop
> BUCKET.CREATE users
OK

> BUCKET.LIST
1) "users"
```

**List multiple buckets:**

```kronotop
> BUCKET.CREATE alpha-bucket
OK

> BUCKET.CREATE beta-bucket
OK

> BUCKET.CREATE gamma-bucket
OK

> BUCKET.LIST
1) "alpha-bucket"
2) "beta-bucket"
3) "gamma-bucket"
```

**Bucket disappears only after purge, not after remove:**

```kronotop
> BUCKET.CREATE users
OK

> BUCKET.REMOVE users
OK

> BUCKET.PURGE users
OK

> BUCKET.LIST
(empty array)
```
