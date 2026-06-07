---
title: "Architecture"
description: "How Kronotop is structured, from the RESP front end down to the FoundationDB and Volume storage layers."
---

RESP-compatible clients talk to a Kronotop member over TCP. The member hosts
the data models: [Bucket](bucket/index.md) for documents, [ZMap](zmap/index.md) for ordered key-value data. Both
share the same sessions, [namespaces](namespaces/index.md), and [transactions](transactions/index.md). Underneath,
FoundationDB stores metadata, indexes, ZMap data, and cluster state, while [Volume](volume/index.md), Kronotop's
storage engine, keeps document bodies on the member's local disk.

The rest of this page explains what Kronotop delegates to FoundationDB, how data is split into shards, and how
shards are replicated.

<svg viewBox="0 0 720 488" width="720" height="488" style="display:block;margin-inline:auto;max-width:100%;height:auto" role="img" aria-label="Kronotop architecture: RESP clients connect to a Kronotop member. Inside the member, the Bucket and ZMap data models share one transaction boundary, and the Volume storage engine sits under Bucket. The volume replicates its segments to a standby member. Both members run on top of FoundationDB." xmlns="http://www.w3.org/2000/svg" font-family="'IBM Plex Sans Variable', sans-serif">
  <defs>
    <marker id="kr-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0L8 4L0 8z" fill="#00c5d1"/>
    </marker>
  </defs>
  <rect x="0" y="0" width="720" height="488" rx="16" fill="#061827"/>
  <rect x="220" y="26" width="280" height="42" rx="8" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="360" y="52" text-anchor="middle" fill="#ffffff" font-size="14" font-weight="600">RESP-compatible clients</text>
  <g stroke="#3b5a70" stroke-width="1.5">
    <line x1="360" y1="68" x2="360" y2="84"/>
    <line x1="260" y1="84" x2="600" y2="84"/>
    <line x1="260" y1="84" x2="260" y2="100"/>
    <line x1="600" y1="84" x2="600" y2="100"/>
  </g>
  <rect x="40" y="100" width="440" height="252" rx="14" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="64" y="127" fill="#cdd9e1" font-size="13" font-weight="600">Kronotop member</text>
  <rect x="56" y="140" width="408" height="110" rx="8" fill="none" stroke="#00c5d1" stroke-width="1.5" stroke-dasharray="6 5"/>
  <text x="260" y="162" text-anchor="middle" fill="#00c5d1" font-size="12" font-weight="600">strictly serializable transactions · namespaces</text>
  <g fill="none" stroke="#3b5a70" stroke-width="1.5">
    <rect x="68" y="174" width="184" height="60" rx="8"/>
    <rect x="268" y="174" width="184" height="60" rx="8"/>
  </g>
  <g text-anchor="middle">
    <text x="160" y="199" fill="#ffffff" font-size="15" font-weight="600">Bucket</text>
    <text x="160" y="219" fill="#93a7b5" font-size="11">documents · BQL · indexes</text>
    <text x="360" y="199" fill="#ffffff" font-size="15" font-weight="600">ZMap</text>
    <text x="360" y="219" fill="#93a7b5" font-size="11">ordered key-value</text>
  </g>
  <line x1="260" y1="250" x2="260" y2="274" stroke="#3b5a70" stroke-width="1.5"/>
  <rect x="148" y="274" width="224" height="58" rx="8" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="260" y="298" text-anchor="middle" fill="#ffffff" font-size="14" font-weight="600">Volume</text>
  <text x="260" y="318" text-anchor="middle" fill="#93a7b5" font-size="11">segments</text>
  <rect x="520" y="100" width="160" height="252" rx="14" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="600" y="127" text-anchor="middle" fill="#93a7b5" font-size="13" font-weight="600">standby member</text>
  <rect x="536" y="274" width="128" height="58" rx="8" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="600" y="298" text-anchor="middle" fill="#cdd9e1" font-size="14" font-weight="600">Volume</text>
  <text x="600" y="318" text-anchor="middle" fill="#93a7b5" font-size="11">segments</text>
  <line x1="372" y1="303" x2="530" y2="303" stroke="#00c5d1" stroke-width="1.5" stroke-dasharray="6 5" marker-end="url(#kr-arrow)"/>
  <text x="452" y="292" text-anchor="middle" fill="#00c5d1" font-size="11" font-weight="600">replication</text>
  <g stroke="#3b5a70" stroke-width="1.5">
    <line x1="260" y1="352" x2="260" y2="384"/>
    <line x1="600" y1="352" x2="600" y2="384"/>
  </g>
  <rect x="40" y="384" width="640" height="72" rx="12" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="360" y="414" text-anchor="middle" fill="#cdd9e1" font-size="14" font-weight="600">FoundationDB</text>
  <text x="360" y="434" text-anchor="middle" fill="#93a7b5" font-size="11">metadata · indexes · ZMap data · cluster state</text>
</svg>

## Wire Protocol and Sessions

Kronotop speaks RESP2 and RESP3 and works with existing RESP-compatible clients. There is no separate query
endpoint or admin protocol; everything, including cluster administration, is a RESP command. Each member listens
on two ports: one for clients, one for cluster administration.

Every client connection is bound to a [session](sessions/index.md). The session holds its attributes, its current
namespace, the active transaction if one is open, and its cursors.

## Kronotop and FoundationDB

Every Kronotop [transaction](transactions/index.md) is a
FoundationDB transaction; strict serializability, conflict detection, and the ordered keyspace come from
FoundationDB. Kronotop adds the RESP front end, the document layer with its query engine and indexes, and the
Volume storage engine on top.

FoundationDB holds everything that must be transactional and small: bucket metadata and index entries, ZMap data,
namespace directories, volume metadata, and cluster state. Document bodies are the exception. FoundationDB is
optimized for small key-value pairs and enforces a 100 KB value-size limit, which document bodies routinely exceed.
[Volume](volume/index.md) offloads them to append-only segment files on the member's local disk and keeps only
pointers in FoundationDB.

The write path preserves transactional guarantees across the split: a document body is appended to a segment file
and flushed to disk first; only after the flush succeeds is the metadata committed to FoundationDB. Metadata never
references content that has not been persisted.

## Sharding

Bucket data is partitioned into shards. Each shard owns exactly one volume, named after the shard
(`bucket-shard-0`, `bucket-shard-1`, and so on). A bucket spans one or more shards, so its documents are
distributed across one or more volumes. Within a volume, each bucket's data is isolated by a prefix.

Shard ownership is assigned through cluster routing: one member is the primary and serves writes, standby members
replicate from it and can be promoted. Each shard also carries a status that controls traffic: `READWRITE`,
`READONLY`, or `INOPERABLE`. Routing and status are managed with the
[admin command interface](admin/cluster/operations-guide.md).

ZMap data is not sharded by Kronotop. It lives directly in FoundationDB, which partitions its own keyspace
automatically.

## Replication

Volume replication is asynchronous and primary-to-standby. Each shard's volume is replicated independently:
standbys pull from the primary, first copying existing segment data in chunks until they reach the primary's
current write position, then streaming incremental changes from a changelog maintained in FoundationDB.
Replication progress is also persisted in FoundationDB, so a standby can restart and resume exactly where it
left off.

Replication starts automatically when a standby is assigned through cluster routing. Promoting a standby and
reassigning shards are explicit operator actions. See [Volume replication](volume/index.md#replication) for the
mechanics and the [operations guide](admin/cluster/operations-guide.md) for the commands.

## Cluster Coordination

All coordination state goes through FoundationDB; there is no separate consensus layer or gossip protocol.
Members do not talk to each other to agree on cluster state, they read and write it in FoundationDB.

Failure detection works the same way. Each member periodically increments a heartbeat counter in FoundationDB,
and other members expect that counter to keep advancing. A member whose counter stalls beyond a configured silent
period is suspected dead. This is a local judgement made by each observer, not a cluster-wide consensus, and a
suspected member drops off the list as soon as its heartbeats resume. See
[health monitoring](admin/cluster/operations-guide.md#health-monitoring) for the details.
