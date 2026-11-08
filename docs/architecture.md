---
title: "Architecture"
description: "How Kronotop is structured, from the RESP front end down to the FoundationDB and Volume storage layers."
---

RESP-compatible clients talk to a Kronotop member over TCP. The member hosts two data models:
[Bucket](bucket/index.md) for documents and [ZMap](zmap/index.md) for ordered key-value data. Both use the same
sessions, [namespaces](namespaces/index.md), and [transactions](transactions/index.md). FoundationDB stores
metadata, indexes, ZMap data, and cluster state. [Volume](volume/index.md), Kronotop's storage engine, keeps
document bodies on the member's local disk.

<svg viewBox="0 0 720 488" width="720" height="488" style="display:block;margin-inline:auto;max-width:100%;height:auto" role="img" aria-label="Kronotop architecture: RESP clients connect to a Kronotop member. Inside the member, the Bucket and ZMap data models share one transaction boundary, and the Volume storage engine sits under Bucket. The volume replicates its segments to a standby member. Both members run on top of FoundationDB." xmlns="http://www.w3.org/2000/svg" font-family="'IBM Plex Sans Variable', sans-serif">
  <defs>
    <marker id="kr-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0L8 4L0 8z" fill="#22C7E0"/>
    </marker>
  </defs>
  <rect x="0" y="0" width="720" height="488" rx="16" fill="#07192B"/>
  <rect x="220" y="26" width="280" height="42" rx="8" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="360" y="52" text-anchor="middle" fill="#ffffff" font-size="14" font-weight="600">RESP-compatible clients</text>
  <g stroke="#3b5a70" stroke-width="1.5">
    <line x1="360" y1="68" x2="360" y2="84"/>
    <line x1="260" y1="84" x2="600" y2="84"/>
    <line x1="260" y1="84" x2="260" y2="100"/>
    <line x1="600" y1="84" x2="600" y2="100"/>
  </g>
  <rect x="40" y="100" width="440" height="252" rx="14" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="64" y="127" fill="#E6F1F5" font-size="13" font-weight="600">Kronotop member</text>
  <rect x="56" y="140" width="408" height="110" rx="8" fill="none" stroke="#22C7E0" stroke-width="1.5" stroke-dasharray="6 5"/>
  <text x="260" y="162" text-anchor="middle" fill="#22C7E0" font-size="12" font-weight="600">strictly serializable transactions · namespaces</text>
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
  <text x="600" y="298" text-anchor="middle" fill="#E6F1F5" font-size="14" font-weight="600">Volume</text>
  <text x="600" y="318" text-anchor="middle" fill="#93a7b5" font-size="11">segments</text>
  <line x1="372" y1="303" x2="530" y2="303" stroke="#22C7E0" stroke-width="1.5" stroke-dasharray="6 5" marker-end="url(#kr-arrow)"/>
  <text x="452" y="292" text-anchor="middle" fill="#22C7E0" font-size="11" font-weight="600">replication</text>
  <g stroke="#3b5a70" stroke-width="1.5">
    <line x1="260" y1="352" x2="260" y2="384"/>
    <line x1="600" y1="352" x2="600" y2="384"/>
  </g>
  <rect x="40" y="384" width="640" height="72" rx="12" fill="none" stroke="#3b5a70" stroke-width="1.5"/>
  <text x="360" y="414" text-anchor="middle" fill="#E6F1F5" font-size="14" font-weight="600">FoundationDB</text>
  <text x="360" y="434" text-anchor="middle" fill="#93a7b5" font-size="11">metadata · indexes · ZMap data · cluster state</text>
</svg>

## Wire protocol and sessions

Kronotop speaks RESP2 and RESP3 and works with existing RESP-compatible clients. Every operation, including
cluster administration, is a RESP command. There is no separate query endpoint or admin protocol. Each member
listens on two ports: one for clients and one for cluster administration.

Every client connection belongs to a [session](sessions/index.md). The session holds its attributes, its current
namespace, the open transaction if there is one, and its cursors.

## Kronotop and FoundationDB

Every Kronotop [transaction](transactions/index.md) is a FoundationDB transaction. Strict serializability,
conflict detection, and the ordered keyspace come from FoundationDB. Kronotop adds the RESP front end, the
document layer with its query engine and indexes, and the Volume storage engine.

FoundationDB holds all data that is small and must be transactional: bucket metadata and index entries, ZMap data,
namespace directories, volume metadata, and cluster state. Document bodies are different. FoundationDB is built
for small key-value pairs and limits a value to 100 KB, and document bodies often exceed that limit.
[Volume](volume/index.md) stores them in append-only segment files on the member's local disk and keeps only
pointers in FoundationDB.

Transactional guarantees still hold across this split. Kronotop first appends the document body to a segment file
and flushes it to disk. It commits the metadata to FoundationDB only after the flush succeeds, so metadata never
points to content that is not on disk.

## Sharding

Bucket data is split into shards. Each shard owns exactly one volume, named after the shard (`bucket-shard-0`,
`bucket-shard-1`, and so on). A bucket spans one or more shards, so its documents are spread across one or more
volumes. Inside a volume, a prefix keeps each bucket's data separate.

Cluster routing assigns shard ownership. One member is the primary and serves all Bucket commands. Standby members
replicate from the primary and can be promoted. Each shard also has a status that controls traffic: `READWRITE`,
`READONLY`, or `INOPERABLE`. You manage routing and status with the
[admin command interface](admin/cluster/operations-guide.md).

Kronotop does not shard ZMap data. ZMap data lives directly in FoundationDB, which partitions its own keyspace
automatically.

## Replication

Volume replication is asynchronous and goes from the primary to the standbys. Each shard's volume replicates on
its own. A standby pulls from the primary. It first copies the existing segment data in chunks until it reaches
the primary's current write position. After that, it streams new changes from a changelog stored in FoundationDB.
Replication progress is also stored in FoundationDB, so a standby can restart and continue from the point where it
stopped.

Replication starts automatically when cluster routing assigns a standby. An operator must promote a standby or
reassign shards by hand. See [Volume replication](volume/index.md#replication) for how replication works and the
[operations guide](admin/cluster/operations-guide.md) for the commands.

## Cluster coordination

All coordination state lives in FoundationDB. Kronotop has no separate consensus layer or gossip protocol. Members
do not talk to each other to agree on cluster state. They read and write it in FoundationDB.

Failure detection uses FoundationDB too. Each member increments a heartbeat counter in FoundationDB at regular
intervals, and other members expect the counter to keep growing. If a member's counter stops for longer than the
configured silent period, other members suspect that it is dead. Each member makes this decision by itself, with
no cluster-wide agreement. A suspected member drops off the list as soon as its heartbeats start again. See
[health monitoring](admin/cluster/operations-guide.md#health-monitoring) for details.
