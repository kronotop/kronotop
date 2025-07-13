# Why Kronotop Runs on the Java Platform

* [Scalable Concurrency Through Virtual Threads](#scalable-concurrency-through-virtual-threads)
* [A Battle‑Hardened Runtime](#a-battlehardened-runtime)
* [Comprehensive and Stable Ecosystem](#comprehensive-and-stable-ecosystem)
* [Enterprise Acceptance and Long‑Term Support](#enterprise-acceptance-and-longterm-support)
* [Native and Polyglot Interoperability](#native-and-polyglot-interoperability)
* [Summary](#summary)

Kronotop is an **I/O‑bound** distributed database: each client request fans out into multiple disk reads from FoundationDB
and network hops between cluster nodes. While the CPU spends most of its time waiting for these operations, throughput hinges
on how many outstanding requests we can keep in flight at any given moment.

Conventional kernel threads become prohibitively expensive when their count climbs into the hundreds of thousands, and
event‑loop frameworks reduce per‑task overhead only by pushing complexity onto the application. The Java platform offers
a third path —virtual threads— that delivers massive concurrency while preserving the clarity of straightforward, blocking code.
This capability, combined with mature tooling and a vast ecosystem, makes Java the natural fit for Kronotop.

## Scalable Concurrency Through Virtual Threads

*Virtual threads* are lightweight, user‑mode constructs that map one logical task—such as a client connection, a replication
RPC, or a background checkpoint—to its own thread. When that task performs a blocking disk read or waits on a network response,
the virtual thread parks and the underlying carrier thread is immediately returned to the scheduler. This arrangement lets
Kronotop manage hundreds of thousands of simultaneous I/O waits—flushing segments, streaming change‑feeds, or performing
multi‑shard fan‑outs—without exhausting memory or triggering scheduler contention.

## A Battle‑Hardened Runtime

The Java Virtual Machine has evolved over decades in mission‑critical environments. Its garbage collectors (G1, ZGC, Shenandoah, and others)
offer predictable pause times, while the JIT compiler optimizes hot paths without compromising source readability. Production observability
is first‑class: Java Flight Recorder, async‑profiler, and eBPF integrations expose low‑level behavior without intrusive instrumentation.

## Comprehensive and Stable Ecosystem

The platform’s extensive standard library and third‑party ecosystem eliminate the need to reinvent security, serialization,
network, or build tooling. Mature dependency‑management systems (Maven and Gradle), high‑quality static‑analysis tools, and
seamless CI/CD integration ensure that engineering velocity scales with team growth. Container‑ready base images and snapshotting
technology (such as CRaC) minimize operational friction.

## Enterprise Acceptance and Long‑Term Support

Java enjoys broad acceptance across regulated industries; security teams, legal departments, and procurement officers are
familiar with its deployment and licensing models. The six‑month release cadence provides rapid access to new features, while
long‑term‑support (LTS) versions give downstream users multi‑year stability. Backwards‑compatibility guarantees to protect existing
deployments as the language and runtime evolve.

## Native and Polyglot Interoperability

The **Java Native Interface (JNI)** provides a stable bridge between managed Java code and native libraries, facilitating direct
use of highly optimized cryptography, compression, or SIMD routines when required. In addition, the emerging **Foreign Function & Memory API**
extends this capability with safer, more ergonomic access patterns.

## Summary

Kronotop demands a runtime that excels at handling vast numbers of concurrent I/O operations, provides mature operational tooling,
and is acceptable to risk‑averse enterprise stakeholders. The contemporary Java platform, _enhanced by virtual threads_, offers
exactly this balance, enabling Kronotop to deliver high throughput, maintainable code, and predictable production behavior
without _exotic technology_ choices.