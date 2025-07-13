# Volume

* [Storage Engine](#storage-engine)
* [Design Philosophy and Architecture](#design-philosophy-and-architecture)
  * [Segments](#segments)
  * [Document Append Workflow](#document-append-workflow)
* [Replication](#replication)
* [Vacuuming (Space Reclamation)](#vacuuming-space-reclamation)
* [Management](management.md)
  
## Storage Engine

Kronotop uses a custom-built storage engine named **Volume**. As the core persistence layer for this distributed,
transactional document store, *Volume* is responsible for reliably storing all document data on local disks and managing
data replication between cluster members.

## Design Philosophy and Architecture

*Volume*'s architecture strategically separates metadata management from the storage of the actual document content,
leveraging the strengths of different systems:

1. **Metadata Management (*FoundationDB*):** All metadata associated with the stored documents (referred to as entries)
   —such as
   their location within storage segments, versioning information, and replication status—is managed within a
   **FoundationDB** cluster. Leveraging *FoundationDB* provides Kronotop with strong `ACID` transactional guarantees for
   all metadata operations, crucial for maintaining consistency in a distributed environment.
2. **Document Content Storage (Local Disk):** The actual content of the documents/entries is stored efficiently on the
   local disk of each Kronotop node. This data is organized into large, pre-allocated files called **segments**.

### Segments

Segments are the fundamental units for storing document data on disk:

* **Pre-allocation:** When needed, *Volume* creates segment files of a predetermined size on the filesystem.
* **Sequential Appends:** Document data is typically written sequentially into the latest active segment file.
  Internally, a segment can be viewed as a large byte array where new entry data is appended.

### Document Append Workflow

When a new document or entry is saved via Kronotop, the *Volume* engine follows these precise steps to ensure atomicity
and durability:

1. **Request Handling:** *Volume* receives the request to store a new entry.
2. **Segment Selection:** It identifies the current active segment file designated for new data writes.
3. **Segment Lifecycle Management:**
    * If no segment is currently active (e.g., on node startup), *Volume* creates and pre-allocates a new segment file.
    * If the current active segment doesn't have enough contiguous space for the new entry, it's typically sealed (
      marked as read-only), and a new segment is created to become the active target.
4. **Data Persistence:** The segment component writes the entry's data into the appropriate location within the active
   segment file.
5. **Disk Synchronization (Flush):** A *flush* operation is executed against the modified segment file. This requests
   the operating system to write any buffered data to the physical storage, ensuring the entry's content is persistent
   on disk before proceeding.
6. **Transactional Metadata Commit:** Crucially, only *after* the disk *flush* confirms successful persistence of the
   entry's data does *Volume* commit the associated metadata (e.g., the entry's location within the segment) to the
   *FoundationDB* cluster. This commit happens within a single, atomic *FoundationDB* transaction.
7. **Identifier Return:** Upon a successful metadata commit, *Volume* returns unique identifiers for the stored entries,
   often based on *FoundationDB*'s versionstamps, providing a consistent system-wide order.

This two-phase process guarantees that the metadata stored transactionally in *FoundationDB* only ever points to
document data that has been safely persisted to disk.

## Replication

*Volume* includes an integrated system for **asynchronous replication** to maintain copies of the data on standby nodes,
enhancing fault tolerance and availability.

* **Change Log (*SegmentLog*):** When an entry is successfully stored on the primary node, *Volume* records this event
  by adding metadata to a specific data structure in *FoundationDB* called *SegmentLog*. This acts as an ordered log of
  data modifications.
* **Notification via Watches:** Standby nodes monitor for changes by using *FoundationDB*'
  s [watch mechanism](https://apple.github.io/foundationdb/developer-guide.html#watches). They set watches on keys
  related to *SegmentLog*. When the primary updates this log, the watches trigger on the standbys, signaling that new
  data is available.
* **Data Synchronization:** Upon being triggered, a standby node fetches the actual document data from the primary
  node's segments corresponding to the new *SegmentLog* entries. This synchronization involves two phases:
    1. **Snapshot Phase:** Initially, or if significantly behind, the standby reads the *FoundationDB* history and
       systematically copies the required document data from the primary's segments until it reaches a reasonably
       current state. Data is often transferred in chunks.
    2. **Streaming Phase:** After the initial catch-up, the standby enters a streaming mode. It continuously watches for
       new *SegmentLog* entries and promptly fetches only the latest changes from the primary, maintaining low
       replication lag.

Kronotop also offers synchronous replication, but this functionality is provided through a distinct component, the
**Redis Volume Syncer**, which coordinates with *Volume*.

## Vacuuming (Space Reclamation)

Since segments are primarily append-based, space occupied by deleted or updated documents isn't immediately reclaimed.
*Volume* provides a **vacuuming** mechanism to manage disk usage.

* **Manual Initiation:** A system administrator can trigger the vacuuming process.
* **Background Operation:** Vacuuming runs as a background task to minimize the impact on live operations.
* **Process:** It involves scanning the *Volume* metadata within *FoundationDB* to identify segments containing a high
  proportion of "garbage" (space used by entries that are no longer valid or visible). If a segment's garbage ratio
  surpasses a defined threshold, the vacuuming process reorganizes the data, typically by copying the valid, live
  entries into new segments and then safely removing the old segment(s), thus reclaiming disk space.
