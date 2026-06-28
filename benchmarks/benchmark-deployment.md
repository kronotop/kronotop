# Benchmark Deployment

Three-host benchmark deployment for Kronotop on EC2. All hosts should be in the same availability zone for minimal
network latency (~0.1-0.3ms).

## Host Layout

| Host | Role             | Instance      | vCPU | RAM  | Storage             |
|------|------------------|---------------|------|------|---------------------|
| A    | FoundationDB     | `m5d.4xlarge` | 16   | 64GB | 2x 300GB NVMe       |
| B    | Kronotop         | `m5d.2xlarge` | 8    | 32GB | NVMe instance store |
| C    | Benchmark Client | `m5.2xlarge`  | 8    | 32GB | EBS gp3             |

**OS**: Ubuntu 26.04 LTS on all hosts.

## Host A: FoundationDB Setup

### 1. Install FoundationDB

```bash
wget https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-clients_7.3.68-1_amd64.deb
wget https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-server_7.3.68-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.3.68-1_amd64.deb foundationdb-server_7.3.68-1_amd64.deb
```

### 2. Stop the default FDB process

```bash
sudo systemctl stop foundationdb
```

### 3. Mount NVMe drives

Two NVMe drives are separated by role: commit path (tlog, proxies, resolver) on NVMe #1, data path (storage) on NVMe #2.

```bash
sudo mkfs.ext4 /dev/nvme1n1
sudo mkdir -p /mnt/nvme0
sudo mount -o defaults,noatime,discard /dev/nvme1n1 /mnt/nvme0

sudo mkfs.ext4 /dev/nvme2n1
sudo mkdir -p /mnt/nvme1
sudo mount -o defaults,noatime,discard /dev/nvme2n1 /mnt/nvme1
```

### 4. Create data directories

```bash
# Commit path (NVMe #1)
for port in 4510 4520 4521 4522 4523 4530; do
    sudo mkdir -p /mnt/nvme0/fdb/$port
    sudo chown foundationdb:foundationdb /mnt/nvme0/fdb/$port
done

# Data path (NVMe #2)
for port in 4500 4501 4502; do
    sudo mkdir -p /mnt/nvme1/fdb/$port
    sudo chown foundationdb:foundationdb /mnt/nvme1/fdb/$port
done
```

### 5. Copy the config

```bash
sudo cp foundationdb.conf /etc/foundationdb/foundationdb.conf
```

### 6. Update the cluster file

The default cluster file may use `127.0.0.1`, which makes FDB bind to loopback only. Replace it with Host A's private IP
so that other hosts can connect:

```bash
echo "benchmark:benchmark@<HOST_A_PRIVATE_IP>:4500" | sudo tee /etc/foundationdb/fdb.cluster
```

### 7. Start FoundationDB

```bash
sudo systemctl start foundationdb
```

### 8. Initialize the database

```bash
fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single ssd-redwood-1"
```

### 9. Verify

```bash
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status"
```

All 9 processes should be listed: 3 storage, 1 transaction, 2 commit_proxy, 2 grv_proxy, 1 resolver.

## Role Layout

| Ports     | Role         | Disk  | Count | Purpose                             |
|-----------|--------------|-------|-------|-------------------------------------|
| 4500-4502 | storage      | nvme1 | 3     | Data storage and read serving       |
| 4510      | transaction  | nvme0 | 1     | Transaction log (commit durability) |
| 4520-4521 | commit_proxy | nvme0 | 2     | Commit path coordination            |
| 4522-4523 | grv_proxy    | nvme0 | 2     | Read version distribution           |
| 4530      | resolver     | nvme0 | 1     | Conflict detection                  |

## Host B: Kronotop Setup

### 1. Install Java 25

```bash
sudo apt install -y openjdk-25-jdk
```

### 2. Install the FDB client library

Kronotop uses the FDB client library to connect to the FoundationDB cluster.

```bash
wget https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-clients_7.3.68-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.3.68-1_amd64.deb
```

### 3. Configure the cluster file

Point the cluster file to Host A's IP address:

```bash
echo "benchmark:benchmark@<HOST_A_PRIVATE_IP>:4500" | sudo tee /etc/foundationdb/fdb.cluster
```

### 4. Mount NVMe for Volume storage

```bash
sudo mkfs.ext4 /dev/nvme1n1
sudo mkdir -p /var/lib/kronotop
sudo mount -o defaults,noatime,discard /dev/nvme1n1 /var/lib/kronotop
```

### 5. Run Kronotop

```bash
java -Xmx16g -Xms16g \
     --enable-native-access=ALL-UNNAMED \
     --add-modules jdk.incubator.vector \
     -jar kronotop.jar \
     -c kronotop.conf
```

16GB heap is enough for vector index workloads where the graph lives on-heap (JVector). The remaining ~16GB serves OS
page cache, which accelerates Volume segment reads.

## Host C: Benchmark Client

### 1. Install Java 25

```bash
sudo apt install -y openjdk-25-jdk
```

### 2. Run benchmarks

```bash
java -jar kronotop-benchmark-2026.06-4.jar bucket insert --host <HOST_B_PRIVATE_IP> --threads 50
java -jar kronotop-benchmark-2026.06-4.jar bucket query --host <HOST_B_PRIVATE_IP> --threads 50
java -jar kronotop-benchmark-2026.06-4.jar bucket mixed --host <HOST_B_PRIVATE_IP> --threads 50
java -jar kronotop-benchmark-2026.06-4.jar vector tweets --host <HOST_B_PRIVATE_IP> --threads 50
```

Threads are virtual threads, so high thread counts do not require proportional CPU cores.

## Notes

- This configuration uses `single` redundancy mode (no replication). Not suitable for production.
- `storage-memory` is set to 6GiB per storage process (18GiB total across 3 storage processes).
- `memory = 8GiB` is the per-process OOM limit, not allocation. FDB will stay well below this under normal operation.
- Transaction log and storage processes are on separate NVMe drives to isolate commit latency from data I/O.
- ext4 is used with `noatime,discard` mount options as recommended by FoundationDB documentation.
