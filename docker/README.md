# How to build and push a Docker image

**1-** Build Kronotop on your local:

```
./mvnw clean install -DskipTests
```

**2-** Copy the JAR files to `docker` folder:

```
cp kronotop/target/kronotop-2026.08-1.jar docker/kronotop.jar
cp kronotop-ctl/target/kronotop-ctl-2026.08-1.jar docker/kronotop-ctl.jar
cp kronotop-cli/target/kronotop-cli-2026.08-1.jar docker/kronotop-cli.jar
```

Go to `docker` folder after copying the JARs.

**3-** Build the image, you may want to change the `tag`.

```
docker build -t ghcr.io/kronotop/kronotop:latest --platform=linux/amd64 .
```

**4-** Push the image to GitHub Container Registry:

```
docker push ghcr.io/kronotop/kronotop:latest
```

*Note:* You need to log in to ghcr.io with the required token.

# How to run Kronotop with Docker Compose

```
docker compose up
```

The primary node will automatically bootstrap the cluster once all nodes are ready.

# Environment variables

The entrypoint script `start.sh` reads these variables:

| Variable                      | Description                                                                                                                                     |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `KRONOTOP_EXTERNAL_ADVERTISE` | Comma-separated list of `host:port` addresses clients use to reach this node. The first entry is the preferred one.                             |
| `KRONOTOP_INTERNAL_ADVERTISE` | Comma-separated list of `host:port` addresses other nodes use to reach this node.                                                               |
| `KRONOTOP_INIT_FDB`           | When set, this node configures a new FoundationDB cluster if none exists. Set it on one node only. Other nodes wait until the cluster is ready. |
| `KRONOTOP_BOOTSTRAP`          | Shard layout for `kronotop-ctl bootstrap`. Set it on one node only.                                                                             |
| `KRONOTOP_STANDBY_HOST`       | Host that must answer PING before bootstrap runs.                                                                                               |
| `KRONOTOP_OPTS`               | Replaces the default JVM options.                                                                                                               |

The container binds both ports to `0.0.0.0`, so it cannot know which address others use to reach it.
Every node needs both advertise variables and fails at startup without them. The compose files in
this folder already set them. `sharding.md` explains them in more detail.
