# How to build and push a Docker image

**1-** Build Kronotop on your local:

```
./mvnw clean install -DskipTests
```

**2-** Copy the JAR files to `docker` folder:

```
cp kronotop/target/kronotop-2026.06-2.jar docker/kronotop.jar
cp kronotop-ctl/target/kronotop-ctl-2026.06-2.jar docker/kronotop-ctl.jar
cp kronotop-cli/target/kronotop-cli-2026.06-2.jar docker/kronotop-cli.jar
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

It's as simple as the following:

```
docker compose up
```

The primary node will automatically bootstrap the cluster once all nodes are ready.
