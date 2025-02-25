# How to build and push a Docker image

**1-** Build Kronotop on your local:

```
./mvnw clean install -DskipTests
```

**2-** Copy the JAR file to `docker` folder, the copied file must be named as `kronotop-SNAPSHOT.jar`.

```
cp kronotop/target/kronotop-$VERSION-SNAPSHOT.jar kronotop-SNAPSHOT.jar
```

Go to `docker` folder after copying the JAR.

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

It's simple as the following:

```
docker compose up
```