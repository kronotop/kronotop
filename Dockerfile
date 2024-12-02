FROM ubuntu:noble

ARG JDK_VERSION="21"
ARG KR_HOME="/opt/kronotop"
ARG FOUNDATIONDB_VERSION="7.3.26"
ARG FOUNDATIONDB_CLIENTS_DEB="foundationdb-clients_7.3.26-1_amd64.deb"

WORKDIR $KR_HOME

# Install
RUN apt-get update && apt-get -y install openjdk-${JDK_VERSION}-jre-headless

# Install the dependency
ADD https://github.com/apple/foundationdb/releases/download/$FOUNDATIONDB_VERSION/$FOUNDATIONDB_CLIENTS_DEB $KR_HOME
RUN dpkg -i $KR_HOME/$FOUNDATIONDB_CLIENTS_DEB

# Cleanup
RUN rm $KR_HOME/foundationdb-clients_7.3.26-1_amd64.deb
RUN apt-get clean

COPY kronotop/target/kronotop-1.0-SNAPSHOT.jar $KR_HOME/kronotop-latest.jar

EXPOSE 5484
ENTRYPOINT ["java", "-jar", "kronotop-latest.jar"]
