FROM ubuntu:noble

ARG JDK_VERSION="21"
ARG KR_HOME="/opt/kronotop"
ARG FOUNDATIONDB_VERSION="7.3.64"
ARG FOUNDATIONDB_CLIENTS_DEB="foundationdb-clients_7.3.64-1_amd64.deb"

WORKDIR $KR_HOME

ENV KR_HOME=$KR_HOME \
    PATH=${KR_HOME}/bin:$PATH

# Install
RUN apt-get update && apt-get -y install openjdk-${JDK_VERSION}-jre-headless

# Install the dependency
ADD https://github.com/apple/foundationdb/releases/download/$FOUNDATIONDB_VERSION/$FOUNDATIONDB_CLIENTS_DEB $KR_HOME
RUN dpkg -i $KR_HOME/$FOUNDATIONDB_CLIENTS_DEB

# Cleanup
RUN rm $KR_HOME/foundationdb-clients_7.3.64-1_amd64.deb
RUN apt-get clean

COPY *.jar $KR_HOME/
COPY start.sh $KR_HOME/bin/

RUN echo "docker:docker@foundationdb:4500" > $KR_HOME/fdb.cluster

EXPOSE 5484
EXPOSE 3320

CMD ["start.sh"]
