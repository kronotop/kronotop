FROM eclipse-temurin:26-jre-noble

ARG KR_HOME="/opt/kronotop"
ARG FOUNDATIONDB_VERSION="7.3.68"
ARG FOUNDATIONDB_CLIENTS_DEB="foundationdb-clients_7.3.68-1_amd64.deb"

WORKDIR $KR_HOME

ENV KR_HOME=$KR_HOME \
    PATH=${KR_HOME}/bin:$PATH

# Install FoundationDB client
ADD https://github.com/apple/foundationdb/releases/download/$FOUNDATIONDB_VERSION/$FOUNDATIONDB_CLIENTS_DEB $KR_HOME
RUN dpkg -i $KR_HOME/$FOUNDATIONDB_CLIENTS_DEB && \
    rm $KR_HOME/$FOUNDATIONDB_CLIENTS_DEB

COPY *.jar $KR_HOME/
COPY start.sh $KR_HOME/bin/
COPY kronotop-cli.sh $KR_HOME/bin/kronotop-cli
RUN chmod +x $KR_HOME/bin/kronotop-cli

RUN echo "docker:docker@foundationdb:4500" > $KR_HOME/fdb.cluster

EXPOSE 5484
EXPOSE 3320

CMD ["start.sh"]
