FROM eclipse-temurin:27-jre-ubi10-minimal

ARG KR_HOME="/opt/kronotop"
ARG FOUNDATIONDB_VERSION="7.3.68"
ARG FOUNDATIONDB_CLIENTS_RPM="foundationdb-clients-7.3.68-1.el7.x86_64.rpm"

WORKDIR $KR_HOME

ENV KR_HOME=$KR_HOME \
    PATH=${KR_HOME}/bin:$PATH

# Install FoundationDB client
RUN curl -fsSL -o /tmp/$FOUNDATIONDB_CLIENTS_RPM https://github.com/apple/foundationdb/releases/download/$FOUNDATIONDB_VERSION/$FOUNDATIONDB_CLIENTS_RPM && \
    rpm -i --nodeps /tmp/$FOUNDATIONDB_CLIENTS_RPM && \
    rm /tmp/$FOUNDATIONDB_CLIENTS_RPM

COPY *.jar $KR_HOME/
COPY start.sh $KR_HOME/bin/
COPY kronotop-cli.sh $KR_HOME/bin/kronotop-cli
RUN chmod +x $KR_HOME/bin/kronotop-cli

RUN echo "docker:docker@foundationdb:4500" > $KR_HOME/fdb.cluster

EXPOSE 5484
EXPOSE 3320

CMD ["start.sh"]
