FROM redhat/ubi9-minimal:9.3

ARG JDK_VERSION="21"
ARG KR_HOME="/opt/kronotop"
ARG FOUNDATIONDB_VERSION="7.3.26"
ARG FOUNDATIONDB_CLIENTS_RPM="foundationdb-clients-7.3.26-1.el7.x86_64.rpm"

WORKDIR $KR_HOME

# Install
RUN echo "Installing new packages" \
    && microdnf -y update --nodocs \
    && microdnf -y --nodocs --disablerepo=* --enablerepo=ubi-9-appstream-rpms --enablerepo=ubi-9-baseos-rpms \
        --disableplugin=subscription-manager install shadow-utils java-${JDK_VERSION}-openjdk-headless tzdata-java pkgconfig

# Install the dependency
ADD https://github.com/apple/foundationdb/releases/download/$FOUNDATIONDB_VERSION/$FOUNDATIONDB_CLIENTS_RPM $KR_HOME
RUN rpm -i $KR_HOME/$FOUNDATIONDB_CLIENTS_RPM

# Cleanup
RUN rm $KR_HOME/foundationdb-clients-7.3.26-1.el7.x86_64.rpm
RUN microdnf -y clean all

COPY kronotop/target/kronotop-1.0-SNAPSHOT.jar $KR_HOME/kronotop-latest.jar

EXPOSE 5484
ENTRYPOINT ["java", "-jar", "kronotop-latest.jar"]
