FROM foundationdb/foundationdb:7.3.26

RUN sed -i '$ s/$/ --knob_disable_posix_kernel_aio=1/' /var/fdb/scripts/fdb.bash

EXPOSE 4500/tcp
ENV FDB_NETWORKING_MODE="host"
ENTRYPOINT ["/var/fdb/scripts/fdb.bash"]