FROM postgres:16

# Install build dependencies
RUN apt-get update && \
    apt-get install -y \
    git \
    make \
    gcc \
    postgresql-server-dev-16 && \
    rm -rf /var/lib/apt/lists/*

# Clone and build wal2json
RUN git clone https://github.com/eulerto/wal2json.git && \
    cd wal2json && \
    make && \
    make install && \
    cd .. && \
    rm -rf wal2json

# Set up PostgreSQL configuration for logical replication
RUN echo "shared_preload_libraries = 'wal2json'" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_senders = 10" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_replication_slots = 10" >> /usr/share/postgresql/postgresql.conf.sample