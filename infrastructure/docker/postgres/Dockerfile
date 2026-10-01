FROM postgres:16-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies for building Apache AGE and pgvector
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    postgresql-server-dev-16 \
    flex \
    bison \
    libreadline-dev \
    zlib1g-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install pgvector
RUN cd /tmp \
    && git clone --branch v0.7.4 https://github.com/pgvector/pgvector.git \
    && cd pgvector \
    && make \
    && make install \
    && rm -rf /tmp/pgvector

# Install Apache AGE for PostgreSQL 16
RUN cd /tmp \
    && git clone --branch PG16/v1.5.0-rc0 https://github.com/apache/age.git || git clone --branch master https://github.com/apache/age.git \
    && cd age \
    && make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config install \
    && rm -rf /tmp/age

# Cleanup build packages to minimize image size
RUN apt-get purge -y --auto-remove build-essential git postgresql-server-dev-16 flex bison \
    && rm -rf /var/lib/apt/lists/*

# Add configuration for AGE in postgresql.conf
RUN echo "shared_preload_libraries = 'age'" >> /usr/share/postgresql/postgresql.conf.sample

EXPOSE 5432
CMD ["postgres"]
