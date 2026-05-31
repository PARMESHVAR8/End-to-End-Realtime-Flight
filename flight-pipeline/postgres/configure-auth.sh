#!/bin/bash
# Postgres init script to configure authentication
# This is run before the main postgres process starts

# Set authentication to allow password connections from localhost
# Modify pg_hba.conf to use md5 for IPv4 localhost connections
sed -i 's/^host    all             all             127.0.0.1\/32            trust$/host    all             all             127.0.0.1\/32            md5/' /var/lib/postgresql/data/pg_hba.conf || true

# Also add a line for all IPv4 if not present
grep -q "^host    all             all             0.0.0.0/0" /var/lib/postgresql/data/pg_hba.conf || \
  echo "host    all             all             0.0.0.0/0               md5" >> /var/lib/postgresql/data/pg_hba.conf

exit 0
