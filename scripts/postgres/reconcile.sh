#!/usr/bin/env bash
set -euo pipefail

# Unlike /docker-entrypoint-initdb.d, this is safe to run on every deployment;
# it applies role password rotations and repairs grants on existing volumes.
bash /docker-entrypoint-initdb.d/01-create-databases-and-users.sh
