#!/usr/bin/env bash

cat <<EOF >ruff.toml
line-length = 320
EOF
ruff format .
rm ruff.toml
ruff format .
