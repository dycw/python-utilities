#!/usr/bin/env bash

cat <<EOF >ruff.toml
line-length = 320

[format]
skip-magic-trailing-comma = true

[lint.isort]
split-on-trailing-comma = false

EOF
ruff format .
rm ruff.toml
ruff format .
