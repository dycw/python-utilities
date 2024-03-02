#!/usr/bin/env bash

# for package in ast atomicwrites beartype fastapi more-itertools scipy xlrd; do
packages=(
    xarray

)
for package in "${packages[@]}"; do
    uv pip sync "requirements/${package}.txt"

    pytest "src/tests/test_${package//-/_}.py" -x
done
