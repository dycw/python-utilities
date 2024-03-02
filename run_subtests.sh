#!/usr/bin/env bash

# for package in ast atomicwrites beartype fastapi more-itertools scipy xlrd; do
packages=(
    numpy
)
for package in "${packages[@]}"; do
    uv pip sync "requirements/${package}.txt"
    pytest "src/tests/test_${package}.py"
done

if false; then
    uv pip sync "requirements/polars-bs4.txt"
    pytest "src/tests/polars/test_bs4.py"
fi
