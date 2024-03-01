#!/usr/bin/env bash

for package in ast atomicwrites beartype fastapi more-itertools scipy xlrd; do
    uv pip compile \
        "--extra=test-${package}" \
        --quiet \
        --prerelease=disallow \
        "--output-file=requirements/${package}.txt" \
        --upgrade \
        --python-version=3.10 \
        pyproject.toml
done
