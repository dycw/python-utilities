#!/usr/bin/env bash

packages=(
    ast
    atomicwrites
    beartype
    bs4
    cacher
    cachetools
    click
    fastapi
    more-itertools
    scipy
    xlrd
)
for package in "${packages[@]}"; do
    uv pip compile \
        "--extra=${package}" \
        "--extra=zzz-test-${package}" \
        "--extra=test" \
        --quiet \
        --prerelease=disallow \
        "--output-file=requirements/${package}.txt" \
        --upgrade \
        --python-version=3.10 \
        pyproject.toml
done
