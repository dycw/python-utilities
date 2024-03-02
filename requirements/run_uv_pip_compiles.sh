#!/usr/bin/env bash

packages=(
    ast
    atomicwrites
    beartype
    bs4
    cacher
    cachetools
    click
    cryptography
    cvxpy
    fastapi
    fpdf2
    holoviews
    ipython
    jupyter
    loguru
    luigi
    memory_profiler
    more_itertools
    numpy
    pandas
    pathvalidate
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
