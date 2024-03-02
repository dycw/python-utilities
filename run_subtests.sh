#!/usr/bin/env bash

packages=(
    zarr
)
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
    memory-profiler
    more-itertools
    numpy
    pandas
    pathvalidate
    polars
    pqdm
    pydantic
    pyinstrument
    pytest-check
    scipy
    semver
    sqlalchemy
    sqlalchemy-polars
    typed-settings
    xarray
    xlrd
    zarr
)
for package in "${packages[@]}"; do
    uv pip sync "requirements/${package}.txt"

    pytest "src/tests/test_${package//-/_}.py" -x
done
