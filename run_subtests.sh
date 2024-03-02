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
    scripts-clean-dir
    scripts-csv-to-markdown
    scripts-generate-snippets
    scripts-luigi-server
    scripts-monitor-memory
    scripts-pypi-server
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
    if [[ "${package}" == scripts-* ]]; then
        name="${package#scripts-}"
        path_test="scripts/test_${name//-/_}.py"
    else
        path_test="test_${package//-/_}.py"
    fi
    pytest "src/tests/${path_test}" -x
done
