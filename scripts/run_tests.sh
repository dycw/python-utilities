#!/usr/bin/env bash

packages=(
    # standard library
    ast
    dataclasses
    datetime
    email
    enum
    errors
    functions
    functools
    getpass
    git
    hashlib
    iterables
    json
    locale
    logging
    math
    modules
    os
    pathlib
    pickle
    platform
    random
    re
    sentinel
    socket
    subprocess
    sys
    tempfile
    text
    timer
    # third-party
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
    hatch
    holoviews
    humps
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
    pytest
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
    pytest --no-cov "src/tests/${path_test}"
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        break
    fi
done
