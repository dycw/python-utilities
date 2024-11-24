#!/usr/bin/env bash

_PATH='scripts/packages.txt'
while IFS= read -r _PACKAGE; do
	uv pip compile \
		"--extra=test" \
		"--extra=zzz-test-${_PACKAGE}" \
		--quiet \
		--prerelease=explicit \
		"--output-file=requirements/${_PACKAGE}.txt" \
		--upgrade \
		--python-version=3.11 \
		pyproject.toml
done <"$_PATH"
