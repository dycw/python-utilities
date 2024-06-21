#!/usr/bin/env bash

_PATH='scripts/packages.txt'
while IFS= read -r _PACKAGE; do
	uv pip sync "requirements/${_PACKAGE}.txt"
	if [[ "${_PACKAGE}" == scripts-* ]]; then
		name="${_PACKAGE#scripts-}"
		path_test="scripts/test_${name//-/_}.py"
	else
		path_test="test_${_PACKAGE//-/_}.py"
	fi
	pytest --no-cov "src/tests/${path_test}"
	exit_code=$?
	if [ $exit_code -ne 0 ]; then
		break
	fi
done <"$_PATH"
uv pip sync requirementst.txt
