#!/usr/bin/env sh

# Add to your `crontab` as:
#     * * * * * /Users/derekwan/work/python-utilities/tests/test_yield_access/script.sh

# helpers
echo_pid_date() { echo "[$$ | $(date +'%Y-%m-%d %H:%M:%S')] $*"; }

# add to `$PATH`
for __dir in "${HOME}/.local/bin" '/opt/homebrew/bin' '/opt/homebrew/opt/postgresql@17/bin'; do
    if [ -d "${__dir}" ]; then
        case ":${PATH}:" in
        *:"$__dir":*) ;;
        *)
            export PATH="${__dir}:${PATH}"
            ;;
        esac
    fi
done

# log file
__package_dir="${HOME}/work/python-utilities"
__log_file="${__package_dir}/.logs/test-yield-access"

# check if binaries are accessible
for __app in direnv just uv; do
    if ! command -v "${__app}" >/dev/null 2>&1; then
        echo_pid_date "ERROR: Command '${__app}' not found; exiting..." 2>&1 | tee -a "${__log_file}"
        exit
    fi
done

# enter package
echo_pid_date "Entering package directory '${__package_dir}'..." 2>&1 | tee -a "${__log_file}"
cd "${__package_dir}" || exit

# trim log
__threshold=$((10 * 1024 * 1024))
if [ -f "${__log_file}" ]; then
    __log_size=$(wc -c <"${__log_file}")
    if [ "${__log_size}" -gt "${__threshold}" ]; then
        echo_pid_date "Truncating log file '${__log_file}' (log size = ${__log_size})..." 2>&1 | tee -a "${__log_file}"
        __total_lines=$(wc -l <"${__log_file}")
        __keep_lines=$((__total_lines / 2))
        __tmp_log_file="${__log_file}.tmp.$$"
        tail -n "${__keep_lines}" "${__log_file}" >"${__tmp_log_file}" && mv "${__tmp_log_file}" "${__log_file}"
    fi
fi

# run the script
echo_pid_date "Running 'PYTHONPATH=src/tests/test_yield_access python -m script'..." 2>&1 | tee -a "${__log_file}"
__start="$(date +%s)"
PYTHONPATH=src/tests/test_yield_access direnv exec . python -m script "$*" 2>&1 | tee -a "${__log_file}"
__exit_code="$?"
