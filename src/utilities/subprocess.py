from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from io import StringIO
from itertools import chain, repeat
from json import JSONDecodeError
from pathlib import Path
from re import MULTILINE, escape, search
from shlex import join
from shutil import rmtree
from string import Template
from subprocess import PIPE, Popen
from threading import Thread
from typing import IO, TYPE_CHECKING, Literal, assert_never, overload, override

import utilities.core
from utilities._core_errors import CopySourceNotFoundError, MoveSourceNotFoundError
from utilities.constants import HOME, HOSTNAME, PWD, SECOND
from utilities.contextlib import enhanced_context_manager
from utilities.core import (
    OneEmptyError,
    Permissions,
    TemporaryDirectory,
    always_iterable,
    copy,
    duration_to_seconds,
    file_or_dir,
    log_info,
    move,
    normalize_multi_line_str,
    normalize_str,
    one,
    repr_str,
    repr_table,
    sync_sleep,
    to_logger,
)
from utilities.errors import ImpossibleCaseError
from utilities.math import safe_round
from utilities.version import (
    ParseVersion2Or3Error,
    Version2,
    Version3,
    parse_version_2_or_3,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from utilities.core import PermissionsLike
    from utilities.types import (
        Duration,
        LoggerLike,
        MaybeIterable,
        MaybeSequenceStr,
        PathLike,
        Retry,
        StrMapping,
        StrStrMapping,
    )


APT_UPDATE: list[str] = ["apt", "update", "-y"]
BASH_LC: list[str] = ["bash", "-lc"]
BASH_LS: list[str] = ["bash", "-ls"]
CHPASSWD: str = "chpasswd"
GIT_BRANCH_SHOW_CURRENT: list[str] = ["git", "branch", "--show-current"]
HOST_KEY_ALGORITHMS: list[str] = ["ssh-ed25519"]
ISOLATED: str = "--isolated"
KNOWN_HOSTS: Path = HOME / ".ssh/known_hosts"
MANAGED_PYTHON: str = "--managed-python"
MKTEMP_DIR_CMD: list[str] = ["mktemp", "-d"]
PRERELEASE_DISALLOW: list[str] = ["--prerelease", "disallow"]
RESOLUTION_HIGHEST: list[str] = ["--resolution", "highest"]
RESTART_SSHD: list[str] = ["systemctl", "restart", "sshd"]
UPDATE_CA_CERTIFICATES: str = "update-ca-certificates"


##


def append_text(
    path: PathLike,
    text: str,
    /,
    *,
    sudo: bool = False,
    skip_if_present: bool = False,
    flags: int = 0,
    blank_lines: int = 1,
) -> None:
    """Append text to a file."""
    try:
        existing = cat(path, sudo=sudo)
    except (FileNotFoundError, RunError):
        tee(path, text, sudo=sudo)
        return
    if existing == "":
        tee(path, text, sudo=sudo)
        return
    if skip_if_present and (search(escape(text), existing, flags=flags) is not None):
        return
    full = "".join([*repeat("\n", times=blank_lines), text])
    tee(path, full, sudo=sudo, append=True)


##


def apt_install(
    package: str, /, *packages: str, update: bool = False, sudo: bool = False
) -> None:
    """Install packages."""
    if update:  # pragma: no cover
        apt_update(sudo=sudo)
    args = maybe_sudo_cmd(  # pragma: no cover
        *apt_install_cmd(package, *packages), sudo=sudo
    )
    run(*args)  # pragma: no cover


def apt_install_cmd(package: str, /, *packages: str) -> list[str]:
    """Command to use 'apt' to install packages."""
    return ["apt", "install", "-y", package, *packages]


##


def apt_remove(package: str, /, *packages: str, sudo: bool = False) -> None:
    """Remove a package."""
    args = maybe_sudo_cmd(  # pragma: no cover
        *apt_remove_cmd(package, *packages), sudo=sudo
    )
    run(*args)  # pragma: no cover


def apt_remove_cmd(package: str, /, *packages: str) -> list[str]:
    """Command to use 'apt' to remove packages."""
    return ["apt", "remove", "-y", package, *packages]


##


def apt_update(*, sudo: bool = False) -> None:
    """Update 'apt'."""
    run(*maybe_sudo_cmd(*APT_UPDATE, sudo=sudo))  # pragma: no cover


##


def cat(path: PathLike, /, *paths: PathLike, sudo: bool = False) -> str:
    """Concatenate and print files."""
    if sudo:  # pragma: no cover
        return run(*sudo_cmd(*cat_cmd(path, *paths)), return_=True)
    all_paths = list(map(Path, [path, *paths]))
    return "\n".join(p.read_text() for p in all_paths)


def cat_cmd(path: PathLike, /, *paths: PathLike) -> list[str]:
    """Command to use 'cat' to concatenate and print files."""
    return ["cat", str(path), *map(str, paths)]


##


def cd_cmd(path: PathLike, /) -> list[str]:
    """Command to use 'cd' to change working directory."""
    return ["cd", str(path)]


##


def chattr(
    path: PathLike, /, *, immutable: bool | None = None, sudo: bool = False
) -> None:
    """Change file attributes."""
    args = maybe_sudo_cmd(  # pragma: no cover
        *chattr_cmd(path, immutable=immutable), sudo=sudo
    )
    run(*args)  # pragma: no cover


def chattr_cmd(path: PathLike, /, *, immutable: bool | None = None) -> list[str]:
    """Command to use 'chattr' to change file attributes."""
    args: list[str] = ["chattr"]
    if immutable is True:
        args.append("+i")
    elif immutable is False:
        args.append("-i")
    return [*args, str(path)]


##


def chmod(
    path: PathLike,
    perms: PermissionsLike,
    /,
    *,
    sudo: bool = False,
    recursive: bool = False,
) -> None:
    """Change file mode."""
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*chmod_cmd(path, perms, recursive=recursive)))
    else:  # pragma: no cover
        utilities.core.chmod(path, perms, recursive=recursive)


def chmod_cmd(
    path: PathLike, perms: PermissionsLike, /, *, recursive: bool = False
) -> list[str]:
    """Command to use 'chmod' to change file mode."""
    args: list[str] = ["chmod"]
    if recursive:
        args.append("-R")
    return [*args, str(Permissions.new(perms)), str(path)]


##


def chown(
    path: PathLike,
    /,
    *,
    sudo: bool = False,
    recursive: bool = False,
    user: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Change file owner and/or group."""
    if sudo:  # pragma: no cover
        if (user is not None) or (group is not None):
            args = sudo_cmd(
                *chown_cmd(path, recursive=recursive, user=user, group=group)
            )
            run(*args)
    else:  # pragma: no cover
        utilities.core.chown(path, recursive=recursive, user=user, group=group)


def chown_cmd(
    path: PathLike,
    /,
    *,
    recursive: bool = False,
    user: str | int | None = None,
    group: str | int | None = None,
) -> list[str]:
    """Command to use 'chown' to change file owner and/or group."""
    args: list[str] = ["chown"]
    if recursive:
        args.append("-R")
    match user, group:
        case None, None:
            raise ChownCmdError
        case str() | int(), None:
            ownership = "user"
        case None, str() | int():
            ownership = f":{group}"
        case str() | int(), str() | int():
            ownership = f"{user}:{group}"
        case never:
            assert_never(never)
    return [*args, ownership, str(path)]


@dataclass(kw_only=True, slots=True)
class ChownCmdError(Exception):
    @override
    def __str__(self) -> str:
        return "At least one of 'user' and/or 'group' must be given; got None"


##


def chpasswd(user_name: str, password: str, /, *, sudo: bool = False) -> None:
    """Update passwords."""
    args = maybe_sudo_cmd(CHPASSWD, sudo=sudo)  # pragma: no cover
    run(*args, input=f"{user_name}:{password}")  # pragma: no cover


##


def copy_text(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    sudo: bool = False,
    substitutions: StrMapping | None = None,
) -> None:
    """Copy the text contents of a file."""
    text = cat(src, sudo=sudo)
    if substitutions is not None:
        text = Template(text).substitute(**substitutions)
    tee(dest, text, sudo=sudo)


##


def cp(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    sudo: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Copy a file/directory."""
    mkdir(dest, sudo=sudo, parent=True)
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*cp_cmd(src, dest)))
        if perms is not None:
            chmod(dest, perms, sudo=True)
        if (owner is not None) or (group is not None):
            chown(dest, sudo=True, user=owner, group=group)
    else:
        try:
            copy(src, dest, overwrite=True, perms=perms, owner=owner, group=group)
        except CopySourceNotFoundError as error:
            raise CpError(src=error.src) from None


@dataclass(kw_only=True, slots=True)
class CpError(Exception):
    src: Path

    @override
    def __str__(self) -> str:
        return f"Source {repr_str(self.src)} does not exist"


def cp_cmd(src: PathLike, dest: PathLike, /) -> list[str]:
    """Command to use 'cp' to copy a file/directory."""
    return ["cp", "-r", str(src), str(dest)]


##


@overload
def curl(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
    sudo: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def curl(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
    sudo: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[True],
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def curl(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
    sudo: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def curl(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
    sudo: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def curl(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
    sudo: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def curl(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
    sudo: bool = False,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    """Transfer a URL."""
    args = maybe_sudo_cmd(  # skipif-ci
        *curl_cmd(
            url,
            fail=fail,
            location=location,
            output=output,
            show_error=show_error,
            silent=silent,
        ),
        sudo=sudo,
    )
    return run(  # skipif-ci
        *args,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        suppress=suppress,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        retry=retry,
        retry_skip=retry_skip,
        logger=logger,
    )


def curl_cmd(
    url: str,
    /,
    *,
    fail: bool = True,
    location: bool = True,
    output: PathLike | None = None,
    show_error: bool = True,
    silent: bool = True,
) -> list[str]:
    """Command to use 'curl' to transfer a URL."""
    args: list[str] = ["curl"]
    if fail:
        args.append("--fail")
    if location:
        args.append("--location")
    if output is not None:
        args.extend(["--create-dirs", "--output", str(output)])
    if show_error:
        args.append("--show-error")
    if silent:
        args.append("--silent")
    return [*args, url]


##


def echo_cmd(text: str, /) -> list[str]:
    """Command to use 'echo' to write arguments to the standard output."""
    return ["echo", text]


##


def env_cmds(env: StrStrMapping, /) -> list[str]:
    return [f"{key}={value}" for key, value in env.items()]


##


def expand_path(
    path: PathLike, /, *, subs: StrMapping | None = None, sudo: bool = False
) -> Path:
    """Expand a path using `subprocess`."""
    if subs is not None:
        path = Template(str(path)).substitute(**subs)
    if sudo:  # pragma: no cover
        return Path(run(*sudo_cmd(*echo_cmd(str(path))), return_=True))
    return Path(path).expanduser()


##


def git_branch_current(path: PathLike, /) -> str:
    """Show the current a branch."""
    return run(*GIT_BRANCH_SHOW_CURRENT, cwd=path, return_=True)


##


def git_checkout(branch: str, path: PathLike, /) -> None:
    """Switch a branch."""
    run(*git_checkout_cmd(branch), cwd=path)


def git_checkout_cmd(branch: str, /) -> list[str]:
    """Command to use 'git checkout' to switch a branch."""
    return ["git", "checkout", branch]


##


def git_clone(
    url: str, path: PathLike, /, *, sudo: bool = False, branch: str | None = None
) -> None:
    """Clone a repository."""
    rm(path, sudo=sudo)
    run(*maybe_sudo_cmd(*git_clone_cmd(url, path), sudo=sudo))
    if branch is not None:
        git_checkout(branch, path)


def git_clone_cmd(url: str, path: PathLike, /) -> list[str]:
    """Command to use 'git clone' to clone a repository."""
    return ["git", "clone", "--recurse-submodules", url, str(path)]


##


def install(
    path: PathLike,
    /,
    *,
    directory: bool = False,
    mode: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
    sudo: bool = False,
) -> None:
    """Install a binary."""
    args = maybe_sudo_cmd(
        *install_cmd(path, directory=directory, mode=mode, owner=owner, group=group),
        sudo=sudo,
    )
    run(*args)


def install_cmd(
    path: PathLike,
    /,
    *,
    directory: bool = False,
    mode: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> list[str]:
    """Command to use 'install' to install a binary."""
    args: list[str] = ["install"]
    if directory:
        args.append("-d")
    if mode is not None:
        args.extend(["-m", str(Permissions.new(mode))])
    if owner is not None:
        args.extend(["-o", str(owner)])
    if group is not None:
        args.extend(["-g", str(group)])
    if directory:
        args.append(str(path))
    else:
        args.extend(["/dev/null", str(path)])
    return args


##


def maybe_parent(path: PathLike, /, *, parent: bool = False) -> Path:
    """Get the parent of a path, if required."""
    path = Path(path)
    return path.parent if parent else path


##


def mkdir(path: PathLike, /, *, sudo: bool = False, parent: bool = False) -> None:
    """Make a directory."""
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*mkdir_cmd(path, parent=parent)))
    else:
        maybe_parent(path, parent=parent).mkdir(parents=True, exist_ok=True)


##


def mkdir_cmd(path: PathLike, /, *, parent: bool = False) -> list[str]:
    """Command to use 'mv' to make a directory."""
    return ["mkdir", "-p", str(maybe_parent(path, parent=parent))]


##


def mv(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    sudo: bool = False,
    perms: PermissionsLike | None = None,
    owner: str | int | None = None,
    group: str | int | None = None,
) -> None:
    """Move a file/directory."""
    mkdir(dest, sudo=sudo, parent=True)
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*cp_cmd(src, dest)))
        if perms is not None:
            chmod(dest, perms, sudo=True)
        if (owner is not None) or (group is not None):
            chown(dest, sudo=True, user=owner, group=group)
    else:
        try:
            move(src, dest, overwrite=True, perms=perms, owner=owner, group=group)
        except MoveSourceNotFoundError as error:
            raise MvFileError(src=error.src) from None


@dataclass(kw_only=True, slots=True)
class MvFileError(Exception):
    src: Path

    @override
    def __str__(self) -> str:
        return f"Source {repr_str(self.src)} does not exist"


def mv_cmd(src: PathLike, dest: PathLike, /) -> list[str]:
    """Command to use 'mv' to move a file/directory."""
    return ["mv", str(src), str(dest)]


##


def replace_text(
    path: PathLike, /, *replacements: tuple[str, str], sudo: bool = False
) -> None:
    """Replace the text in a file."""
    path = Path(path)
    text = cat(path, sudo=sudo)
    for old, new in replacements:
        text = text.replace(old, new)
    tee(path, text, sudo=sudo)


##


def ripgrep(*args: str, path: PathLike = PWD) -> str | None:
    """Search for lines."""
    try:  # skipif-ci
        return run(*ripgrep_cmd(*args, path=path), return_=True)
    except RunError as error:  # skipif-ci
        if error.return_code == 1:
            return None
        raise


def ripgrep_cmd(*args: str, path: PathLike = PWD) -> list[str]:
    """Command to use 'ripgrep' to search for lines."""
    return ["rg", *args, str(path)]


##


def rm(path: PathLike, /, *paths: PathLike, sudo: bool = False) -> None:
    """Remove a file/directory."""
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*rm_cmd(path, *paths)))
    else:
        all_paths = list(map(Path, [path, *paths]))
        for p in all_paths:
            match file_or_dir(p):
                case "file":
                    p.unlink(missing_ok=True)
                case "dir":
                    rmtree(p, ignore_errors=True)
                case None:
                    ...
                case never:
                    assert_never(never)


def rm_cmd(path: PathLike, /, *paths: PathLike) -> list[str]:
    """Command to use 'rm' to remove a file/directory."""
    return ["rm", "-rf", str(path), *map(str, paths)]


##


def rsync(
    src_or_srcs: MaybeIterable[PathLike],
    user: str,
    hostname: str,
    dest: PathLike,
    /,
    *,
    sudo: bool = False,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    print: bool = False,  # noqa: A002
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    chown_user: str | None = None,
    chown_group: str | None = None,
    exclude: MaybeSequenceStr | None = None,
    timeout: Duration | None = None,
    chmod: PermissionsLike | None = None,
) -> None:
    """Remote & local file copying."""
    mkdir_args = maybe_sudo_cmd(*mkdir_cmd(dest, parent=True), sudo=sudo)  # skipif-ci
    ssh(  # skipif-ci
        user,
        hostname,
        *mkdir_args,
        batch_mode=batch_mode,
        host_key_algorithms=host_key_algorithms,
        strict_host_key_checking=strict_host_key_checking,
        print=print,
        retry=retry,
        logger=logger,
    )
    srcs = list(always_iterable(src_or_srcs))  # skipif-ci
    rsync_args = rsync_cmd(  # skipif-ci
        srcs,
        user,
        hostname,
        dest,
        archive=any(Path(s).is_dir() for s in srcs),
        chown_user=chown_user,
        chown_group=chown_group,
        exclude=exclude,
        batch_mode=batch_mode,
        host_key_algorithms=host_key_algorithms,
        strict_host_key_checking=strict_host_key_checking,
        sudo=sudo,
        timeout=timeout,
    )
    run(*rsync_args, print=print, retry=retry, logger=logger)  # skipif-ci
    if chmod is not None:  # skipif-ci
        chmod_args = maybe_sudo_cmd(*chmod_cmd(dest, chmod), sudo=sudo)
        ssh(
            user,
            hostname,
            *chmod_args,
            batch_mode=batch_mode,
            host_key_algorithms=host_key_algorithms,
            strict_host_key_checking=strict_host_key_checking,
            print=print,
            retry=retry,
            logger=logger,
        )


def rsync_cmd(
    src_or_srcs: MaybeIterable[PathLike],
    user: str,
    hostname: str,
    dest: PathLike,
    /,
    *,
    archive: bool = False,
    chown_user: str | None = None,
    chown_group: str | None = None,
    exclude: MaybeSequenceStr | None = None,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    sudo: bool = False,
    timeout: Duration | None = None,
) -> list[str]:
    """Command to use 'rsync' to do remote & local file copying."""
    args: list[str] = ["rsync"]
    if archive:
        args.append("--archive")
    args.append("--checksum")
    match chown_user, chown_group:
        case None, None:
            ...
        case str(), None:
            args.extend(["--chown", chown_user])
        case None, str():
            args.extend(["--chown", f":{chown_group}"])
        case str(), str():
            args.extend(["--chown", f"{chown_user}:{chown_group}"])
        case never:
            assert_never(never)
    args.append("--compress")
    if exclude is not None:
        for exclude_i in always_iterable(exclude):
            args.extend(["--exclude", exclude_i])
    rsh_args: list[str] = ssh_opts_cmd(
        batch_mode=batch_mode,
        host_key_algorithms=host_key_algorithms,
        strict_host_key_checking=strict_host_key_checking,
    )
    args.extend(["--rsh", join(rsh_args)])
    if sudo:
        args.extend(["--rsync-path", join(sudo_cmd("rsync"))])
    if timeout is not None:
        args.extend(["--timeout", str(safe_round(duration_to_seconds(timeout)))])
    srcs = list(always_iterable(src_or_srcs))  # do not Path()
    if len(srcs) == 0:
        raise _RsyncCmdNoSourcesError(user=user, hostname=hostname, dest=dest)
    missing = [s for s in srcs if not Path(s).exists()]
    if len(missing) >= 1:
        raise _RsyncCmdSourcesNotFoundError(
            sources=missing, user=user, hostname=hostname, dest=dest
        )
    return [*args, *map(str, srcs), f"{user}@{hostname}:{dest}"]


@dataclass(kw_only=True, slots=True)
class RsyncCmdError(Exception):
    user: str
    hostname: str
    dest: PathLike


@dataclass(kw_only=True, slots=True)
class _RsyncCmdNoSourcesError(RsyncCmdError):
    @override
    def __str__(self) -> str:
        return f"No sources selected to send to {self.user}@{self.hostname}:{self.dest}"


@dataclass(kw_only=True, slots=True)
class _RsyncCmdSourcesNotFoundError(RsyncCmdError):
    sources: list[PathLike]

    @override
    def __str__(self) -> str:
        desc = ", ".join(map(repr, map(str, self.sources)))
        return f"Sources selected to send to {self.user}@{self.hostname}:{self.dest} but not found: {desc}"


##


def rsync_many(
    user: str,
    hostname: str,
    /,
    *items: tuple[PathLike, PathLike]
    | tuple[Literal["sudo"], PathLike, PathLike]
    | tuple[PathLike, PathLike, PermissionsLike]
    | tuple[Literal["sudo"], PathLike, PathLike, PermissionsLike],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    keep: bool = False,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    print: bool = False,  # noqa: A002
    exclude: MaybeSequenceStr | None = None,
) -> None:
    cmds: list[list[str]] = []  # skipif-ci
    with (  # skipif-ci
        TemporaryDirectory() as temp_src,
        yield_ssh_temp_dir(
            user, hostname, retry=retry, logger=logger, keep=keep
        ) as temp_dest,
    ):
        for item in items:
            match item:
                case Path() | str() as src, Path() | str() as dest:
                    cmds.extend(_rsync_many_prepare(src, dest, temp_src, temp_dest))
                case (  # pragma: no cover
                    "sudo",
                    Path() | str() as src,
                    Path() | str() as dest,
                ):
                    cmds.extend(
                        _rsync_many_prepare(src, dest, temp_src, temp_dest, sudo=True)
                    )
                case (
                    Path() | str() as src,
                    Path() | str() as dest,
                    Permissions() | int() | str() as perms,
                ):
                    cmds.extend(
                        _rsync_many_prepare(src, dest, temp_src, temp_dest, perms=perms)
                    )
                case (  # pragma: no cover
                    "sudo",
                    Path() | str() as src,
                    Path() | str() as dest,
                    Permissions() | int() | str() as perms,
                ):
                    cmds.extend(
                        _rsync_many_prepare(
                            src, dest, temp_src, temp_dest, sudo=True, perms=perms
                        )
                    )
                case never:
                    assert_never(never)
        rsync(
            f"{temp_src}/",
            user,
            hostname,
            temp_dest,
            batch_mode=batch_mode,
            host_key_algorithms=host_key_algorithms,
            strict_host_key_checking=strict_host_key_checking,
            print=print,
            retry=retry,
            logger=logger,
            exclude=exclude,
        )
        ssh(
            user,
            hostname,
            *BASH_LS,
            input="\n".join(map(join, cmds)),
            print=print,
            retry=retry,
            logger=logger,
        )


def _rsync_many_prepare(
    src: PathLike,
    dest: PathLike,
    temp_src: PathLike,
    temp_dest: PathLike,
    /,
    *,
    sudo: bool = False,
    perms: PermissionsLike | None = None,
) -> list[list[str]]:
    dest, temp_src, temp_dest = map(Path, [dest, temp_src, temp_dest])
    n = len(list(temp_src.iterdir()))
    name = str(n)
    match src:
        case Path():
            cp(src, temp_src / name)
        case str():
            try:
                exists = Path(src).exists()
            except OSError:
                tee(temp_src / name, src)
            else:
                if exists:
                    cp(src, temp_src / name)
                else:
                    tee(temp_src / name, src)
        case never:
            assert_never(never)
    cmds: list[list[str]] = [
        maybe_sudo_cmd(*rm_cmd(dest), sudo=sudo),
        maybe_sudo_cmd(*mkdir_cmd(dest, parent=True), sudo=sudo),
        maybe_sudo_cmd(*cp_cmd(temp_dest / name, dest), sudo=sudo),
    ]
    if perms is not None:
        cmds.append(maybe_sudo_cmd(*chmod_cmd(dest, perms), sudo=sudo))
    return cmds


##


@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,  # noqa: A002
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    retry_skip: Callable[[int, str, str], bool] | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    """Run a command in a subprocess."""
    args: list[str] = []
    if user is not None:  # pragma: no cover
        args.extend(["su", "-", str(user)])
    args.extend([cmd, *cmds_or_args])
    buffer = StringIO()
    stdout = StringIO()
    stderr = StringIO()
    stdout_outputs: list[IO[str]] = [buffer, stdout]
    if print or print_stdout:
        stdout_outputs.append(sys.stdout)
    stderr_outputs: list[IO[str]] = [buffer, stderr]
    if print or print_stderr:
        stderr_outputs.append(sys.stderr)
    with Popen(
        args,
        bufsize=1,
        executable=executable,
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        shell=shell,
        cwd=cwd,
        env=env,
        text=True,
        user=user,
    ) as proc:
        if proc.stdin is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdin=}"])
        if proc.stdout is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdout=}"])
        if proc.stderr is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stderr=}"])
        with (
            _run_yield_write(proc.stdout, *stdout_outputs),
            _run_yield_write(proc.stderr, *stderr_outputs),
        ):
            if input is not None:
                _ = proc.stdin.write(input)
                proc.stdin.flush()
                proc.stdin.close()
            return_code = proc.wait()
        match return_code, suppress, return_ or return_stdout, return_ or return_stderr:
            case (0, _, True, True) | (_, True, True, True):
                _ = buffer.seek(0)
                return buffer.read().rstrip("\n")
            case (0, _, True, False) | (_, True, True, False):
                _ = stdout.seek(0)
                return stdout.read().rstrip("\n")
            case (0, _, False, True) | (_, True, False, True):
                _ = stderr.seek(0)
                return stderr.read().rstrip("\n")
            case (0, _, False, False) | (_, True, False, False):
                return None
            case _, False, _, _:
                _ = stdout.seek(0)
                stdout_text = stdout.read()
                _ = stderr.seek(0)
                stderr_text = stderr.read()
                if (retry is None) or (
                    (retry is not None)
                    and (retry_skip is not None)
                    and retry_skip(return_code, stdout_text, stderr_text)
                ):
                    attempts = duration = None
                else:
                    attempts, duration = retry
                error = RunError(
                    cmd=cmd,
                    cmds_or_args=list(cmds_or_args),
                    user=user,
                    hostname=HOSTNAME,
                    executable=executable,
                    shell=shell,
                    cwd=cwd,
                    env=env,
                    return_code=return_code,
                    input=input,
                    stdout=stdout_text,
                    stderr=stderr_text,
                )
                if logger is not None:
                    msg = str(error)
                    if (attempts is not None) and (attempts >= 1):
                        if duration is None:
                            msg = f"{msg}\n\nRetrying {attempts} more time(s)..."
                        else:
                            msg = f"{msg}\n\nRetrying {attempts} more time(s) after {duration}..."
                    to_logger(logger).error(msg)
                if (attempts is None) or (attempts <= 0):
                    raise error
                if duration is not None:
                    sync_sleep(duration)
                return run(
                    cmd,
                    *cmds_or_args,
                    user=user,
                    executable=executable,
                    shell=shell,
                    cwd=cwd,
                    env=env,
                    input=input,
                    print=print,
                    print_stdout=print_stdout,
                    print_stderr=print_stderr,
                    return_=return_,
                    return_stdout=return_stdout,
                    return_stderr=return_stderr,
                    retry=(attempts - 1, duration),
                    logger=logger,
                )
            case never:
                assert_never(never)


@contextmanager
def _run_yield_write(input_: IO[str], /, *outputs: IO[str]) -> Iterator[None]:
    thread = Thread(target=_run_daemon_target, args=(input_, *outputs), daemon=True)
    thread.start()
    try:
        yield
    finally:
        thread.join()


def _run_daemon_target(input_: IO[str], /, *outputs: IO[str]) -> None:
    with input_:
        for text in iter(input_.readline, ""):
            _run_write_to_streams(text, *outputs)


def _run_write_to_streams(text: str, /, *outputs: IO[str]) -> None:
    for output in outputs:
        _ = output.write(text)


@dataclass(kw_only=True, slots=True)
class RunError(Exception):
    cmd: str
    cmds_or_args: list[str] = field(default_factory=list)
    user: str | int | None = None
    hostname: str = HOSTNAME
    executable: str | None = None
    shell: bool = False
    cwd: PathLike | None = None
    env: StrStrMapping | None = None
    return_code: int = 0
    input: str | None = None
    stdout: str
    stderr: str

    @override
    def __str__(self) -> str:
        table = repr_table(
            ("cmd", self.cmd),
            ("cmds_or_args", self.cmds_or_args),
            ("user", self.user),
            ("hostname", self.hostname),
            ("executable", self.executable),
            ("shell", self.shell),
            ("cwd", self.cwd),
            ("env", self.env),
            ("return_code", self.return_code),
        )
        stdin = normalize_multi_line_str(f"""
-- stdin ----------------------------------------------------------------------
{"" if self.input is None else self.input}-------------------------------------------------------------------------------""")
        stdout = normalize_multi_line_str(f"""
-- stdout ---------------------------------------------------------------------
{self.stdout}-------------------------------------------------------------------------------""")
        stderr = normalize_multi_line_str(f"""
-- stderr ---------------------------------------------------------------------
{self.stderr}-------------------------------------------------------------------------------""")
        return f"{table}\n{stdin}\n{stdout}\n{stderr}"


##


def set_hostname_cmd(hostname: str, /) -> list[str]:
    """Command to set the system hostname."""
    return ["hostnamectl", "set-hostname", hostname]


##


@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,  # noqa: A002
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    """Execute a command on a remote machine."""
    run_cmd_and_args = ssh_cmd(  # skipif-ci
        user,
        hostname,
        *cmd_and_args,
        batch_mode=batch_mode,
        host_key_algorithms=host_key_algorithms,
        strict_host_key_checking=strict_host_key_checking,
        port=port,
        env=env,
    )
    try:  # skipif-ci
        return run(
            *run_cmd_and_args,
            input=input,
            print=print,
            print_stdout=print_stdout,
            print_stderr=print_stderr,
            suppress=suppress,
            return_=return_,
            return_stdout=return_stdout,
            return_stderr=return_stderr,
            retry=retry,
            retry_skip=_ssh_retry_skip,
            logger=logger,
        )
    except RunError as error:  # pragma: no cover
        if not _ssh_is_strict_checking_error(error.stderr):
            raise
        ssh_keyscan(hostname, port=port)
        return ssh(
            user,
            hostname,
            *cmd_and_args,
            batch_mode=batch_mode,
            host_key_algorithms=host_key_algorithms,
            strict_host_key_checking=strict_host_key_checking,
            port=port,
            input=input,
            print=print,
            print_stdout=print_stdout,
            print_stderr=print_stderr,
            suppress=suppress,
            return_=return_,
            return_stdout=return_stdout,
            return_stderr=return_stderr,
            retry=retry,
            logger=logger,
        )


def _ssh_retry_skip(return_code: int, stdout: str, stderr: str, /) -> bool:
    _ = (return_code, stdout)
    return _ssh_is_strict_checking_error(stderr)


def _ssh_is_strict_checking_error(text: str, /) -> bool:
    match = search(
        "(Host key for .* has changed|No ED25519 host key is known for .*) and you have requested strict checking",
        text,
        flags=MULTILINE,
    )
    return match is not None


def ssh_cmd(
    user: str,
    hostname: str,
    /,
    *cmd_and_args: str,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
    env: StrStrMapping | None = None,
) -> list[str]:
    """Command to use 'ssh' to execute a command on a remote machine."""
    args: list[str] = ssh_opts_cmd(
        batch_mode=batch_mode,
        host_key_algorithms=host_key_algorithms,
        strict_host_key_checking=strict_host_key_checking,
        port=port,
    )
    args.append(f"{user}@{hostname}")
    if env is not None:
        args.extend(env_cmds(env))
    return [*args, *cmd_and_args]


def ssh_opts_cmd(
    *,
    batch_mode: bool = True,
    host_key_algorithms: MaybeSequenceStr = HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    port: int | None = None,
) -> list[str]:
    """Command to use prepare 'ssh' to execute a command on a remote machine."""
    args: list[str] = ["ssh"]
    if batch_mode:
        args.extend(["-o", "BatchMode=yes"])
    joined = ",".join(always_iterable(host_key_algorithms))
    args.extend(["-o", f"HostKeyAlgorithms={joined}"])
    if strict_host_key_checking:
        args.extend(["-o", "StrictHostKeyChecking=yes"])
    if port is not None:
        args.extend(["-p", str(port)])
    return [*args, "-T"]


##


def ssh_await(
    user: str,
    hostname: str,
    /,
    *,
    logger: LoggerLike | None = None,
    duration: Duration = SECOND,
) -> None:
    while True:  # skipif-ci
        log_info(logger, "Waiting for %r...", hostname)
        try:
            ssh(user, hostname, "true")
        except RunError:  # pragma: no cover
            sync_sleep(duration)
        else:
            log_info(logger, "%r is up", hostname)
            return


##


def ssh_keyscan(
    hostname: str,
    /,
    *,
    path: PathLike = KNOWN_HOSTS,
    retry: Retry | None = None,
    port: int | None = None,
) -> None:
    """Add a known host."""
    ssh_keygen_remove(hostname, path=path, retry=retry)  # skipif-ci
    result = normalize_str(  # skipif-ci
        run(*ssh_keyscan_cmd(hostname, port=port), return_=True, retry=retry)
    )
    tee(path, result, append=True)  # skipif-ci


def ssh_keyscan_cmd(hostname: str, /, *, port: int | None = None) -> list[str]:
    """Command to use 'ssh-keyscan' to add a known host."""
    args: list[str] = ["ssh-keyscan"]
    if port is not None:
        args.extend(["-p", str(port)])
    return [*args, "-q", "-t", "ed25519", hostname]


##


def ssh_keygen_remove(
    hostname: str, /, *, path: PathLike = KNOWN_HOSTS, retry: Retry | None = None
) -> None:
    """Remove a known host."""
    path = Path(path)
    if path.exists():
        run(*ssh_keygen_remove_cmd(hostname, path=path), retry=retry)


def ssh_keygen_remove_cmd(
    hostname: str, /, *, path: PathLike = KNOWN_HOSTS
) -> list[str]:
    """Command to use 'ssh-keygen' to remove a known host."""
    return ["ssh-keygen", "-f", str(path), "-R", hostname]


##


def sudo_cmd(cmd: str, /, *args: str) -> list[str]:
    """Command to use 'sudo' to execute a command as another user."""
    return ["sudo", cmd, *args]


def maybe_sudo_cmd(cmd: str, /, *args: str, sudo: bool = False) -> list[str]:
    """Command to use 'sudo' to execute a command as another user, if required."""
    parts: list[str] = [cmd, *args]
    return sudo_cmd(*parts) if sudo else parts


##


def sudo_nopasswd_cmd(user: str, /) -> str:
    """Command to allow a user to use password-free `sudo`."""
    return f"{user} ALL=(ALL) NOPASSWD: ALL"


##


def symlink(target: PathLike, link: PathLike, /, *, sudo: bool = False) -> None:
    """Make a symbolic link."""
    rm(link, sudo=sudo)
    mkdir(link, sudo=sudo, parent=True)
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*symlink_cmd(target, link)))
    else:
        target, link = map(Path, [target, link])
        link.symlink_to(target)


def symlink_cmd(target: PathLike, link: PathLike, /) -> list[str]:
    """Command to use 'symlink' to make a symbolic link."""
    return ["ln", "-s", str(target), str(link)]


##


def tee(
    path: PathLike, text: str, /, *, sudo: bool = False, append: bool = False
) -> None:
    """Duplicate standard input."""
    mkdir(path, sudo=sudo, parent=True)
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*tee_cmd(path, append=append)), input=text)
    else:
        path = Path(path)
        with path.open(mode="a" if append else "w") as fh:
            _ = fh.write(text)


def tee_cmd(path: PathLike, /, *, append: bool = False) -> list[str]:
    """Command to use 'tee' to duplicate standard input."""
    args: list[str] = ["tee"]
    if append:
        args.append("-a")
    return [*args, str(path)]


##


def touch(path: PathLike, /, *, sudo: bool = False) -> None:
    """Change file access and modification times."""
    run(*maybe_sudo_cmd(*touch_cmd(path), sudo=sudo))


def touch_cmd(path: PathLike, /) -> list[str]:
    """Command to use 'touch' to change file access and modification times."""
    return ["touch", str(path)]


##


def update_ca_certificates(*, sudo: bool = False) -> None:
    """Update the system CA certificates."""
    run(*maybe_sudo_cmd(UPDATE_CA_CERTIFICATES, sudo=sudo))  # pragma: no cover


##


def useradd(
    login: str,
    /,
    *,
    create_home: bool = True,
    groups: MaybeIterable[str] | None = None,
    shell: PathLike | None = None,
    sudo: bool = False,
    password: str | None = None,
) -> None:
    """Create a new user."""
    args = maybe_sudo_cmd(  # pragma: no cover
        *useradd_cmd(login, create_home=create_home, groups=groups, shell=shell)
    )
    run(*args)  # pragma: no cover
    if password is not None:  # pragma: no cover
        chpasswd(login, password, sudo=sudo)


def useradd_cmd(
    login: str,
    /,
    *,
    create_home: bool = True,
    groups: MaybeIterable[str] | None = None,
    shell: PathLike | None = None,
) -> list[str]:
    """Command to use 'useradd' to create a new user."""
    args: list[str] = ["useradd"]
    if create_home:
        args.append("--create-home")
    if groups is not None:
        args.extend(["--groups", *always_iterable(groups)])
    if shell is not None:
        args.extend(["--shell", str(shell)])
    return [*args, login]


##


def uv_index_cmd(*, index: MaybeSequenceStr | None = None) -> list[str]:
    """Generate the `--index` command if necessary."""
    return [] if index is None else ["--index", ",".join(always_iterable(index))]


##


type _UvPipListFormat = Literal["columns", "freeze", "json"]


@dataclass(order=True, unsafe_hash=True, kw_only=True, slots=True)
class _UvPipListOutput:
    name: str
    version: Version2 | Version3
    editable_project_location: Path | None = None
    latest_version: Version2 | Version3 | None = None
    latest_filetype: str | None = None


def uv_pip_list(
    *,
    editable: bool = False,
    exclude_editable: bool = False,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
) -> list[_UvPipListOutput]:
    """List packages installed in an environment."""
    cmds_base, cmds_outdated = [
        uv_pip_list_cmd(
            editable=editable,
            exclude_editable=exclude_editable,
            format_="json",
            outdated=outdated,
            index=index,
            native_tls=native_tls,
        )
        for outdated in [False, True]
    ]
    text_base, text_outdated = [
        run(*cmds, return_stdout=True) for cmds in [cmds_base, cmds_outdated]
    ]
    dicts_base, dicts_outdated = list(
        map(_uv_pip_list_loads, [text_base, text_outdated])
    )
    return [_uv_pip_list_assemble_output(d, dicts_outdated) for d in dicts_base]


def _uv_pip_list_loads(text: str, /) -> list[StrMapping]:
    try:
        return json.loads(text)
    except JSONDecodeError:
        raise _UvPipListJsonError(text=text) from None


def _uv_pip_list_assemble_output(
    dict_: StrMapping, outdated: Iterable[StrMapping], /
) -> _UvPipListOutput:
    name = dict_["name"]
    try:
        version = parse_version_2_or_3(dict_["version"])
    except ParseVersion2Or3Error:
        raise _UvPipListBaseVersionError(data=dict_) from None
    try:
        location = Path(dict_["editable_project_location"])
    except KeyError:
        location = None
    try:
        outdated_i = one(d for d in outdated if d["name"] == name)
    except OneEmptyError:
        latest_version = latest_filetype = None
    else:
        try:
            latest_version = parse_version_2_or_3(outdated_i["latest_version"])
        except ParseVersion2Or3Error:
            raise _UvPipListOutdatedVersionError(data=outdated_i) from None
        latest_filetype = outdated_i["latest_filetype"]
    return _UvPipListOutput(
        name=dict_["name"],
        version=version,
        editable_project_location=location,
        latest_version=latest_version,
        latest_filetype=latest_filetype,
    )


@dataclass(kw_only=True, slots=True)
class UvPipListError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _UvPipListJsonError(UvPipListError):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse JSON; got {self.text!r}"


@dataclass(kw_only=True, slots=True)
class _UvPipListBaseVersionError(UvPipListError):
    data: StrMapping

    @override
    def __str__(self) -> str:
        return f"Unable to parse version; got {self.data}"


@dataclass(kw_only=True, slots=True)
class _UvPipListOutdatedVersionError(UvPipListError):
    data: StrMapping

    @override
    def __str__(self) -> str:
        return f"Unable to parse version; got {self.data}"


def uv_pip_list_cmd(
    *,
    editable: bool = False,
    exclude_editable: bool = False,
    format_: _UvPipListFormat = "columns",
    outdated: bool = False,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
) -> list[str]:
    """Command to use 'uv' to list packages installed in an environment."""
    args: list[str] = ["uv", "pip", "list"]
    if editable:
        args.append("--editable")
    if exclude_editable:
        args.append("--exclude-editable")
    args.extend(["--format", format_])
    if outdated:
        args.append("--outdated")
    return [
        *args,
        "--strict",
        *uv_index_cmd(index=index),
        MANAGED_PYTHON,
        *uv_native_tls_cmd(native_tls=native_tls),
    ]


##


def uv_native_tls_cmd(*, native_tls: bool = False) -> list[str]:
    """Generate the `--native-tls` command if necessary."""
    return ["--native-tls"] if native_tls else []


##


@overload
def uv_run(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    env: StrStrMapping | None = None,
    cwd: PathLike | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_run(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    env: StrStrMapping | None = None,
    cwd: PathLike | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_run(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    env: StrStrMapping | None = None,
    cwd: PathLike | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_run(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    env: StrStrMapping | None = None,
    cwd: PathLike | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def uv_run(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    env: StrStrMapping | None = None,
    cwd: PathLike | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def uv_run(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    """Run a command or script."""
    return run(  # pragma: no cover
        *uv_run_cmd(
            module,
            *args,
            extra=extra,
            all_extras=all_extras,
            group=group,
            all_groups=all_groups,
            only_dev=only_dev,
            with_=with_,
            index=index,
            native_tls=native_tls,
        ),
        cwd=cwd,
        env=env,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        suppress=suppress,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        retry=retry,
        logger=logger,
    )


def uv_run_cmd(
    module: str,
    /,
    *args: str,
    extra: MaybeSequenceStr | None = None,
    all_extras: bool = False,
    group: MaybeSequenceStr | None = None,
    all_groups: bool = False,
    only_dev: bool = False,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
) -> list[str]:
    """Command to use 'uv' to run a command or script."""
    parts: list[str] = ["uv", "run"]
    if extra is not None:
        for extra_i in always_iterable(extra):
            parts.extend(["--extra", extra_i])
    if all_extras:
        parts.append("--all-extras")
    if not only_dev:
        parts.append("--no-dev")
    if group is not None:
        for group_i in always_iterable(group):
            parts.extend(["--group", group_i])
    if all_groups:
        parts.append("--all-groups")
    if only_dev:
        parts.append("--only-dev")
    return [
        *parts,
        "--exact",
        *uv_with_cmd(with_=with_),
        ISOLATED,
        *uv_index_cmd(index=index),
        *RESOLUTION_HIGHEST,
        *PRERELEASE_DISALLOW,
        "--reinstall",
        *uv_native_tls_cmd(native_tls=native_tls),
        MANAGED_PYTHON,
        "python",
        "-m",
        module,
        *args,
    ]


##


@overload
def uv_tool_install(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_tool_install(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_tool_install(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_tool_install(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def uv_tool_install(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def uv_tool_install(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    """Install commands provided by a Python package."""
    return run(  # pragma: no cover
        *uv_tool_install_cmd(package, with_=with_, index=index, native_tls=native_tls),
        cwd=cwd,
        env=env,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        suppress=suppress,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        retry=retry,
        logger=logger,
    )


def uv_tool_install_cmd(
    package: str,
    /,
    *,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
) -> list[str]:
    """Command to use 'uv' to install commands provided by a Python package."""
    return [
        "uv",
        "tool",
        "install",
        *uv_with_cmd(with_=with_),
        *uv_index_cmd(index=index),
        *RESOLUTION_HIGHEST,
        *PRERELEASE_DISALLOW,
        "--reinstall",
        MANAGED_PYTHON,
        *uv_native_tls_cmd(native_tls=native_tls),
        package,
    ]


##


@overload
def uv_tool_run(
    command: str,
    /,
    *args: str,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_tool_run(
    command: str,
    /,
    *args: str,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_tool_run(
    command: str,
    /,
    *args: str,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def uv_tool_run(
    command: str,
    /,
    *,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def uv_tool_run(
    command: str,
    /,
    *args: str,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def uv_tool_run(
    command: str,
    /,
    *args: str,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    """Run a command provided by a Python package."""
    return run(  # pragma: no cover
        *uv_tool_run_cmd(
            command,
            *args,
            from_=from_,
            latest=latest,
            with_=with_,
            index=index,
            native_tls=native_tls,
        ),
        cwd=cwd,
        env=env,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        suppress=suppress,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        retry=retry,
        logger=logger,
    )


def uv_tool_run_cmd(
    command: str,
    /,
    *args: str,
    from_: str | None = None,
    latest: bool = True,
    with_: MaybeSequenceStr | None = None,
    index: MaybeSequenceStr | None = None,
    native_tls: bool = False,
) -> list[str]:
    """Command to use 'uv' to run a command provided by a Python package."""
    parts: list[str] = ["uv", "tool", "run"]
    if from_ is not None:
        from_use = f"{from_}@latest" if latest else from_
        parts.extend(["--from", from_use])
    return [
        *parts,
        *uv_with_cmd(with_=with_),
        ISOLATED,
        *uv_index_cmd(index=index),
        *RESOLUTION_HIGHEST,
        *PRERELEASE_DISALLOW,
        MANAGED_PYTHON,
        *uv_native_tls_cmd(native_tls=native_tls),
        command,
        *args,
    ]


##


def uv_with_cmd(*, with_: MaybeSequenceStr | None = None) -> list[str]:
    """Generate the `--with` commands if necessary."""
    return (
        []
        if with_ is None
        else list(chain.from_iterable(["--with", w] for w in always_iterable(with_)))
    )


##


@contextmanager
def yield_git_repo(url: str, /, *, branch: str | None = None) -> Iterator[Path]:
    """Yield a temporary git repository."""
    with TemporaryDirectory() as temp_dir:
        git_clone(url, temp_dir, branch=branch)
        yield temp_dir


##


@enhanced_context_manager
def yield_ssh_temp_dir(
    user: str,
    hostname: str,
    /,
    *,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    keep: bool = False,
) -> Iterator[Path]:
    """Yield a temporary directory on a remote machine."""
    path = Path(  # skipif-ci
        ssh(user, hostname, *MKTEMP_DIR_CMD, return_=True, retry=retry, logger=logger)
    )
    try:  # skipif-ci
        yield path
    finally:  # skipif-ci
        if keep:
            log_info(logger, "Keeping temporary directory %s...", repr_str(path))
        else:
            ssh(user, hostname, *rm_cmd(path), retry=retry, logger=logger)


__all__ = [
    "APT_UPDATE",
    "BASH_LC",
    "BASH_LS",
    "CHPASSWD",
    "GIT_BRANCH_SHOW_CURRENT",
    "HOST_KEY_ALGORITHMS",
    "ISOLATED",
    "MANAGED_PYTHON",
    "MKTEMP_DIR_CMD",
    "PRERELEASE_DISALLOW",
    "RESOLUTION_HIGHEST",
    "RESTART_SSHD",
    "UPDATE_CA_CERTIFICATES",
    "ChownCmdError",
    "CpError",
    "MvFileError",
    "RsyncCmdError",
    "UvPipListError",
    "append_text",
    "apt_install",
    "apt_install_cmd",
    "apt_remove",
    "apt_remove_cmd",
    "apt_update",
    "cat",
    "cd_cmd",
    "chattr",
    "chattr_cmd",
    "chmod",
    "chmod_cmd",
    "chown",
    "chown_cmd",
    "chpasswd",
    "copy_text",
    "cp",
    "cp_cmd",
    "curl",
    "curl_cmd",
    "echo_cmd",
    "env_cmds",
    "expand_path",
    "git_branch_current",
    "git_checkout",
    "git_checkout_cmd",
    "git_clone",
    "git_clone_cmd",
    "install",
    "install_cmd",
    "maybe_parent",
    "maybe_sudo_cmd",
    "mkdir",
    "mkdir_cmd",
    "mv",
    "mv_cmd",
    "replace_text",
    "ripgrep",
    "ripgrep_cmd",
    "rm",
    "rm_cmd",
    "rsync",
    "rsync_cmd",
    "rsync_many",
    "run",
    "set_hostname_cmd",
    "ssh",
    "ssh_await",
    "ssh_cmd",
    "ssh_keygen_remove",
    "ssh_keygen_remove_cmd",
    "ssh_keyscan",
    "ssh_keyscan_cmd",
    "ssh_opts_cmd",
    "sudo_cmd",
    "sudo_nopasswd_cmd",
    "symlink",
    "symlink_cmd",
    "tee_cmd",
    "touch",
    "touch_cmd",
    "update_ca_certificates",
    "useradd",
    "useradd_cmd",
    "uv_native_tls_cmd",
    "uv_pip_list",
    "uv_pip_list_cmd",
    "uv_run",
    "uv_run_cmd",
    "uv_tool_install",
    "uv_tool_install_cmd",
    "uv_tool_run",
    "uv_tool_run_cmd",
    "uv_with_cmd",
    "yield_git_repo",
    "yield_ssh_temp_dir",
]
