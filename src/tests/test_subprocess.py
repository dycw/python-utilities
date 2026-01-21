from __future__ import annotations

from logging import INFO, getLogger
from pathlib import Path
from re import MULTILINE, search
from subprocess import CalledProcessError
from typing import TYPE_CHECKING
from uuid import uuid4

from pytest import LogCaptureFixture, approx, mark, param, raises
from pytest_lazy_fixtures import lf

from utilities.constants import (
    EFFECTIVE_GROUP_NAME,
    EFFECTIVE_USER_NAME,
    HOME,
    MINUTE,
    PWD,
    SECOND,
)
from utilities.core import (
    TemporaryDirectory,
    TemporaryFile,
    normalize_multi_line_str,
    unique_str,
)
from utilities.iterables import one
from utilities.pathlib import get_file_group, get_file_owner
from utilities.permissions import Permissions
from utilities.pytest import skipif_ci, skipif_mac, throttle_test
from utilities.shutil import which
from utilities.subprocess import (
    BASH_LC,
    BASH_LS,
    KNOWN_HOSTS,
    ChownCmdError,
    CpError,
    MvFileError,
    RsyncCmdNoSourcesError,
    RsyncCmdSourcesNotFoundError,
    _rsync_many_prepare,
    _ssh_is_strict_checking_error,
    _uv_pip_list_assemble_output,
    _uv_pip_list_loads,
    _UvPipListBaseVersionError,
    _UvPipListJsonError,
    _UvPipListOutdatedVersionError,
    _UvPipListOutput,
    append_text,
    apt_install_cmd,
    apt_remove_cmd,
    cat,
    cat_cmd,
    cd_cmd,
    chattr_cmd,
    chmod,
    chmod_cmd,
    chown,
    chown_cmd,
    copy_text,
    cp,
    cp_cmd,
    curl,
    curl_cmd,
    echo_cmd,
    env_cmds,
    expand_path,
    git_branch_current,
    git_checkout,
    git_checkout_cmd,
    git_clone,
    git_clone_cmd,
    install,
    install_cmd,
    maybe_parent,
    maybe_sudo_cmd,
    mkdir,
    mkdir_cmd,
    mv,
    mv_cmd,
    replace_text,
    ripgrep,
    ripgrep_cmd,
    rm,
    rm_cmd,
    rsync,
    rsync_cmd,
    rsync_many,
    run,
    set_hostname_cmd,
    ssh,
    ssh_await,
    ssh_cmd,
    ssh_keygen_remove,
    ssh_keygen_remove_cmd,
    ssh_keyscan,
    ssh_keyscan_cmd,
    ssh_opts_cmd,
    sudo_cmd,
    sudo_nopasswd_cmd,
    symlink,
    symlink_cmd,
    tee,
    tee_cmd,
    touch_cmd,
    useradd_cmd,
    uv_index_cmd,
    uv_native_tls_cmd,
    uv_pip_list,
    uv_pip_list_cmd,
    uv_run_cmd,
    uv_tool_install_cmd,
    uv_tool_run_cmd,
    uv_with_cmd,
    yield_git_repo,
    yield_ssh_temp_dir,
)
from utilities.typing import is_sequence_of
from utilities.version import Version3

if TYPE_CHECKING:
    from pytest import CaptureFixture

    from utilities.types import PathLike


class TestAppendText:
    def test_non_existing(self, *, temp_path_not_exist: Path) -> None:
        append_text(temp_path_not_exist, "text")
        result = temp_path_not_exist.read_text()
        assert result == "text"

    def test_existing_empty(self, *, temp_file: Path) -> None:
        append_text(temp_file, "text")
        result = temp_file.read_text()
        assert result == "text"

    def test_existing_non_empty(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        append_text(temp_file, "post")
        result = temp_file.read_text()
        expected = "init\npost"
        assert result == expected

    def test_skip_if_present_with_effect(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        append_text(temp_file, "text", skip_if_present=True)
        result = temp_file.read_text()
        assert result == "text"

    def test_skip_if_present_with_special_character(self, *, temp_file: Path) -> None:
        append_text(temp_file, "*", skip_if_present=True)
        result = temp_file.read_text()
        assert result == "*"

    def test_skip_if_present_without_effect(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        append_text(temp_file, "post", skip_if_present=True)
        result = temp_file.read_text()
        expected = "init\npost"
        assert result == expected

    def test_blank_lines(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        append_text(temp_file, "post", blank_lines=2)
        result = temp_file.read_text()
        expected = "init\n\npost"
        assert result == expected


class TestAptInstallCmd:
    def test_single(self) -> None:
        result = apt_install_cmd("package")
        expected = ["apt", "install", "-y", "package"]
        assert result == expected

    def test_multiple(self) -> None:
        result = apt_install_cmd("package1", "package2")
        expected = ["apt", "install", "-y", "package1", "package2"]
        assert result == expected


class TestAptRemoveCmd:
    def test_single(self) -> None:
        result = apt_remove_cmd("package")
        expected = ["apt", "remove", "-y", "package"]
        assert result == expected

    def test_multiple(self) -> None:
        result = apt_remove_cmd("package1", "package2")
        expected = ["apt", "remove", "-y", "package1", "package2"]
        assert result == expected


class TestCat:
    def test_single(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("text")
        result = cat(temp_file)
        assert result == "text"

    def test_multiple(self, *, temp_files: tuple[Path, Path]) -> None:
        path1, path2 = temp_files
        _ = path1.write_text("text1")
        _ = path2.write_text("text2")
        result = cat(path1, path2)
        expected = "text1\ntext2"
        assert result == expected


class TestCatCmd:
    def test_single(self) -> None:
        result = cat_cmd("path")
        expected = ["cat", "path"]
        assert result == expected

    def test_multiple(self) -> None:
        result = cat_cmd("path1", "path2")
        expected = ["cat", "path1", "path2"]
        assert result == expected


class TestCDCmd:
    def test_main(self) -> None:
        result = cd_cmd("path")
        expected = ["cd", "path"]
        assert result == expected


class TestChAttrCmd:
    def test_main(self) -> None:
        result = chattr_cmd("path")
        expected = ["chattr", "path"]
        assert result == expected

    def test_immutable(self) -> None:
        result = chattr_cmd("path", immutable=True)
        expected = ["chattr", "+i", "path"]
        assert result == expected

    def test_mutable(self) -> None:
        result = chattr_cmd("path", immutable=False)
        expected = ["chattr", "-i", "path"]
        assert result == expected


class TestChMod:
    def test_main(self, *, temp_file: Path) -> None:
        perms = Permissions.from_text("u=rw,g=r,o=r")
        _ = chmod(temp_file, perms)
        result = Permissions.from_path(temp_file)
        assert result == perms


class TestChModCmd:
    def test_main(self) -> None:
        result = chmod_cmd("path", "u=rw,g=r,o=r")
        expected = ["chmod", "u=rw,g=r,o=r", "path"]
        assert result == expected


class TestChOwn:
    def test_none(self, *, temp_file: Path) -> None:
        chown(temp_file)

    def test_recursive(self, *, temp_file: Path) -> None:
        chown(temp_file, recursive=True)

    def test_user(self, *, temp_file: Path) -> None:
        chown(temp_file, user=EFFECTIVE_USER_NAME)
        result = get_file_owner(temp_file)
        assert result == EFFECTIVE_USER_NAME

    def test_group(self, *, temp_file: Path) -> None:
        chown(temp_file, group=EFFECTIVE_GROUP_NAME)
        result = get_file_group(temp_file)
        assert result == EFFECTIVE_GROUP_NAME

    def test_user_and_group(self, *, temp_file: Path) -> None:
        chown(temp_file, user=EFFECTIVE_USER_NAME, group=EFFECTIVE_GROUP_NAME)
        owner = get_file_owner(temp_file)
        assert owner == EFFECTIVE_USER_NAME
        group = get_file_group(temp_file)
        assert group == EFFECTIVE_GROUP_NAME


class TestChOwnCmd:
    def test_user(self) -> None:
        result = chown_cmd("path", user="user")
        expected = ["chown", "user", "path"]
        assert result == expected

    def test_recursive(self) -> None:
        result = chown_cmd("path", recursive=True, user="user")
        expected = ["chown", "-R", "user", "path"]
        assert result == expected

    def test_group(self) -> None:
        result = chown_cmd("path", group="group")
        expected = ["chown", ":group", "path"]
        assert result == expected

    def test_user_and_group(self) -> None:
        result = chown_cmd("path", user="user", group="group")
        expected = ["chown", "user:group", "path"]
        assert result == expected

    def test_error(self) -> None:
        with raises(
            ChownCmdError,
            match=r"At least one of 'user' and/or 'group' must be given; got None",
        ):
            _ = chown_cmd("path")


class TestCopyText:
    def test_main(self, *, temp_files: tuple[Path, Path]) -> None:
        src, dest = temp_files
        _ = src.write_text("${KEY}")
        copy_text(src, dest, substitutions={"KEY": "value"})
        result = dest.read_text()
        assert result == "value"


class TestCp:
    def test_file(self, *, temp_file: Path, temp_path_nested_not_exist: Path) -> None:
        dest = temp_path_nested_not_exist / temp_file.name
        cp(temp_file, dest)
        assert temp_file.is_file()
        assert dest.is_file()

    def test_directory(
        self, *, tmp_path: Path, temp_path_nested_not_exist: Path
    ) -> None:
        src = tmp_path / tmp_path.name
        src.mkdir()
        dest = temp_path_nested_not_exist / tmp_path.name
        cp(src, dest)
        assert src.is_dir()
        assert dest.is_dir()

    def test_perms(self, *, temp_file: Path, temp_path_not_exist: Path) -> None:
        dest = temp_path_not_exist / temp_file.name
        perms = Permissions.from_text("u=rwx,g=,o=")
        cp(temp_file, dest, perms=perms)
        result = Permissions.from_path(dest)
        assert result == perms

    def test_owner(self, *, temp_file: Path, temp_path_not_exist: Path) -> None:
        dest = temp_path_not_exist / temp_file.name
        cp(temp_file, dest, owner=EFFECTIVE_USER_NAME)
        result = get_file_owner(dest)
        assert result == EFFECTIVE_USER_NAME

    def test_error(self, *, temp_path_not_exist: Path, tmp_path: Path) -> None:
        dest = tmp_path / temp_path_not_exist.name
        with raises(CpError, match=r"Source '.*' does not exist"):
            cp(temp_path_not_exist, dest)


class TestCpCmd:
    def test_main(self) -> None:
        result = cp_cmd("src", "dest")
        expected = ["cp", "-r", "src", "dest"]
        assert result == expected


class TestCurl:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_main(self) -> None:
        curl("https://example.com")


class TestCurlCmd:
    def test_main(self) -> None:
        result = curl_cmd("url")
        expected = ["curl", "--fail", "--location", "--show-error", "--silent", "url"]
        assert result == expected

    def test_fail(self) -> None:
        result = curl_cmd("url", fail=False)
        expected = ["curl", "--location", "--show-error", "--silent", "url"]
        assert result == expected

    def test_location(self) -> None:
        result = curl_cmd("url", location=False)
        expected = ["curl", "--fail", "--show-error", "--silent", "url"]
        assert result == expected

    def test_output(self, *, tmp_path: Path) -> None:
        result = curl_cmd("url", output=tmp_path)
        expected = [
            "curl",
            "--fail",
            "--location",
            "--create-dirs",
            "--output",
            str(tmp_path),
            "--show-error",
            "--silent",
            "url",
        ]
        assert result == expected

    def test_show_error(self) -> None:
        result = curl_cmd("url", show_error=False)
        expected = ["curl", "--fail", "--location", "--silent", "url"]
        assert result == expected

    def test_silent(self) -> None:
        result = curl_cmd("url", silent=False)
        expected = ["curl", "--fail", "--location", "--show-error", "url"]
        assert result == expected


class TestEchoCmd:
    def test_main(self) -> None:
        result = echo_cmd("'hello world'")
        expected = ["echo", "'hello world'"]
        assert result == expected


class TestEnvCmds:
    def test_main(self) -> None:
        result = env_cmds({"KEY": "value"})
        expected = ["KEY=value"]
        assert result == expected


class TestExpandPath:
    def test_main(self) -> None:
        result = expand_path("~")
        assert result == HOME

    def test_subs(self) -> None:
        result = expand_path("~/${dir}", subs={"dir": "foo"})
        expected = Path("~/foo").expanduser()
        assert result == expected


class TestGitBranchCurrent:
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, git_repo_url: str, tmp_path: Path) -> None:
        git_clone(git_repo_url, tmp_path)
        result = git_branch_current(tmp_path)
        assert result == "master"


class TestGitCheckout:
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, git_repo_url: str, tmp_path: Path) -> None:
        git_clone(git_repo_url, tmp_path)
        git_checkout("branch", tmp_path)
        result = git_branch_current(tmp_path)
        assert result == "branch"


class TestGitCheckoutCmd:
    def test_main(self) -> None:
        result = git_checkout_cmd("branch")
        expected = ["git", "checkout", "branch"]
        assert result == expected


class TestGitClone:
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, git_repo_url: str, tmp_path: Path) -> None:
        git_clone(git_repo_url, tmp_path)
        assert (tmp_path / ".git").is_dir()

    @throttle_test(duration=5 * MINUTE)
    def test_branch(self, *, git_repo_url: str, tmp_path: Path) -> None:
        git_clone(git_repo_url, tmp_path, branch="branch")
        result = git_branch_current(tmp_path)
        assert result == "branch"


class TestGitCloneCmd:
    def test_main(self, *, git_repo_url: str) -> None:
        result = git_clone_cmd(git_repo_url, "path")
        expected = ["git", "clone", "--recurse-submodules", git_repo_url, "path"]
        assert result == expected


class TestInstall:
    def test_file(self, *, temp_path_not_exist: Path) -> None:
        install(temp_path_not_exist)
        assert temp_path_not_exist.is_file()

    def test_directory(self, *, temp_path_not_exist: Path) -> None:
        install(temp_path_not_exist, directory=True)
        assert temp_path_not_exist.is_dir()

    def test_mode(self, *, temp_path_not_exist: Path) -> None:
        perms = Permissions.from_text("u=rwx,g=,o=")
        install(temp_path_not_exist, mode=perms)
        result = Permissions.from_path(temp_path_not_exist)
        assert result == perms

    def test_owner(self, *, temp_path_not_exist: Path) -> None:
        install(temp_path_not_exist, owner=EFFECTIVE_USER_NAME)
        result = get_file_owner(temp_path_not_exist)
        assert result == EFFECTIVE_USER_NAME

    def test_group(self, *, temp_path_not_exist: Path) -> None:
        install(temp_path_not_exist, group=EFFECTIVE_GROUP_NAME)
        result = get_file_group(temp_path_not_exist)
        assert result == EFFECTIVE_GROUP_NAME

    def test_error_file(self, *, temp_path_nested_not_exist: Path) -> None:
        with raises(CalledProcessError):
            install(temp_path_nested_not_exist)


class TestInstallCmd:
    def test_file(self) -> None:
        result = install_cmd("path")
        expected = ["install", "/dev/null", "path"]
        assert result == expected

    def test_directory(self) -> None:
        result = install_cmd("path", directory=True)
        expected = ["install", "-d", "path"]
        assert result == expected

    def test_mode(self) -> None:
        result = install_cmd("path", mode="u=rwx,g=,o=")
        expected = ["install", "-m", "u=rwx,g=,o=", "/dev/null", "path"]
        assert result == expected

    def test_owner(self) -> None:
        result = install_cmd("path", owner="owner")
        expected = ["install", "-o", "owner", "/dev/null", "path"]
        assert result == expected

    def test_group(self) -> None:
        result = install_cmd("path", group="group")
        expected = ["install", "-g", "group", "/dev/null", "path"]
        assert result == expected


class TestMaybeParent:
    def test_main(self) -> None:
        result = maybe_parent("~/path")
        expected = Path("~/path")
        assert result == expected

    def test_parent(self) -> None:
        result = maybe_parent("~/path", parent=True)
        expected = Path("~")
        assert result == expected


class TestMaybeSudoCmd:
    def test_main(self) -> None:
        result = maybe_sudo_cmd("echo", "hi")
        expected = ["echo", "hi"]
        assert result == expected

    def test_sudo(self) -> None:
        result = maybe_sudo_cmd("echo", "hi", sudo=True)
        expected = ["sudo", "echo", "hi"]
        assert result == expected


class TestMkDir:
    def test_main(self, *, temp_path_not_exist: Path) -> None:
        assert not temp_path_not_exist.exists()
        mkdir(temp_path_not_exist)
        assert temp_path_not_exist.is_dir()

    def test_idempotent(self, *, tmp_path: Path) -> None:
        mkdir(tmp_path)


class TestMkDirCmd:
    def test_main(self) -> None:
        result = mkdir_cmd("~/path")
        expected = ["mkdir", "-p", "~/path"]
        assert result == expected

    def test_parent(self) -> None:
        result = mkdir_cmd("~/path", parent=True)
        expected = ["mkdir", "-p", "~"]
        assert result == expected


class TestMv:
    def test_file(self, *, temp_file: Path, temp_path_nested_not_exist: Path) -> None:
        dest = temp_path_nested_not_exist / temp_file.name
        mv(temp_file, dest)
        assert not temp_file.exists()
        assert dest.is_file()

    def test_dir(self, *, tmp_path: Path, temp_path_nested_not_exist: Path) -> None:
        src = tmp_path / tmp_path.name
        src.mkdir()
        dest = temp_path_nested_not_exist / tmp_path.name
        mv(src, dest)
        assert not src.exists()
        assert dest.is_dir()

    def test_perms(self, *, temp_file: Path, temp_path_not_exist: Path) -> None:
        dest = temp_path_not_exist / temp_file.name
        perms = Permissions.from_text("u=rwx,g=,o=")
        mv(temp_file, dest, perms=perms)
        result = Permissions.from_path(dest)
        assert result == perms

    def test_owner(self, *, temp_file: Path, temp_path_not_exist: Path) -> None:
        dest = temp_path_not_exist / temp_file.name
        mv(temp_file, dest, owner=EFFECTIVE_USER_NAME)
        result = get_file_owner(dest)
        assert result == EFFECTIVE_USER_NAME

    def test_error(self, *, temp_path_not_exist: Path, tmp_path: Path) -> None:
        dest = tmp_path / temp_path_not_exist.name
        with raises(MvFileError, match=r"Source '.*' does not exist"):
            mv(temp_path_not_exist, dest)


class TestMvCmd:
    def test_main(self) -> None:
        result = mv_cmd("src", "dest")
        expected = ["mv", "src", "dest"]
        assert result == expected


class TestReplaceText:
    def test_main(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        replace_text(temp_file, ("init", "post"))
        result = temp_file.read_text()
        assert result == "post"


class TestRipGrep:
    @skipif_ci
    def test_main(self, *, tmp_path: Path, temp_files: tuple[Path, Path]) -> None:
        path1, _ = temp_files
        _ = path1.write_text("foo")
        result = ripgrep("--files-with-matches", "foo", path=tmp_path)
        expected = str(path1)
        assert result == expected

    @skipif_ci
    def test_no_files(self, *, tmp_path: Path) -> None:
        result = ripgrep("pattern", path=tmp_path)
        assert result is None

    @skipif_ci
    def test_error(self, *, tmp_path: Path) -> None:
        with raises(CalledProcessError) as exc_info:
            _ = ripgrep("--invalid", path=tmp_path)
        assert exc_info.value.returncode == 2


class TestRipGrepCmd:
    def test_main(self) -> None:
        result = ripgrep_cmd("pattern")
        expected = ["rg", "pattern", str(Path.cwd())]
        assert result == expected


class TestRm:
    def test_single_file(self, *, temp_file: Path) -> None:
        rm(temp_file)
        assert not temp_file.exists()

    def test_multiple_files(self, *, temp_files: tuple[Path, Path]) -> None:
        path1, path2 = temp_files
        rm(path1, path2)
        assert not path1.exists()
        assert not path2.exists()

    def test_single_directory(self, *, tmp_path: Path) -> None:
        rm(tmp_path)
        assert not tmp_path.exists()

    def test_non_existent(self, *, temp_path_not_exist: Path) -> None:
        rm(temp_path_not_exist)


class TestRmCmd:
    def test_single(self) -> None:
        result = rm_cmd("path")
        expected = ["rm", "-rf", "path"]
        assert result == expected

    def test_multiple(self) -> None:
        result = rm_cmd("path1", "path2")
        expected = ["rm", "-rf", "path1", "path2"]
        assert result == expected


class TestRsync:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_file(self, *, temp_file: Path, ssh_user: str, ssh_hostname: str) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest:
            dest = temp_dest / temp_file.name
            rsync(temp_file, ssh_user, ssh_hostname, dest)
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=f"if ! [ -f {dest} ]; then exit 1; fi",
            )

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_dir_without_trailing_slash(
        self, *, tmp_path: Path, temp_file: Path, ssh_user: str, ssh_hostname: str
    ) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as dest:
            rsync(tmp_path, ssh_user, ssh_hostname, dest)
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=normalize_multi_line_str(f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -d {dest}/{tmp_path.name} ]; then exit 1; fi
                    if ! [ -f {dest}/{tmp_path.name}/{temp_file.name} ]; then exit 1; fi
                """),
            )

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_dir_with_trailing_slash(
        self, *, tmp_path: Path, temp_file: Path, ssh_user: str, ssh_hostname: str
    ) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as dest:
            rsync(f"{tmp_path}/", ssh_user, ssh_hostname, dest)
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=normalize_multi_line_str(f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -f {dest}/{temp_file.name} ]; then exit 1; fi
                """),
            )


class TestRsyncCmd:
    def test_main(self, *, temp_file: Path) -> None:
        result = rsync_cmd(temp_file, "user", "hostname", "dest")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_multiple_sources(self, *, temp_files: tuple[Path, Path]) -> None:
        src1, src2 = temp_files
        result = rsync_cmd([src1, src2], "user", "hostname", "dest")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src1),
            str(src2),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_source_with_trailing_slash(self, *, tmp_path: Path) -> None:
        src = f"{tmp_path}/"
        result = rsync_cmd(src, "user", "hostname", "dest")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            src,
            "user@hostname:dest",
        ]
        assert result == expected

    def test_archive(self, *, tmp_path: Path) -> None:
        result = rsync_cmd(tmp_path, "user", "hostname", "dest", archive=True)
        expected: list[str] = [
            "rsync",
            "--archive",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(tmp_path),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_chown_user(self, *, temp_file: Path) -> None:
        result = rsync_cmd(temp_file, "user", "hostname", "dest", chown_user="user2")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--chown",
            "user2",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_chown_group(self, *, temp_file: Path) -> None:
        result = rsync_cmd(temp_file, "user", "hostname", "dest", chown_group="group")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--chown",
            ":group",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_chown_user_and_group(self, *, temp_file: Path) -> None:
        result = rsync_cmd(
            temp_file,
            "user",
            "hostname",
            "dest",
            chown_user="user2",
            chown_group="group",
        )
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--chown",
            "user2:group",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_exclude(self, *, temp_file: Path) -> None:
        result = rsync_cmd(temp_file, "user", "hostname", "dest", exclude="exclude")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--exclude",
            "exclude",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_exclude_multiple(self, *, temp_file: Path) -> None:
        result = rsync_cmd(
            temp_file, "user", "hostname", "dest", exclude=["exclude1", "exclude2"]
        )
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--exclude",
            "exclude1",
            "--exclude",
            "exclude2",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_sudo(self, *, temp_file: Path) -> None:
        result = rsync_cmd(temp_file, "user", "hostname", "dest", sudo=True)
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            "--rsync-path",
            "sudo rsync",
            str(temp_file),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_error_no_sources(self) -> None:
        with raises(
            RsyncCmdNoSourcesError,
            match=r"No sources selected to send to user@hostname:dest",
        ):
            _ = rsync_cmd([], "user", "hostname", "dest")

    def test_error_sources_not_found(self, *, temp_path_not_exist: Path) -> None:
        with raises(
            RsyncCmdSourcesNotFoundError,
            match=r"Sources selected to send to user@hostname:dest but not found: '.*'",
        ):
            _ = rsync_cmd(temp_path_not_exist, "user", "hostname", "dest")


class TestRsyncMany:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_single_file(
        self, *, temp_file: Path, ssh_user: str, ssh_hostname: str
    ) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest:
            dest = temp_dest / temp_file.name
            rsync_many(ssh_user, ssh_hostname, (temp_file, dest))
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=f"if ! [ -f {dest} ]; then exit 1; fi",
            )

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_multiple_files(
        self, *, temp_files: tuple[Path, Path], ssh_user: str, ssh_hostname: str
    ) -> None:
        src1, src2 = temp_files
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest:
            dest1, dest2 = [temp_dest / src.name for src in temp_files]
            rsync_many(ssh_user, ssh_hostname, (src1, dest1), (src2, dest2))
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=normalize_multi_line_str(f"""
                    if ! [ -f {dest1} ]; then exit 1; fi
                    if ! [ -f {dest2} ]; then exit 1; fi
                """),
            )

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_single_directory(
        self, *, tmp_path: Path, temp_file: Path, ssh_user: str, ssh_hostname: str
    ) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as dest:
            rsync_many(ssh_user, ssh_hostname, (tmp_path, dest))
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=(
                    f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -f {dest}/{temp_file.name} ]; then exit 1; fi
                    """
                ),
            )

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_file_and_directory(
        self, *, tmp_path: Path, ssh_user: str, ssh_hostname: str
    ) -> None:
        with (
            TemporaryDirectory(dir=tmp_path) as temp_src,
            TemporaryFile(dir=temp_src) as src_file,
            TemporaryDirectory(dir=temp_src) as src_dir,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as dest,
        ):
            rsync_many(
                ssh_user,
                ssh_hostname,
                (src_file, dest / src_file.name),
                (src_dir, dest / src_dir.name),
            )
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=(
                    f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -f {dest}/{src_file.name} ]; then exit 1; fi
                    if ! [ -d {dest}/{src_dir.name} ]; then exit 1; fi
                    """
                ),
            )


class TestRsyncManyPrepare:
    @mark.parametrize("as_path", [param(False), param(True)])
    def test_single_file(
        self, *, tmp_path: Path, temp_file: Path, as_path: bool
    ) -> None:
        dest = tmp_path / "dest"
        with (
            TemporaryDirectory(dir=tmp_path) as temp_src,
            TemporaryDirectory(dir=tmp_path) as temp_dest,
        ):
            src = temp_file if as_path else str(temp_file)
            result = _rsync_many_prepare(src, dest, temp_src, temp_dest)
            assert one(temp_src.iterdir()) == temp_src / "0"
        expected: list[list[str]] = [
            rm_cmd(dest),
            mkdir_cmd(dest, parent=True),
            cp_cmd(temp_dest / "0", dest),
        ]
        assert result == expected

    def test_multiple_files(
        self, *, tmp_path: Path, temp_files: tuple[Path, Path]
    ) -> None:
        path1, path2 = temp_files
        dest1, dest2 = [tmp_path / "dest" / p.name for p in temp_files]
        with (
            TemporaryDirectory(dir=tmp_path) as temp_src,
            TemporaryDirectory(dir=tmp_path) as temp_dest,
        ):
            result1 = _rsync_many_prepare(path1, dest1, temp_src, temp_dest)
            result2 = _rsync_many_prepare(path2, dest2, temp_src, temp_dest)
            assert set(temp_src.iterdir()) == {temp_src / str(i) for i in [0, 1]}
        expected1: list[list[str]] = [
            rm_cmd(dest1),
            mkdir_cmd(dest1, parent=True),
            cp_cmd(temp_dest / "0", dest1),
        ]
        assert result1 == expected1
        expected2: list[list[str]] = [
            rm_cmd(dest2),
            mkdir_cmd(dest2, parent=True),
            cp_cmd(temp_dest / "1", dest2),
        ]
        assert result2 == expected2

    @mark.parametrize("text", [param("text"), param(100 * "text")])
    def test_text(self, *, tmp_path: Path, text: str) -> None:
        dest = tmp_path / "dest"
        with (
            TemporaryDirectory(dir=tmp_path) as temp_src,
            TemporaryDirectory(dir=tmp_path) as temp_dest,
        ):
            result = _rsync_many_prepare(text, dest, temp_src, temp_dest)
            assert one(temp_src.iterdir()) == temp_src / "0"
            assert (temp_src / "0").read_text() == text
        expected: list[list[str]] = [
            rm_cmd(dest),
            mkdir_cmd(dest, parent=True),
            cp_cmd(temp_dest / "0", dest),
        ]
        assert result == expected


class TestRun:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        result = run("echo", "hi")
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    @skipif_ci
    @skipif_mac
    def test_user(self, *, capsys: CaptureFixture) -> None:
        result = run("whoami", user="root", print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "root\n"
        assert cap.err == ""

    @mark.parametrize("executable", [param("sh"), param("bash")])
    def test_executable(self, *, executable: str, capsys: CaptureFixture) -> None:
        result = run("echo $0", executable=executable, shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == f"{executable}\n"
        assert cap.err == ""

    def test_shell(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_cwd(self, *, capsys: CaptureFixture, tmp_path: Path) -> None:
        result = run("pwd", cwd=tmp_path, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == f"{tmp_path}\n"
        assert cap.err == ""

    def test_env(self, *, capsys: CaptureFixture) -> None:
        result = run("env | grep KEY", env={"KEY": "value"}, shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "KEY=value\n"
        assert cap.err == ""

    def test_input_bash(self, *, capsys: CaptureFixture) -> None:
        input_ = normalize_multi_line_str("""
            key=value
            echo ${key}@stdout
            echo ${key}@stderr 1>&2
        """)
        result = run(*BASH_LC, input_, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "value@stdout\n"
        assert cap.err == "value@stderr\n"

    def test_input_cat(self, *, capsys: CaptureFixture) -> None:
        input_ = normalize_multi_line_str("""
            foo
            bar
            baz
        """)
        result = run("cat", input=input_, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == input_
        assert cap.err == ""

    def test_input_and_return(self, *, capsys: CaptureFixture) -> None:
        input_ = normalize_multi_line_str("""
            foo
            bar
            baz
        """)
        result = run("cat", input=input_, return_=True)
        assert result == input_
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_print(self, *, capsys: CaptureFixture) -> None:
        result = run("echo stdout; echo stderr 1>&2", shell=True, print=True)  # noqa: S604
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_print_stdout(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; echo stderr 1>&2", shell=True, print_stdout=True
        )
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == ""

    def test_print_stderr(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; echo stderr 1>&2", shell=True, print_stderr=True
        )
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == "stderr\n"

    def test_return(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2", shell=True, return_=True
        )
        expected = "stdout\nstderr"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_return_stdout(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; echo stderr 1>&2", shell=True, return_stdout=True
        )
        expected = "stdout"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_return_stderr(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; echo stderr 1>&2", shell=True, return_stderr=True
        )
        expected = "stderr"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_print_and_return(self, *, capsys: CaptureFixture) -> None:
        result = run(  # noqa: S604
            "echo stdout; sleep 0.5; echo stderr 1>&2",
            shell=True,
            print=True,
            return_=True,
        )
        expected = "stdout\nstderr"
        assert result == expected
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_error(self, *, capsys: CaptureFixture) -> None:
        with raises(CalledProcessError) as exc_info:
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True)  # noqa: S604
        assert exc_info.value.returncode == 1
        assert exc_info.value.stdout == "stdout\n"
        assert exc_info.value.stderr == "stderr\n"
        cap = capsys.readouterr()
        assert cap.out == ""
        assert cap.err == ""

    def test_error_and_print(self, *, capsys: CaptureFixture) -> None:
        with raises(CalledProcessError) as exc_info:
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True, print=True)  # noqa: S604
        assert exc_info.value.returncode == 1
        assert exc_info.value.stdout == "stdout\n"
        assert exc_info.value.stderr == "stderr\n"
        cap = capsys.readouterr()
        assert cap.out == "stdout\n"
        assert cap.err == "stderr\n"

    def test_retry_1_attempt_success(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = unique_str()
        result = run(
            *BASH_LS,
            input=self._test_retry_cmd(tmp_path, 1),
            retry=(1, None),
            logger=name,
        )
        assert result is None
        record = one(r for r in caplog.records if r.name == name)
        assert search(
            r"^Retrying 1 more time\(s\)...$", record.message, flags=MULTILINE
        )

    def test_retry_1_attempt_failure(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = unique_str()
        with raises(CalledProcessError):
            _ = run(
                *BASH_LS,
                input=self._test_retry_cmd(tmp_path, 2),
                retry=(1, None),
                logger=name,
            )
        first, second = (r for r in caplog.records if r.name == name)
        assert search(r"^Retrying 1 more time\(s\)...$", first.message, flags=MULTILINE)
        assert not search("Retrying", second.message, flags=MULTILINE)

    def test_retry_2_attempts_success(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = unique_str()
        result = run(
            *BASH_LS,
            input=self._test_retry_cmd(tmp_path, 2),
            retry=(2, None),
            logger=name,
        )
        assert result is None
        first, second = [r for r in caplog.records if r.name == name]
        assert search(r"^Retrying 2 more time\(s\)...$", first.message, flags=MULTILINE)
        assert search(
            r"^Retrying 1 more time\(s\)...$", second.message, flags=MULTILINE
        )

    def test_retry_2_attempts_failure(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = unique_str()
        with raises(CalledProcessError):
            _ = run(
                *BASH_LS,
                input=self._test_retry_cmd(tmp_path, 3),
                retry=(2, None),
                logger=name,
            )
        first, second, third = (r for r in caplog.records if r.name == name)
        assert search(r"^Retrying 2 more time\(s\)...$", first.message, flags=MULTILINE)
        assert search(
            r"^Retrying 1 more time\(s\)...$", second.message, flags=MULTILINE
        )
        assert not search("Retrying", third.message, flags=MULTILINE)

    def test_retry_and_sleep(
        self, *, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        name = unique_str()
        result = run(
            *BASH_LS,
            input=self._test_retry_cmd(tmp_path, 1),
            retry=(1, SECOND),
            logger=name,
        )
        assert result is None
        record = one(r for r in caplog.records if r.name == name)
        assert search(
            r"^Retrying 1 more time\(s\) after PT1S...$",
            record.message,
            flags=MULTILINE,
        )

    def test_retry_skip(self, *, tmp_path: Path, caplog: LogCaptureFixture) -> None:
        def retry_skip(return_code: int, stdout: str, stderr: str, /) -> bool:
            _ = (return_code, stdout, stderr)
            return True

        name = unique_str()
        with raises(CalledProcessError):
            _ = run(
                *BASH_LS,
                input=self._test_retry_cmd(tmp_path, 1),
                retry=(1, SECOND),
                retry_skip=retry_skip,
                logger=name,
            )
        record = one(r for r in caplog.records if r.name == name)
        assert not search("Retrying", record.message, flags=MULTILINE)

    def test_logger(self, *, caplog: LogCaptureFixture) -> None:
        name = unique_str()
        with raises(CalledProcessError):
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True, logger=name)  # noqa: S604
        record = one(r for r in caplog.records if r.name == name)
        expected = normalize_multi_line_str("""
'run' failed with:
 - cmd          = echo stdout; echo stderr 1>&2; exit 1
 - cmds_or_args = ()
 - user         = None
 - executable   = None
 - shell        = True
 - cwd          = None
 - env          = None

-- stdin ----------------------------------------------------------------------
-------------------------------------------------------------------------------
-- stdout ---------------------------------------------------------------------
stdout
-------------------------------------------------------------------------------
-- stderr ---------------------------------------------------------------------
stderr
-------------------------------------------------------------------------------
""")
        assert record.message == expected

    def test_logger_and_input(self, *, caplog: LogCaptureFixture) -> None:
        name = unique_str()
        input_ = normalize_multi_line_str(
            """
            key=value
            echo ${key}@stdout
            echo ${key}@stderr 1>&2
            exit 1
            """,
            trailing=True,
        )
        with raises(CalledProcessError):
            _ = run(*BASH_LS, input=input_, logger=name)
        record = one(r for r in caplog.records if r.name == name)
        expected = normalize_multi_line_str("""
'run' failed with:
 - cmd          = bash
 - cmds_or_args = ('-ls',)
 - user         = None
 - executable   = None
 - shell        = False
 - cwd          = None
 - env          = None

-- stdin ----------------------------------------------------------------------
key=value
echo ${key}@stdout
echo ${key}@stderr 1>&2
exit 1
-------------------------------------------------------------------------------
-- stdout ---------------------------------------------------------------------
value@stdout
-------------------------------------------------------------------------------
-- stderr ---------------------------------------------------------------------
value@stderr
-------------------------------------------------------------------------------
""")
        assert record.message == expected

    def _test_retry_cmd(self, path: PathLike, attempts: int, /) -> str:
        return normalize_multi_line_str(
            f"""
            count=$(ls -1A "{path}" 2>/dev/null | wc -l)
            if [ "${{count}}" -lt {attempts} ]; then
                mktemp "{path}/XXX"
                exit 1
            fi
        """,
            trailing=True,
        )


class TestSetHostnameCmd:
    def test_main(self) -> None:
        result = set_hostname_cmd("hostname")
        expected = ["hostnamectl", "set-hostname", "hostname"]
        assert result == expected


class TestSSH:
    @mark.parametrize(
        ("cmd", "expected"),
        [
            param("whoami", lf("ssh_user")),
            param("hostname", lf("ssh_hostname_internal")),
        ],
    )
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_main(
        self,
        *,
        capsys: CaptureFixture,
        ssh_user: str,
        ssh_hostname: str,
        cmd: str,
        expected: str,
    ) -> None:
        result = ssh(ssh_user, ssh_hostname, cmd, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == f"{expected}\n"
        assert cap.err == ""

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_env(
        self, *, capsys: CaptureFixture, ssh_user: str, ssh_hostname: str
    ) -> None:
        name = f"ENV_{uuid4()}".replace("-", "")
        result = ssh(
            ssh_user, ssh_hostname, "printenv", name, env={name: "1234"}, print=True
        )
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == "1234\n"
        assert cap.err == ""


class TestSSHAwait:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, ssh_user: str, ssh_hostname: str) -> None:
        ssh_await(ssh_user, ssh_hostname)

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_logger(
        self, *, caplog: LogCaptureFixture, ssh_user: str, ssh_hostname: str
    ) -> None:
        name = unique_str()
        logger = getLogger(name=name)
        logger.setLevel(INFO)
        ssh_await(ssh_user, ssh_hostname, logger=name)
        first, second = [r for r in caplog.records if r.name == name]
        assert search(r"^Waiting for '.*'...$", first.message)
        assert search(r"^'.*' is up$", second.message)


class TestSSHCmd:
    def test_main(self) -> None:
        result = ssh_cmd("user", "hostname", "true")
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
            "user@hostname",
            "true",
        ]
        assert result == expected

    def test_env(self) -> None:
        result = ssh_cmd("user", "hostname", "true", env={"KEY": "value"})
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
            "user@hostname",
            "KEY=value",
            "true",
        ]
        assert result == expected


class TestSSHIsStrictCheckingError:
    @mark.parametrize(
        "text",
        [
            param(
                normalize_multi_line_str("""
No ED25519 host key is known for XXX and you have requested strict checking.
Host key verification failed.
""")
            ),
            param(
                normalize_multi_line_str("""
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!
Someone could be eavesdropping on you right now (man-in-the-middle attack)!
It is also possible that a host key has just been changed.
The fingerprint for the ED25519 key sent by the remote host is
SHA256:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.
Please contact your system administrator.
Add correct host key in /Users/XXX/.ssh/known_hosts to get rid of this message.
Offending ED25519 key in /Users/XXX/.ssh/known_hosts:38
Host key for XXX has changed and you have requested strict checking.
Host key verification failed.
""")
            ),
        ],
    )
    def test_main(self, *, text: str) -> None:
        assert _ssh_is_strict_checking_error(text)


class TestSSHKeyScan:
    @skipif_ci
    def test_missing(
        self, *, temp_path_not_exist: Path, github_public_key: str
    ) -> None:
        ssh_keyscan("github.com", path=temp_path_not_exist)
        result = temp_path_not_exist.read_text()
        assert result == github_public_key

    @skipif_ci
    def test_existing(self, *, temp_file: Path, github_public_key: str) -> None:
        ssh_keyscan("github.com", path=temp_file)
        result = temp_file.read_text()
        assert result == github_public_key


class TestSSHKeyScanCmd:
    def test_main(self) -> None:
        result = ssh_keyscan_cmd("hostname")
        expected = ["ssh-keyscan", "-q", "-t", "ed25519", "hostname"]
        assert result == expected

    def test_port(self) -> None:
        result = ssh_keyscan_cmd("hostname", port=22)
        expected = ["ssh-keyscan", "-p", "22", "-q", "-t", "ed25519", "hostname"]
        assert result == expected


class TestSSHKeyGenRemove:
    @mark.parametrize("write", [param(True), param(False)])
    def test_main(self, *, tmp_path: Path, write: bool, github_public_key: str) -> None:
        path = tmp_path / "file.txt"
        if write:
            _ = path.write_text(github_public_key)
        ssh_keygen_remove("github.com", path=path)
        if write:
            result = path.read_text()
            assert result == ""
        else:
            assert not path.exists()


class TestSSHKeyGenRemoveCmd:
    def test_main(self) -> None:
        result = ssh_keygen_remove_cmd("hostname")
        expected = ["ssh-keygen", "-f", str(KNOWN_HOSTS), "-R", "hostname"]
        assert result == expected


class TestSSHOptsCmd:
    def test_main(self) -> None:
        result = ssh_opts_cmd()
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
        ]
        assert result == expected

    def test_batch_mode(self) -> None:
        result = ssh_opts_cmd(batch_mode=False)
        expected = [
            "ssh",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
        ]
        assert result == expected

    def test_host_key_algorithms(self) -> None:
        result = ssh_opts_cmd(host_key_algorithms=["rsa-sha-256"])
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=rsa-sha-256",
            "-o",
            "StrictHostKeyChecking=yes",
            "-T",
        ]
        assert result == expected

    def test_strict_host_key_checking(self) -> None:
        result = ssh_opts_cmd(strict_host_key_checking=False)
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-T",
        ]
        assert result == expected

    def test_port(self) -> None:
        result = ssh_opts_cmd(port=22)
        expected = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "StrictHostKeyChecking=yes",
            "-p",
            "22",
            "-T",
        ]
        assert result == expected


class TestSudoCmd:
    def test_main(self) -> None:
        result = sudo_cmd("echo", "hi")
        expected = ["sudo", "echo", "hi"]
        assert result == expected


class TestSudoNoPasswdCmd:
    def test_main(self) -> None:
        result = sudo_nopasswd_cmd("user")
        expected = "user ALL=(ALL) NOPASSWD: ALL"
        assert result == expected


class TestSymLink:
    def test_main(self, *, temp_file: Path, temp_path_nested_not_exist: Path) -> None:
        symlink(temp_file, temp_path_nested_not_exist)
        assert temp_path_nested_not_exist.is_symlink()
        assert temp_path_nested_not_exist.resolve() == temp_file


class TestSymLinkCmd:
    def test_main(self) -> None:
        result = symlink_cmd("target", "link")
        expected = ["ln", "-s", "target", "link"]
        assert result == expected


class TestTee:
    def test_non_existing(self, *, temp_path_nested_not_exist: Path) -> None:
        tee(temp_path_nested_not_exist, "text")
        result = temp_path_nested_not_exist.read_text()
        assert result == "text"

    def test_existing(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        tee(temp_file, "post")
        result = temp_file.read_text()
        assert result == "post"

    def test_append(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("init")
        tee(temp_file, "post", append=True)
        result = temp_file.read_text()
        expected = "initpost"
        assert result == expected


class TestTeeCmd:
    def test_main(self) -> None:
        result = tee_cmd("path")
        expected = ["tee", "path"]
        assert result == expected

    def test_append(self) -> None:
        result = tee_cmd("path", append=True)
        expected = ["tee", "-a", "path"]
        assert result == expected


class TestTouchCmd:
    def test_main(self) -> None:
        result = touch_cmd("path")
        expected = ["touch", "path"]
        assert result == expected


class TestUserAddCmd:
    def test_main(self) -> None:
        result = useradd_cmd("login")
        expected = ["useradd", "--create-home", "login"]
        assert result == expected

    def test_create_home(self) -> None:
        result = useradd_cmd("login", create_home=False)
        expected = ["useradd", "login"]
        assert result == expected

    def test_groups(self) -> None:
        result = useradd_cmd("login", groups=["group"])
        expected = ["useradd", "--create-home", "--groups", "group", "login"]
        assert result == expected

    def test_shell(self) -> None:
        path = which("bash")
        result = useradd_cmd("login", shell=path)
        expected = ["useradd", "--create-home", "--shell", str(path), "login"]
        assert result == expected


class TestUvIndexCmd:
    def test_none(self) -> None:
        result = uv_index_cmd()
        expected = []
        assert result == expected

    def test_single(self) -> None:
        result = uv_index_cmd(index="index")
        expected = ["--index", "index"]
        assert result == expected

    def test_multiple(self) -> None:
        result = uv_index_cmd(index=["index1", "index2"])
        expected = ["--index", "index1,index2"]
        assert result == expected


class TestUvPipList:
    @skipif_ci
    def test_main(self) -> None:
        result = uv_pip_list()
        assert len(result) == approx(159, rel=0.1)
        assert is_sequence_of(result, _UvPipListOutput)


class TestUvPipListLoadsOutput:
    def test_main(self) -> None:
        text = normalize_multi_line_str("""
            [{"name":"name","version":"0.0.1"}]
        """)
        result = _uv_pip_list_loads(text)
        expected = [{"name": "name", "version": "0.0.1"}]
        assert result == expected

    def test_error(self) -> None:
        text = normalize_multi_line_str("""
            [{"name":"name","version":"0.0.1"}]
            # warning: The package `name` requires `dep>=1.2.3`, but `1.2.2` is installed
        """)
        with raises(_UvPipListJsonError, match=r"Unable to parse JSON; got '.*'"):
            _ = _uv_pip_list_loads(text)


class TestUvPipListAssembleOutput:
    def test_main(self) -> None:
        dict_ = {"name": "name", "version": "0.0.1"}
        outdated = []
        result = _uv_pip_list_assemble_output(dict_, outdated)
        expected = _UvPipListOutput(name="name", version=Version3(0, 0, 1))
        assert result == expected

    def test_editable(self) -> None:
        dict_ = {
            "name": "name",
            "version": "0.0.1",
            "editable_project_location": str(PWD),
        }
        outdated = []
        result = _uv_pip_list_assemble_output(dict_, outdated)
        expected = _UvPipListOutput(
            name="name", version=Version3(0, 0, 1), editable_project_location=PWD
        )
        assert result == expected

    def test_outdated(self) -> None:
        dict_ = {"name": "name", "version": "0.0.1"}
        outdated = [
            {
                "name": "name",
                "version": "0.0.1",
                "latest_version": "0.0.2",
                "latest_filetype": "wheel",
            }
        ]
        result = _uv_pip_list_assemble_output(dict_, outdated)
        expected = _UvPipListOutput(
            name="name",
            version=Version3(0, 0, 1),
            latest_version=Version3(0, 0, 2),
            latest_filetype="wheel",
        )
        assert result == expected

    def test_error_base(self) -> None:
        dict_ = {"name": "name", "version": "invalid"}
        outdated = []
        with raises(
            _UvPipListBaseVersionError, match=r"Unable to parse version; got .*"
        ):
            _ = _uv_pip_list_assemble_output(dict_, outdated)

    def test_error_outdated(self) -> None:
        dict_ = {"name": "name", "version": "0.0.1"}
        outdated = [{"name": "name", "latest_version": "invalid"}]
        with raises(
            _UvPipListOutdatedVersionError, match=r"Unable to parse version; got .*"
        ):
            _ = _uv_pip_list_assemble_output(dict_, outdated)


class TestUvPipListCmd:
    def test_main(self) -> None:
        result = uv_pip_list_cmd()
        expected = [
            "uv",
            "pip",
            "list",
            "--format",
            "columns",
            "--strict",
            "--managed-python",
        ]
        assert result == expected

    def test_editable(self) -> None:
        result = uv_pip_list_cmd(editable=True)
        expected = [
            "uv",
            "pip",
            "list",
            "--editable",
            "--format",
            "columns",
            "--strict",
            "--managed-python",
        ]
        assert result == expected

    def test_exclude_editable(self) -> None:
        result = uv_pip_list_cmd(exclude_editable=True)
        expected = [
            "uv",
            "pip",
            "list",
            "--exclude-editable",
            "--format",
            "columns",
            "--strict",
            "--managed-python",
        ]
        assert result == expected

    def test_format(self) -> None:
        result = uv_pip_list_cmd(format_="json")
        expected = [
            "uv",
            "pip",
            "list",
            "--format",
            "json",
            "--strict",
            "--managed-python",
        ]
        assert result == expected

    def test_outdated(self) -> None:
        result = uv_pip_list_cmd(outdated=True)
        expected = [
            "uv",
            "pip",
            "list",
            "--format",
            "columns",
            "--outdated",
            "--strict",
            "--managed-python",
        ]
        assert result == expected


class TestUvNativeTLSCmd:
    def test_none(self) -> None:
        result = uv_native_tls_cmd()
        expected = []
        assert result == expected

    def test_native_tls(self) -> None:
        result = uv_native_tls_cmd(native_tls=True)
        expected = ["--native-tls"]
        assert result == expected


class TestUvRunCmd:
    def test_main(self) -> None:
        result = uv_run_cmd("foo.bar")
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_args(self) -> None:
        result = uv_run_cmd("foo.bar", "arg1", "arg2")
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
            "arg1",
            "arg2",
        ]
        assert result == expected

    def test_extra_single(self) -> None:
        result = uv_run_cmd("foo.bar", extra="extra")
        expected = [
            "uv",
            "run",
            "--extra",
            "extra",
            "--no-dev",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_extra_multiple(self) -> None:
        result = uv_run_cmd("foo.bar", extra=["extra1", "extra2"])
        expected = [
            "uv",
            "run",
            "--extra",
            "extra1",
            "--extra",
            "extra2",
            "--no-dev",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_all_extras(self) -> None:
        result = uv_run_cmd("foo.bar", all_extras=True)
        expected = [
            "uv",
            "run",
            "--all-extras",
            "--no-dev",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_group_single(self) -> None:
        result = uv_run_cmd("foo.bar", group="group")
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--group",
            "group",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_group_multiple(self) -> None:
        result = uv_run_cmd("foo.bar", group=["group1", "group2"])
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--group",
            "group1",
            "--group",
            "group2",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_all_groups(self) -> None:
        result = uv_run_cmd("foo.bar", all_groups=True)
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--all-groups",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_only_dev(self) -> None:
        result = uv_run_cmd("foo.bar", only_dev=True)
        expected = [
            "uv",
            "run",
            "--only-dev",
            "--exact",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected


class TestUvToolInstallCmd:
    def test_main(self) -> None:
        result = uv_tool_install_cmd("package")
        expected = [
            "uv",
            "tool",
            "install",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--reinstall",
            "--managed-python",
            "package",
        ]
        assert result == expected


class TestUvToolRunCmd:
    def test_main(self) -> None:
        result = uv_tool_run_cmd("command")
        expected = [
            "uv",
            "tool",
            "run",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--managed-python",
            "command",
        ]
        assert result == expected

    def test_args(self) -> None:
        result = uv_tool_run_cmd("command", "arg1", "arg2")
        expected = [
            "uv",
            "tool",
            "run",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--managed-python",
            "command",
            "arg1",
            "arg2",
        ]
        assert result == expected

    def test_from_(self) -> None:
        result = uv_tool_run_cmd("command", from_="from")
        expected = [
            "uv",
            "tool",
            "run",
            "--from",
            "from@latest",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--managed-python",
            "command",
        ]
        assert result == expected

    def test_from_not_latest(self) -> None:
        result = uv_tool_run_cmd("command", from_="from", latest=False)
        expected = [
            "uv",
            "tool",
            "run",
            "--from",
            "from",
            "--isolated",
            "--resolution",
            "highest",
            "--prerelease",
            "disallow",
            "--managed-python",
            "command",
        ]
        assert result == expected


class TestUvWithCmd:
    def test_none(self) -> None:
        result = uv_with_cmd()
        expected = []
        assert result == expected

    def test_single(self) -> None:
        result = uv_with_cmd(with_="with")
        expected = ["--with", "with"]
        assert result == expected

    def test_multiple(self) -> None:
        result = uv_with_cmd(with_=["with1", "with2"])
        expected = ["--with", "with1", "--with", "with2"]
        assert result == expected


class TestYieldGitRepo:
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, git_repo_url: str) -> None:
        with yield_git_repo(git_repo_url) as temp:
            assert (temp / "README.md").is_file()


class TestYieldSSHTempDir:
    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_main(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp:
            ssh(ssh_user, ssh_hostname, input=self._raise_missing(temp))
            with raises(CalledProcessError):
                ssh(ssh_user, ssh_hostname, input=self._raise_present(temp))
        ssh(ssh_user, ssh_hostname, input=self._raise_present(temp))
        with raises(CalledProcessError):
            ssh(ssh_user, ssh_hostname, input=self._raise_missing(temp))

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_keep(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname, keep=True) as temp:
            ...
        ssh(ssh_user, ssh_hostname, input=self._raise_missing(temp))

    @skipif_ci
    @throttle_test(duration=5 * MINUTE)
    def test_keep_and_logger(
        self, *, caplog: LogCaptureFixture, ssh_user: str, ssh_hostname: str
    ) -> None:
        name = unique_str()
        logger = getLogger(name=name)
        logger.setLevel(INFO)
        with yield_ssh_temp_dir(ssh_user, ssh_hostname, keep=True, logger=name):
            ...
        record = one(r for r in caplog.records if r.name == name)
        assert search(
            r"^Keeping temporary directory '[/\.\w]+'...$",
            record.message,
            flags=MULTILINE,
        )

    def _raise_missing(self, path: PathLike, /) -> str:
        return f"if ! [ -d {path} ]; then exit 1; fi"

    def _raise_present(self, path: PathLike, /) -> str:
        return f"if [ -d {path} ]; then exit 1; fi"
