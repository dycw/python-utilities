from __future__ import annotations

from logging import INFO, getLogger
from pathlib import Path
from re import MULTILINE, search
from subprocess import CalledProcessError
from typing import TYPE_CHECKING

from pytest import LogCaptureFixture, mark, param, raises

from utilities.grp import EFFECTIVE_GROUP_NAME
from utilities.iterables import one
from utilities.pathlib import get_file_group, get_file_owner
from utilities.permissions import Permissions
from utilities.pwd import EFFECTIVE_USER_NAME
from utilities.pytest import skipif_ci, skipif_mac, throttle
from utilities.subprocess import (
    BASH_LC,
    BASH_LS,
    ChownCmdError,
    CpError,
    MvFileError,
    RsyncCmdNoSourcesError,
    RsyncCmdSourcesNotFoundError,
    apt_install_cmd,
    cat_cmd,
    cd_cmd,
    chmod,
    chmod_cmd,
    chown,
    chown_cmd,
    cp,
    cp_cmd,
    echo_cmd,
    expand_path,
    git_clone_cmd,
    git_hard_reset_cmd,
    maybe_parent,
    maybe_sudo_cmd,
    mkdir,
    mkdir_cmd,
    mv,
    mv_cmd,
    rm,
    rm_cmd,
    rsync,
    rsync_cmd,
    rsync_many,
    run,
    set_hostname_cmd,
    ssh,
    ssh_cmd,
    ssh_keygen_cmd,
    ssh_opts_cmd,
    sudo_cmd,
    sudo_nopasswd_cmd,
    symlink,
    symlink_cmd,
    tee,
    tee_cmd,
    touch_cmd,
    uv_run_cmd,
    yield_ssh_temp_dir,
)
from utilities.tempfile import TemporaryDirectory, TemporaryFile
from utilities.text import strip_and_dedent, unique_str
from utilities.whenever import MINUTE, SECOND

if TYPE_CHECKING:
    from pytest import CaptureFixture

    from utilities.types import PathLike


class TestAptInstallCmd:
    def test_main(self) -> None:
        result = apt_install_cmd("package")
        expected = ["apt", "install", "-y", "package"]
        assert result == expected


class TestCatCmd:
    def test_main(self) -> None:
        result = cat_cmd("path")
        expected = ["cat", "path"]
        assert result == expected


class TestCDCmd:
    def test_main(self) -> None:
        result = cd_cmd("path")
        expected = ["cd", "path"]
        assert result == expected


class TestChMod:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        perms = Permissions.from_text("u=rw,g=r,o=r")
        _ = chmod(path, perms)
        current = Permissions.from_path(path)
        assert current == perms


class TestChModCmd:
    def test_main(self) -> None:
        result = chmod_cmd("path", "u=rw,g=r,o=r")
        expected = ["chmod", "u=rw,g=r,o=r", "path"]
        assert result == expected


class TestChOwn:
    def test_none(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        chown(path)

    def test_user(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        chown(path, user=EFFECTIVE_USER_NAME)
        current = get_file_owner(path)
        assert current == EFFECTIVE_USER_NAME

    def test_group(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        chown(path, group=EFFECTIVE_GROUP_NAME)
        current_group = get_file_group(path)
        assert current_group == EFFECTIVE_GROUP_NAME

    def test_user_and_group(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        chown(path, user=EFFECTIVE_USER_NAME, group=EFFECTIVE_GROUP_NAME)
        current_owner = get_file_owner(path)
        assert current_owner == EFFECTIVE_USER_NAME
        current_group = get_file_group(path)
        assert current_group == EFFECTIVE_GROUP_NAME


class TestChOwnCmd:
    def test_user(self) -> None:
        result = chown_cmd("path", user="user")
        expected = ["chown", "user", "path"]
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


class TestCp:
    def test_file(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        dest = tmp_path / "file2.txt"
        cp(src, dest)
        assert src.is_file()
        assert dest.is_file()

    def test_dir(self, *, tmp_path: Path) -> None:
        src = tmp_path / "dir"
        src.mkdir()
        dest = tmp_path / "dir2"
        cp(src, dest)
        assert src.is_dir()
        assert dest.is_dir()

    def test_perms(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        dest = tmp_path / "file2.txt"
        perms = Permissions.from_text("u=rwx,g=,o=")
        cp(src, dest, perms=perms)
        current = Permissions.from_path(dest)
        assert current == perms

    def test_owner(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        dest = tmp_path / "file2.txt"
        cp(src, dest, owner=EFFECTIVE_USER_NAME)
        current = get_file_owner(dest)
        assert current == EFFECTIVE_USER_NAME

    def test_error(self, *, tmp_path: Path) -> None:
        src = tmp_path / "dir"
        dest = tmp_path / "dir2"
        with raises(
            CpError, match=r"Unable to copy '.+' to '.+'; source does not exist"
        ):
            cp(src, dest)


class TestCpCmd:
    def test_main(self) -> None:
        result = cp_cmd("src", "dest")
        expected = ["cp", "-r", "src", "dest"]
        assert result == expected


class TestEchoCmd:
    def test_main(self) -> None:
        result = echo_cmd("'hello world'")
        expected = ["echo", "'hello world'"]
        assert result == expected


class TestExpandPath:
    def test_main(self) -> None:
        result = expand_path("~")
        expected = Path.home()
        assert result == expected

    def test_subs(self) -> None:
        result = expand_path("~/${dir}", subs={"dir": "foo"})
        expected = Path("~/foo").expanduser()
        assert result == expected


class TestGitCloneCmd:
    def test_main(self) -> None:
        result = git_clone_cmd("https://github.com/foo/bar", "path")
        expected = [
            "git",
            "clone",
            "--recurse-submodules",
            "https://github.com/foo/bar",
            "path",
        ]
        assert result == expected


class TestGitHardResetCmd:
    def test_main(self) -> None:
        result = git_hard_reset_cmd()
        expected = ["git", "hard-reset", "master"]
        assert result == expected

    def test_branch(self) -> None:
        result = git_hard_reset_cmd(branch="dev")
        expected = ["git", "hard-reset", "dev"]
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
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "dir"
        mkdir(path)
        assert Path(path).is_dir()


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
    def test_file(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        dest = tmp_path / "file2.txt"
        mv(src, dest)
        assert not src.is_file()
        assert dest.is_file()

    def test_dir(self, *, tmp_path: Path) -> None:
        src = tmp_path / "dir"
        src.mkdir()
        dest = tmp_path / "dir2"
        mv(src, dest)
        assert not src.is_dir()
        assert dest.is_dir()

    def test_perms(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        dest = tmp_path / "file2.txt"
        perms = Permissions.from_text("u=rwx,g=,o=")
        mv(src, dest, perms=perms)
        current = Permissions.from_path(dest)
        assert current == perms

    def test_owner(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        dest = tmp_path / "file2.txt"
        mv(src, dest, owner=EFFECTIVE_USER_NAME)
        current = get_file_owner(dest)
        assert current == EFFECTIVE_USER_NAME

    def test_error(self, *, tmp_path: Path) -> None:
        src = tmp_path / "dir"
        dest = tmp_path / "dir2"
        with raises(
            MvFileError, match=r"Unable to move '.+' to '.+'; source does not exist"
        ):
            mv(src, dest)


class TestMvCmd:
    def test_main(self) -> None:
        result = mv_cmd("src", "dest")
        expected = ["mv", "src", "dest"]
        assert result == expected


class TestRemove:
    def test_file(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        path.touch()
        assert path.is_file()
        rm(path)
        assert not path.is_file()

    def test_dir(self, *, tmp_path: Path) -> None:
        path = tmp_path / "dir"
        path.mkdir()
        assert path.is_dir()
        rm(path)
        assert not path.is_dir()


class TestRmCmd:
    def test_main(self) -> None:
        result = rm_cmd("path")
        expected = ["rm", "-rf", "path"]
        assert result == expected


class TestRsync:
    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_file(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with (
            TemporaryFile() as src,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest,
        ):
            dest = temp_dest / src.name
            rsync(src, ssh_user, ssh_hostname, dest)
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=f"if ! [ -f {dest} ]; then exit 1; fi",
            )

    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_dir_without_trailing_slash(
        self, *, ssh_user: str, ssh_hostname: str
    ) -> None:
        with (
            TemporaryDirectory() as src,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest,
        ):
            (src / "file.txt").touch()
            name = src.name
            dest = temp_dest / name
            rsync(src, ssh_user, ssh_hostname, dest)
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=strip_and_dedent(f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -d {dest}/{name} ]; then exit 1; fi
                    if ! [ -f {dest}/{name}/file.txt ]; then exit 1; fi
                """),
            )

    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_dir_with_trailing_slash(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with (
            TemporaryDirectory() as src,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest,
        ):
            (src / "file.txt").touch()
            dest = temp_dest / src.name
            rsync(f"{src}/", ssh_user, ssh_hostname, dest)
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=strip_and_dedent(f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -f {dest}/file.txt ]; then exit 1; fi
                """),
            )


class TestRsyncCmd:
    def test_main(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(src, "user", "hostname", "dest")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_multiple_sources(self, *, tmp_path: Path) -> None:
        src1, src2 = [tmp_path / f"file{i}.txt" for i in [1, 2]]
        src1.touch()
        src2.touch()
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
        src = tmp_path / "src"
        src.mkdir()
        result = rsync_cmd(f"{src}/", "user", "hostname", "dest")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            f"{src}/",
            "user@hostname:dest",
        ]
        assert result == expected

    def test_archive(self, *, tmp_path: Path) -> None:
        src = tmp_path / "src"
        src.mkdir()
        result = rsync_cmd(src, "user", "hostname", "dest", archive=True)
        expected: list[str] = [
            "rsync",
            "--archive",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_chown_user(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(src, "user", "hostname", "dest", chown_user="user2")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--chown",
            "user2",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_chown_group(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(src, "user", "hostname", "dest", chown_group="group")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--chown",
            ":group",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_chown_user_and_group(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(
            src, "user", "hostname", "dest", chown_user="user2", chown_group="group"
        )
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--chown",
            "user2:group",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_exclude(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(src, "user", "hostname", "dest", exclude="exclude")
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--exclude",
            "exclude",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_exclude_multiple(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(
            src, "user", "hostname", "dest", exclude=["exclude1", "exclude2"]
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
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_sudo(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        src.touch()
        result = rsync_cmd(src, "user", "hostname", "dest", sudo=True)
        expected: list[str] = [
            "rsync",
            "--checksum",
            "--compress",
            "--rsh",
            "ssh -o BatchMode=yes -o HostKeyAlgorithms=ssh-ed25519 -o StrictHostKeyChecking=yes -T",
            "--rsync-path",
            "sudo rsync",
            str(src),
            "user@hostname:dest",
        ]
        assert result == expected

    def test_error_no_sources(self) -> None:
        with raises(
            RsyncCmdNoSourcesError,
            match=r"No sources selected to send to user@hostname:dest",
        ):
            _ = rsync_cmd([], "user", "hostname", "dest")

    def test_error_sources_not_found(self, *, tmp_path: Path) -> None:
        src = tmp_path / "file.txt"
        with raises(
            RsyncCmdSourcesNotFoundError,
            match=r"Sources selected to send to user@hostname:dest but not found: '.*/file\.txt'",
        ):
            _ = rsync_cmd(src, "user", "hostname", "dest")


class TestRsyncMany:
    @throttle(delta=5 * MINUTE)
    def test_single_file(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with (
            TemporaryDirectory() as temp_src,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest,
        ):
            src = temp_src / "file.txt"
            src.touch()
            dest = temp_dest / src.name
            rsync_many(ssh_user, ssh_hostname, (src, dest))
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=f"if ! [ -f {dest} ]; then exit 1; fi",
            )

    @throttle(delta=5 * MINUTE)
    def test_multiple_files(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with (
            TemporaryDirectory() as temp_src,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest,
        ):
            src1, src2 = [temp_src / f"file{i}.txt" for i in [1, 2]]
            src1.touch()
            src2.touch()
            dest1, dest2 = [temp_dest / src.name for src in [src1, src2]]
            rsync_many(ssh_user, ssh_hostname, (src1, dest1), (src2, dest2))
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=strip_and_dedent(f"""
                    if ! [ -f {dest1} ]; then exit 1; fi
                    if ! [ -f {dest2} ]; then exit 1; fi
                """),
            )

    @throttle(delta=5 * MINUTE)
    def test_single_directory(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with (
            TemporaryDirectory() as temp_src,
            yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp_dest,
        ):
            src = temp_src / "dir"
            src.mkdir()
            (src / "file.txt").touch()
            dest = temp_dest / src.name
            rsync_many(ssh_user, ssh_hostname, (src, dest))
            ssh(
                ssh_user,
                ssh_hostname,
                *BASH_LS,
                input=(
                    f"""
                    if ! [ -d {dest} ]; then exit 1; fi
                    if ! [ -f {dest}/file.txt ]; then exit 1; fi
                """
                ),
            )


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
        input_ = strip_and_dedent("""
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
        input_ = strip_and_dedent("""
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
        input_ = strip_and_dedent("""
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

    def test_retry_1_attempt(
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

    def test_retry_2_attempts(
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

    def test_retry_and_leep(self, *, tmp_path: Path, caplog: LogCaptureFixture) -> None:
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

    def test_logger(self, *, caplog: LogCaptureFixture) -> None:
        name = unique_str()
        with raises(CalledProcessError):
            _ = run("echo stdout; echo stderr 1>&2; exit 1", shell=True, logger=name)  # noqa: S604
        record = one(r for r in caplog.records if r.name == name)
        expected = strip_and_dedent("""
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
        input_ = strip_and_dedent(
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
        expected = strip_and_dedent("""
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
        return strip_and_dedent(
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
    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_main(
        self,
        *,
        capsys: CaptureFixture,
        ssh_user: str,
        ssh_hostname: str,
        ssh_hostname_internal: str,
    ) -> None:
        input_ = strip_and_dedent("""
            whoami
            hostname 1>&2
        """)
        result = ssh(ssh_user, ssh_hostname, input=input_, print=True)
        assert result is None
        cap = capsys.readouterr()
        assert cap.out == f"{ssh_user}\n"
        assert cap.err == f"{ssh_hostname_internal}\n"


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


class TestSSHKeyGenCmd:
    def test_main(self) -> None:
        result = ssh_keygen_cmd("hostname")
        expected = ["ssh-keygen", "-f", "~/.ssh/known_hosts", "-R", "hostname"]
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
    def test_main(self, *, tmp_path: Path) -> None:
        target = tmp_path / "file.txt"
        target.touch()
        link = tmp_path / "link.txt"
        symlink(target, link)
        assert link.is_symlink()
        assert link.resolve() == target


class TestSymLinkCmd:
    def test_main(self) -> None:
        result = symlink_cmd("target", "link")
        expected = ["ln", "-s", "target", "link"]
        assert result == expected


class TestTee:
    def test_main(self, *, tmp_path: Path) -> None:
        path = tmp_path / "file.txt"
        text = "text"
        tee(path, text)
        result = path.read_text()
        assert result == text


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


class TestUvRunCmd:
    def test_main(self) -> None:
        result = uv_run_cmd("foo.bar")
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--active",
            "--prerelease=disallow",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
        ]
        assert result == expected

    def test_args(self) -> None:
        result = uv_run_cmd("foo.bar", "--arg")
        expected = [
            "uv",
            "run",
            "--no-dev",
            "--active",
            "--prerelease=disallow",
            "--managed-python",
            "python",
            "-m",
            "foo.bar",
            "--arg",
        ]
        assert result == expected


class TestYieldSSHTempDir:
    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_main(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname) as temp:
            ssh(ssh_user, ssh_hostname, input=self._raise_missing(temp))
            with raises(CalledProcessError):
                ssh(ssh_user, ssh_hostname, input=self._raise_present(temp))
        ssh(ssh_user, ssh_hostname, input=self._raise_present(temp))
        with raises(CalledProcessError):
            ssh(ssh_user, ssh_hostname, input=self._raise_missing(temp))

    @skipif_ci
    @throttle(delta=5 * MINUTE)
    def test_keep(self, *, ssh_user: str, ssh_hostname: str) -> None:
        with yield_ssh_temp_dir(ssh_user, ssh_hostname, keep=True) as temp:
            ...
        ssh(ssh_user, ssh_hostname, input=self._raise_missing(temp))

    @skipif_ci
    @throttle(delta=5 * MINUTE)
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
