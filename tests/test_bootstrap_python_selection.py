import pathlib
import stat
import textwrap
from unittest.mock import patch

import bootstrap
import pytest


def _repo_venv_python(repo_root: pathlib.Path) -> pathlib.Path:
    rel = pathlib.Path(
        "Scripts/python.exe" if bootstrap.platform.system() == "Windows" else "bin/python"
    )
    return repo_root / ".venv" / rel


def test_ensure_python_prefers_agent_venv_when_launcher_cannot_import_agent(monkeypatch, tmp_path):
    """Avoid starting WebUI with a local venv that later cannot import AIAgent."""
    local_python = tmp_path / "webui" / ".venv" / "bin" / "python"
    agent_python = tmp_path / "agent" / "venv" / "bin" / "python"
    agent_python.parent.mkdir(parents=True)
    agent_python.write_text("", encoding="utf-8")

    probes = []

    def fake_can_run(python_exe: str, agent_dir: pathlib.Path | None = None) -> bool:
        probes.append(pathlib.Path(python_exe))
        return pathlib.Path(python_exe) == agent_python

    monkeypatch.setattr(bootstrap, "_python_can_run_webui_and_agent", fake_can_run)

    selected = bootstrap.ensure_python_has_webui_deps(str(local_python), tmp_path / "agent")

    assert selected == str(agent_python)
    assert probes == [local_python, agent_python]


def test_explicit_webui_python_fails_closed_instead_of_falling_back_to_agent_venv(
    monkeypatch, tmp_path
):
    """An explicit WebUI runtime must never drift to Hermes's mutable venv."""
    webui_python = tmp_path / "webui" / ".venv" / "bin" / "python"
    agent_dir = tmp_path / "agent"
    agent_python = agent_dir / "venv" / "bin" / "python"
    agent_python.parent.mkdir(parents=True)
    agent_python.write_text("", encoding="utf-8")
    monkeypatch.setenv("HERMES_WEBUI_PYTHON", str(webui_python))

    probes = []

    def fake_can_run(python_exe: str, agent_dir_arg: pathlib.Path | None = None) -> bool:
        probes.append(pathlib.Path(python_exe))
        return pathlib.Path(python_exe) == agent_python

    monkeypatch.setattr(bootstrap, "_python_can_run_webui_and_agent", fake_can_run)

    with patch.object(bootstrap.venv, "EnvBuilder") as mock_builder:
        with pytest.raises(RuntimeError, match="Refusing to fall back"):
            bootstrap.ensure_python_has_webui_deps(str(webui_python), agent_dir)

    assert probes == [webui_python]
    mock_builder.assert_not_called()


def test_native_windows_keeps_existing_explicit_python_fallback(
    monkeypatch, tmp_path
):
    webui_python = tmp_path / "webui" / ".venv" / "Scripts" / "python.exe"
    agent_dir = tmp_path / "agent"
    agent_python = agent_dir / "venv" / "Scripts" / "python.exe"
    agent_python.parent.mkdir(parents=True)
    agent_python.write_text("", encoding="utf-8")
    monkeypatch.setenv("HERMES_WEBUI_PYTHON", str(webui_python))
    monkeypatch.setattr(bootstrap.platform, "system", lambda: "Windows")
    monkeypatch.setattr(
        bootstrap,
        "_python_can_run_webui_and_agent",
        lambda python_exe, _agent_dir=None: pathlib.Path(python_exe) == agent_python,
    )

    assert bootstrap.ensure_python_has_webui_deps(
        str(webui_python), agent_dir
    ) == str(agent_python)


@pytest.mark.skipif(bootstrap.sys.platform == "win32", reason="POSIX exec fixture")
def test_agent_lazy_reexec_into_pm_python_missing_yaml_is_reproduced_and_guarded(
    monkeypatch, tmp_path
):
    """Reproduce run_agent replacing WebUI Python with PM Python lacking yaml."""
    agent_dir = tmp_path / "hermes-agent"
    agent_dir.mkdir()
    pm_python = tmp_path / "pm-python"
    pm_python.write_text(
        "#!/bin/sh\n"
        "echo \"ModuleNotFoundError: No module named 'yaml'\" >&2\n"
        "exit 1\n",
        encoding="utf-8",
    )
    pm_python.chmod(pm_python.stat().st_mode | stat.S_IXUSR)
    (agent_dir / "run_agent.py").write_text(
        textwrap.dedent(
            """
            import os

            if os.environ.get("HERMES_DISABLE_LAZY_INSTALLS") != "1":
                pm_python = os.environ["HERMES_TEST_PM_PYTHON"]
                os.execv(pm_python, [pm_python, "-c", "import yaml"])

            class AIAgent:
                pass
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_TEST_PM_PYTHON", str(pm_python))
    monkeypatch.delenv("HERMES_DISABLE_LAZY_INSTALLS", raising=False)

    assert not bootstrap._python_can_run_webui_and_agent(
        bootstrap.sys.executable, agent_dir
    )

    monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", "1")
    assert bootstrap._python_can_run_webui_and_agent(
        bootstrap.sys.executable, agent_dir
    )


def test_main_scopes_lazy_install_guard_to_explicit_posix_webui_runtime(
    monkeypatch, tmp_path
):
    """Bootstrap owns the import environment without changing manual/Windows starts."""
    webui_python = tmp_path / "webui" / ".venv" / "bin" / "python"
    agent_dir = tmp_path / "hermes-agent"
    agent_dir.mkdir()
    state_dir = tmp_path / "state"
    monkeypatch.setenv("HERMES_WEBUI_PYTHON", str(webui_python))
    monkeypatch.setenv("HERMES_WEBUI_STATE_DIR", str(state_dir))
    monkeypatch.delenv("HERMES_DISABLE_LAZY_INSTALLS", raising=False)
    monkeypatch.delenv("HERMES_WEBUI_FOREGROUND", raising=False)
    for supervisor_var in bootstrap._SUPERVISOR_ENV_VARS:
        monkeypatch.delenv(supervisor_var, raising=False)
    monkeypatch.setattr(bootstrap.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        bootstrap,
        "parse_args",
        lambda: type(
            "Args", (), {"host": "127.0.0.1", "port": 18787, "skip_agent_install": True,
                         "foreground": False, "no_browser": True}
        )(),
    )
    monkeypatch.setattr(bootstrap, "ensure_supported_platform", lambda: None)
    monkeypatch.setattr(bootstrap, "discover_agent_dir", lambda: agent_dir)
    monkeypatch.setattr(bootstrap, "discover_launcher_python", lambda _agent_dir: str(webui_python))

    def fake_ensure(python_exe, _agent_dir):
        assert python_exe == str(webui_python)
        assert bootstrap.os.environ["HERMES_DISABLE_LAZY_INSTALLS"] == "1"
        return python_exe

    monkeypatch.setattr(bootstrap, "ensure_python_has_webui_deps", fake_ensure)
    health_urls = []

    def fake_wait_for_health(url):
        health_urls.append(url)
        return "http"

    monkeypatch.setattr(bootstrap, "wait_for_health", fake_wait_for_health)
    launched = {}

    class FakeProcess:
        pid = 4321

    def fake_popen(argv, **kwargs):
        launched["argv"] = argv
        launched["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr(bootstrap.subprocess, "Popen", fake_popen)

    assert bootstrap.main() == 0
    assert launched["argv"][0] == str(webui_python)
    assert launched["env"]["HERMES_WEBUI_PYTHON"] == str(webui_python)
    assert launched["env"]["HERMES_DISABLE_LAZY_INSTALLS"] == "1"
    assert health_urls == ["http://127.0.0.1:18787/health"]


@pytest.mark.parametrize(
    ("platform_name", "webui_python", "lazy_value", "expected"),
    [
        ("Linux", "", None, None),
        ("Windows", "/webui/.venv/bin/python", None, None),
        ("Linux", "/webui/.venv/bin/python", "0", "0"),
    ],
)
def test_lazy_install_guard_preserves_automatic_windows_and_operator_behavior(
    monkeypatch, tmp_path, platform_name, webui_python, lazy_value, expected
):
    agent_dir = tmp_path / "hermes-agent"
    agent_dir.mkdir()
    monkeypatch.setattr(bootstrap.platform, "system", lambda: platform_name)
    if webui_python:
        monkeypatch.setenv("HERMES_WEBUI_PYTHON", webui_python)
    else:
        monkeypatch.delenv("HERMES_WEBUI_PYTHON", raising=False)
    if lazy_value is None:
        monkeypatch.delenv("HERMES_DISABLE_LAZY_INSTALLS", raising=False)
    else:
        monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", lazy_value)

    bootstrap._prepare_explicit_webui_runtime(agent_dir)

    assert bootstrap.os.environ.get("HERMES_DISABLE_LAZY_INSTALLS") == expected


def test_ensure_python_fails_loudly_when_no_interpreter_can_import_agent(monkeypatch, tmp_path):
    """Do not report health OK when chat would fail with missing AIAgent."""
    local_python = tmp_path / "webui" / ".venv" / "bin" / "python"
    agent_python = tmp_path / "agent" / "venv" / "bin" / "python"
    agent_python.parent.mkdir(parents=True)
    agent_python.write_text("", encoding="utf-8")

    # Pretend REPO_ROOT/.venv already exists with a python binary so the function
    # skips venv.EnvBuilder.create() entirely. Without this, CI runners that
    # don't have a .venv try to build one and the monkey-patched subprocess
    # stub (which only covers subprocess.run, not the venv module's internal
    # subprocess.check_output) fails with AttributeError on .stdout. The
    # behavior under test is "what happens when no interpreter can import
    # both WebUI deps and the agent", not the venv-creation path itself.
    fake_venv_python = tmp_path / "fake-repo-venv-python"
    fake_venv_python.write_text("", encoding="utf-8")
    monkeypatch.setattr(bootstrap, "REPO_ROOT", tmp_path)
    # Ensure the platform-native repo-local venv python already exists so
    # EnvBuilder.create() is skipped and the test stays focused on interpreter
    # selection rather than venv creation internals.
    venv_python = _repo_venv_python(tmp_path)
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    venv_python.write_text("", encoding="utf-8")
    if (tmp_path / ".venv").exists():  # platform-independent guard
        pass

    monkeypatch.setattr(bootstrap, "_python_can_run_webui_and_agent", lambda *a, **k: False)
    # Cover both subprocess.run (used for pip install) and any other subprocess
    # entry points the venv module might invoke. Returning None is fine because
    # we never inspect the result on this code path.
    monkeypatch.setattr(bootstrap.subprocess, "run", lambda *a, **k: None)

    try:
        bootstrap.ensure_python_has_webui_deps(str(local_python), tmp_path / "agent")
    except RuntimeError as exc:
        assert "cannot import both WebUI dependencies and Hermes Agent" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_packaged_launch_disables_repo_local_venv_creation(monkeypatch, tmp_path):
    local_python = tmp_path / "webui" / ".venv" / "bin" / "python"
    monkeypatch.setattr(bootstrap, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("HERMES_WEBUI_DISABLE_LOCAL_VENV", "1")
    monkeypatch.setattr(bootstrap, "_python_can_run_webui_and_agent", lambda *a, **k: False)

    with patch.object(bootstrap.venv, "EnvBuilder") as mock_builder:
        try:
            bootstrap.ensure_python_has_webui_deps(str(local_python), tmp_path / "agent")
        except RuntimeError as exc:
            assert "local .venv creation is disabled" in str(exc)
            assert "HERMES_WEBUI_PYTHON" in str(exc)
        else:
            raise AssertionError("expected RuntimeError")

    mock_builder.assert_not_called()


def test_local_venv_is_created_with_symlinks(monkeypatch, tmp_path):
    """Regression: mise/asdf macOS Pythons need symlinks=True to avoid SIGABRT.

    Their copy-mode venv produces a python binary referencing
    @executable_path/../lib/libpython3.X.dylib that never gets copied into the
    new .venv. Symlinking keeps @executable_path resolving back to the original
    install. CPython's venv falls back to copy mode if symlink creation fails,
    so this is safe to set unconditionally.
    """
    local_python = tmp_path / "webui" / ".venv" / "bin" / "python"
    monkeypatch.setattr(bootstrap, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(bootstrap, "_python_can_run_webui_and_agent", lambda *a, **k: False)
    monkeypatch.setattr(bootstrap.subprocess, "run", lambda *a, **k: None)

    with patch.object(bootstrap.venv, "EnvBuilder") as mock_builder:
        # Make EnvBuilder().create() materialize the venv python so the post-create
        # `_python_can_run_webui_and_agent` retry path doesn't trip on a missing file.
        venv_python = _repo_venv_python(tmp_path)

        def fake_create(target):
            venv_python.parent.mkdir(parents=True, exist_ok=True)
            venv_python.write_text("", encoding="utf-8")

        mock_builder.return_value.create.side_effect = fake_create

        try:
            bootstrap.ensure_python_has_webui_deps(str(local_python), None)
        except RuntimeError:
            pass  # expected — fake _python_can_run_webui_and_agent always returns False

        mock_builder.assert_called_once_with(with_pip=True, symlinks=True)
