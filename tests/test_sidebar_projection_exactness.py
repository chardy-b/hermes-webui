"""Exactness contract for the ``/api/sessions`` CLI/state.db projection.

The WebUI-only sidebar request
(``sidebar_source=webui&exclude_hidden=1&all_profiles=1``) still pays for the
full state.db projection (``get_cli_sessions``) because the response is not
only the WebUI rows: ``cli_count`` / ``cli_session_count`` / ``archived_cli_count``
come from the projection, orphaned imported sidecars are reconciled against it,
and messaging rows are merged/deduped across both buckets.

Two optimizations were considered for that hot path:

* skip the projection for WebUI-only row requests;
* order the bounded candidate window by ``sessions.last_activity_at`` instead of
  the correlated ``MAX(messages.timestamp)``.

Neither can be proven equivalent from the WebUI side (see
``docs/perf-sidebar-implementation-notes.md``). These tests drive the real
endpoint and the real SQLite projection (no mocks of the code under test) so
that any future optimization has to demonstrate it is semantically exact:

* a resumed *old* session must still displace newer ones;
* a stale or NULL ``last_activity_at`` must not decide ordering;
* legacy schemas (no ``last_activity_at``, no ``messages.timestamp``, no
  ``messages`` table) must keep projecting rows;
* source / archived / WebUI counts and orphan reconciliation must not change;
* external state.db writes must still become visible.

The last section covers the per-profile projection entries that sit beneath the
all-profiles aggregate cache in ``api/models.py``: after one profile commits,
only that profile may be recomputed, and every result must equal an uncached
reference loop (the pre-cache behavior). The earlier sections pin behavior that
already held before that change; only the work-reuse assertions in the last
section are red on the base commit.
"""

import io
import json
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import pytest

import api.agent_sessions as agent_sessions
import api.models as models
import api.profiles as profiles
import api.route_session_list_cache as route_cache
import api.routes as routes

NOW = 1_800_000_000.0


class _FakeHandler:
    def __init__(self):
        self.status = None
        self.headers = {}
        self.wfile = io.BytesIO()

    def send_response(self, status):
        self.status = status

    def send_header(self, key, value):
        self.headers[key] = value

    def end_headers(self):
        pass

    def json_body(self):
        return json.loads(self.wfile.getvalue().decode("utf-8"))


def _get(url):
    handler = _FakeHandler()
    routes.handle_get(handler, urlparse(url))
    assert handler.status == 200, url
    return handler.json_body()


# ── state.db fixtures ──────────────────────────────────────────────────────

def make_state_db(
    path: Path,
    sessions,
    *,
    last_activity_at: bool = True,
    messages_table: bool = True,
    messages_timestamp: bool = True,
):
    """Create an Agent-shaped state.db.

    ``sessions`` is a list of dicts: ``id``, ``source``, ``title``,
    ``started_at``, optional ``parent``/``end_reason`` and ``messages`` (list of
    ``(role, timestamp)``). ``last_activity_at`` may be given per session
    (including ``None`` and deliberately stale values) when the column exists.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    last_col = ", last_activity_at REAL" if last_activity_at else ""
    conn.execute(
        "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, title TEXT,"
        " model TEXT, started_at REAL NOT NULL, message_count INTEGER DEFAULT 0,"
        f" parent_session_id TEXT, ended_at REAL, end_reason TEXT{last_col})"
    )
    conn.execute("CREATE INDEX idx_sessions_started ON sessions(started_at)")
    if messages_table:
        ts_col = ", timestamp REAL" if messages_timestamp else ""
        conn.execute(
            "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            f" session_id TEXT, role TEXT, content TEXT{ts_col})"
        )
        if messages_timestamp:
            conn.execute(
                "CREATE INDEX idx_messages_session ON messages(session_id, timestamp)"
            )
    for s in sessions:
        add_session(conn, s, last_activity_at=last_activity_at,
                    messages_table=messages_table,
                    messages_timestamp=messages_timestamp)
    conn.commit()
    conn.close()


def add_session(conn, s, *, last_activity_at=True, messages_table=True,
                messages_timestamp=True):
    msgs = s.get("messages", [])
    cols = ["id", "source", "title", "model", "started_at", "message_count",
            "parent_session_id", "end_reason"]
    vals = [s["id"], s.get("source", "cli"), s.get("title", f"title {s['id']}"),
            "m", s["started_at"], len(msgs), s.get("parent"), s.get("end_reason")]
    if last_activity_at:
        cols.append("last_activity_at")
        vals.append(s.get("last_activity_at"))
    conn.execute(
        f"INSERT INTO sessions ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
        vals,
    )
    if messages_table:
        for role, ts in msgs:
            if messages_timestamp:
                conn.execute(
                    "INSERT INTO messages (session_id, role, content, timestamp)"
                    " VALUES (?, ?, 'x', ?)", (s["id"], role, ts))
            else:
                conn.execute(
                    "INSERT INTO messages (session_id, role, content)"
                    " VALUES (?, ?, 'x')", (s["id"], role))


def chat(n, base):
    """n alternating user/assistant messages ending at ``base``."""
    return [("user" if i % 2 == 0 else "assistant", base - (n - 1 - i)) for i in range(n)]


def cli_session(sid, started, last=None, n=4, **extra):
    return {"id": sid, "source": "cli", "started_at": started,
            "messages": chat(n, last if last is not None else started + 10), **extra}


# ── endpoint harness ───────────────────────────────────────────────────────

class Harness:
    def __init__(self, tmp_path, monkeypatch):
        self.home = tmp_path / "hermes"
        self.work = self.home / "profiles" / "work"
        self.session_dir = tmp_path / "sessions"
        self.session_dir.mkdir()
        self.webui_rows = []
        self.cli_calls = []
        self.pruned = []
        self.sql = []
        self.loader_calls = []      # one entry per _load_cli_sessions_uncached call
        self.loader_hook = None     # optional callable(profile) run inside the loader
        self.profile_names = ["default", "work"]
        self.active = "default"
        self.home.mkdir(parents=True, exist_ok=True)
        self.work.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(models, "SESSION_DIR", self.session_dir)
        monkeypatch.setattr(profiles, "get_active_hermes_home", lambda: self.home)
        monkeypatch.setattr(profiles, "get_active_profile_name", lambda: self.active)
        monkeypatch.setattr(
            profiles, "get_hermes_home_for_profile",
            lambda name=None: self.home if name in (None, "", "default") else self.home / "profiles" / name,
        )
        monkeypatch.setattr(profiles, "list_profiles_api",
                            lambda: [{"name": n} for n in self.profile_names])
        monkeypatch.setattr(models, "SESSION_INDEX_FILE", self.session_dir / "_index.json")
        monkeypatch.setattr(models, "PROJECTS_FILE", tmp_path / "projects.json")
        monkeypatch.setattr(profiles, "_profiles_root", lambda: self.home / "profiles")
        self.real_claude_code_sessions = models.get_claude_code_sessions
        monkeypatch.setattr(models, "get_claude_code_sessions", lambda *a, **k: [])
        monkeypatch.setattr(models, "get_last_workspace", lambda: str(tmp_path))
        monkeypatch.setattr(models, "ensure_cron_project", lambda create=True: None)
        monkeypatch.setattr(models, "ensure_webhook_project", lambda: None)
        monkeypatch.setattr(models, "_profile_has_user_projects", lambda: False)

        monkeypatch.setattr(routes, "load_settings", lambda: {
            "show_cli_sessions": True,
            "show_claude_code_sessions": False,
            "show_previous_messaging_sessions": False,
            "show_cron_sessions": False,
        })
        monkeypatch.setattr(routes, "all_sessions",
                            lambda diag=None, **kw: [dict(r) for r in self.webui_rows])
        monkeypatch.setattr(routes, "_reconcile_stale_stream_state_for_session_rows",
                            lambda _rows: False)
        monkeypatch.setattr(routes, "_enrich_sidebar_lineage_metadata", lambda rows: None)
        monkeypatch.setattr(routes, "agent_session_zero_message_sids",
                            lambda ids, profile=None: frozenset())
        monkeypatch.setattr(routes, "prune_session_from_index",
                            lambda sid: self.pruned.append(sid))

        real_get_cli = models.get_cli_sessions

        def spy(source_filter=None, *, all_profiles=False, include_claude_code=True):
            rows = real_get_cli(source_filter, all_profiles=all_profiles,
                                include_claude_code=include_claude_code)
            self.cli_calls.append({"all_profiles": all_profiles, "rows": len(rows)})
            return rows

        monkeypatch.setattr(routes, "get_cli_sessions", spy)

        self.real_loader = models._load_cli_sessions_uncached

        def loader_spy(hermes_home, db_path, cli_profile, source_filter=None, **kw):
            self.loader_calls.append({
                "profile": cli_profile, "source_filter": source_filter,
                "include_claude_code": kw.get("include_claude_code", True),
            })
            if self.loader_hook is not None:
                self.loader_hook(cli_profile)
            return self.real_loader(hermes_home, db_path, cli_profile, source_filter, **kw)

        monkeypatch.setattr(models, "_load_cli_sessions_uncached", loader_spy)

        real_connect = sqlite3.connect

        def traced_connect(*a, **k):
            conn = real_connect(*a, **k)
            try:
                conn.set_trace_callback(self.sql.append)
            except sqlite3.Error:
                pass
            return conn

        monkeypatch.setattr(sqlite3, "connect", traced_connect)
        self.clear()

    def clear(self):
        routes._session_list_cache_clear()
        models.clear_cli_sessions_cache()

    def loaded(self, profile=None):
        """Profiles whose projection was recomputed (in call order)."""
        return [c["profile"] for c in self.loader_calls
                if profile is None or c["profile"] == profile]

    def reference(self, source_filter=None, include_claude_code=True):
        """Uncached ground truth: the exact loop get_cli_sessions(all_profiles=True)
        ran before per-profile entries existed."""
        contexts, _ = models._all_profiles_cli_contexts()
        out = []
        for idx, (home, db, prof) in enumerate(contexts):
            out.extend(self.real_loader(
                home, db, prof, source_filter=source_filter,
                visible_session_limit=None, cron_project_limit=None,
                webhook_project_limit=None, kanban_project_limit=None,
                include_claude_code=include_claude_code and idx == 0,
            ))
        return out

    def commit_message(self, profile_home, sid, ts):
        """Commit one new message into an existing session (external writer)."""
        conn = sqlite3.connect(str(profile_home / "state.db"))
        conn.execute("INSERT INTO messages (session_id, role, content, timestamp)"
                     " VALUES (?, 'user', 'x', ?)", (sid, ts))
        conn.execute("UPDATE sessions SET message_count = message_count + 1 WHERE id = ?", (sid,))
        conn.commit()
        conn.close()

    def archive_sidecar(self, sid):
        (self.session_dir / f"{sid}.json").write_text(
            json.dumps({"session_id": sid, "title": f"title {sid}", "archived": True,
                        "messages": [], "created_at": 1.0, "updated_at": 2.0}),
            encoding="utf-8")

    @staticmethod
    def webui_row(sid, ts, *, archived=False, profile="default"):
        return {
            "session_id": sid, "title": f"WebUI {sid}", "profile": profile,
            "archived": archived, "message_count": 3, "updated_at": ts,
            "last_message_at": ts, "source": "webui", "raw_source": "webui",
            "session_source": "webui", "source_tag": "webui", "is_cli_session": False,
        }

    @staticmethod
    def imported_sidecar(sid, ts, *, profile="default"):
        return {
            "session_id": sid, "title": f"imported {sid}", "profile": profile,
            "archived": False, "message_count": 3, "updated_at": ts,
            "last_message_at": ts, "source": "cli", "raw_source": "cli",
            "session_source": "cli", "source_tag": "cli", "is_cli_session": True,
        }


@pytest.fixture
def env(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    yield h
    h.clear()


WEBUI_ONLY = "/api/sessions?sidebar_source=webui&exclude_hidden=1&all_profiles=1"
ALL_SOURCES = "/api/sessions?exclude_hidden=1&all_profiles=1"
COUNT_KEYS = ("cli_count", "cli_session_count", "webui_session_count",
              "archived_count", "archived_webui_count", "archived_cli_count",
              "other_profile_count")


def _ids(body):
    return [r["session_id"] for r in body["sessions"]]


# ── WebUI-only response semantics ──────────────────────────────────────────

def test_webui_only_all_profiles_keeps_projection_counts_and_rows(env):
    """Row filtering is cosmetic: counts still come from the full projection.

    A WebUI-only request must report the same CLI/archived counts as the
    unfiltered request and must have consulted the all-profiles projection.
    """
    make_state_db(env.home / "state.db", [
        cli_session("cli-d1", 100), cli_session("cli-d2", 200), cli_session("cli-d3", 300),
        # hidden background rows: never in rows or counts under exclude_hidden
        {"id": "cron_job_1", "source": "cron", "started_at": 250,
         "messages": chat(4, 260), "title": "nightly"},
    ])
    make_state_db(env.work / "state.db", [
        cli_session("cli-w1", 150),
        cli_session("cli-w2", 350),
    ])
    env.archive_sidecar("cli-d1")  # archived CLI row in the default profile
    env.webui_rows = [
        env.webui_row("web-1", 400),
        env.webui_row("web-2", 450),
        env.webui_row("web-arch", 90, archived=True),
        env.webui_row("web-work", 410, profile="work"),
    ]

    webui_only = _get(WEBUI_ONLY)
    everything = _get(ALL_SOURCES)

    # The WebUI-only request still hit the all-profiles projection, and that
    # projection saw rows from BOTH profiles' state.db files (5 non-cron).
    assert env.cli_calls and env.cli_calls[0]["all_profiles"] is True
    assert env.cli_calls[0]["rows"] >= 5

    assert set(_ids(webui_only)) == {"web-1", "web-2", "web-work"}
    assert not any(i.startswith(("cli-", "cron_")) for i in _ids(webui_only))
    for key in COUNT_KEYS:
        assert webui_only[key] == everything[key], key
    # Spot-check absolute values so the equality above can't be vacuous.
    assert webui_only["cli_count"] == 5
    assert webui_only["cli_session_count"] == 4          # archived cli-d1 hidden by default
    assert webui_only["archived_cli_count"] == 1
    assert webui_only["archived_webui_count"] == 1
    assert webui_only["webui_session_count"] == 3
    assert webui_only["all_profiles"] is True


def test_webui_only_counts_use_per_profile_window_then_global_cap(env):
    """``cli_count`` is the sum of each profile's 20-row projection window;
    ``cli_session_count`` is the global post-cap (20) count."""
    make_state_db(env.home / "state.db",
                  [cli_session(f"cli-d{i:02d}", 1000 + i) for i in range(26)])
    make_state_db(env.work / "state.db",
                  [cli_session(f"cli-w{i:02d}", 2000 + i) for i in range(26)])
    env.webui_rows = [env.webui_row("web-1", 5000)]

    body = _get(WEBUI_ONLY)

    assert _ids(body) == ["web-1"]
    assert body["cli_count"] == 40
    assert body["cli_session_count"] == 20
    assert body["webui_session_count"] == 1


def test_project_assigned_background_rows_count_in_cli_count_but_not_rows(env, monkeypatch):
    """Project-pinned cron rows reach ``cli_count`` (pre ``exclude_hidden``) and
    are then dropped from rows and every other count. Skipping the background
    passes would silently change ``cli_count``."""
    monkeypatch.setattr(models, "ensure_cron_project", lambda create=True: "p-cron")
    make_state_db(env.home / "state.db", [
        cli_session("cli-a", 100),
        {"id": "cron_job_1", "source": "cron", "started_at": 250,
         "messages": chat(4, 260), "title": "nightly"},
    ])
    env.webui_rows = [env.webui_row("web-1", 500)]

    body = _get(WEBUI_ONLY)

    assert _ids(body) == ["web-1"]
    assert body["cli_count"] == 2
    assert body["cli_session_count"] == 1
    assert body["webui_session_count"] == 1


# ── orphan reconciliation ──────────────────────────────────────────────────

def test_orphaned_imported_sidecar_is_pruned_but_window_evicted_one_is_kept(env):
    """#3238: a sidecar whose backing agent row is gone is pruned; one that is
    merely older than the visible CLI window must be kept (probed, not assumed
    deleted from cli_by_id absence)."""
    make_state_db(env.home / "state.db",
                  [cli_session(f"cli-{i:02d}", 1000 + i) for i in range(30)])
    env.webui_rows = [
        env.webui_row("web-1", 5000),
        env.imported_sidecar("gone-from-agent", 4000),   # no state.db row
        env.imported_sidecar("cli-00", 1000),            # oldest real row
    ]

    body = _get("/api/sessions?sidebar_source=webui&exclude_hidden=1")  # active profile

    assert env.pruned == ["gone-from-agent"]
    assert _ids(body) == ["web-1"]
    assert body["cli_session_count"] <= 20
    assert "gone-from-agent" not in _ids(_get("/api/sessions?exclude_hidden=1"))


# ── live external updates ──────────────────────────────────────────────────

def _wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_external_active_profile_write_is_visible_on_next_request(env):
    make_state_db(env.home / "state.db", [cli_session("cli-a", 100)])
    env.webui_rows = [env.webui_row("web-1", 500)]
    assert _get(WEBUI_ONLY)["cli_session_count"] == 1

    conn = sqlite3.connect(str(env.home / "state.db"))
    add_session(conn, cli_session("cli-b", 200))
    conn.commit()
    conn.close()

    body = _get(WEBUI_ONLY)   # no cache clear, no sleep
    assert body["cli_count"] == 2
    assert body["cli_session_count"] == 2


def test_external_other_profile_write_converges_after_cache_ttl(env, monkeypatch):
    """The route cache stamp watches the *active* profile's state.db, so a write
    to another profile surfaces via TTL + background rebuild (stale-while-
    revalidate). Pin that it converges and never needs a manual clear."""
    monkeypatch.setattr(route_cache, "_SESSIONS_CACHE_TTL_SECONDS", 0.0)
    make_state_db(env.home / "state.db", [cli_session("cli-a", 100)])
    make_state_db(env.work / "state.db", [cli_session("cli-w", 120)])
    env.webui_rows = [env.webui_row("web-1", 500)]
    assert _get(WEBUI_ONLY)["cli_session_count"] == 2

    conn = sqlite3.connect(str(env.work / "state.db"))
    add_session(conn, cli_session("cli-w-new", 300))
    conn.commit()
    conn.close()

    assert _wait_for(lambda: _get(WEBUI_ONLY)["cli_session_count"] == 3)


# ── ordering exactness (candidate window) ──────────────────────────────────

def _many_recent(n, base):
    return [cli_session(f"recent-{i:03d}", base + i) for i in range(n)]


def test_resumed_old_session_displaces_recent_ones_in_bounded_window(tmp_path):
    """An old session resumed *now* belongs at the top of the bounded window
    even when its ``started_at`` is older than the whole oversampled candidate
    set and ``last_activity_at`` is stale / NULL."""
    for stale in (None, 150.0):                     # NULL and stale-but-set
        db = tmp_path / f"state-{stale}.db"
        sessions = _many_recent(40, 10_000)          # limit=2 -> 16 candidates
        sessions.append({
            "id": "old-resumed", "source": "cli", "started_at": 100.0,
            "last_activity_at": stale,
            "messages": chat(4, 100.0 + 1) + chat(2, 90_000.0),  # resumed late
        })
        make_state_db(db, sessions)

        rows = agent_sessions.read_importable_agent_session_rows(db, limit=2)

        assert rows[0]["id"] == "old-resumed", stale
        assert rows[0]["last_activity"] == 90_000.0
        assert rows[1]["id"] == "recent-039"


def test_last_activity_at_never_overrides_message_timestamps(tmp_path):
    """Idle sessions carrying a *higher* stale ``last_activity_at`` must not
    crowd genuinely recent sessions out of the bounded candidate window (the
    column is not the ordering authority). 20 flagged sessions > the 16-slot
    window for ``limit=2``."""
    db = tmp_path / "state.db"
    sessions = _many_recent(40, 10_000)
    sessions += [
        {"id": f"idle-flagged-{i:02d}", "source": "cli", "started_at": 100.0 + i,
         "last_activity_at": 999_999.0 + i,           # bogus: no such message
         "messages": chat(4, 101.0 + i)}
        for i in range(20)
    ]
    make_state_db(db, sessions)

    rows = agent_sessions.read_importable_agent_session_rows(db, limit=2)

    assert [r["id"] for r in rows] == ["recent-039", "recent-038"]


def test_resumed_old_session_tops_the_endpoint_window(env):
    """Active-profile request (limit 20 -> 160 candidates): 170 newer-started
    sessions fill the whole oversampled window by ``started_at``, so only the
    authoritative message ordering can surface the resumed old session."""
    sessions = _many_recent(170, 10_000)
    sessions.append({"id": "old-resumed", "source": "cli", "started_at": 100.0,
                     "last_activity_at": 120.0,
                     "messages": chat(4, 101.0) + chat(2, 90_000.0)})
    make_state_db(env.home / "state.db", sessions)
    env.webui_rows = [env.webui_row("web-1", 5)]

    body = _get("/api/sessions?exclude_hidden=1")

    cli_ids = [i for i in _ids(body) if not i.startswith("web-")]
    assert cli_ids[0] == "old-resumed"


# ── compression lineage / legacy schema ────────────────────────────────────

def test_compression_chain_collapses_and_surfaces_tip_activity(tmp_path):
    db = tmp_path / "state.db"
    make_state_db(db, [
        {"id": "root", "source": "cli", "started_at": 100.0,
         "end_reason": "compression", "messages": chat(4, 110.0)},
        {"id": "tip", "source": "cli", "started_at": 200.0, "parent": "root",
         "title": "tip", "messages": chat(4, 80_000.0)},
        *_many_recent(10, 10_000),
    ])

    rows = agent_sessions.read_importable_agent_session_rows(db, limit=3)

    assert rows[0]["id"] == "tip"                    # navigation points at the tip
    assert rows[0]["_lineage_root_id"] == "root"
    assert rows[0]["last_activity"] == 80_000.0
    assert "root" not in [r["id"] for r in rows]


@pytest.mark.parametrize("kwargs", [
    {"last_activity_at": False},                                  # pre-column Agent
    {"messages_timestamp": False},                                # no messages.timestamp
    {"messages_table": False},                                    # no messages table
], ids=["no-last_activity_at", "no-messages-timestamp", "no-messages-table"])
def test_legacy_schemas_still_project_rows(tmp_path, kwargs):
    db = tmp_path / "state.db"
    make_state_db(db, [
        {"id": "a", "source": "cli", "started_at": 100.0, "messages": chat(4, 110.0)},
        {"id": "b", "source": "cli", "started_at": 200.0, "messages": chat(4, 210.0)},
    ], **kwargs)
    # Without a messages table the projection falls back to the denormalized
    # sessions.message_count.
    if kwargs.get("messages_table") is False:
        conn = sqlite3.connect(str(db))
        conn.execute("UPDATE sessions SET message_count = 4")
        conn.commit()
        conn.close()

    for limit in (2, None):                          # bounded + unbounded paths
        rows = agent_sessions.read_importable_agent_session_rows(db, limit=limit)
        assert {r["id"] for r in rows} == {"a", "b"}
        assert [r["id"] for r in rows][0] == "b"


# ── projection call / SQL shape (observable, not source strings) ───────────

def _agent_statements(env):
    return [s for s in env.sql if "FROM sessions" in s and "messages" in s]


def test_bounded_projection_limits_the_messages_aggregate(env):
    """Active-profile request: the messages aggregate is joined only for the
    oversampled candidate window, never for every session."""
    make_state_db(env.home / "state.db", _many_recent(40, 10_000))
    env.webui_rows = [env.webui_row("web-1", 5)]

    _get("/api/sessions?sidebar_source=webui&exclude_hidden=1")

    stmts = _agent_statements(env)
    assert stmts, "projection never ran"
    assert all("candidates" in s and "LIMIT" in s.upper() for s in stmts)
    # limit 20 * oversample 8
    assert any("LIMIT 160" in s for s in stmts)


def test_all_profiles_projection_runs_once_per_request_and_is_route_cached(env):
    """Documents what a WebUI-only all-profiles request pays today: a single
    ``get_cli_sessions(all_profiles=True)`` call, one bounded (LIMIT 160) main
    pass per profile DB, and unbounded passes only for the background sources
    (cron/webhook/kanban). A second request is served from the route cache."""
    make_state_db(env.home / "state.db", _many_recent(10, 10_000))
    make_state_db(env.work / "state.db", _many_recent(10, 20_000))
    env.webui_rows = [env.webui_row("web-1", 5)]

    _get(WEBUI_ONLY)

    assert len(env.cli_calls) == 1
    assert env.cli_calls[0]["all_profiles"] is True
    stmts = _agent_statements(env)
    bounded = [s for s in stmts if "candidates" in s and "LIMIT 160" in s]
    unbounded = [s for s in stmts if "candidates" not in s and "GROUP BY s.id" in s]
    assert len(bounded) == 2                                   # one per profile DB
    assert unbounded and all(
        any(f"s.source IN ('{src}')" in s for src in ("cron", "webhook", "kanban"))
        for s in unbounded
    )

    env.sql.clear()
    _get(WEBUI_ONLY)                                           # route cache hit
    assert not _agent_statements(env)
    assert len(env.cli_calls) == 1


# ── per-profile projection reuse (all_profiles aggregate miss) ─────────────
#
# ``get_cli_sessions(all_profiles=True)`` is keyed on EVERY profile's SQLite
# fingerprint, so one profile's commit used to re-run the loader for all of
# them. These tests pin the work-reuse invariant (only the changed profile is
# recomputed) together with exactness against an uncached reference loop, for
# every input that can change a profile's rows.

def _two_profiles(env):
    make_state_db(env.home / "state.db",
                  [cli_session("cli-a1", 100), cli_session("cli-a2", 200)])
    make_state_db(env.work / "state.db",
                  [cli_session("cli-b1", 150), cli_session("cli-b2", 250)])
    env.webui_rows = [env.webui_row("web-1", 900)]


def _strip(body):
    return {k: v for k, v in body.items() if k != "server_time"}


def _cold(env, url):
    """Response with every cache (route, aggregate, per-profile) dropped."""
    env.clear()
    return _get(url)


def test_commit_in_one_profile_recomputes_only_that_profile(env):
    """The reported invariant: warm both profiles, commit a message in B, and
    the next WebUI-only all-profiles GET has identical JSON/counts to a cold
    rebuild but recomputes B only (no loader call for A)."""
    _two_profiles(env)
    first = _get(WEBUI_ONLY)
    assert sorted(env.loaded()) == ["default", "work"]            # cold: both

    env.loader_calls.clear()
    env.commit_message(env.work, "cli-b1", 9_000.0)
    routes._session_list_cache_clear()    # route-cache expiry; CLI caches intact
    second = _get(WEBUI_ONLY)

    assert env.loaded() == ["work"]
    assert env.loaded("default") == []
    assert _strip(second) == _strip(_cold(env, WEBUI_ONLY))
    for key in COUNT_KEYS:
        assert second[key] == first[key], key
    # ...and the refreshed B row is what the all-sources view serves.
    env.loader_calls.clear()
    everything = _get(ALL_SOURCES)
    assert env.loaded() == []                                      # aggregate hit
    assert _strip(everything) == _strip(_cold(env, ALL_SOURCES))
    cli_b1 = next(r for r in everything["sessions"] if r["session_id"] == "cli-b1")
    assert cli_b1["updated_at"] == 9_000.0


def test_commit_in_active_profile_recomputes_only_the_active_profile(env):
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)
    env.loader_calls.clear()
    env.commit_message(env.home, "cli-a1", 9_000.0)

    rows = models.get_cli_sessions(all_profiles=True)

    assert env.loaded() == ["default"]
    assert rows == env.reference()


def test_unchanged_state_serves_aggregate_without_any_loader_call(env):
    _two_profiles(env)
    first = models.get_cli_sessions(all_profiles=True)
    env.loader_calls.clear()
    assert models.get_cli_sessions(all_profiles=True) == first
    assert env.loaded() == []


def test_active_profile_reorder_reassigns_claude_code_rows_exactly(env, monkeypatch):
    """Claude Code rows are emitted by the FIRST context only, so a profile's
    projection depends on its position. Switching the active profile must not
    reuse an entry built under the other assignment."""
    cc = {"session_id": "claude_code_x", "title": "cc", "profile": None,
          "source_tag": "claude_code", "raw_source": "claude_code",
          "is_cli_session": True, "updated_at": 5.0, "message_count": 3}
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda *a, **k: [dict(cc)])
    _two_profiles(env)
    make_state_db(env.home / "profiles" / "extra" / "state.db", [cli_session("cli-x1", 300)])
    env.profile_names = ["default", "work", "extra"]

    before = models.get_cli_sessions(all_profiles=True)
    assert before == env.reference()
    assert sum(r["session_id"] == "claude_code_x" for r in before) == 1

    env.active = "work"                       # order becomes work, default, extra
    env.loader_calls.clear()
    after = models.get_cli_sessions(all_profiles=True)

    assert after == env.reference()
    assert sum(r["session_id"] == "claude_code_x" for r in after) == 1
    # work gains Claude Code, default loses it. cron/webhook project ids are
    # resolved for the ACTIVE profile, so an active-profile switch is part of
    # every entry's identity and nothing is reused (conservative, not exact-
    # by-accident).
    assert sorted(env.loaded()) == ["default", "extra", "work"]
    cc_calls = {c["profile"]: c["include_claude_code"] for c in env.loader_calls}
    assert cc_calls == {"work": True, "default": False, "extra": False}


def test_claude_code_transcript_edit_refreshes_the_first_profile_entry(env, monkeypatch, tmp_path):
    """Claude Code rows ride in the first profile's entry, and the aggregate key
    only watches the projects *directory* stat; an in-place transcript append
    must still refresh that entry when another profile forces a rebuild."""
    projects = tmp_path / "claude"
    (projects / "proj").mkdir(parents=True)
    transcript = projects / "proj" / "t.jsonl"

    def line(role, text, ts):
        return json.dumps({"type": role, "message": {"role": role, "content": text},
                           "timestamp": ts}) + "\n"

    transcript.write_text(line("user", "hello", 1000.0), encoding="utf-8")
    monkeypatch.setenv("HERMES_WEBUI_CLAUDE_PROJECTS_DIR", str(projects))
    monkeypatch.setattr(models, "get_claude_code_sessions", env.real_claude_code_sessions)
    _two_profiles(env)

    def claude_rows():
        return [r for r in models.get_cli_sessions(all_profiles=True)
                if r.get("raw_source") == "claude_code"]

    assert [r["message_count"] for r in claude_rows()] == [1]
    with transcript.open("a", encoding="utf-8") as fh:            # dir stat unchanged
        fh.write(line("assistant", "hi", 1001.0))
    env.commit_message(env.work, "cli-b1", 9_000.0)               # aggregate miss
    env.loader_calls.clear()

    assert [r["message_count"] for r in claude_rows()] == [2]
    assert models.get_cli_sessions(all_profiles=True) == env.reference()
    assert sorted(env.loaded()) == ["default", "work"]


def test_source_filter_variants_never_alias_each_other(env):
    _two_profiles(env)
    conn = sqlite3.connect(str(env.work / "state.db"))
    add_session(conn, {"id": "tg-1", "source": "telegram", "started_at": 400.0,
                       "messages": chat(4, 410.0)})
    conn.commit()
    conn.close()

    for sf in (None, "telegram", "cli", None):
        assert models.get_cli_sessions(sf, all_profiles=True) == env.reference(sf), sf

    env.loader_calls.clear()
    env.commit_message(env.work, "cli-b1", 9_000.0)
    for sf in ("cli", None, "telegram"):
        assert models.get_cli_sessions(sf, all_profiles=True) == env.reference(sf), sf
    # one recompute of B per source filter; A is reused for every variant
    assert env.loaded("default") == []
    assert [c["source_filter"] for c in env.loader_calls] == ["cli", None, "telegram"]


@pytest.mark.parametrize("mode", ["index-touch", "explicit-clear"])
def test_sidecar_title_rename_archive_edits_are_never_served_stale(env, mode):
    """Sidecars are shared, UI-owned inputs of every profile's rows. Either the
    session index moves (what a sidecar save does) or the in-app mutation path
    clears the cache explicitly; nothing may be reused across that edit."""
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)
    env.loader_calls.clear()

    (env.session_dir / "cli-b1.json").write_text(json.dumps({
        "session_id": "cli-b1", "title": "Renamed by user", "archived": True,
        "messages": [], "created_at": 1.0, "updated_at": 2.0}), encoding="utf-8")
    if mode == "index-touch":
        (env.session_dir / "_index.json").write_text(json.dumps([{"session_id": "cli-b1"}]))
    else:
        models.clear_cli_sessions_cache()

    rows = models.get_cli_sessions(all_profiles=True)

    renamed = next(r for r in rows if r["session_id"] == "cli-b1")
    assert renamed["title"] == "Renamed by user" and renamed["archived"] is True
    assert rows == env.reference()
    assert sorted(env.loaded()) == ["default", "work"]            # nothing reused


def test_global_invalidation_drops_every_profile_entry(env):
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)
    env.loader_calls.clear()

    models.clear_cli_sessions_cache()
    models.get_cli_sessions(all_profiles=True)
    assert sorted(env.loaded()) == ["default", "work"]

    env.loader_calls.clear()
    models.get_cli_sessions(all_profiles=True)                    # warm again
    assert env.loaded() == []


def test_entries_expire_with_the_cache_ttl(env, monkeypatch):
    monkeypatch.setattr(models, "_CLI_SESSIONS_CACHE_TTL_SECONDS", 0.2)
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)
    env.loader_calls.clear()

    models.get_cli_sessions(all_profiles=True)
    assert env.loaded() == []                                      # inside TTL

    time.sleep(0.35)
    rows = models.get_cli_sessions(all_profiles=True)
    assert sorted(env.loaded()) == ["default", "work"]            # TTL is a hard bound
    assert rows == env.reference()


def test_global_workspace_change_is_not_served_from_a_profile_entry(env, monkeypatch):
    workspace = {"v": "/workspace/one"}
    monkeypatch.setattr(models, "get_last_workspace", lambda: workspace["v"])
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)

    workspace["v"] = "/workspace/two"
    env.commit_message(env.work, "cli-b1", 9_000.0)               # force an aggregate miss
    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)

    assert rows == env.reference()
    assert {r["workspace"] for r in rows} == {"/workspace/two"}
    assert sorted(env.loaded()) == ["default", "work"]


def test_projects_file_change_is_not_served_from_a_profile_entry(env, monkeypatch):
    """cron/webhook project ids are resolved from projects.json (for the ACTIVE
    profile) while projecting every profile's rows."""
    pid = {"v": "p-one"}
    monkeypatch.setattr(models, "ensure_cron_project", lambda create=True: pid["v"])
    make_state_db(env.home / "state.db", [
        {"id": "cron_job_1", "source": "cron", "started_at": 250,
         "messages": chat(4, 260), "title": "nightly"}])
    make_state_db(env.work / "state.db", [cli_session("cli-b1", 150)])
    models.get_cli_sessions(all_profiles=True)

    pid["v"] = "p-two"
    models.PROJECTS_FILE.write_text(json.dumps([{"project_id": "p-two"}]))
    env.commit_message(env.work, "cli-b1", 9_000.0)
    rows = models.get_cli_sessions(all_profiles=True)

    assert rows == env.reference()
    assert next(r for r in rows if r["session_id"] == "cron_job_1")["project_id"] == "p-two"


@pytest.mark.parametrize("schema", [
    {"last_activity_at": False},
    {"messages_table": False},
], ids=["no-last_activity_at", "no-messages-table"])
def test_legacy_schema_profile_is_reused_and_refreshed_exactly(env, schema):
    make_state_db(env.home / "state.db", [cli_session("cli-a1", 100)])
    make_state_db(env.work / "state.db", [
        {"id": "cli-b1", "source": "cli", "started_at": 150.0,
         "messages": chat(4, 160.0)}], **schema)
    for _ in range(2):
        assert models.get_cli_sessions(all_profiles=True) == env.reference()

    conn = sqlite3.connect(str(env.work / "state.db"))
    add_session(conn, {"id": "cli-b-new", "source": "cli", "started_at": 400.0,
                       "messages": chat(4, 410.0)}, **schema)
    conn.commit()
    conn.close()
    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)

    assert "cli-b-new" in [r["session_id"] for r in rows]
    assert rows == env.reference()
    assert env.loaded() == ["work"]


def test_profile_without_state_db_gains_one_later(env):
    make_state_db(env.home / "state.db", [cli_session("cli-a1", 100)])
    assert models.get_cli_sessions(all_profiles=True) == env.reference()

    env.loader_calls.clear()
    make_state_db(env.work / "state.db", [cli_session("cli-b1", 150)])
    rows = models.get_cli_sessions(all_profiles=True)

    assert rows == env.reference()
    assert "cli-b1" in [r["session_id"] for r in rows]
    assert env.loaded() == ["work"]


def test_profile_delete_then_add_reuses_the_survivor_and_loads_the_new_one(env):
    _two_profiles(env)
    assert models.get_cli_sessions(all_profiles=True) == env.reference()

    shutil.rmtree(env.work)
    env.profile_names = ["default"]
    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)
    assert rows == env.reference()
    assert not any(r["session_id"].startswith("cli-b") for r in rows)   # no ghost rows
    assert env.loaded() == []

    make_state_db(env.work / "state.db", [cli_session("cli-b9", 500)])
    env.profile_names = ["default", "work"]
    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)
    assert rows == env.reference()
    assert [r["session_id"] for r in rows if r["profile"] == "work"] == ["cli-b9"]
    assert env.loaded() == ["work"]


def test_overlapping_rebuilds_share_one_load_per_profile(env):
    """Two requests with different aggregate keys (Claude Code on/off) overlap
    while profile B is loading; B is computed once and shared. The profile that
    receives Claude Code rows (A, first context) is keyed separately."""
    _two_profiles(env)
    entered, release = threading.Event(), threading.Event()

    def hook(profile):
        if profile == "work":
            entered.set()
            release.wait(10)

    env.loader_hook = hook
    results = {}

    def run(flag):
        results[flag] = models.get_cli_sessions(all_profiles=True, include_claude_code=flag)

    t1 = threading.Thread(target=run, args=(True,))
    t1.start()
    assert entered.wait(5)
    t2 = threading.Thread(target=run, args=(False,))
    t2.start()
    assert _wait_for(lambda: len(env.loaded("default")) == 2)     # both reached stage 2
    time.sleep(0.3)
    release.set()
    t1.join(10)
    t2.join(10)

    assert len(env.loaded("work")) == 1
    assert results[True] == env.reference(include_claude_code=True)
    assert results[False] == env.reference(include_claude_code=False)


def test_invalidation_during_a_rebuild_is_never_cached(env):
    _two_profiles(env)
    entered, release = threading.Event(), threading.Event()

    def hook(profile):
        if profile == "work" and not entered.is_set():
            entered.set()
            release.wait(10)

    env.loader_hook = hook
    out = {}
    t = threading.Thread(
        target=lambda: out.setdefault("rows", models.get_cli_sessions(all_profiles=True)))
    t.start()
    assert entered.wait(5)
    models.clear_cli_sessions_cache()                              # mid-rebuild
    release.set()
    t.join(10)

    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)
    assert sorted(env.loaded()) == ["default", "work"]            # nothing survived
    assert rows == env.reference()


def test_failed_profile_load_is_not_cached_and_keeps_the_empty_contract(env):
    _two_profiles(env)
    boom = {"on": True}

    def hook(profile):
        if profile == "work" and boom["on"]:
            raise sqlite3.OperationalError("database is locked")

    env.loader_hook = hook
    assert models.get_cli_sessions(all_profiles=True) == []       # existing contract
    boom["on"] = False
    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)

    assert rows == env.reference() and rows
    assert "work" in env.loaded()                                  # failure not memoized


def test_profile_entries_are_lru_bounded(env, monkeypatch):
    monkeypatch.setattr(models, "_CLI_PROFILE_PROJECTION_CACHE_MAX_ENTRIES", 2)
    _two_profiles(env)
    make_state_db(env.home / "profiles" / "extra" / "state.db", [cli_session("cli-x1", 300)])
    env.profile_names = ["default", "work", "extra"]

    assert models.get_cli_sessions(all_profiles=True) == env.reference()

    assert len(models._CLI_PROFILE_PROJECTION_CACHE) <= 2


def test_sidecar_edit_without_index_or_clear_is_not_served_stale(env):
    """A sidecar title/archived edit that moves neither the session index nor
    the invalidation generation (another process, a skip_index save) must still
    show up when another profile's commit forces an aggregate rebuild; the
    unchanged profile may only be reused while the sidecars it read are
    unchanged."""
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)
    assert not (env.session_dir / "_index.json").exists()

    (env.session_dir / "cli-a1.json").write_text(json.dumps({
        "session_id": "cli-a1", "title": "Edited elsewhere", "archived": True,
        "messages": [], "created_at": 1.0, "updated_at": 2.0}), encoding="utf-8")
    env.commit_message(env.work, "cli-b1", 9_000.0)               # aggregate miss
    env.loader_calls.clear()

    rows = models.get_cli_sessions(all_profiles=True)

    edited = next(r for r in rows if r["session_id"] == "cli-a1")
    assert edited["title"] == "Edited elsewhere" and edited["archived"] is True
    assert rows == env.reference()
    assert sorted(env.loaded()) == ["default", "work"]


def test_sidecar_edit_for_a_session_no_profile_projects_does_not_force_a_reload(env):
    _two_profiles(env)
    models.get_cli_sessions(all_profiles=True)

    (env.session_dir / "unrelated.json").write_text(json.dumps({
        "session_id": "unrelated", "title": "x", "messages": []}), encoding="utf-8")
    env.commit_message(env.work, "cli-b1", 9_000.0)
    env.loader_calls.clear()

    assert models.get_cli_sessions(all_profiles=True) == env.reference()
    assert env.loaded() == ["work"]


def test_claude_code_stamp_walk_is_capped(env, monkeypatch, tmp_path):
    projects = tmp_path / "claude"
    (projects / "proj").mkdir(parents=True)
    for i in range(6):
        (projects / "proj" / f"t{i}.jsonl").write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_WEBUI_CLAUDE_PROJECTS_DIR", str(projects))
    monkeypatch.setattr(models, "CLAUDE_CODE_MAX_FILES", 3)

    assert len(models._claude_code_projection_stamp()[2]) == 3


def _claude_line(role, text, ts):
    return json.dumps({"type": role, "message": {"role": role, "content": text},
                       "timestamp": ts}) + "\n"


def _count_iterator(monkeypatch):
    calls = []
    real = models._iter_claude_code_jsonl_files

    def counting(*a, **k):
        calls.append(1)
        return real(*a, **k)

    monkeypatch.setattr(models, "_iter_claude_code_jsonl_files", counting)
    return calls


def test_cold_all_profiles_load_walks_claude_code_once(env, monkeypatch, tmp_path):
    projects = tmp_path / "claude"
    (projects / "proj").mkdir(parents=True)
    transcript = projects / "proj" / "t.jsonl"
    transcript.write_text(_claude_line("user", "hello", 1000.0), encoding="utf-8")
    monkeypatch.setenv("HERMES_WEBUI_CLAUDE_PROJECTS_DIR", str(projects))
    monkeypatch.setattr(models, "get_claude_code_sessions", env.real_claude_code_sessions)
    _two_profiles(env)
    calls = _count_iterator(monkeypatch)

    rows = [r for r in models.get_cli_sessions(all_profiles=True)
            if r.get("raw_source") == "claude_code"]
    assert len(calls) == 1
    assert [r["message_count"] for r in rows] == [1]

    # Edit in place (dir stat unchanged) + forced aggregate miss: still one walk
    # per rebuild, and the edit is visible.
    with transcript.open("a", encoding="utf-8") as fh:
        fh.write(_claude_line("assistant", "hi", 1001.0))
    env.commit_message(env.work, "cli-b1", 9_000.0)
    calls.clear()
    rows = [r for r in models.get_cli_sessions(all_profiles=True)
            if r.get("raw_source") == "claude_code"]
    assert len(calls) == 1
    assert [r["message_count"] for r in rows] == [2]

    # Added file: selection is re-read, new row appears, still one walk.
    (projects / "proj" / "u.jsonl").write_text(
        _claude_line("user", "new", 2000.0), encoding="utf-8")
    env.commit_message(env.work, "cli-b2", 9_100.0)
    calls.clear()
    rows = [r for r in models.get_cli_sessions(all_profiles=True)
            if r.get("raw_source") == "claude_code"]
    assert len(calls) == 1
    assert sorted(r["message_count"] for r in rows) == [1, 2]


def test_disabled_claude_code_never_walks_the_projects_dir(env, monkeypatch, tmp_path):
    projects = tmp_path / "claude"
    (projects / "proj").mkdir(parents=True)
    (projects / "proj" / "t.jsonl").write_text(
        _claude_line("user", "hello", 1000.0), encoding="utf-8")
    monkeypatch.setenv("HERMES_WEBUI_CLAUDE_PROJECTS_DIR", str(projects))
    monkeypatch.setattr(models, "get_claude_code_sessions", env.real_claude_code_sessions)
    _two_profiles(env)
    calls = _count_iterator(monkeypatch)

    rows = models.get_cli_sessions(all_profiles=True, include_claude_code=False)
    assert calls == []
    assert not [r for r in rows if r.get("raw_source") == "claude_code"]


def test_commit_landing_during_a_profile_load_is_picked_up_next_time(env):
    """The entry is keyed on the stamp taken BEFORE the load, so a commit that
    lands mid-load can make the stored rows newer than their key but never
    older: the next request sees a new stamp and recomputes only that profile."""
    _two_profiles(env)
    fired = {"done": False}

    def hook(profile):
        if profile == "work" and not fired["done"]:
            fired["done"] = True
            env.commit_message(env.work, "cli-b1", 9_000.0)

    env.loader_hook = hook
    models.get_cli_sessions(all_profiles=True)
    env.loader_hook = None
    env.loader_calls.clear()

    rows = models.get_cli_sessions(all_profiles=True)

    assert rows == env.reference()
    assert env.loaded() == ["work"]


def test_streaming_freeze_still_holds_the_aggregate_and_entries_use_real_stamps(env, monkeypatch):
    _two_profiles(env)
    frozen = {"on": True}
    monkeypatch.setattr(models, "_cli_sessions_streaming_freeze_marker",
                        lambda: ("streaming", ("turn-1",)) if frozen["on"] else None)
    models.get_cli_sessions(all_profiles=True)            # warm while a turn streams

    env.loader_calls.clear()
    env.commit_message(env.work, "cli-b1", 9_000.0)
    first = models.get_cli_sessions(all_profiles=True)   # frozen key: existing behavior
    assert env.loaded() == []

    frozen["on"] = False
    env.loader_calls.clear()
    rows = models.get_cli_sessions(all_profiles=True)

    assert rows == env.reference()
    assert env.loaded() == ["work"]                       # real stamps: default reused
    assert first != rows


def _frozen_marker(monkeypatch):
    """A streaming marker that stays constant for the whole test."""
    monkeypatch.setattr(models, "_cli_sessions_streaming_freeze_marker",
                        lambda: ("streaming", ("turn-1",)))


def test_streaming_freeze_keeps_active_profile_order_and_claude_first_assignment(env, monkeypatch):
    """Only the volatile SQLite stamps are frozen while streaming. Switching the
    active profile under a CONSTANT marker reorders contexts and moves Claude
    Code rows, so the old aggregate must not be served."""
    cc = {"session_id": "claude_code_x", "title": "cc", "profile": None,
          "source_tag": "claude_code", "raw_source": "claude_code",
          "is_cli_session": True, "updated_at": 5.0, "message_count": 3}
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda *a, **k: [dict(cc)])
    monkeypatch.setattr(routes, "load_settings", lambda: {
        "show_cli_sessions": True, "show_claude_code_sessions": True,
        "show_previous_messaging_sessions": False, "show_cron_sessions": False})
    _two_profiles(env)
    _frozen_marker(monkeypatch)

    _get(ALL_SOURCES)
    assert models.get_cli_sessions(all_profiles=True) == env.reference()

    env.active = "work"                       # marker unchanged
    env.loader_calls.clear()
    routes._session_list_cache_clear()        # route layer only; CLI aggregate stays warm
    after = models.get_cli_sessions(all_profiles=True)

    assert after == env.reference()
    assert sum(r["session_id"] == "claude_code_x" for r in after) == 1
    assert {c["profile"]: c["include_claude_code"] for c in env.loader_calls} \
        .get("work") is True
    body = _get(ALL_SOURCES)
    assert sum(r["session_id"] == "claude_code_x" for r in body["sessions"]) == 1


def test_streaming_freeze_sees_profile_add_and_remove(env, monkeypatch):
    _two_profiles(env)
    _frozen_marker(monkeypatch)
    models.get_cli_sessions(all_profiles=True)

    make_state_db(env.home / "profiles" / "extra" / "state.db", [cli_session("cli-x1", 300)])
    env.profile_names = ["default", "work", "extra"]
    routes._session_list_cache_clear()
    added = _get(ALL_SOURCES)
    assert "cli-x1" in _ids(added)
    assert models.get_cli_sessions(all_profiles=True) == env.reference()

    shutil.rmtree(env.home / "profiles" / "extra")
    env.profile_names = ["default", "work"]
    routes._session_list_cache_clear()
    removed = _get(ALL_SOURCES)
    assert "cli-x1" not in _ids(removed)
    assert models.get_cli_sessions(all_profiles=True) == env.reference()


def test_streaming_freeze_still_ignores_message_writes_under_constant_marker(env, monkeypatch):
    _two_profiles(env)
    _frozen_marker(monkeypatch)
    first = models.get_cli_sessions(all_profiles=True)
    env.loader_calls.clear()
    env.commit_message(env.work, "cli-b1", 9_000.0)
    assert models.get_cli_sessions(all_profiles=True) == first
    assert env.loaded() == []
