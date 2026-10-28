# Sidebar projection latency: implementation notes

Status:

- **Implemented in this PR (not merged/deployed):** exact per-profile projection entries beneath the all-profiles
  `get_cli_sessions` aggregate cache (`api/models.py`), so one profile's
  `state.db` commit no longer recomputes every profile.
- **Still blocked (unchanged):** skipping the CLI projection for WebUI-only
  requests, and ordering the candidate window by `sessions.last_activity_at`.
  Neither can be proven semantically exact from the WebUI side; see the two
  "still blocked" sections below. The per-profile cache relies on neither.

`tests/test_sidebar_projection_exactness.py` pins the response/ordering
semantics any optimization must preserve and the work-reuse invariant of the
per-profile cache.

State layers: reads the Agent `state.db` (`sessions`, `messages`), the WebUI
session sidecars/index, `projects.json` and Claude Code transcripts; mutates
only in-process cache state (`_CLI_PROFILE_PROJECTION_CACHE` and the shared
in-flight map). It writes nothing to disk.

## Symptom

`GET /api/sessions?sidebar_source=webui&exclude_hidden=1&all_profiles=1` with
`show_cli_sessions` enabled still builds the full CLI/state.db projection
(`get_cli_sessions(all_profiles=True)` in
`_build_session_list_cache_payload`), and on large Agent stores the rebuild is
dominated by that stage. The route cache and the CLI-sessions cache already
exist; the cost is paid on every miss (any state.db commit outside the
streaming freeze).

## What the projection actually costs (verified by `test_all_profiles_projection_runs_once_per_request_and_is_route_cached`)

Per profile `state.db`, `_load_cli_sessions_uncached` runs:

1. a **bounded** main pass: `visible_session_limit=None` falls back to
   `CLI_VISIBLE_SESSION_LIMIT` (20), so `read_importable_agent_session_rows`
   builds a `LIMIT 160` candidate CTE ordered by
   `COALESCE((SELECT MAX(timestamp) FROM messages WHERE session_id = s.id), started_at)`;
   the correlated `MAX` is evaluated for **every** session before the limit;
2. three **unbounded** background-source passes (cron, webhook, kanban).

## Implemented in this PR: per-profile projection entries

### Problem

`get_cli_sessions(all_profiles=True)` keys its aggregate entry on every
profile's SQLite fingerprint. On an aggregate miss `_load_sessions()` looped all
contexts and ran `_load_cli_sessions_uncached` for **every** profile, even when
only one profile had changed (the usual case: one profile is running an agent).

### Change

On an aggregate miss, `_load_all_profiles_cli_sessions` asks
`_cli_profile_projection_cached` for each profile's rows and concatenates them
in the existing context order (active profile first, then the profile list).
Only profiles whose entry is missing, expired, keyed differently, or whose
sidecars changed are recomputed. The aggregate cache, the route cache and the
response are unchanged; callers still only receive the aggregate.

### Cache identity (`_cli_profile_projection_cache_key`)

| Input | Source |
| --- | --- |
| resolved profile home, `state.db` path, profile name | the context |
| the profile's own SQLite stamp | the same `(home, profile, fingerprint + file stats)` entry the aggregate key embeds, taken before the load; always the real stamp, even when the aggregate key is swapped for the streaming-freeze marker |
| `source_filter` | argument |
| the `include_claude_code` value handed to **this** context (true only for the first context) | an active-profile reorder moves Claude Code rows to a different key |
| Claude Code transcript signature `(path, mtime_ns, size, ctime_ns)`, only for the context that receives Claude Code rows | the aggregate key only watches the projects *directory* stat, so an in-place append would otherwise be reused |
| `cron/jobs.json` stat of that home | cron titles |
| shared stamp: sidecar dir, session-index stat, deleted-WebUI tombstone stat, `projects.json` path+stat, **active profile name**, `get_last_workspace()` | cron/webhook project ids are resolved for the *active* profile; the workspace is echoed on every row |

If a stamp cannot be computed the per-profile cache is bypassed for that call.
Changing any shared input, or switching the active profile, therefore
invalidates every entry (nothing is reused); that is deliberately conservative.

### Sidecar edits that move nothing else

The projection overlays each row's title/archived flag from its WebUI sidecar
(`_state_projection_sidecar_metadata`). Supported in-app writes
(rename/archive/create/...) publish `session_list_changed`, whose listener calls
`clear_cli_sessions_cache()`, and sidecar saves normally move the session
index, so both the generation and the shared stamp already drop every entry. An
edit that does neither (another process, a `skip_index` save) would otherwise
leave a reused entry stale. So each stored entry also carries the stat
signature the loader actually used for every sidecar it consulted (recorded
inside `_state_projection_sidecar_metadata`, so it cannot race the load), and a
reused entry is re-checked against those files: one `stat` per consulted row,
which is the same per-row work the loader already does, without the database
read. A changed sidecar means the entry is recomputed.

Residual: a sidecar created *externally* for a row that the loader skipped
because of the deleted-WebUI tombstone is not in any signature; it is covered
only by the tombstone/index stamps and in-app invalidation.

### Claude Code rows

`get_claude_code_sessions` rows ride in the first context's entry. On each
aggregate rebuild that gives the first context Claude Code rows, the exact
selection (the lexicographically first `CLAUDE_CODE_MAX_FILES` = 200 eligible
JSONLs under `CLAUDE_CODE_MAX_FILE_BYTES`) is materialized **once** and shared:
it builds the per-transcript stat signature in the entry's key and, on a miss,
is handed to the loader (`claude_code_files` -> `get_claude_code_sessions`
`_selected_files`) so the cold load does not walk again. Contexts with Claude
Code disabled never enumerate the projects directory. Selection is exact (no
early entry caps, no approximation); only the *yielded eligible files* are
capped, so directory enumeration and sorting can still scale with the total
number of entries, as in the baseline's single walk. A cache hit still pays
that one walk (needed for the stamp); a miss no longer pays two.

### Invalidation and lifetime

- `_CLI_PROFILE_PROJECTION_CACHE`: LRU bounded by
  `_CLI_PROFILE_PROJECTION_CACHE_MAX_ENTRIES` (32), separate from the aggregate
  LRU so a few profiles cannot evict aggregate entries.
- Shares the aggregate's lock, invalidation generation and TTL
  (`_cli_sessions_cache_ttl_seconds`, including the streaming-widened value).
  `clear_cli_sessions_cache()` empties it and bumps the generation; a load that
  started before a clear is never stored, and a stored entry carries its
  generation and is dropped on read if it moved.
- Single-flight per entry through the existing in-flight map: overlapping
  rebuilds with different aggregate keys (e.g. Claude Code on/off) compute a
  shared profile once. A waiter gives up after
  `_CLI_PROFILE_PROJECTION_WAIT_SECONDS` (30 s) and loads directly.
- Failures are not cached. A raising profile load still propagates to the
  aggregate caller, which keeps the existing contract (warn, serve the stale
  aggregate if valid, else `[]`).
- The key's SQLite stamp is taken before the load, so a commit that lands
  mid-load can make stored rows newer than their key but never older; the next
  request sees a new stamp and recomputes only that profile.

### Staleness bound

Inputs that are in no stamp (content changes under an unchanged
`cron/jobs.json` stat, transcript edits that preserve mtime/size/ctime, the
SQLite stamp's documented gaps such as a title-only `UPDATE` with no message
insert) can be as old as the entry TTL: 5 s, or 45 s while a turn streams. The
aggregate cache already served the same data for the same TTL; this only changes
which request pays the rebuild, not the bound.

### Measured (synthetic)

`tests/test_sidebar_projection_exactness.py` asserts call counts, not time. A
throwaway script (not committed) generated 4 profiles x 3,000 sessions x 12
messages in an isolated directory, committed one message to one profile, and
timed the next aggregate miss on a warm OS page cache (median of 5):

| | loader calls per miss | median miss |
| --- | --- | --- |
| base | 4 | 48 ms |
| per-profile entries | 1 | 18 ms |

Synthetic, small and page-cached: this shows the work reduction (N profiles to
the changed ones), not a production latency guarantee. The profile that did
change costs exactly what it did before.

## Why a WebUI-only request still needs the projection (still blocked)

`sidebar_source=webui` filters *rows* only. The response also carries values
that are defined by the projection, and several WebUI rows depend on it:

- `cli_count` = sum of each profile's projected window plus project-assigned
  background rows (`_include_project_hidden_background_sidebar_sessions`),
  counted **before** `exclude_hidden` drops them from the rows. Pinned by
  `test_project_assigned_background_rows_count_in_cli_count_but_not_rows` and
  `test_webui_only_counts_use_per_profile_window_then_global_cap`.
- `cli_session_count`, `archived_cli_count`, and `webui_session_count` are
  computed after the global CLI cap and the messaging dedupe across **both**
  buckets (`_keep_latest_messaging_session_per_source`).
- WebUI-owned sidecars that share an id with a state.db row get their
  metadata merged and their `is_cli_session` flag recomputed
  (`_merge_cli_sidebar_metadata`), which can move a row between buckets.
- Orphaned imported sidecars (#3238/#4591) are reconciled against the
  projection; skipping it would either leave dangling rows or require a second
  existence probe per row.

Skipping the projection for WebUI-only requests therefore changes public
response values, not just latency. A mutation that returns `cli = []` for
`sidebar_source=webui` fails several tests in the new file (see evidence).
Computing the counts some other way (a cheaper exact count) would need the same
per-row visibility, lineage-collapse, source-normalization, cap and dedupe
rules, i.e. a second implementation of the projection; that is not a focused
change and not provably equivalent.

## Why the indexed `last_activity_at` candidate window is not provably exact (still blocked)

Using `sessions.last_activity_at` (and its index) to pick the 160 candidates is
exact only if, for every session outside the window,
`last_activity_at >= MAX(messages.timestamp)` never understates activity and the
column is populated. The WebUI cannot establish that:

- The column is absent on older Agent schemas and can be NULL for sessions
  written before it existed (operator-reported: a sizable fraction of rows in
  one store). A NULL-only fallback is still an unbounded correlated scan.
- A non-NULL value can be stale. The WebUI's own contract comment
  (`_state_db_target_session_revision`) says supported writers update the
  column but that raw writes bypassing session metadata are outside the writer
  contract. A stale value outside the window silently hides an old session that
  was resumed (`started_at` and `last_activity_at` old, newest `messages`
  timestamp recent).
- No Agent schema revision/marker visible to the WebUI proves the column is
  complete and monotonic for all rows, and verifying it per session costs the
  same indexed `MAX` seek the current query already pays.
- A bounded exact alternative from the `messages` table is not available:
  `idx_messages_session` is `(session_id, timestamp)`, so there is no index to
  walk messages by recency, and message id order is not guaranteed to follow
  timestamp order.

An approximate window was explicitly out of scope, so it was not implemented.
`test_resumed_old_session_displaces_recent_ones_in_bounded_window`,
`test_last_activity_at_never_overrides_message_timestamps` and
`test_resumed_old_session_tops_the_endpoint_window` fail if candidate ordering
is replaced by `COALESCE(last_activity_at, started_at)`.

### What would unblock it

An Agent-owned guarantee the WebUI can read and fail closed on, for example a
schema revision/meta marker asserting that `last_activity_at` is backfilled and
maintained to be `>=` the newest message timestamp for every session. With such
a marker the WebUI could use the indexed window when the marker is present and
fall back to the current correlated `MAX` query on any legacy/unknown schema,
NULL, or error. Until then the existing exact query stays.

## What the tests cover

`tests/test_sidebar_projection_exactness.py` drives the real endpoint
(`routes.handle_get`) and `get_cli_sessions` against real SQLite `state.db`
files for two or three profiles (WebUI row sources are the only fixture), with
spies on `get_cli_sessions` and `_load_cli_sessions_uncached` and a SQLite
statement trace.

Response/ordering semantics (these pin behavior that already held on base; they
are guards, not red-on-base tests): `all_profiles`, source/archived/WebUI
counts, hidden background rows, orphaned imported sidecars, live external
updates, legacy schemas, compression-lineage collapse to the tip, and a resumed
old session displacing recent ones with stale and NULL `last_activity_at`.

Per-profile cache. Each asserts the result equals an uncached reference loop
(the exact pre-change behavior); some additionally assert which profiles were
recomputed, which is what fails on base:

- one profile commits: only that profile is recomputed, JSON and counts
  unchanged (endpoint and `get_cli_sessions` level); the active profile too;
  unchanged state serves the aggregate with zero loads;
- active-profile reorder with Claude Code rows on the first context only;
  source-filter variants; shared sidecar edits via the index stamp, explicit
  clear, and with neither; an unrelated sidecar not forcing a reload;
- global invalidation, TTL expiry, global workspace and `projects.json`
  changes, Claude Code transcript edits and the capped stamp walk;
- legacy schemas, a missing `state.db`, profile delete and add;
- overlapping rebuilds, invalidation during a rebuild, a commit landing
  mid-load, failed loads not cached, LRU bound, streaming freeze.

## Evidence

Isolated `HOME`/`HERMES_HOME`/`HERMES_WEBUI_TEST_STATE_ROOT`, stable `TMPDIR`
outside the state root, Python 3.12 `.venv` via `./scripts/test.sh`.

| Run | Result |
| --- | --- |
| `tests/test_sidebar_projection_exactness.py` with base `api/models.py` | 13 failed, 27 passed. The 13 are work-reuse/shape assertions (loader called for unchanged profiles, missing cache/stamp helpers, no LRU bound); the 27 pin existing semantics |
| same file with the change | 40 passed (stable over repeated runs) |
| 71 neighboring modules (every test referencing the CLI-sessions cache, `get_cli_sessions`, the sidecar projection, plus the earlier sidebar/cache/orphan/profile set) | 771 passed, 9 skipped |
| Full `./scripts/test.sh` | 15211 passed, 300 skipped, 2 xfailed, 1 xpassed |
| Mutations of the change, one at a time: no sidecar re-check, uncapped Claude walk, no shared stamp, no SQLite stamp, no Claude transcript stamp, TTL ignored | each fails at least one test |
| Earlier mutations of base: candidate order `COALESCE(last_activity_at, started_at)`; `cli = []` for `sidebar_source == "webui"` | ordering tests and count/row tests fail |

Mutations were applied to the working tree and reverted; none is in the diff.
Equivalent mutants (not distinguishable by behavior): dropping the
`include_claude_code` flag from the key (redundant with the transcript stamp and
irrelevant for non-Claude filters) and the write-side generation check (the read
side rejects a stale generation).

## Streaming-freeze key fix (inherited bug)

The all-profiles aggregate key, while a turn streams (#4842), replaced the whole
context component with `('streaming-frozen', marker)`. That froze more than the
volatile SQLite stamps: active-profile order, the Claude Code first-profile
assignment and profile membership too, so switching the active profile or
adding/removing a profile under a constant marker kept serving the old
aggregate. This predates the per-profile cache (it is in the base key); the
per-profile cache never got to correct it because the aggregate hit first.
The frozen key now keeps the ordered `(home, profile)` identity and collapses
only the per-profile SQLite stamps. The non-streaming key and the per-profile
entries' real stamps are unchanged; message writes during a stream are still
ignored by the aggregate. Tests: `test_streaming_freeze_*` (3 new; fail on the
previous key, pass now).

Scope limit: other profile-level inputs are still frozen by the marker for its
lifetime (e.g. a profile's own state.db appearing), by design.

## Limitations / not verified

- The SQLite stamp is inherited from the aggregate key
  (`_sqlite_content_fingerprint` plus file stats) with its documented gaps,
  bounded by the TTL.
- The Agent source defining `last_activity_at` / `idx_sessions_effective_activity`
  was not inspected, which is why the indexed window stays blocked.
- Timing numbers are synthetic; no production store, real latency, or
  concurrency-under-load measurement was made.
- Memory: up to 32 per-profile deep copies in addition to the aggregate copies
  (each is a slice of the aggregate).
- Browser behavior (sidebar virtualization, SSE/polling) is untouched and was
  not exercised.
