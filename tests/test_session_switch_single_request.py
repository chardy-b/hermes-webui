"""Regression coverage for the one-request chat-switch fast path."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SESSIONS_JS = (ROOT / "static" / "sessions.js").read_text(encoding="utf-8")


def _extract_function(source: str, name: str) -> str:
    for marker in (f"async function {name}(", f"function {name}("):
        start = source.find(marker)
        if start >= 0:
            break
    assert start >= 0, f"{name} not found"
    brace = source.find("{", start)
    depth = 0
    quote = None
    escaped = False
    line_comment = False
    block_comment = False
    for index in range(brace, len(source)):
        char = source[index]
        nxt = source[index + 1] if index + 1 < len(source) else ""
        if line_comment:
            if char == "\n":
                line_comment = False
            continue
        if block_comment:
            if char == "*" and nxt == "/":
                block_comment = False
            continue
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char == "/" and nxt == "/":
            line_comment = True
            continue
        if char == "/" and nxt == "*":
            block_comment = True
            continue
        if char in ("'", '"', "`"):
            quote = char
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"unterminated function {name}")


def test_load_session_uses_full_tail_response_as_metadata_and_messages():
    """A normal switch must not serialize metadata GET then transcript GET."""
    load_session = _extract_function(SESSIONS_JS, "loadSession")

    assert "messages=0&resolve_model=0" not in load_session
    assert "data = await api(_sessionMessagesRequestUrl(sid), {timeoutMs:120000})" in load_session
    assert load_session.count("initialData:_initialMessagesData") == 2


def test_ensure_messages_can_adopt_prefetched_response_without_another_get():
    ensure_messages = _extract_function(SESSIONS_JS, "_ensureMessagesLoaded")

    assert "letdata=opts.initialData||null" in ensure_messages.replace(" ", "")
    assert "if(!data)" in ensure_messages.replace(" ", "")
