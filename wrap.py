#!/usr/bin/env python3
"""
Feedbin Wrap
============

Reads new entries from tagged Feedbin feeds, asks Claude to triage them,
and writes the result as an Atom feed you subscribe to back in Feedbin.

Order of operations matters. The state file is only advanced and entries are
only marked read *after* the Atom file has been written successfully. If the
Anthropic call or the render fails, the next run re-covers the same window
rather than silently dropping a day.

Environment:
    FEEDBIN_EMAIL, FEEDBIN_PASSWORD
    ANTHROPIC_API_KEY when provider is "anthropic",
    MISTRAL_API_KEY when provider is "mistral".

The triage provider is selected with the `provider` key in config.yml.
Currently "anthropic" (Claude) and "mistral" are supported. See
config.yml and docs/migration-mistral.md.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from xml.sax.saxutils import escape as xml_escape

import requests
import yaml

FEEDBIN = "https://api.feedbin.com/v2"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"

PROVIDERS = ("anthropic", "mistral")

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yml"
STATE_PATH = ROOT / "state.json"
DOCS_DIR = ROOT / "docs"

TIMEOUT = 30


# ---------------------------------------------------------------- utilities

def log(msg: str) -> None:
    print(msg, flush=True)


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


def env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        die(f"missing environment variable {name}")
    return value


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"tags": {}}
    with STATE_PATH.open(encoding="utf-8") as fh:
        state = json.load(fh)
    # Migration from the single-feed layout: the archive used to live at the
    # top level. Drop it, keep the per-tag timestamps, which are what matter.
    state.pop("published", None)
    state.setdefault("tags", {})
    for tag_state in state["tags"].values():
        tag_state.setdefault("published", [])
    return state


def save_state(state: dict) -> None:
    with STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)
        fh.write("\n")


# ----------------------------------------------------------------- feedbin

def feedbin_session() -> requests.Session:
    session = requests.Session()
    session.auth = (env("FEEDBIN_EMAIL"), env("FEEDBIN_PASSWORD"))
    session.headers["User-Agent"] = "feedbin-wrap/1.0"
    return session


def check_auth(session: requests.Session) -> None:
    response = session.get(f"{FEEDBIN}/authentication.json", timeout=TIMEOUT)
    if response.status_code == 401:
        die("Feedbin rejected the credentials (401)")
    if response.status_code != 200:
        # Don't hard-fail on anything else; the real calls below will surface
        # a genuine problem soon enough.
        log(f"note: auth check returned {response.status_code}, continuing")


def feed_ids_by_tag(session: requests.Session) -> dict[str, list[int]]:
    """Resolve tag names to feed IDs via the Taggings API.

    This is why you never touch code when adding or dropping a feed: tag it
    in Feedbin and the next run picks it up.
    """
    response = session.get(f"{FEEDBIN}/taggings.json", timeout=TIMEOUT)
    response.raise_for_status()
    mapping: dict[str, list[int]] = {}
    for tagging in response.json():
        mapping.setdefault(tagging["name"], []).append(tagging["feed_id"])
    return mapping


def feed_titles(session: requests.Session) -> dict[int, str]:
    response = session.get(f"{FEEDBIN}/subscriptions.json", timeout=TIMEOUT)
    response.raise_for_status()
    return {sub["feed_id"]: sub.get("title", "") for sub in response.json()}


def entries_for_feed(session: requests.Session, feed_id: int, since: str) -> list[dict]:
    """Fetch entries for one feed created after `since`.

    Per-feed rather than the global /entries.json endpoint, so we never pull
    down the rest of the Feedbin flow just to throw it away.
    """
    collected: list[dict] = []
    page = 1
    while page <= 10:  # 1000 entries per feed per run is already absurd
        response = session.get(
            f"{FEEDBIN}/feeds/{feed_id}/entries.json",
            params={"since": since, "per_page": 100, "page": page},
            timeout=TIMEOUT,
        )
        if response.status_code == 404:
            log(f"  ! feed {feed_id} returned 404, skipping")
            return collected
        response.raise_for_status()
        batch = response.json()
        collected.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return collected


def fetch_extracted(session: requests.Session, entry: dict) -> str | None:
    """Feedbin's full-text extraction for entries whose feed only gives a stub.

    The extract URL is pre-signed. If it needs credentials in your account's
    case, the authed session covers that too; either way a failure here is
    non-fatal and we fall back to the feed's own content.
    """
    url = entry.get("extracted_content_url")
    if not url:
        return None
    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        return payload.get("content") or None
    except Exception as exc:  # noqa: BLE001 - deliberately non-fatal
        log(f"  ! extraction failed for entry {entry.get('id')}: {exc}")
        return None


def mark_read(session: requests.Session, entry_ids: list[int]) -> None:
    """DELETE /v2/unread_entries.json, chunked at the documented 1000 limit."""
    for start in range(0, len(entry_ids), 1000):
        chunk = entry_ids[start:start + 1000]
        response = session.delete(
            f"{FEEDBIN}/unread_entries.json",
            json={"unread_entries": chunk},
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=TIMEOUT,
        )
        if response.status_code != 200:
            # Non-fatal: the digest is already published, which is the part
            # that matters. Worst case those items stay unread.
            log(f"  ! mark-read returned {response.status_code}")
            return
    log(f"  marked {len(entry_ids)} entries read")


# ------------------------------------------------------------------- dedupe

TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid"}


def normalize_url(url: str | None) -> str:
    """Canonical form for duplicate detection: lowercase scheme/host, tracking
    query params removed, remaining params sorted, fragment and trailing slash
    dropped. Empty string when there is no usable URL."""
    url = (url or "").strip()
    if not url:
        return ""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query = sorted(
        ((key, value)
         for key, value in parse_qsl(parts.query, keep_blank_values=True)
         if not key.lower().startswith("utm_") and key.lower() not in TRACKING_PARAMS),
        key=lambda pair: (pair[0].lower(), pair[0]),
    )
    return urlunsplit((parts.scheme.lower() or "http", host, path,
                       urlencode(query), ""))


def normalize_title(title: str | None) -> str:
    return " ".join(re.findall(r"\w+", (title or "").lower()))


def word_shingles(text: str, size: int = 5) -> set[str]:
    words = re.findall(r"\w+", text.lower())
    if len(words) < size:
        return {" ".join(words)} if words else set()
    return {" ".join(words[i:i + size]) for i in range(len(words) - size + 1)}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def entry_text(entry: dict) -> str:
    return strip_html(entry.get("_text") or entry.get("content")
                      or entry.get("summary"))


def dedupe_entries(entries: list[dict], text_similarity: float,
                   window: int) -> list[dict]:
    """Collapse items that are the same story, so the model never sees, and
    never picks, the same article twice.

    Exact pass over the whole batch: same normalized URL or same normalized
    title. Content pass over the tail window the model will actually see
    (bounded by max_entries_per_tag): word-shingle Jaccard of title+text at or
    above `text_similarity` counts as the same story, catching syndicated
    copies with a tweaked URL or headline. The first (oldest) copy wins.
    """
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    kept: list[dict] = []
    dropped = 0
    for entry in entries:
        url_key = normalize_url(entry.get("url"))
        title_key = normalize_title(entry.get("title"))
        if (url_key and url_key in seen_urls) or (title_key and title_key in seen_titles):
            dropped += 1
            continue
        if url_key:
            seen_urls.add(url_key)
        if title_key:
            seen_titles.add(title_key)
        kept.append(entry)

    if text_similarity > 0 and len(kept) > 1:
        if window and window < len(kept):
            head, tail = kept[:-window], kept[-window:]
        else:
            head, tail = [], kept
        shingles = [word_shingles(f"{e.get('title') or ''} {entry_text(e)}"[:4000])
                    for e in tail]
        remove = set()
        for i in range(len(tail)):
            if i in remove:
                continue
            for j in range(i + 1, len(tail)):
                if j not in remove and jaccard(shingles[i], shingles[j]) >= text_similarity:
                    remove.add(j)
        if remove:
            dropped += len(remove)
            tail = [e for i, e in enumerate(tail) if i not in remove]
        kept = head + tail

    if dropped:
        log(f"  dedup: dropped {dropped} duplicate item(s), "
            f"{len(kept)} of {len(entries)} remain")
    return kept


# ---------------------------------------------------------------- provider

def strip_html(raw: str | None) -> str:
    if not raw:
        return ""
    text = raw
    for tag in ("</p>", "<br>", "<br/>", "<br />", "</div>", "</li>"):
        text = text.replace(tag, "\n")
    out, inside = [], False
    for char in text:
        if char == "<":
            inside = True
        elif char == ">":
            inside = False
        elif not inside:
            out.append(char)
    return html.unescape("".join(out)).strip()


def build_prompt(tag_cfg: dict, entries: list[dict], titles: dict[int, str],
                 snippet_chars: int) -> tuple[str, str]:
    system = f"""You are an editor curating a digest from a batch of RSS items.

There are two separate jobs here. Keep them apart.

--- SELECTION (private) ---
Use this profile to decide what to include. It is the reader's, and it never
appears in what you write:

{tag_cfg['interests']}

--- WRITING (public) ---
{tag_cfg['job']}

Write as though for the whole readership of a good publication, not for one
person. This matters:
- Never refer to the reader, in any grammatical person. No "he", "his", "you",
  "your", "the reader", "our reader".
- Never say an item suits someone's taste, interests, field or work. If the
  only thing you can say is that someone would like it, you have not found the
  hook yet, so look again at the item itself.
- The second line is not a justification for the pick. It is the thing that
  makes the item worth attention: the claim, the angle, the stakes, the person
  involved, what is unusual about it.

Rules:
- Never select two items about the same event, story or artefact. Two items
  are the same event when they report the same underlying proposal,
  decision, announcement, release or occurrence by the same person or
  institution in the same timeframe, even when the URL, headline, figures or
  angle differ between outlets. When in doubt, treat them as the same event:
  select at most one of them, the strongest source, and list the other
  items' indexes in "also" so their links are published with the pick.
  Omit "also" for items that stand alone.
- Select at most {tag_cfg['max_items']} items. Fewer is fine and often correct.
- If nothing clears the bar, return an empty list. Never pad.
- Judge each item on its own merits, not on how many you have already picked.
- Language: {tag_cfg['language']}
- Plain, direct sentences. No marketing register, no "delve", no rhetorical \
questions, no em dashes.
- Reply with JSON only. No preamble, no markdown fences.

Reply with exactly this shape ("also" is optional):
{{"selected": [{{"index": <int>, "summary": "<one sentence>", "why": "<one sentence>", "also": [<int>, ...]}}]}}"""

    lines = []
    for index, entry in enumerate(entries):
        source = titles.get(entry.get("feed_id"), "unknown source")
        body = strip_html(entry.get("_text") or entry.get("content") or entry.get("summary"))
        lines.append(
            f"[{index}] source: {source}\n"
            f"title: {entry.get('title') or '(untitled)'}\n"
            f"url: {entry.get('url') or ''}\n"
            f"author: {entry.get('author') or 'unknown'}\n"
            f"published: {entry.get('published', '')[:10]}\n"
            f"text: {body[:snippet_chars]}\n"
        )
    user = "Here are the new items.\n\n" + "\n".join(lines)
    return system, user


def extract_json(text: str) -> str:
    """Pull the JSON object out, tolerating fences or stray preamble."""
    text = text.strip()
    text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    if not text.startswith("{"):
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            text = text[start:end + 1]
    return text


def call_anthropic(model: str, system: str, user: str, api_key: str,
                   max_tokens: int = 8000) -> list[dict]:
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    log(f"  sending {len(user):,} characters to {model}")

    last_error = None
    for attempt in range(3):
        try:
            response = requests.post(ANTHROPIC_URL, json=payload,
                                     headers=headers, timeout=180)
            if response.status_code in (429, 500, 502, 503, 529):
                wait = 5 * (attempt + 1)
                log(f"  retrying after {response.status_code} in {wait}s")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            stop = data.get("stop_reason")
            usage = data.get("usage", {})
            log(f"  stop_reason={stop} in={usage.get('input_tokens')} "
                f"out={usage.get('output_tokens')}")

            text = "".join(
                block.get("text", "")
                for block in data.get("content", [])
                if block.get("type") == "text"
            )
            if not text.strip():
                types = [b.get("type") for b in data.get("content", [])]
                raise RuntimeError(
                    f"no text in response (stop_reason={stop}, blocks={types}). "
                    "If stop_reason is max_tokens, raise max_tokens in config.yml."
                )
            return parse_selection(text, stop)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(3)
    raise RuntimeError(f"Anthropic call failed after 3 attempts: {last_error}")


def call_mistral(model: str, system: str, user: str, api_key: str,
                 max_tokens: int = 8000) -> list[dict]:
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }
    log(f"  sending {len(user):,} characters to {model}")

    last_error = None
    for attempt in range(3):
        try:
            response = requests.post(MISTRAL_URL, json=payload,
                                     headers=headers, timeout=180)
            if response.status_code in (429, 500, 502, 503):
                wait = 5 * (attempt + 1)
                log(f"  retrying after {response.status_code} in {wait}s")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            choice = (data.get("choices") or [{}])[0]
            stop = choice.get("finish_reason")
            usage = data.get("usage", {})
            log(f"  finish_reason={stop} in={usage.get('prompt_tokens')} "
                f"out={usage.get('completion_tokens')}")

            text = choice.get("message", {}).get("content") or ""
            if not text.strip():
                raise RuntimeError(
                    f"no text in response (finish_reason={stop}). "
                    "If finish_reason is length, raise max_tokens in config.yml."
                )
            return parse_selection(text, stop)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(3)
    raise RuntimeError(f"Mistral call failed after 3 attempts: {last_error}")


def parse_selection(text: str, stop: str | None) -> list[dict]:
    try:
        return json.loads(extract_json(text)).get("selected", [])
    except json.JSONDecodeError as exc:
        # Show what actually came back rather than failing blind.
        log(f"  ! unparseable response (stop={stop}): {text[:300]!r}")
        raise RuntimeError(f"could not parse response as JSON: {exc}") from exc


def call_provider(provider: str, model: str, system: str, user: str,
                  api_key: str, max_tokens: int) -> list[dict]:
    if provider == "mistral":
        return call_mistral(model, system, user, api_key, max_tokens)
    return call_anthropic(model, system, user, api_key, max_tokens)


# --------------------------------------------------------------------- atom

def source_link(url: str | None, name: str | None) -> str:
    url = (url or "").strip()
    text = html.escape(name or url or "")
    if not url:
        return text
    return f'<a href="{html.escape(url)}">{text}</a>'


def render_entry_html(label: str, picks: list[dict]) -> str:
    """One pick as HTML: plain (unlinked) title, the blurb paragraph, then the
    source name(s) at the bottom as the link, small caps where supported.
    Merged picks list every outlet's link on that bottom line."""
    parts = []
    for pick in picks:
        entry = pick["entry"]
        title = html.escape(entry.get("title") or "(untitled)")
        summary = html.escape(pick.get("summary", ""))
        why = html.escape(pick.get("why", ""))
        blurb = " ".join(part for part in (summary, why) if part)
        links = [source_link(entry.get("url"), pick.get("source"))]
        for a in pick.get("also") or []:
            links.append(source_link(a.get("url"), a.get("source")))
        sources = " ".join(links)
        parts.append(
            f"<h3>{title}</h3>\n"
            f"<p>{blurb}</p>\n"
            f'<p><small style="font-variant: small-caps">{sources}</small></p>'
        )
    return f"<p><strong>{html.escape(label)}</strong></p>\n" + "\n<hr>\n".join(parts)


PAGE_CSS = """
  body { max-width: 38rem; margin: 3rem auto; padding: 0 1.2rem;
         font: 16px/1.6 -apple-system, Georgia, serif; color: #222; }
  article { margin-bottom: 4rem; }
  h2 { font-size: 1.3rem; border-bottom: 1px solid #ddd; padding-bottom: .4rem; }
  h3 { font-size: 1rem; margin-bottom: .2rem; }
  small { color: #777; font-variant: small-caps; }
  em { color: #555; }
  hr { border: 0; border-top: 1px solid #eee; margin: 1.5rem 0; }
  a { color: #0a5; }
  nav { margin-bottom: 2.5rem; font-size: .9rem; }
"""


def build_index(config: dict, tag_cfg: dict, published: list[dict]) -> str:
    """Archive page for one tag, so each Feedbin entry has a link target."""
    title = f"{config['feed_title']}: {tag_cfg['label']}"
    blocks = "\n".join(
        f'<article id="{html.escape(item["slug"])}">\n'
        f'<h2>{html.escape(item["title"])}</h2>\n{item["html"]}\n</article>'
        for item in published
    )
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>{PAGE_CSS}</style>
</head><body>
<nav><a href="./">All wraps</a></nav>
<h1>{html.escape(title)}</h1>
{blocks}
</body></html>
"""


def build_home(config: dict) -> str:
    """Tiny landing page linking the per-tag archives and feeds."""
    rows = "\n".join(
        f'<li><a href="{tag["slug"]}.html">{html.escape(tag["label"])}</a> '
        f'&middot; <a href="{tag["slug"]}.xml">feed</a></li>'
        for tag in config["tags"]
    )
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(config['feed_title'])}</title>
<style>{PAGE_CSS}</style>
</head><body>
<h1>{html.escape(config['feed_title'])}</h1>
<ul>
{rows}
</ul>
</body></html>
"""


def build_atom(config: dict, tag_cfg: dict, published: list[dict]) -> str:
    site = config["site_url"].rstrip("/")
    feed_url = f"{site}/{tag_cfg['slug']}.xml"
    page_url = f"{site}/{tag_cfg['slug']}.html"
    updated = published[0]["updated"] if published else now_iso()
    title = f"{config['feed_title']}: {tag_cfg['label']}"

    out = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        f'  <title>{xml_escape(title)}</title>',
        f'  <id>{xml_escape(feed_url)}</id>',
        f'  <link rel="self" href="{xml_escape(feed_url)}"/>',
        f'  <link rel="alternate" href="{xml_escape(page_url)}"/>',
        f'  <updated>{updated}</updated>',
        f'  <author><name>{xml_escape(config["feed_author"])}</name></author>',
    ]
    for item in published:
        out.extend([
            '  <entry>',
            f'    <title>{xml_escape(item["title"])}</title>',
            f'    <id>{xml_escape(item["id"])}</id>',
            f'    <updated>{item["updated"]}</updated>',
            f'    <link rel="alternate" href="{xml_escape(page_url)}#{xml_escape(item["slug"])}"/>',
            f'    <content type="html">{xml_escape(item["html"])}</content>',
            '  </entry>',
        ])
    out.append('</feed>')
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------- main

WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

# Default schedule.time (UTC) when a tag does not set one: the time the
# workflow cron used to run at when it swept only once a day.
DEFAULT_SCHEDULE_TIME = "05:30"


def plan_tags(tags: list[dict], state: dict,
              now: datetime | None = None) -> list[dict]:
    """Tags due now, honouring each tag's schedule.days and schedule.time.

    Days are matched against the UTC weekday and the optional time (UTC
    "HH:MM", default DEFAULT_SCHEDULE_TIME) against the current UTC time: a
    tag runs at the first sweep at or after its time, and at most once per
    day, using the per-tag last_run recorded in state. The ONLY_TAGS
    environment variable overrides the schedule entirely and runs exactly
    the named tags; it is set by the workflow's only_tags input.
    """
    only = [t.strip() for t in os.environ.get("ONLY_TAGS", "").split(",") if t.strip()]
    if only:
        known = {tag["name"] for tag in tags}
        unknown = [t for t in only if t not in known]
        if unknown:
            die(f"ONLY_TAGS names unknown tags: {', '.join(unknown)}")
        log(f"ONLY_TAGS set: running {', '.join(only)} regardless of schedule")
        return [tag for tag in tags if tag["name"] in only]

    now = now or datetime.now(timezone.utc)
    today = WEEKDAYS[now.weekday()]
    today_str = now.strftime("%Y-%m-%d")
    due = []
    for tag_cfg in tags:
        name = tag_cfg["name"]
        schedule = tag_cfg.get("schedule", {})
        days = schedule.get("days")
        if days and today not in [d.strip().lower() for d in days]:
            log(f"{name}: not due on {today}, skipping")
            continue
        time_str = str(schedule.get("time") or DEFAULT_SCHEDULE_TIME).strip()
        try:
            hour, minute = (int(part) for part in time_str.split(":"))
            scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            die(f"{name}: invalid schedule.time {time_str!r}, expected \"HH:MM\"")
        if now < scheduled:
            log(f"{name}: not due before {time_str} UTC, skipping")
            continue
        last_run = state.get("tags", {}).get(name, {}).get("last_run", "")
        if last_run[:10] == today_str:
            log(f"{name}: already ran today, skipping")
            continue
        due.append(tag_cfg)
    return due


def build_picks(selected: list[dict], entries: list[dict], tag_cfg: dict,
                titles: dict[int, str]) -> list[dict]:
    picks = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    for choice in selected[:tag_cfg["max_items"]]:
        try:
            entry = entries[int(choice["index"])]
        except (KeyError, ValueError, IndexError):
            continue
        url_key = normalize_url(entry.get("url"))
        title_key = normalize_title(entry.get("title"))
        if (url_key and url_key in seen_urls) or (title_key and title_key in seen_titles):
            # Final guard: the prompt forbids picking the same event twice,
            # but do not trust the model blindly.
            log(f"  dropping duplicate pick: {entry.get('title')}")
            continue
        if url_key:
            seen_urls.add(url_key)
        if title_key:
            seen_titles.add(title_key)
        also = []
        for other_index in (choice.get("also") or [])[:5]:
            try:
                other = entries[int(other_index)]
            except (ValueError, IndexError, TypeError):
                continue
            other_url = normalize_url(other.get("url"))
            other_title = normalize_title(other.get("title"))
            if (other_url and other_url in seen_urls) or \
                    (other_title and other_title in seen_titles):
                continue
            if other_url:
                seen_urls.add(other_url)
            if other_title:
                seen_titles.add(other_title)
            also.append({
                "url": other.get("url") or "",
                "source": titles.get(other.get("feed_id"), ""),
            })
        picks.append({
            "entry": entry,
            "source": titles.get(entry.get("feed_id"), ""),
            "summary": choice.get("summary", ""),
            "why": choice.get("why", ""),
            "also": also,
        })
    return picks


def build_published_entry(tag_cfg: dict, picks: list[dict],
                          suffix: str = "") -> dict:
    name = tag_cfg["name"]
    stamp = datetime.now(timezone.utc)
    digest = hashlib.sha1(
        f"{name}{stamp.date()}{picks[0]['entry']['id']}{suffix}".encode()
    ).hexdigest()[:12]
    return {
        "id": f"tag:feedbin-wrap,{stamp.date()}:{digest}",
        "slug": digest,
        "title": f"{tag_cfg['label']}, {stamp.strftime('%-d %B')}{suffix}",
        "updated": now_iso(),
        "html": render_entry_html(tag_cfg["label"], picks),
    }


def process_tag(session, config, tag_cfg, tag_feeds, titles, api_key, state):
    name = tag_cfg["name"]
    log(f"\n== {name}")

    feed_ids = tag_feeds.get(name)
    if not feed_ids:
        log("  no feeds carry this tag, skipping")
        return None

    tag_state = state["tags"].get(name, {})
    since = tag_state.get("since")
    if not since:
        # First run: look back two days rather than the whole archive.
        since = (datetime.now(timezone.utc) - timedelta(days=2)).strftime(
            "%Y-%m-%dT%H:%M:%S.%f") + "Z"
        log(f"  first run, looking back to {since}")

    entries: list[dict] = []
    for feed_id in feed_ids:
        entries.extend(entries_for_feed(session, feed_id, since))
    log(f"  {len(entries)} new entries across {len(feed_ids)} feeds")

    if not entries:
        return {"considered": [], "published_entry": None, "next_since": since}

    entries.sort(key=lambda e: e.get("created_at", ""))
    # Feedbin's docs warn that rounding the timestamp causes duplicates, so we
    # reuse the server's own value verbatim rather than generating our own.
    next_since = entries[-1].get("created_at") or since

    considered_ids = [e["id"] for e in entries]
    entries = dedupe_entries(entries, config.get("dedupe_text_similarity", 0.5),
                            config["max_entries_per_tag"])
    entries = entries[-config["max_entries_per_tag"]:]

    if config.get("extract_full_content"):
        for entry in entries:
            extracted = fetch_extracted(session, entry)
            if extracted:
                entry["_text"] = extracted

    system, user = build_prompt(tag_cfg, entries, titles, config["snippet_chars"])

    if isinstance(api_key, dict):
        # Side-by-side comparison mode: every enabled provider gets the same
        # batch and publishes its own entry, suffixed with its display name.
        selected_by_provider = {}
        for p in PROVIDERS:
            model = config["compare_models"][p]["model"]
            selected_by_provider[p] = call_provider(
                p, model, system, user, api_key[p],
                config["compare_models"][p].get("max_tokens", 8000))
            log(f"  {p} selected {len(selected_by_provider[p])} of {len(entries)}")

        published_entries = []
        for p in PROVIDERS:
            picks = build_picks(selected_by_provider[p], entries, tag_cfg, titles)
            if not picks:
                continue
            published_entries.append(
                build_published_entry(tag_cfg, picks, suffix=f" ({p})")
            )

        return {
            "considered": considered_ids,
            "published_entries": published_entries,
            "next_since": next_since,
        }

    selected = call_provider(config["provider"], config["model"], system, user,
                             api_key, config.get("max_tokens", 8000))
    log(f"  {config['provider']} selected {len(selected)} of {len(entries)}")

    picks = build_picks(selected, entries, tag_cfg, titles)
    published_entry = build_published_entry(tag_cfg, picks) if picks else None

    return {
        "considered": considered_ids,
        "published_entries": [published_entry] if published_entry else [],
        "next_since": next_since,
    }


def main() -> None:
    config = load_config()
    state = load_state()
    provider = config.get("provider", "anthropic")
    if provider not in PROVIDERS:
        die(f"unknown provider {provider!r}, expected one of {PROVIDERS}")
    if config.get("compare_providers"):
        api_keys = {}
        for p in PROVIDERS:
            var = "MISTRAL_API_KEY" if p == "mistral" else "ANTHROPIC_API_KEY"
            try:
                api_keys[p] = env(var)
            except SystemExit:
                die(f"compare_providers needs {var} set")
    else:
        api_key = env("MISTRAL_API_KEY" if provider == "mistral"
                      else "ANTHROPIC_API_KEY")

    planned = plan_tags(config["tags"], state)
    if not planned:
        log("no tags due today, nothing to do")
        return

    session = feedbin_session()
    check_auth(session)
    tag_feeds = feed_ids_by_tag(session)
    titles = feed_titles(session)

    results = {}
    for tag_cfg in planned:
        try:
            results[tag_cfg["name"]] = process_tag(
                session, config, tag_cfg, tag_feeds, titles,
                api_keys if config.get("compare_providers") else api_key, state
            )
        except Exception as exc:  # noqa: BLE001
            # One tag failing must not advance the other tag's state.
            log(f"  ! {tag_cfg['name']} failed: {exc}")
            results[tag_cfg["name"]] = None

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    (DOCS_DIR / "index.html").write_text(build_home(config), encoding="utf-8")

    failures = []
    planned_names = {tag_cfg["name"] for tag_cfg in planned}
    for tag_cfg in config["tags"]:
        result = results.get(tag_cfg["name"])

        tag_state = state["tags"].setdefault(tag_cfg["name"], {"published": []})
        tag_state.setdefault("published", [])

        if tag_cfg["name"] not in planned_names:
            # Not due today: state and mark-read are untouched, but the files
            # are still rewritten so the feed stays valid and in step with
            # config changes.
            (DOCS_DIR / f"{tag_cfg['slug']}.xml").write_text(
                build_atom(config, tag_cfg, tag_state["published"]), encoding="utf-8")
            (DOCS_DIR / f"{tag_cfg['slug']}.html").write_text(
                build_index(config, tag_cfg, tag_state["published"]), encoding="utf-8")
            continue

        if result is None:
            # Errored or no feeds carry the tag. Still write the files, so the
            # feed URL keeps resolving instead of 404ing, but flag it loudly.
            failures.append(tag_cfg["name"])
            log(f"{tag_cfg['label']}: FAILED, feed left at its previous contents")
        elif result.get("published_entries"):
            for entry in result["published_entries"]:
                tag_state["published"].insert(0, entry)
            tag_state["published"] = tag_state["published"][:config["keep_entries"]]
            log(f"{tag_cfg['label']}: published")
        else:
            log(f"{tag_cfg['label']}: nothing cleared the bar, no entry added")

        # Rewrite both files every run, even when nothing was added, so the
        # feed stays valid and in step with config changes.
        (DOCS_DIR / f"{tag_cfg['slug']}.xml").write_text(
            build_atom(config, tag_cfg, tag_state["published"]), encoding="utf-8")
        (DOCS_DIR / f"{tag_cfg['slug']}.html").write_text(
            build_index(config, tag_cfg, tag_state["published"]), encoding="utf-8")

        if result is None:
            continue

        # Only now, after a successful write, advance state and mark read.
        tag_state["since"] = result["next_since"]
        tag_state["last_run"] = now_iso()
        if tag_cfg.get("mark_read") and result["considered"]:
            mark_read(session, result["considered"])

    save_state(state)

    if failures:
        die("these tags did not run: " + ", ".join(failures))
    log("Done.")


if __name__ == "__main__":
    main()