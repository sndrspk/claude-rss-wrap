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
    FEEDBIN_EMAIL, FEEDBIN_PASSWORD, ANTHROPIC_API_KEY
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

import requests
import yaml

FEEDBIN = "https://api.feedbin.com/v2"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yml"
STATE_PATH = ROOT / "state.json"
OUTPUT_PATH = ROOT / "docs" / "digest.xml"

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
        return {"tags": {}, "published": []}
    with STATE_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


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


# ------------------------------------------------------------------- claude

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
    system = f"""You triage RSS items for one specific reader, Sander. He works in \
applied research and communication at a Flemish university of applied sciences.

Your job for this batch:
{tag_cfg['job']}

What he cares about:
{tag_cfg['interests']}

Rules:
- Select at most {tag_cfg['max_items']} items. Fewer is fine and often correct.
- If nothing clears the bar, return an empty list. Never pad.
- Judge each item on its own merits, not on how many you have already picked.
- Language: {tag_cfg['language']}
- Plain, direct sentences. No marketing register, no "delve", no rhetorical \
questions, no em dashes.
- Reply with JSON only. No preamble, no markdown fences.

Reply with exactly this shape:
{{"selected": [{{"index": <int>, "summary": "<one sentence>", "why": "<one sentence>"}}]}}"""

    lines = []
    for index, entry in enumerate(entries):
        source = titles.get(entry.get("feed_id"), "unknown source")
        body = strip_html(entry.get("_text") or entry.get("content") or entry.get("summary"))
        lines.append(
            f"[{index}] source: {source}\n"
            f"title: {entry.get('title') or '(untitled)'}\n"
            f"author: {entry.get('author') or 'unknown'}\n"
            f"published: {entry.get('published', '')[:10]}\n"
            f"text: {body[:snippet_chars]}\n"
        )
    user = "Here are the new items.\n\n" + "\n".join(lines)
    return system, user


def call_claude(model: str, system: str, user: str, api_key: str) -> list[dict]:
    payload = {
        "model": model,
        "max_tokens": 2000,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }

    last_error = None
    for attempt in range(3):
        try:
            response = requests.post(ANTHROPIC_URL, json=payload,
                                     headers=headers, timeout=120)
            if response.status_code in (429, 500, 502, 503, 529):
                wait = 5 * (attempt + 1)
                log(f"  retrying after {response.status_code} in {wait}s")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            text = "".join(
                block.get("text", "")
                for block in data.get("content", [])
                if block.get("type") == "text"
            ).strip()
            text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            return json.loads(text).get("selected", [])
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(3)
    raise RuntimeError(f"Anthropic call failed after 3 attempts: {last_error}")


# --------------------------------------------------------------------- atom

def render_entry_html(label: str, picks: list[dict]) -> str:
    parts = []
    for pick in picks:
        entry = pick["entry"]
        title = html.escape(entry.get("title") or "(untitled)")
        url = html.escape(entry.get("url") or "")
        source = html.escape(pick.get("source", ""))
        summary = html.escape(pick.get("summary", ""))
        why = html.escape(pick.get("why", ""))
        parts.append(
            f'<h3><a href="{url}">{title}</a></h3>\n'
            f'<p><small>{source}</small></p>\n'
            f'<p>{summary}</p>\n'
            f'<p><em>{why}</em></p>'
        )
    return f"<p><strong>{html.escape(label)}</strong></p>\n" + "\n<hr>\n".join(parts)


def build_index(config: dict, published: list[dict]) -> str:
    """A plain archive page, so the link on each Feedbin entry goes somewhere."""
    blocks = "\n".join(
        f'<article id="{html.escape(item["slug"])}">\n'
        f'<h2>{html.escape(item["title"])}</h2>\n{item["html"]}\n</article>'
        for item in published
    )
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(config['feed_title'])}</title>
<style>
  body {{ max-width: 38rem; margin: 3rem auto; padding: 0 1.2rem;
         font: 16px/1.6 -apple-system, Georgia, serif; color: #222; }}
  article {{ margin-bottom: 4rem; }}
  h2 {{ font-size: 1.3rem; border-bottom: 1px solid #ddd; padding-bottom: .4rem; }}
  h3 {{ font-size: 1rem; margin-bottom: .2rem; }}
  small {{ color: #777; }}
  em {{ color: #555; }}
  hr {{ border: 0; border-top: 1px solid #eee; margin: 1.5rem 0; }}
  a {{ color: #0a5; }}
</style>
</head><body>
<h1>{html.escape(config['feed_title'])}</h1>
{blocks}
</body></html>
"""


def build_atom(config: dict, published: list[dict]) -> str:
    site = config["site_url"].rstrip("/")
    feed_url = f"{site}/digest.xml"
    updated = published[0]["updated"] if published else now_iso()

    out = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        f'  <title>{xml_escape(config["feed_title"])}</title>',
        f'  <id>{xml_escape(feed_url)}</id>',
        f'  <link rel="self" href="{xml_escape(feed_url)}"/>',
        f'  <link rel="alternate" href="{xml_escape(site)}/"/>',
        f'  <updated>{updated}</updated>',
        f'  <author><name>{xml_escape(config["feed_author"])}</name></author>',
    ]
    for item in published:
        out.extend([
            '  <entry>',
            f'    <title>{xml_escape(item["title"])}</title>',
            f'    <id>{xml_escape(item["id"])}</id>',
            f'    <updated>{item["updated"]}</updated>',
            f'    <link rel="alternate" href="{xml_escape(site)}/#{xml_escape(item["slug"])}"/>',
            f'    <content type="html">{xml_escape(item["html"])}</content>',
            '  </entry>',
        ])
    out.append('</feed>')
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------- main

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
    entries = entries[-config["max_entries_per_tag"]:]

    if config.get("extract_full_content"):
        for entry in entries:
            extracted = fetch_extracted(session, entry)
            if extracted:
                entry["_text"] = extracted

    system, user = build_prompt(tag_cfg, entries, titles, config["snippet_chars"])
    selected = call_claude(config["model"], system, user, api_key)
    log(f"  Claude selected {len(selected)} of {len(entries)}")

    picks = []
    for choice in selected[:tag_cfg["max_items"]]:
        try:
            entry = entries[int(choice["index"])]
        except (KeyError, ValueError, IndexError):
            continue
        picks.append({
            "entry": entry,
            "source": titles.get(entry.get("feed_id"), ""),
            "summary": choice.get("summary", ""),
            "why": choice.get("why", ""),
        })

    published_entry = None
    if picks:
        stamp = datetime.now(timezone.utc)
        digest = hashlib.sha1(
            f"{name}{stamp.date()}{picks[0]['entry']['id']}".encode()
        ).hexdigest()[:12]
        published_entry = {
            "id": f"tag:feedbin-wrap,{stamp.date()}:{digest}",
            "slug": digest,
            "title": f"{tag_cfg['label']}, {stamp.strftime('%-d %B')}",
            "updated": now_iso(),
            "html": render_entry_html(tag_cfg["label"], picks),
        }

    return {
        "considered": considered_ids,
        "published_entry": published_entry,
        "next_since": next_since,
    }


def main() -> None:
    config = load_config()
    state = load_state()
    api_key = env("ANTHROPIC_API_KEY")

    session = feedbin_session()
    check_auth(session)
    tag_feeds = feed_ids_by_tag(session)
    titles = feed_titles(session)

    results = {}
    for tag_cfg in config["tags"]:
        try:
            results[tag_cfg["name"]] = process_tag(
                session, config, tag_cfg, tag_feeds, titles, api_key, state
            )
        except Exception as exc:  # noqa: BLE001
            # One tag failing must not advance the other tag's state.
            log(f"  ! {tag_cfg['name']} failed: {exc}")
            results[tag_cfg["name"]] = None

    new_entries = [
        r["published_entry"] for r in results.values()
        if r and r.get("published_entry")
    ]

    if not new_entries:
        log("\nNothing cleared the bar. Not publishing an empty entry.")
    else:
        state["published"] = new_entries + state.get("published", [])
        state["published"] = state["published"][:config["keep_entries"]]
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(build_atom(config, state["published"]), encoding="utf-8")
        (OUTPUT_PATH.parent / "index.html").write_text(
            build_index(config, state["published"]), encoding="utf-8")
        log(f"\nWrote {OUTPUT_PATH} with {len(state['published'])} entries")

    # Only now, after a successful publish, do we advance state and mark read.
    for tag_cfg in config["tags"]:
        result = results.get(tag_cfg["name"])
        if not result:
            continue
        state["tags"].setdefault(tag_cfg["name"], {})["since"] = result["next_since"]
        if tag_cfg.get("mark_read") and result["considered"]:
            mark_read(session, result["considered"])

    save_state(state)
    log("Done.")


if __name__ == "__main__":
    main()
