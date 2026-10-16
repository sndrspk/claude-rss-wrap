# claude-rss-wrap

A scheduled script that reads new entries from tagged Feedbin feeds, asks an
LLM to triage them, and publishes the result as an Atom feed you subscribe to
back in Feedbin. Runs on GitHub Actions, commits its output to `docs/`, and
serves it via GitHub Pages.

## How it works

1. Authenticates to the Feedbin API and resolves your Feedbin tags to feed IDs
   via the Taggings API. Adding or dropping a feed requires no code change:
   tag it in Feedbin and the next run picks it up.
2. Fetches entries created after each tag's last `since` timestamp
   (per-feed, capped by `max_entries_per_tag`).
3. Sends the batch to the configured AI model with the tag's prompt
   (a private interests profile and a public writing job). The model replies
   with strict JSON: which items to include, a summary, and the hook.
4. Writes an Atom feed and an archive page per tag into `docs/`, commits
   them, and marks the considered entries read in Feedbin.

Order of operations matters. The state file is only advanced and entries are
only marked read *after* the Atom file has been written successfully. If the
AI call or the render fails, the next run re-covers the same window rather
than silently dropping a day.

## Files

| File | Purpose |
|---|---|
| `wrap.py` | The whole application |
| `config.yml` | Everything you are likely to want to change |
| `state.json` | Per-tag `since` timestamps and published entries (managed by the script) |
| `docs/` | Published output: `index.html`, one `<slug>.html` and `<slug>.xml` per tag |
| `.github/workflows/wrap.yml` | Schedule and run definition |
| `docs/migration-mistral.md` | Plan for switching the AI provider from Anthropic to Mistral |

## Configuration

See the comments in `config.yml`. Key knobs:

- `provider` — AI provider for the triage step: `anthropic` (Claude) or
  `mistral`. The matching secret (`ANTHROPIC_API_KEY` or `MISTRAL_API_KEY`)
  is picked automatically.
- `model` — AI model ID used for the triage step, for the chosen provider.
- `max_entries_per_tag` — hard ceiling on entries sent per tag per run.
- `snippet_chars` — characters of article text sent per entry.
- `max_tokens` — output budget per triage call. Raise it if logs show
  the model running out of room mid-JSON.
- `extract_full_content` — fetch Feedbin's full-text extraction instead of
  the feed's own summary. Better input, slower runs, more tokens. Leave off
  for the first few runs, then turn on if summaries feel thin.
- `tags` — one block per Feedbin tag: `label`, `slug`, `max_items`,
  `mark_read`, `language`, `job` (public writing instructions), and
  `interests` (private selection profile, never shown in output).

The run schedule is the cron line in `.github/workflows/wrap.yml` (GitHub
only accepts cron there).

## Secrets

Set as GitHub repository secrets (Settings → Secrets and variables →
Actions):

- `FEEDBIN_EMAIL`
- `FEEDBIN_PASSWORD`
- `ANTHROPIC_API_KEY` — used when `provider: anthropic` (the current
  default; see `docs/migration-mistral.md` for the planned switch)
- `MISTRAL_API_KEY` — used when `provider: mistral`

## Running

The workflow runs on the cron schedule in `.github/workflows/wrap.yml` and
can also be triggered manually from the Actions tab (`workflow_dispatch`).
Locally, with the three environment variables set:

```
pip install -r requirements.txt
python wrap.py
```

## Documentation convention

The app is documented in Markdown files in this repository (`README.md` and
`docs/*.md`). Every meaningful change to the app must be accompanied by an
update to these files in the same commit.
