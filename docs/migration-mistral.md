# Migration plan: Anthropic → Mistral API

Status: steps 1-2 done; step 3 tooling in place (compare mode); steps 4-5
pending. The app currently runs Claude via the Anthropic API. This document is the
step-by-step plan to switch it to the Mistral API without interrupting the
daily scheduled runs.

## Current state

The Claude dependency is confined to a few places:

- `wrap.py` — `ANTHROPIC_URL` / `ANTHROPIC_VERSION` constants, the
  `call_claude()` function (payload shape, auth headers, response parsing),
  and `env("ANTHROPIC_API_KEY")` in `main()`
- `config.yml` — `model: "claude-sonnet-5"`
- `.github/workflows/wrap.yml` — the `ANTHROPIC_API_KEY` secret passthrough

Everything else (Feedbin fetching, state handling, Atom rendering, the safe
write-then-advance ordering) is provider-agnostic.

## API differences to handle

| | Anthropic (now) | Mistral (target) |
|---|---|---|
| Endpoint | `POST https://api.anthropic.com/v1/messages` | `POST https://api.mistral.ai/v1/chat/completions` |
| Auth | `x-api-key` header | `Authorization: Bearer <key>` |
| System prompt | top-level `system` field | a `{"role": "system"}` message in `messages` |
| Response text | `data["content"]` blocks | `data["choices"][0]["message"]["content"]` (plain string) |
| Stop signal | `stop_reason` | `choices[0]["finish_reason"]` |
| Token usage | `usage.input_tokens` / `output_tokens` | `usage.prompt_tokens` / `completion_tokens` |
| Retryable codes | 429, 5xx, 529 | 429, 5xx (529 does not exist) |

The JSON-extraction logic and the prompt text carry over unchanged.

## Model choice

Target model: `mistral-medium-latest`. It is a good cost/quality fit for the
editorial curation job (selection against the interests profiles, two tight
lines per pick, strict JSON output). If selection quality disappoints during
validation, `mistral-large-latest` is the fallback to try before touching
anything else.

## Steps

### Step 1 — Add Mistral support alongside Claude (no cutover) — DONE

Implemented as described below: `provider` key in `config.yml` (default
`anthropic`), `call_mistral()` alongside `call_anthropic()` in `wrap.py`,
and the Mistral-specific `finish_reason="length"` truncation hint.

Original plan:

- Refactor `call_claude()` into a thin provider layer in `wrap.py`: a new
  `call_mistral()` implementing the Mistral request/response shape per the
  table above.
- New `provider` key in `config.yml` (values: `anthropic` or `mistral`,
  default `anthropic`), with the model set per provider.
- `main()` picks the call function and env var (`ANTHROPIC_API_KEY` or
  `MISTRAL_API_KEY`) based on `provider`.
- Deploy with `provider: anthropic` unchanged — behaviour stays identical, so
  the scheduled runs keep using Claude without interruption while the Mistral
  path is validated.
- Error-hint gotcha: the Anthropic path advises on `stop_reason=max_tokens`;
  the Mistral equivalent is `finish_reason="length"`. The refactor must map
  this correctly or truncation failures become confusing.

### Step 2 — Secrets and workflow — DONE

`MISTRAL_API_KEY` secret added by the owner; `wrap.yml` now passes both
keys.

- Add `MISTRAL_API_KEY` to the GitHub repository secrets.
- Update `wrap.yml` to pass the Mistral secret as well, so a branch or a
  config flip is all a test run needs.
- Keep `ANTHROPIC_API_KEY` in place until step 4 has settled, so rollback is
  one config line.

### Step 3 — Validation (side-by-side)

Comparison mode is built in: set `compare_providers: true` in `config.yml`
and each run queries both providers on the same batch, publishing two
entries per tag per run, suffixed `(anthropic)` and `(mistral)` in the
title (models from the `compare_models` section). Run it for a few days,
subscribing in Feedbin as usual, and compare:

- JSON parse reliability on real batches
- selection quality against each tag's `interests` profile
- truncation behaviour: check whether `max_tokens: 8000` needs adjusting for
  `mistral-medium-latest`
- token usage in the logs to compare cost

### Step 4 — Cutover

Flip `provider: mistral` in `config.yml`. The per-tag `since` timestamps in
`state.json` are untouched by the migration, so no entries fall through the
gap: the first Mistral run picks up where the last Claude run ended.

### Step 5 — Cleanup

After a few clean runs: remove the Anthropic path and the `ANTHROPIC_API_KEY`
secret if a single-provider setup is preferred, and update this file, the
README, `config.yml` comments, and the `wrap.py` docstring. A repository
rename is optional and out of scope; note that `site_url` and the Atom feed
`<id>` embed the current URL, so a rename needs a coordinated update.

## Rollback

At any point before cleanup: set `provider: anthropic` in `config.yml`. State
and output formats do not change during the migration, so nothing else is
affected.
