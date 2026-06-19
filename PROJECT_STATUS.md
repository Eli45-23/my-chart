# Project Status

## Overall

- Completion estimate: 75%
- Last updated: 2026-06-19
- Application: local read-only chart and trading-review dashboard at `http://127.0.0.1:8900/`

## Completed Features

- Validated Alpaca candle pipeline with raw-vs-rebuilt audit support
- Multi-symbol intraday charting, live-stream state, and 1m/5m/15m views
- Core levels, support/resistance, supply/demand, strict FVGs, Clean Mode, and Line Audit
- Read-only AI review, deterministic marker gates, Candle Compare, and performance dashboard
- Local-only paper trade planner and editable chart drawing tools

## Known Issues / Manual Review

- Exchange-holiday recognition is not yet implemented; session status currently warns users to verify holidays manually.
- Local logs, exports, virtual environments, archives, and backups are ignored. No ignored secrets are tracked.
- Live Alpaca/OpenAI checks require the relevant local environment variables and account access.

## Next Priorities

1. Expand deterministic backend coverage and cautiously extract pure helpers.
2. Add holiday-calendar support and clarify paper-option planning limits.

## Safety Rules

- Read-only analysis only: no broker orders, Webull connection, or automatic trade execution.
- AI cannot override deterministic grading, risk/reward, regime, or entry-marker gates.
- Keep the required review language: not financial advice, not an order, confirm manually, do not chase.
- Never commit `.env`, API keys, tokens, local exports, backups, or virtual environments.

## Last Checks

Baseline recorded 2026-06-19:

- `python3 -m py_compile server_stream.py`: passed
- `python3 -m compileall -q .`: passed
- `node --check static/app_stream.js`: passed
- `node --check static/drawing_tools.js`: passed
- `python3 -m unittest discover -s tests -v`: passed (32 tests)
- `git diff --check`: passed

## Task History

| Task | Status | Result |
| --- | --- | --- |
| Baseline | Complete | Existing checks pass; local `server_stream.log` left untracked. |
| 1. Project status tracker | Complete | Added this living status document. |
| 2. Requirements file | Complete | Added `requirements.txt`; README now installs from it. |
| 3. Git ignore review | Complete | Expanded local/secrets ignore coverage; no ignored secrets or artifacts are tracked. |
| 4. Extract chart CSS | Complete | Moved main-page inline CSS into `static/chart.css` with a cache-busted stylesheet link. |
| 5. Archive drawing references | Complete | Moved obsolete drawing-only reference files into `docs/reference/`. |
| 6. Core backend tests | Complete | Added deterministic coverage for symbols, dates, session status, indicators, levels, and safe fallback review. |
| 7. Frontend smoke checklist | Complete | Added a reusable manual chart, drawing-tool, dashboard, and console checklist. |
| 8. Pure-helper module split | Pending | |
| 9. Market holiday calendar | Pending | |
| 10. Option paper-trade wording | Pending | |
