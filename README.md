# My Chart — Trader-First Live Trading Review

A read-only live market chart and review dashboard for studying intraday price action with accurate candles, key market levels, Fair Value Gaps, supply/demand, support/resistance, and optional AI-assisted review.

The project is built for manual chart review and trading education. It is not an auto-trader.

## Current Focus

The chart is being simplified around the tools that matter most for live decision-making:

- Accurate candles
- PDH / PDL
- PMH / PML
- High of day / low of day
- First 5-minute candle high / low
- Fair Value Gaps / FVGs
- Supply and demand zones
- Support and resistance
- Line Audit / Candle Compare for trust and debugging
- Optional AI review for read-only commentary

Everything else is secondary, muted, hidden in Clean Mode, or reserved for Full Mode / audit review.

## Safety

This project is read-only.

It does **not**:

- place trades
- send Alpaca orders
- connect to Webull
- automate entries or exits
- override manual user confirmation
- allow AI to override backend gates

Every review is educational chart context only.

> Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.

## Main Features

### Market Data And Symbols

- Alpaca live data integration
- AAPL default symbol
- Multi-symbol support for AAPL, SPY, QQQ, NVDA, MSFT, AMD, TSLA, and other supported stock/ETF symbols
- Symbol-aware chart APIs
- Related-market context per symbol when available
- 1Min, 5Min, and 15Min timeframes
- Eastern Time chart formatting

### Candle Accuracy And Data Integrity

Candles are treated as the foundation of the chart.

- Raw Alpaca candles are preserved for audit.
- `1Min` candles are validated before display/calculation use.
- `5Min` and `15Min` candles are rebuilt from validated `1Min` candles.
- Suspicious/rejected candles are blocked from corrupting indicators, levels, zones, AI, setup grading, and marker gates.
- The chart exposes candle quality states: `CLEAN`, `WARNING`, and `DEGRADED`.
- The chart shows whether candles are `VALIDATED` or `REBUILT_FROM_1MIN`.
- The known bad AAPL raw Alpaca print at `290.403` is preserved for audit and rejected from display/calculations.

Useful debug endpoint:

```text
/api/debug/candles?symbol=AAPL&timeframe=5Min
```

### Trader Clean Mode

Clean Mode is the default trading view. It is intended to show only the most useful live trading structure.

Clean Mode focuses on:

- candles
- current price
- PDH / PDL
- PMH / PML
- HOD / LOD
- opening 5-minute high / low
- active FVGs
- relevant supply/demand zones
- relevant support/resistance
- candle data warnings
- backend-approved read-only marker only if all strict gates pass

Clean Mode hides or mutes:

- failed zones
- weak zones
- research-only context
- low-priority clutter
- duplicate nearby levels
- blocked setup triggers
- weak/filled FVGs

Full Mode remains available for audit/debugging and can show more deterministic context.

### Key Levels

The chart tracks and displays core levels:

- `PDH` — previous day high
- `PDL` — previous day low
- `PDC` — previous day close, shown in audit/context when useful
- `PMH` — premarket high
- `PML` — premarket low
- `HOD` — high of day from validated candles
- `LOD` — low of day from validated candles
- `OPEN 5M HIGH` — high of the completed 9:30–9:35 ET candle
- `OPEN 5M LOW` — low of the completed 9:30–9:35 ET candle

These are core trading levels and should not be treated as research clutter.

### Fair Value Gap / FVG Engine

The chart includes a deterministic FVG engine for 1Min, 5Min, and 15Min.

A bullish FVG is based on the strict 3-candle imbalance:

```text
candle 1 high < candle 3 low
bottom = candle 1 high
top = candle 3 low
midpoint = (top + bottom) / 2
```

A bearish FVG is based on:

```text
candle 1 low > candle 3 high
top = candle 1 low
bottom = candle 3 high
midpoint = (top + bottom) / 2
```

The FVG box represents only the imbalance between candle 1 and candle 3. It is not the whole impulse move and it is not the demand/supply base.

FVG objects include status and context such as:

- `ACTIVE`
- `PARTIALLY_FILLED`
- `FILLED`
- top / bottom / midpoint
- fill percentage
- touch count
- quality score / grade
- confluence context
- Line Audit metadata

FVGs are context only. They are not automatic entries.

### Supply / Demand And Zone Reactions

The chart detects and audits supply/demand zones and reaction behavior.

Reaction labels include:

- Demand `HOLD`
- Demand `RECLAIM`
- Demand `FAILED`
- Supply `HOLD`
- Supply `REJECTION`
- Supply `FAILED`

Failed and weak zones are not allowed to create misleading trade excitement. Failed demand cannot create bullish demand setups, and failed supply cannot create bearish supply setups. Weak zones are capped at watch context.

### Support And Resistance

Support/resistance levels are scored and audited. Trader Clean Mode should show the nearest relevant support/resistance context without flooding the chart with every possible level.

### Chart Line Audit

The deterministic Chart Line Audit explains every plotted line/zone/level that the chart knows about.

It tracks:

- id
- type
- label
- price or range
- source
- calculation method
- reason
- status
- confidence
- priority
- Clean Mode visibility
- hidden reason, when hidden

Open the **Line Audit** panel on the chart, or use:

```text
/api/debug/chart-lines?symbol=AAPL&timeframe=5Min
```

No source means no line. No valid calculation means the item should not be trusted or displayed as actionable.

### Candle Compare

The Candle Compare tool is used to inspect raw vs validated/rebuilt candle behavior and to confirm that rejected Alpaca bad prints are not being displayed or used in calculations.

Use it from the chart UI or related debug endpoints.

### Rebuild Chart Data

The **Rebuild Chart Data** button clears/rebuilds chart data from validated candles. Use it before the open or whenever the chart needs a clean reload.

Recommended before market open:

```text
9:25–9:29 AM ET: click Rebuild Chart Data
```

### Performance Dashboard

The `/performance` dashboard reviews logged setup outcomes.

Views include:

- Tradable Setups
- Research / NO_TRADE
- All

The default view is focused on tradable setups and excludes `NO_TRADE` rows by default. Research/no-trade outcomes remain available separately for tuning and missed-move study.

Open:

```text
http://127.0.0.1:8900/performance
```

### AI Trade Review

AI Trade Review is optional and read-only. It uses chart data, market context, candle warnings, zones, levels, FVG context, risk rules, and the trading playbook to explain what the chart is showing.

AI can discuss:

- trend
- candles/wicks
- FVG context
- support/resistance
- supply/demand
- market context
- no-trade conditions
- options contract quality when available
- risk/reward

AI cannot place trades, send broker orders, connect to Webull, or override backend safety gates.

If no fresh OpenAI review has been requested, latest-review may return a fallback message such as `No fresh AI review requested.`

## Main Files

- `server_stream.py` — Flask backend, Alpaca stream, chart APIs, candle validation, levels, zones, FVGs, AI review context
- `static/index_stream.html` — main chart page
- `static/app_stream.js` — chart frontend, drawing logic, controls, Clean Mode, Line Audit, Candle Compare
- `docs/ai_trading_playbook.md` — AI review doctrine and trading safety rules
- `tests/` — deterministic tests for candle/FVG/zone logic
- `.gitignore` — keeps secrets, virtual environments, local exports, and zip files out of GitHub

## Requirements

Python 3 is required.

Install core packages inside the virtual environment:

```bash
python3 -m pip install flask python-dotenv websocket-client requests openai
```

Depending on your local environment, more packages may be required by future features.

## Environment Variables

Use a local `.env` file. Never commit real keys.

Required for Alpaca data:

```text
ALPACA_API_KEY=your_alpaca_key_here
ALPACA_SECRET_KEY=your_alpaca_secret_here
ALPACA_STOCK_FEED=sip
```

Use `iex` if SIP is not available on the account.

Optional OpenAI review:

```text
OPENAI_API_KEY=your_openai_key_here
OPENAI_MODEL=gpt-4o-mini
ENABLE_AI_AUTO_REVIEW=false
```

Never paste real keys into chat or commit them to GitHub.

## Run Locally

From the project folder:

```bash
cd "/Users/DayTrade/Documents/my chart"

lsof -ti :8900 | xargs kill -9 2>/dev/null || true

source .venv/bin/activate
set -a
source .env
set +a

python3 server_stream.py
```

Leave that terminal open.

Open the chart:

```text
http://127.0.0.1:8900/?v=stream43
```

Open the dashboard:

```text
http://127.0.0.1:8900/performance
```

To stop the server, press `Control + C` in the server terminal.

## Pre-Market Test Routine

Before live testing:

```bash
cd "/Users/DayTrade/Documents/my chart"
source .venv/bin/activate
set -a
source .env
set +a

python3 -m py_compile server_stream.py
python3 -m compileall -q .
node --check static/app_stream.js
python3 -m unittest discover -s tests -v
git diff --check
```

Start the server, open the chart, and check:

- AAPL, SPY, QQQ load
- 1Min, 5Min, 15Min work
- candles match expected market movement
- candle badge is `CLEAN` or clearly explains `WARNING`
- Rebuild Chart Data works
- PDH/PDL, PMH/PML, HOD/LOD, OPEN 5M levels show
- FVG boxes and midpoint lines render correctly
- supply/demand and support/resistance are visible when relevant
- Line Audit opens
- Candle Compare opens
- `/performance` loads
- AI source is `openai` after requesting a fresh AI review

## Useful API Endpoints

```text
/api/chart?symbol=AAPL&timeframe=5Min
/api/debug/candles?symbol=AAPL&timeframe=5Min
/api/debug/chart-lines?symbol=AAPL&timeframe=5Min
/api/ai/snapshot?symbol=AAPL&timeframe=5Min
/api/ai/latest-review?symbol=AAPL
/api/debug/setup-performance?limit=500
```

Generic symbol/timeframe support should work with `AAPL`, `SPY`, `QQQ`, and other supported symbols.

## Export / Review Notes

Local review exports and zip files are ignored by Git:

```text
exports/
*.zip
```

Use exports for end-of-day review, but do not commit them.

## Current Limitations

- This is a local development Flask server, not a production deployment.
- Live behavior must be tested during active market sessions.
- Exchange-holiday logic is not fully implemented and should be verified manually.
- Candle validation can protect against obvious bad prints, but raw market data should still be cross-checked when something looks wrong.
- FVGs, zones, support/resistance, and AI review are context only, not trade commands.
- No order execution is implemented.
- No Webull connection is implemented.
- macOS Python may show a LibreSSL / urllib3 warning even when requests still work.

## Important Note

This project is for educational chart review only.

Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.
