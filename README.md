# My Chart

A local, read-only live trading review dashboard for studying intraday stock price action.

The app combines a Flask backend, Alpaca market data, Lightweight Charts, deterministic chart-analysis engines, editable manual drawing tools, local paper-trade planning, chart audit tools, and optional read-only AI review.

This project is built for manual chart study, trade planning, and review. It is **not** an auto-trading system.

## Safety First

This project is read-only.

It does **not**:

- place trades
- send Alpaca orders
- connect to Webull
- automate entries or exits
- override manual confirmation
- let AI override backend gates
- treat chart drawings as trade orders

Every review is educational chart context only.

> Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.

## What The App Does

The main page gives a live intraday chart with:

- stock/ETF symbol input
- 1-minute, 5-minute, and 15-minute timeframes
- live Alpaca stream updates
- validated candle display
- VWAP
- EMA9 / EMA20
- premarket high / low
- previous day high / low / close
- high of day / low of day
- opening 5-minute high / low
- support and resistance
- supply and demand zones
- Fair Value Gaps
- liquidity sweeps
- level clusters
- reaction zones
- confirmation setup context
- Clean Mode / Full Mode layer controls
- Line Audit
- Candle Compare
- local paper-trade planner
- editable TradingView-style drawing tools
- optional AI chart review
- performance dashboard for setup review

## Tech Stack

- Python 3
- Flask
- Alpaca market data APIs
- Alpaca websocket stream
- Lightweight Charts
- Vanilla JavaScript
- Browser `localStorage` for local chart drawings and local paper-trade planner state
- Optional OpenAI API review flow

## Main Files

```text
server_stream.py                 Flask app, APIs, Alpaca data, stream, chart engines, AI review
static/index_stream.html         Main chart page served at /
static/app_stream.js             Chart frontend, overlays, controls, audits, paper planner, live updates
static/drawing_tools.js          Editable TradingView-style manual drawing tools
static/chart.css                 Main chart stylesheet
docs/reference/                  Archived drawing-page reference files (not used by the app)
docs/drawing_tools.md            Drawing tools documentation
docs/ai_trading_playbook.md      AI review doctrine and safety rules
tests/                           Deterministic test suite
README.md                        Project documentation
```

## Main URL

Start the Flask server and open:

```text
http://127.0.0.1:8900/
```

The normal page is the main chart page. Drawing tools are loaded directly on this page.

The older drawing-only page and stylesheet live in `docs/reference/` as backups only. Normal use happens on `/`.

## Installation

From the project folder:

```bash
cd "/Users/DayTrade/Documents/my chart"
```

Create or activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install required packages:

```bash
python3 -m pip install -r requirements.txt
```

If future features add packages, install them into the same `.venv`.

## Environment Variables

Use a local `.env` file. Never commit real API keys.

Required for Alpaca market data:

```text
ALPACA_API_KEY=your_alpaca_key_here
ALPACA_SECRET_KEY=your_alpaca_secret_here
ALPACA_STOCK_FEED=sip
```

If SIP is not available on the Alpaca account, use:

```text
ALPACA_STOCK_FEED=iex
```

Optional OpenAI review:

```text
OPENAI_API_KEY=your_openai_key_here
OPENAI_MODEL=gpt-4o-mini
ENABLE_AI_AUTO_REVIEW=false
```

The app also attempts to load environment variables from:

```text
.env
~/elite_scanner/.env
```

## Run Locally

From the repo folder:

```bash
cd "/Users/DayTrade/Documents/my chart"
source .venv/bin/activate
python3 server_stream.py
```

Leave that terminal open.

Open the chart:

```text
http://127.0.0.1:8900/
```

Open the performance dashboard:

```text
http://127.0.0.1:8900/performance
```

To stop the server, press:

```text
Control + C
```

If port `8900` is already in use:

```bash
lsof -nP -iTCP:8900 -sTCP:LISTEN
kill -9 <PID>
python3 server_stream.py
```

## Chart Page Features

### Symbols

The app defaults to `AAPL`, but the chart supports other stock/ETF symbols that Alpaca can return.

Examples:

```text
AAPL
SPY
QQQ
NVDA
MSFT
AMD
TSLA
```

The backend validates symbol format before requesting market data.

### Timeframes

Supported intraday timeframes:

```text
1Min
5Min
15Min
```

The frontend displays them as:

```text
1m / 5m / 15m
```

### Candle Accuracy

Candle quality is a major part of the project.

The backend preserves raw Alpaca data for audit, validates candles, and rebuilds higher timeframes from validated lower-timeframe data where needed.

The app tracks:

- raw provider candles
- validated 1-minute candles
- rebuilt 5-minute and 15-minute candles
- rejected candles
- suspicious candles
- displayed candles
- data-quality status

Possible chart quality states include:

```text
CLEAN
WARNING
DEGRADED
```

### Clean Mode

Clean Mode is the default trading view.

It focuses on the most important live chart structure and hides or mutes lower-priority clutter.

Clean Mode prioritizes:

- current price
- core levels
- relevant support/resistance
- relevant supply/demand
- active FVG context
- candle warnings
- backend-approved read-only marker only if strict gates pass

Full Mode/audit panels can show more research and debugging context.

## Drawing Tools

Drawing tools are loaded on the normal chart page.

They are browser-local only and save to `localStorage` by symbol and timeframe.

They do not call the backend, place trades, or send broker orders.

### Included Drawing Tools

- Select
- Delete
- Trend line
- Ray
- Extended line
- Horizontal line
- Vertical line
- Rectangle / zone box
- Brush / freehand
- Arrow
- Text label
- Price range
- Risk/reward box
- Fibonacci retracement
- Undo
- Lock drawings
- Clear all drawings
- Color control
- Line-width control

### Drawing Interaction Model

Drawings start locked so the chart can pan, zoom, and scroll normally.

Use the toolbar like this:

```text
Locked = normal chart movement
Unlocked = drawing/editing mode
```

### Editing Drawings

- Pick a drawing tool to create a drawing.
- Pick **Select** or press `V` to select an existing drawing.
- Drag a selected drawing body to move it.
- Drag handles to edit endpoints, box corners, text anchors, range anchors, or fib/risk-reward anchors.
- Double-click a text label to edit the text.
- Press `Delete` or `Backspace` to remove the selected drawing.
- Press `Esc` to cancel a draft or deselect.
- Press `Cmd+Z` / `Ctrl+Z` to undo.
- Use color and width controls to update the selected drawing style.

### Drawing Storage

Drawings are scoped by:

```text
symbol:timeframe
```

For example:

```text
AAPL:1Min
AAPL:5Min
SPY:1Min
```

This means drawings can be different for each symbol and timeframe.

## Paper Trade Planner

The paper trade planner is a local planning tool.

It can track simulated entries, stops, targets, quantity/contracts, and notes.

It does **not** send real orders.

The planner is useful for:

- planning risk before entry
- marking stop-loss and take-profit zones
- reviewing ideas after the move
- practicing discipline without automation

## Line Audit

Line Audit explains the deterministic chart lines and zones.

It can show:

- id
- type
- label
- price or range
- source engine
- reason
- status
- confidence
- priority
- Clean Mode visibility
- hidden reason
- FVG proof details
- support/resistance details
- supply/demand context

Open it from the chart with the **Line Audit** button.

Useful endpoint:

```text
/api/debug/chart-lines?symbol=AAPL&timeframe=5Min
```

## Candle Compare

Candle Compare helps inspect raw vs validated/rebuilt candle behavior.

Use it to confirm suspicious provider candles are not corrupting displayed candles or calculations.

Useful endpoint:

```text
/api/debug/candles?symbol=AAPL&timeframe=5Min
```

## AI Review

AI review is optional and read-only.

The backend builds a structured chart snapshot and sends it to the OpenAI API only when configured and requested.

AI review may discuss:

- current chart context
- trend and structure
- support/resistance
- supply/demand
- FVG context
- market alignment
- no-trade conditions
- options risk notes
- contract quality warnings when available
- risk/reward context

AI review cannot:

- place trades
- send orders
- override backend gates
- remove risk warnings
- claim certainty
- tell the user to chase

If no fresh OpenAI review has been requested, the app returns a safe fallback review.

## Performance Dashboard

Open:

```text
http://127.0.0.1:8900/performance
```

The performance dashboard reviews logged setup outcomes and helps separate:

- tradable setups
- research/no-trade setups
- missed moves
- weak signals
- outcome patterns

## Useful API Endpoints

```text
GET /api/chart?symbol=AAPL&timeframe=1Min
GET /api/chart?symbol=AAPL&timeframe=5Min
GET /api/chart?symbol=AAPL&timeframe=15Min
GET /api/stream?symbol=AAPL&timeframe=1Min
GET /api/debug/candles?symbol=AAPL&timeframe=5Min
GET /api/debug/chart-lines?symbol=AAPL&timeframe=5Min
GET /api/ai/snapshot?symbol=AAPL&timeframe=5Min
GET /api/ai/latest-review?symbol=AAPL
GET /api/ai/review-current-chart?symbol=AAPL&timeframe=5Min
GET /api/debug/setup-performance?limit=500
GET /performance
```

## Local Development Checks

Before pushing changes, run:

```bash
python3 -m py_compile server_stream.py
python3 -m compileall -q .
node --check static/app_stream.js
node --check static/drawing_tools.js
python3 -m unittest discover -s tests -v
git diff --check
```

If tests fail, fix them before committing.

## Suggested Smoke Test

After starting the server, open:

```text
http://127.0.0.1:8900/
```

Then check:

- chart loads
- AAPL loads by default
- symbol input works
- 1m / 5m / 15m buttons work
- chart can pan/zoom when drawings are locked
- drawing toolbar appears
- trend line can be drawn
- selected drawings show handles
- drawings can be moved and resized
- Delete removes selected drawing
- `V`, `Esc`, and undo shortcuts work
- Clean Mode toggle works
- PMH/PML and PDH/PDL display when available
- FVGs render when available
- Line Audit opens
- Candle Compare opens
- Paper Trade Planner opens
- AI latest review endpoint responds
- `/performance` loads
- browser console has no errors

## Git Workflow

Normal update flow:

```bash
git pull origin main
python3 server_stream.py
```

After local changes:

```bash
git status
git add <files>
git commit -m "Describe the change"
git push origin main
```

## Secrets And Local Files

Never commit:

- `.env`
- API keys
- account tokens
- local exports
- zip backups
- virtual environments

Local review exports and generated files should stay local unless intentionally sanitized.

## Current Limitations

- Local development Flask server only; not production deployment.
- Live market behavior must be tested during active market sessions.
- Exchange holidays should be verified manually if holiday-calendar logic is not enabled.
- Market data can still contain provider anomalies, even with validation.
- FVGs, zones, support/resistance, drawings, and AI review are context only.
- No order execution is implemented.
- No Webull connection is implemented.
- Drawing tools are intentionally browser-local; drawings do not sync across devices unless that feature is added later.
- Brush/freehand drawings can be moved as a whole, but point-by-point brush editing is a future enhancement.

## Project Philosophy

This app is designed to help a trader slow down, read the chart clearly, plan risk, and avoid chasing.

The dashboard should support discipline, not replace it.

Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.
