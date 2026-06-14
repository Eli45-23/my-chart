# My Chart — Live Trading Review Dashboard

A read-only live market chart and AI-assisted review dashboard for studying intraday price action, supply/demand zones, liquidity sweeps, and options trade context.

This project is designed for manual trading education, live chart review, and testing market-structure tools before moving anything into a larger scanner system.

## What It Does

My Chart displays live and recent market data with professional price-action overlays. It helps review possible bullish or bearish setups using candles, wicks, market structure, VWAP, moving averages, prior-session levels, supply/demand zones, liquidity sweeps, volume context, and AI-assisted commentary.

AAPL is the default symbol. The chart also supports common stocks and ETFs such as SPY, QQQ, TSLA, NVDA, MSFT, and AMD.

## Safety

This project is read-only.

It does **not**:

- place trades
- send Alpaca orders
- connect to Webull
- automate entries or exits
- send Telegram alerts
- override manual user confirmation

Every review should be treated as educational context only.

## Main Features

- Live Alpaca SIP trade stream
- AAPL default chart
- Multi-symbol chart support
- 1Min, 5Min, and 15Min chart timeframes
- Eastern Time chart formatting
- Matching hover tooltip and bottom-axis time labels
- VWAP
- EMA 9 / EMA 20
- PMH / PML
- PDH / PDL / PDC
- Support and resistance reliability scoring
- Supply and demand zones
- Zone Reaction Engine
- Demand `HOLD`
- Demand `RECLAIM`
- Supply `HOLD`
- Supply `REJECTION`
- Failed zones
- Liquidity sweep zones
- Merged price clusters
- 30-minute reaction zones
- AI-assisted chart review
- Read-only volume/RVOL confirmation context
- Options contract-quality context when available
- `/performance` dashboard

## Main Files

- `server_stream.py` — Flask backend, Alpaca stream, chart APIs, indicators, zones, AI review context
- `static/index_stream.html` — main chart page
- `static/app_stream.js` — chart frontend, drawing logic, time formatting, and UI behavior
- `docs/ai_trading_playbook.md` — AI review doctrine and trading safety rules
- `.gitignore` — keeps secrets, virtual environments, and local backup files out of GitHub

## Requirements

Python 3 is required.

Install the core packages:

```bash
python3 -m pip install flask python-dotenv websocket-client requests
```

Depending on your local environment, more packages may be needed if new features are added.

## Environment Variables

Configure Alpaca credentials before starting the server:

```bash
export ALPACA_API_KEY="your-key-here"
export ALPACA_SECRET_KEY="your-secret-here"
```

OpenAI review is optional:

```bash
export OPENAI_API_KEY="your-key-here"
export OPENAI_MODEL="model-name-optional"
export ENABLE_AI_AUTO_REVIEW=false
```

Never commit real API keys or secrets.

## Run Locally

From the project folder:

```bash
cd "/Users/DayTrade/Documents/my chart"
python3 server_stream.py
```

Then open:

```text
http://127.0.0.1:8900/
```

Performance dashboard:

```text
http://127.0.0.1:8900/performance
```

## Testing Before Market Open

Run these checks before live testing:

```bash
python3 -m compileall .
python3 -m py_compile server_stream.py
node --check static/app_stream.js
git diff --check
```

Then start the server:

```bash
python3 server_stream.py
```

Check:

- AAPL 1Min, 5Min, 15Min
- SPY 1Min, 5Min, 15Min
- symbol switching
- timeframe switching
- hover time and bottom-axis time
- supply/demand zones
- zone reactions
- liquidity sweeps
- AI latest review
- browser console errors
- server terminal errors

## Chart Layers

The chart currently includes:

- live candles from Alpaca SIP stream
- VWAP
- EMA 9 / EMA 20
- PMH / PML
- PDH / PDL / PDC
- support/resistance scoring
- supply/demand scoring
- liquidity sweep zones
- merged clusters
- 30-minute reaction zones
- multi-timeframe context

## Zone Reaction Engine

The Zone Reaction Engine provides read-only context around nearby supply and demand zones.

Reaction labels include:

- `HOLD`
- `RECLAIM`
- `REJECTION`
- `FAILED`

These labels are watch context only. They are not trade signals and cannot create an order.

Strong reactions such as demand reclaims and supply rejections are preserved while later candles continue respecting the defended edge.

## AI Trade Review

AI Trade Review is a read-only review assistant. It uses structured chart data, market context, risk rules, and the trading playbook to explain possible setups.

It can discuss:

- trend
- momentum
- candles
- wicks
- liquidity sweeps
- traps
- support and resistance
- supply and demand
- SPY/QQQ confirmation
- options contract quality
- risk/reward
- no-trade conditions

The chart may display a possible-entry marker only when strict backend gates pass, including confirmed setup grade, acceptable risk/reward, market confirmation, non-chop regime, and valid entry/invalidation levels.

AI Trade Review cannot place trades, send broker orders, connect to Webull, or override backend safety gates.

## Trading Playbook

`docs/ai_trading_playbook.md` defines the AI assistant's trading doctrine, options-risk knowledge, setup standards, marker rules, decision language, and safety boundaries.

The playbook guides interpretation only. Backend chart logic, structured snapshot data, and hard gates remain the source of truth.

Every review follows this safety doctrine:

> Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.

## Current Limitations

- Live trade aggregation can only be fully tested during an active market session.
- Exchange-holiday logic is not fully implemented and should be verified manually.
- No order execution is implemented.
- No Webull connection is implemented.
- No automated test suite or dependency manifest is currently included.
- Some macOS Python environments may show a LibreSSL / urllib3 warning even when HTTPS requests still work.

## Important Note

This project is for educational chart review only.

Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.
