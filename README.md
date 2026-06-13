# AAPL Live Chart Test

Standalone read-only AAPL live chart prototype.

## Purpose

This project is for testing chart visuals and market-structure overlays before moving anything into Mr. Scanner.

It is **read-only**:

- no order placement
- no auto-trading
- no Telegram alerts
- no broker actions

## Main files

- `server_stream.py` — Flask backend, Alpaca SIP live trade stream, chart API, indicators/zones
- `static/index_stream.html` — chart page
- `static/app_stream.js` — chart frontend and drawing logic
- `.gitignore` — keeps secrets, virtualenv, and local backup files out of GitHub

## Run locally

```bash
cd ~/aapl_live_chart_test
source .venv/bin/activate
python server_stream.py
```

Then open:

```text
http://127.0.0.1:8900/
```

The professional performance dashboard and AI Trade Review panel are available at:

```text
http://127.0.0.1:8900/performance
```

## Current chart layers

- live candles from Alpaca SIP stream
- VWAP
- EMA 9 / EMA 20
- PMH / PML
- PDH / PDL / PDC
- support / resistance reliability scoring
- supply / demand scoring
- liquidity sweep zones
- merged clusters
- 30-minute reaction zones

## AI Trade Review

AI Trade Review is a strict, read-only intraday chart and options review assistant. It uses structured chart data, a compact multi-timeframe snapshot, existing backend grading and risk rules, and the doctrine in `docs/ai_trading_playbook.md`.

From the AI panel inside `/performance`, you can review the current chart or ask questions about setups, confirmation, traps, risk/reward, market regime, and SPY/QQQ confirmation. The chart can display a possible-entry marker for a confirmed A or A+ setup only after every backend safety gate passes.

AI Trade Review:

- does not place trades
- does not send Alpaca orders
- does not connect to Webull
- does not automate entries or exits
- does not replace manual user confirmation
- cannot override backend marker gates

### Enable OpenAI Reviews

OpenAI-powered reviews are optional. Set environment variables before starting the server:

```bash
export OPENAI_API_KEY="your-key-here"
export OPENAI_MODEL="model-name-optional"
python server_stream.py
```

Never commit a real API key. When `OPENAI_API_KEY` is not configured, AI Trade Review continues working with deterministic chart logic and returns a warning that OpenAI is not configured.

Automatic OpenAI review is disabled by default:

```bash
export ENABLE_AI_AUTO_REVIEW=false
```

The chart may recommend requesting a review after meaningful setup or market events, but manual Review/Ask actions remain the primary trigger. The chart page only reads the latest review and does not call OpenAI.

### AI Entry Marker

The read-only AI entry marker is allowed only when strict backend gates pass, including:

- setup is confirmed and graded A or A+
- risk/reward is `OK` or `GOOD`
- setup is not failed or invalidated
- regime is not `CHOP`
- action label is not `NO_NEW_TRADES`
- market confirmation is not directly against the setup
- suggested entry and invalidation are valid
- current price is not too extended from the suggested entry

Mixed, opposing, no-trade, choppy, weak-risk/reward, or extended conditions block the marker. An allowed marker includes:

```text
ENTER TRADE SETUP
POSSIBLE ENTRY — NOT AN ORDER
```

### Trading Playbook

`docs/ai_trading_playbook.md` defines the AI assistant's professional trading doctrine, options-risk knowledge, setup standards, marker rules, decision language, and safety boundaries. Its educational principles are grounded in summarized material from the OCC Options Disclosure Document, FINRA options investor education, Cboe Options Institute, and SEC Investor.gov. It does not copy or replace those source documents.

The playbook guides interpretation only. Backend chart logic, structured snapshot data, and hard gates remain the source of truth.

Every review follows this safety doctrine:

> Read-only review. Not financial advice. Not an order. Confirm manually. Do not chase.

## Notes

Reaction zones are short-term watch areas only. They are meant to show where price is reacting in the most recent 30 minutes. They are not trade signals.
