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
http://127.0.0.1:8900/?v=stream14
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

## Notes

Reaction zones are short-term watch areas only. They are meant to show where price is reacting in the most recent 30 minutes. They are not trade signals.
