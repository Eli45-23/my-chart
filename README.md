# AAPL Live Chart Test

Standalone AAPL live chart prototype using Alpaca market data.

## Main files

- `server_stream.py` — Flask backend, Alpaca SIP/streaming data, indicators, zones, and API endpoints.
- `static/index_stream.html` — chart page.
- `static/app_stream.js` — chart frontend and layer drawing.
- `.gitignore` — keeps local secrets, virtual environment files, and backup checkpoints out of Git.

## Local run

```bash
cd ~/aapl_live_chart_test
source .venv/bin/activate
python server_stream.py
```

Then open:

```text
http://127.0.0.1:8900/?v=stream14
```

## Notes

This is a read-only chart prototype. It does not place trades, send orders, or give buy/sell signals.
