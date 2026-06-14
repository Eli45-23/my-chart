# AGENTS.md

## Project Purpose
This repository is for a local AAPL live chart and professional trading dashboard.
It includes:
- Live AAPL chart data
- Support/resistance, supply/demand, market regime, SPY/QQQ context
- Read-only AI Trade Review
- Performance dashboard
- Read-only possible-entry marker

This project is not an auto-trader.

## Absolute Safety Rules
- Never add trade execution.
- Never send Alpaca orders.
- Never connect to Webull for order placement.
- Never create auto-buy or auto-sell logic.
- Never create background order execution.
- Never make the AI place trades.
- Never tell the user to blindly enter or exit.
- Never remove read-only safety language.
- Never label AI output as financial advice.

## Secrets and API Keys
- Never commit `.env`.
- Never hardcode OpenAI, Alpaca, or any broker API keys.
- Never print API keys in logs.
- Use environment variables only.
- If secrets appear in git status, stop and warn the user.

## AI Trade Review Rules
- OpenAI is a read-only reviewer/coach only.
- Backend deterministic logic is the source of truth.
- AI cannot override strict grades, confirmation stages, risk/reward, market regime, or marker gates.
- AI cannot create entry markers by itself.
- AI responses must keep:
  - Read-only review
  - Not financial advice
  - Not an order
  - Confirm manually
  - Do not chase
- AI should prefer WAIT over forced trades.
- AI should explain uncertainty professionally.
- AI must not say:
  - buy now
  - sell now
  - guaranteed
  - this will work
  - ignore your stop

## Entry Marker Rules
The chart marker is read-only visual guidance.

Marker text must include:
- ENTER TRADE SETUP
- POSSIBLE ENTRY — NOT AN ORDER

Marker can only appear when backend gates allow it.
Do not let frontend JavaScript infer marker eligibility.
Do not let OpenAI override marker eligibility.

## Trading Logic Rules
Keep these engines deterministic:
- Confirmation setup detection
- Strict trade grading
- Risk/reward engine
- Market regime detection
- SPY/QQQ confirmation
- Volume/RVOL context
- Option chain/contract quality context
- Market session status

Trading logic changes must be separate from UI-only changes.

## UI Rules
For visual polish tasks:
- Do not change trading logic.
- Do not change AI prompt logic.
- Do not change marker gates.
- Do not change Alpaca/OpenAI behavior.
- Keep Clean Mode working.
- Keep chart interactions working.
- Keep `/performance` working.

## Required Checks
Run these when relevant:

```bash
python3 -m py_compile server_stream.py
node --check static/app_stream.js
git diff --check
```
