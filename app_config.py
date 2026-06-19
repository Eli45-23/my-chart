"""Static, non-secret application defaults shared by the chart backend."""

import re


SYMBOL = "AAPL"
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
TIMEFRAMES = {
    "1Min": 60,
    "5Min": 300,
    "15Min": 900,
}
AI_SNAPSHOT_CACHE_SECONDS = 20
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
