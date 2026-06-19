"""Pure Eastern-time market session helpers."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


def today_et():
    return datetime.now(ET).date()


def et_datetime(day, hour, minute=0):
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def build_market_session_status(now=None):
    current = now or datetime.now(ET)
    if current.tzinfo is None:
        current = current.replace(tzinfo=ET)
    else:
        current = current.astimezone(ET)

    is_weekend = current.weekday() >= 5
    minutes = current.hour * 60 + current.minute
    is_premarket = not is_weekend and 4 * 60 <= minutes < 9 * 60 + 30
    is_regular = not is_weekend and 9 * 60 + 30 <= minutes < 16 * 60
    is_after_hours = not is_weekend and 16 * 60 <= minutes < 20 * 60

    if is_weekend:
        session_label, closed_reason = "CLOSED", "Weekend"
    elif is_premarket:
        session_label, closed_reason = "PREMARKET", None
    elif is_regular:
        session_label, closed_reason = "REGULAR", None
    elif is_after_hours:
        session_label, closed_reason = "AFTER_HOURS", None
    else:
        session_label, closed_reason = "CLOSED", "Outside supported session hours"

    return {
        "timezone": "America/New_York",
        "current_time_et": current.isoformat(),
        "date_et": current.date().isoformat(),
        "weekday": current.strftime("%A"),
        "is_weekend": is_weekend,
        "is_regular_session_open": is_regular,
        "is_premarket_open": is_premarket,
        "is_after_hours_open": is_after_hours,
        "is_market_open_for_trading": bool(is_premarket or is_regular or is_after_hours),
        "session_label": session_label,
        "market_closed_reason": closed_reason,
        "regular_session_hours_et": "09:30-16:00",
        "premarket_hours_et": "04:00-09:30",
        "after_hours_et": "16:00-20:00",
        "holiday_calendar_enabled": False,
        "holiday_warning": "Holiday calendar not implemented; verify exchange holidays manually.",
        "read_only": True,
    }


def previous_weekday(day):
    previous = day - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous
