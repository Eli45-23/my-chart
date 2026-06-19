"""Pure Eastern-time market session helpers."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


def today_et():
    return datetime.now(ET).date()


def et_datetime(day, hour, minute=0):
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def _observed_fixed_holiday(year, month, day):
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year, month, weekday, occurrence):
    current = date(year, month, 1)
    current += timedelta(days=(weekday - current.weekday()) % 7 + (occurrence - 1) * 7)
    return current


def _last_weekday(year, month, weekday):
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    current = next_month - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _easter_sunday(year):
    """Return Gregorian Easter using the Meeus/Jones/Butcher computus."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def market_holidays(year):
    """Return full-day U.S. equities holidays observed in the supplied year."""
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Presidents' Day
        _easter_sunday(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),  # Memorial Day
        _observed_fixed_holiday(year, 6, 19),  # Juneteenth
        _observed_fixed_holiday(year, 7, 4),  # Independence Day
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        _observed_fixed_holiday(year, 12, 25),  # Christmas
    }
    # New Year's Day may be observed on the preceding December 31.
    holidays.add(_observed_fixed_holiday(year + 1, 1, 1))
    return holidays


def is_market_holiday(day):
    return day in market_holidays(day.year) or day in market_holidays(day.year - 1)


def build_market_session_status(now=None):
    current = now or datetime.now(ET)
    if current.tzinfo is None:
        current = current.replace(tzinfo=ET)
    else:
        current = current.astimezone(ET)

    is_weekend = current.weekday() >= 5
    is_holiday = not is_weekend and is_market_holiday(current.date())
    minutes = current.hour * 60 + current.minute
    is_premarket = not is_weekend and not is_holiday and 4 * 60 <= minutes < 9 * 60 + 30
    is_regular = not is_weekend and not is_holiday and 9 * 60 + 30 <= minutes < 16 * 60
    is_after_hours = not is_weekend and not is_holiday and 16 * 60 <= minutes < 20 * 60

    if is_weekend:
        session_label, closed_reason = "CLOSED", "Weekend"
    elif is_holiday:
        session_label, closed_reason = "CLOSED", "Market holiday"
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
        "holiday_calendar_enabled": True,
        "holiday_warning": None,
        "read_only": True,
    }


def previous_weekday(day):
    previous = day - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous
