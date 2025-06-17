import datetime

def epoch_ms_to_datetime(epoch_ms: int) -> str:
    """
    Helper for converting timestamps in epoch milliseconds format to a datetime string.
    Timezone is always set to UTC.
    """
    epoch_s = epoch_ms * 0.001
    try:
        return datetime.datetime.fromtimestamp(epoch_s, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    except ValueError as ve:
        print("Given timestamp is out of the range of values supported by the platform C localtime() "
              "or gmtime() functions (i.e. the given timestamp is likely outside the year range of 1970 through 2038).")
        return ""
    
def ms_since(epoch_ms: int) -> int:
    """Returns the milliseconds passed since a given epoch milliseconds time (assumed since 1/1/1970 UTC)."""
    seconds_since_epoch_start = (datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds()
    return int(seconds_since_epoch_start * 1000) - epoch_ms

def hours_to_ms(hours: float) -> int:
    return int(hours * 3600000)

def ms_to_hours(ms: int) -> float:
    return float(ms / 3600000) # Note: multiply by precomputed reciprocal if division slow-down is significant.