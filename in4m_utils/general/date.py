from datetime import date, datetime, timedelta
from time import strptime

DATE_FORMAT = "%Y-%m-%d"

def _validate_date(ref_date, format=DATE_FORMAT):
    if isinstance(ref_date, str):
        return strptime(ref_date, format)
    if isinstance(ref_date, (date, datetime)):
        return ref_date
    raise ValueError("Unexpected date format")


def get_date_range(from_date, to_date):
    """takes dates in the format "YYYY-MM-DD" or in datetime, date  formats and returns a range of dates in between
    the range

    Args:
        from_date (str, datetime or date): start date inclusive
        to_date (str, datetime or date): end date inclusive

    Returns:
        date_range ([date]): list of dates in the range
    """
    from_date = _validate_date(from_date)
    to_date = _validate_date(to_date)

    date_range = []
    temp_date = from_date

    while temp_date <= to_date:
        date_range.append(temp_date)
        temp_date += timedelta(days=1)

    return date_range


def get_date_component(ref_date, format=DATE_FORMAT):
    """ takes a date object and returns a string with the given format

    Args:
        ref_date (datetime, or str): date in datetime or string format
        format (str): desired format of an output date

    Returns:
        str: date in the desired format
    """
    ref_date = _validate_date(ref_date)
    return ref_date.strftime(format)


