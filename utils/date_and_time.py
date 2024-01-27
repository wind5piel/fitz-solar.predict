from datetime import datetime


def generate_full_datetime(date_obj, time_str) -> datetime:
    
    """
        Generate a full datetime object by combining a date object and a time string.
    """
    
    time_obj = datetime.strptime(time_str, '%H:%M')
    datetime_obj = datetime.combine(date_obj.date(), time_obj.time())
    return datetime_obj