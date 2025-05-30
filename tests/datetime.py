from src.cleaners.DatetimeCleaner import DatetimeCleaner

cleaner = DatetimeCleaner(
    parse_formats=["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"],
    format="%Y-%m-%d %H:%M:%S"
)
