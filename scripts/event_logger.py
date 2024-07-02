from datetime import datetime


class EventLogger:
    # class to log events in database

    @staticmethod
    def log_event(exception_obj):

        log_date = str(datetime.now().date())

        event_to_log = {"exceptionType": type(exception_obj).__name__,
                        'exceptionMessage': str(exception_obj),
                        'log_date': log_date}

        return event_to_log




