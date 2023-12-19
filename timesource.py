from datetime import datetime,timedelta

class TimeSource:
    def now(self):
        return datetime.now()