from datetime import datetime as dtm
from datetime import timedelta as tdm
import time


def calculate_time(moment_start):
    if type(moment_start) is float:
        return str(tdm(seconds=(int(time.time() - moment_start))))
    elif type(moment_start) is dtm:
        return str(tdm(seconds=(dtm.now() - moment_start).seconds))
    else:
        return ""
