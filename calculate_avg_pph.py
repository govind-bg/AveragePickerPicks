# Get data from here:

# LOGIN/LOGOUT - https://sunflower.kb.us-central1.gcp.cloud.es.io:9243/app/discover?#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'2022-11-08T05:00:00.000Z',to:'2022-12-17T04:59:00.000Z'))&_a=(columns:!(EventType,UserId),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'05a29340-0f59-11ed-bd81-e7f3585b181b',key:EventType,negate:!f,params:!(UserLogout,UserLogin),type:phrases),query:(bool:(minimum_should_match:1,should:!((match_phrase:(EventType:UserLogout)),(match_phrase:(EventType:UserLogin))))))),index:'05a29340-0f59-11ed-bd81-e7f3585b181b',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))

import cv2
import time
import sys
from pandas import *
from datetime import datetime
import re
import numpy as np
import glob
from matplotlib import pyplot as plt
from openpyxl import load_workbook
import pandas as pd

# used to measure the total time the program was run for

time_start = time.time()


def find_time_delta(logout_time, login_time):
    """
    Gets the time difference between 2 timestampts whie
    iterating through the loop in main script. Not
    returning in string format because we need to add it
    in the main loop. Returns time in minutes
    """
    login_time = datetime.strptime(
        login_time, "%b %d, %Y @ %H:%M:%S.%f")
    logout_time = datetime.strptime(
        logout_time, "%b %d, %Y @ %H:%M:%S.%f")

    time_delta = logout_time - login_time

    # returns (minutes, seconds) tuple format
    time_minutes = divmod(time_delta.total_seconds(), 60)

    # this returns the time in minutes
    time_minutes = time_minutes[0] + (time_minutes[1]/60)
    time_minutes = round(time_minutes, 2)
    return time_minutes


# reading the raw file

login_logout_data = "source_files/login_logout.csv"
picker_data = "source_files/pickdata.csv"
data = read_csv(login_logout_data)

# convert all the messages to list

logs = data["message"].tolist()

# convert all the time stamps to list

time_stamps = data["@timestamp"].tolist()

