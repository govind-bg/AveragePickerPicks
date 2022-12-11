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

    print('Comparing datetime : login > logout ? : ', login_time<logout_time)
    print(login_time)
    print(logout_time)

logout_time = "Dec 10, 2022 @ 18:20:03.957"
login_time = "Dec 9, 2022 @ 13:27:30.472"

find_time_delta(logout_time,login_time)

# d1 = datetime.datetime(2018, 5, 3)
# d2 = datetime.datetime(2018, 6, 1)