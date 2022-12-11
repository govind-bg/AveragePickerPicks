# Get data from here:

# https://sunflower.kb.us-central1.gcp.cloud.es.io:9243/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-6h,to:now))&_a=(columns:!(system_name,message,operator_id),filters:!(),index:ce572630-0f58-11ed-bd81-e7f3585b181b,interval:auto,query:(language:kuery,query:'(%22user_logout%22%20or%20%22user_login%22%20or%20(%22OB1-Interface%22%20and%20%22PICKCOMPLETE%22))%20'),sort:!(!('@timestamp',desc)))

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

    time_delta = logout_time - login_time

    # returns (minutes, seconds) tuple format
    time_minutes = divmod(time_delta.total_seconds(), 60)

    # this returns the time in minutes
    time_minutes = time_minutes[0] + (time_minutes[1]/60)
    time_minutes = round(time_minutes, 2)
    return time_minutes

# reading the raw file


RAW_FILE_NAME = "raw_data.csv"
data = read_csv(RAW_FILE_NAME)

logs = data["message"].tolist()

time_stamps = data["@timestamp"].tolist()

system_names = data["system_name"].tolist()

set_system_names = set(system_names)

pickers = set(data["operator_id"].tolist())

# empty dictionary that keeps count of every picker and how many
# picks they do

pick_count_dic = {}

pick_time_dic = {}

# ======= preparing dictionary to look like:

# {'station_num':
# {'login':{<each_picke>:[<list of logins>]}
# 'logout':[<list of logouts>]}
# }

for each_picker in pickers:

    pick_count_dic[each_picker] = 0

time_logged_in_dic = pick_count_dic.copy()

# removing unnecessary key
if '-' in time_logged_in_dic.keys():
    del time_logged_in_dic['-']

# countin the number of picks done by every picker

total_picks = 0

for msg in logs:
    if msg[0:3] == "OB1":
        msg_split = msg.split('^')
        # adding count of picks for every picker
        pick_count_dic[msg_split[-1]] += int(msg_split[-2])
        total_picks += int(msg_split[-2])

for each_station in set_system_names:

    pick_time_dic[each_station] = {}

for station_key, _ in pick_time_dic.items():
    for each_picker in pickers:
        pick_time_dic[station_key]['login'] = {}
        pick_time_dic[station_key]['login'][each_picker] = []
        pick_time_dic[station_key]['logout'] = []

# =======

count = 0

for msg in logs:

    if msg[0] == "{":
        msg_split = msg.split('"')
        station_name = system_names[count]
        if msg_split[3] == 'user_login':
            user_name = msg_split[7]
            try:
                pick_time_dic[station_name]['login'][user_name].append(
                    time_stamps[count])
            except KeyError:
                pick_time_dic[station_name]['login'][user_name] = []
                pick_time_dic[station_name]['login'][user_name].append(
                    time_stamps[count])
        else:
            pick_time_dic[station_name]['logout'].append(time_stamps[count])

    count += 1

# ======= Removing off packout / no logouts occured

packout_stations = ['bcs1', 'bcs2', 'bcs3',
                    'bcs4', 'bcs5', 'bcs6', 'bcs7', 'bcs8']

# no_logout_stations = set(no_logout_stations)

for packout_station in packout_stations:
    if packout_station in set_system_names:
        del pick_time_dic[packout_station]

# =======

# sort all the times together


for station_key_value, login_logout_status in pick_time_dic.copy().items():
    print('<------', station_key_value, ' ------>')
    logout_times = login_logout_status["logout"]

    for login_logout_stat, timestamp_list in login_logout_status.copy().items():

        if login_logout_stat != "logout":

            for user, time_list in timestamp_list.copy().items():
                print('\n')
                print(user, ' ::: login times ::: ', time_list)
                for login_time in time_list:
                    logout_time = [
                        i for i in logout_times if i > login_time][-1]
                    print('Login Time : ', login_time, ' | Logout Time : ', logout_time, " --- ",  find_time_delta(
                        logout_time, login_time), ' minutes')
                    try:
                        # keep adding to the time the user logged in
                        time_logged_in_dic[user] += find_time_delta(
                            logout_time, login_time)
                    except Exception as KeyError:
                        continue
    print('\n')

# ======= Print out the stats and the dictionaries

print('Dictionary for time stayed logged in (minutes): ')
print(time_logged_in_dic)
print('\n')

print('Dictionary for picks done : ')
print(pick_count_dic)
print('\n')

average_time_dictionary = {}

print('Complete Pick Stats : ')
print('\n')


for k, v in time_logged_in_dic.items():
    try:
        print(k, 'stayed logged in for ', v, ' minutes and completed ',
              pick_count_dic[k], 'picks')
        avg_pph = pick_count_dic[k]/(v/60)
        average_time_dictionary[k] = avg_pph
    except Exception as KeyError:
        continue

# removing unnecessary key
if '-' in pick_count_dic.keys():
    del pick_count_dic['-']


print('\n')
print('Average time per picker : ')
print(average_time_dictionary)
print('\n')

print('Total Picks done in this time frame : ', total_picks)
print('\n')

names = list(average_time_dictionary.keys())
list_avg_pph = list(average_time_dictionary.values())

plt.bar(names, list_avg_pph)
plt.grid()
plt.title('Average picks by every associate')
plt.xticks(rotation=90)
plt.xlabel('Associate Login ID')
plt.ylabel('Average PPH (login time adjusted)')

# =======


# ======= Lists prepared for excel writing

list_operators = list(time_logged_in_dic.keys())
list_picks = list(pick_count_dic.values())
list_login_time = list(time_logged_in_dic.values())
list_avg_pph = list(average_time_dictionary.values())

print(len(list_operators), len(list_picks),
      len(list_login_time), len(list_avg_pph))
writer = pd.ExcelWriter('output.xlsx', engine='openpyxl')
wb = writer.book
df = pd.DataFrame({'Operator': list_operators,
                   'Total Picks': list_picks,
                   'Total Login Time': list_login_time,
                   'Average PPH (adjusted for login time)': list_avg_pph})

df.to_excel(writer, index=False)
wb.save('output.xlsx')

# =======

plt.show()
