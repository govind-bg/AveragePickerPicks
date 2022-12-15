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

RAW_FILE_NAME = "csv_files/combined_csv.csv"
data = read_csv(RAW_FILE_NAME)

# convert all the messages to list

logs = data["message"].tolist()

# convert all the time stamps to list

time_stamps = data["@timestamp"].tolist()

# To calculate if some are still logged in, we assume they logged out at the
# end of the queried time range.
print('Sorting time to find the last data time / max_time .... ')
time_stamps_copy = time_stamps.copy()
time_stamps_copy.sort(key=lambda date: datetime.strptime(
    date, "%b %d, %Y @ %H:%M:%S.%f"))

# time_max is the last time for which we have the data
# reading from the copied list because we do not want to tamper with the
# original data

time_max = time_stamps_copy[-1]
print('Data captured till : ', time_max)

# converting system names to list and removing duplicate system names

system_names = data["system_name"].tolist()

# removing duplicate system names

set_system_names = set(system_names)

# converting operators to list and removing duplicate operator IDs

pickers = set(data["operator_id"].tolist())

# empty dictionary that keeps count of every picker and how many
# picks they do

# pick_count_dic : This dictionary keeps track of every pickers name and the number of picks they did.
# A sample format would be {picker_a : x, picker_b : y, ..... }
pick_count_dic = {}

# Keeps track of the login and logout times of every picker
pick_time_dic = {}

# ======= preparing dictionary to look like:

# {'station_num':
# {'login':{<each_picke>:[<list of logins>]}
# 'logout':[<list of logouts>]}
# }

# first we create a dictionary and assign 0 to each picker
for each_picker in pickers:

    pick_count_dic[each_picker] = 0

# creatinga copy of the picker dictionary
time_logged_in_dic = pick_count_dic.copy()

# removing unnecessary keys
if '-' in time_logged_in_dic.keys():
    del time_logged_in_dic['-']

# counting the number of picks done by every picker, can be used to validate with
# bulk data from kibana if all the info processed is correct

# basic counter that can be added to in every loop, adding upto the total counts
total_picks = 0


for msg in logs:
    # Logging data that counts for picks starts with OB1, and look like
    # OB1-Interface: Queuing message to send to the WMS: PICKCOMPLETE:8519217^00900777770000551919^0037650058^02026^BG11985^SPS10^4^rfoksa006
    # can extract the pick number from this

    if msg[0:3] == "OB1":

        try:
            # splitting messages at ^
            msg_split = msg.split('^')
            # adding picker name and associated picker
            pick_count_dic[str(msg_split[-1])] += int(msg_split[-2])
            # adding to total picks
            total_picks += int(msg_split[-2])

        except Exception as KeyError:
            print('Could not account for >>> ', str(msg_split[-1]))
            continue

# dictionary format:

# creating an empty dictionary of dictionaries where the design looks like
# {
#     system name{
#         'login':{
#             'user_name' : [<all_the_times_the_user_logged_into_this_very_station>]
#         }
#         'logout':{<all_the_times_someone_logged_out_of_this_cell>}
#     }
# }

for each_station in set_system_names:

    pick_time_dic[each_station] = {}

for station_key, _ in pick_time_dic.items():

    for each_picker in pickers:

        # for every picker keep a record of when all they logged in
        # sample login data looks like : {"message_type": "user_login", "user_id": "rfoksa039", "event_time": 1667940308.993677, "time": 1667940308.993671}
        pick_time_dic[station_key]['login'] = {}
        pick_time_dic[station_key]['login'][each_picker] = []

        # and for all the times the system was logged out. Due to the way the logs are recorded
        # we cannot see who logged out of a system, ONLY when the system was logged out
        # sample log out data looks like : {"reason": "user", "message_type": "user_logout", "event_time": 1667940265.475309, "time": 1667940265.475286}
        pick_time_dic[station_key]['logout'] = []

# number of systems the data has been processed for. We go sequentially along the system_names
system_read_count = 0

for msg in logs:

    # every login/logout message starts with '{'. So we are reading those only for now ...

    if msg[0] == "{":
        msg_split = msg.split('"')

        # reading every system in the list
        station_name = system_names[system_read_count]

        # If its a login message
        if msg_split[3] == 'user_login':

            # extracting user login name
            user_name = msg_split[7]

            # if the user data was processed previously

            try:

                pick_time_dic[station_name]['login'][user_name].append(
                    time_stamps[system_read_count])

            # If the user data was not processed previosuly, i.e: we do not have
            # a key in the dictionary with that users name

            except KeyError:
                pick_time_dic[station_name]['login'][user_name] = []
                pick_time_dic[station_name]['login'][user_name].append(
                    time_stamps[system_read_count])

        # If its a logout message
        else:
            # adding logout data to the list
            pick_time_dic[station_name]['logout'].append(
                time_stamps[system_read_count])

    # now we move on to the next system. i.e: sps01 followed by sps04 ,, etc .....

    system_read_count += 1

# ======= Removing packout stations since that does not account
# for any picks. OTT has 8 packout

packout_stations = ['bcs1', 'bcs2', 'bcs3',
                    'bcs4', 'bcs5', 'bcs6', 'bcs7', 'bcs8']

# no_logout_stations = set(no_logout_stations)

for packout_station in packout_stations:
    if packout_station in set_system_names:
        del pick_time_dic[packout_station]

# =======

# sort all the times together


for station_key_value, login_logout_status in pick_time_dic.copy().items():
    print('=================================================')
    print('Calculating for ', station_key_value, ' now .... ')
    # print('<-------------------------- ',
    #       station_key_value, ' -------------------------->')

    # all logout times are now converted to a list
    logout_times = login_logout_status["logout"]

    # sort the logout times from earliest to latest
    logout_times.sort(key=lambda date: datetime.strptime(
        date, "%b %d, %Y @ %H:%M:%S.%f"))

    for login_logout_stat, timestamp_list in login_logout_status.copy().items():

        # if the statu is logout
        if login_logout_stat != "logout":

            # Calculating by adding the total time a user stayed logged in

            all_cell_login_times_list = []
            for user, user_specific_login_times in timestamp_list.copy().items():
                all_cell_login_times_list += user_specific_login_times

            # sort all the times for which the user has logged in

            all_cell_login_times_list.sort(
                key=lambda date: datetime.strptime(date, "%b %d, %Y @ %H:%M:%S.%f"))

            for user, user_specific_login_times in timestamp_list.copy().items():

                for login_time in user_specific_login_times:

                    try:

                        # check for the next biggest login time

                        index_login = all_cell_login_times_list.index(
                            login_time)

                        if index_login != len(all_cell_login_times_list)-1:

                            # if the login time is not the biggets login time

                            next_biggest_login_time = all_cell_login_times_list[index_login+1]

                            # print('\n')
                            # print('current login_time is : ', login_time)
                            # print('next_biggest_login_time was found to be : ', next_biggest_login_time)
                            # print('\n')

                            logout_times_copy = logout_times.copy()
                            logout_times_copy.append(next_biggest_login_time)
                            logout_times_copy.sort(key=lambda date: datetime.strptime(
                                date, "%b %d, %Y @ %H:%M:%S.%f"))

                            # the logout time has to be smaller than the next biggest login time

                            logout_time_sorted = [t for t in logout_times_copy if datetime.strptime(
                                t, "%b %d, %Y @ %H:%M:%S.%f") > datetime.strptime(next_biggest_login_time, "%b %d, %Y @ %H:%M:%S.%f")]
                            logout_time_sorted.sort(
                                key=lambda date: datetime.strptime(date, "%b %d, %Y @ %H:%M:%S.%f"))

                            logout_time = logout_time_sorted[0]

                            # print('         Login Time : ', login_time, ' | Logout Time : ', logout_time, " = ",  find_time_delta(
                            #     logout_time, login_time), ' minutes')

                        else:

                            # if the login time is the last one, then assume that to be the next
                            # biggest login time anyway and continue
                            # in this case the logout time is at the end of the day

                            next_biggest_login_time = login_time
                            logout_time = time_max

                            # print('         Login Time : ', login_time, ' | Logout Time : ', logout_time, " = ",  find_time_delta(
                            #     logout_time, login_time), ' minutes')

                    except Exception as IndexError:

                        # since we are stopping our kibana query at a certain point, the user must have stayed logged in to
                        # continue picking, which means that we assume the log out time = max tim, which is basically the last time for which
                        # we have a data

                        # print('The user ', user, ' kept picking till end of timerange,\
                        #     marking max time as logout time')

                        logout_time = time_max

                        # print('Login Time : ', login_time, ' | Logout Time : ', logout_time, " = ",  find_time_delta(
                        #     logout_time, login_time), ' minutes')

                    try:
                        # keep adding to the time the user logged in
                        time_logged_in_dic[user] += find_time_delta(
                            logout_time, login_time)
                    except Exception as KeyError:
                        continue

    print('Processing completed for .... ', station_key_value)
    print('=================================================', '\n', '\n')

# ======= Print out the stats and the dictionaries

print('Dictionary for time stayed logged in (minutes): ')
print(time_logged_in_dic)
print('\n')

print('Dictionary for picks done : ')
print(pick_count_dic)
print('\n')


# Dictionary used to calculate the average time for every picker
average_time_dictionary = {}

print('Complete Pick Stats : ')
print('\n')


for k, v in time_logged_in_dic.items():
    try:
        # print(k, 'stayed logged in for ', round(v, 2), ' minutes and completed ',
        #       pick_count_dic[k], 'picks')

        # if the time logged in was 0, then assign the PPH as NaN
        if v == 0:

            # this is because the data logged has some gaps in it and can't see to get the data for when user X was logged and
            # this causes the math to think that the user was logged in for 0 minutes and the calculation uses 0 in the
            # denominator, which is impossible. Hence, its better to assign NaN to the value

            average_time_dictionary[k] = 'NaN'
        else:

            avg_pph = pick_count_dic[k]/(v/60)
            average_time_dictionary[k] = round(avg_pph, 2)
    except Exception as KeyError:
        continue

# removing unnecessary key
if '-' in pick_count_dic.keys():
    del pick_count_dic['-']


print('\n')
print('Average time per picker : ')
print('\n')
print(average_time_dictionary)
print('\n')

print('Total Picks done in this time frame : ', total_picks)
print('\n')


# ======= Prepare bar raph

# names = list(average_time_dictionary.keys())
# list_avg_pph = list(average_time_dictionary.values())
# plt.bar(names, list_avg_pph)
# plt.grid()
# plt.title('Average picks by every associate')
# plt.xticks(rotation=90)
# plt.xlabel('Associate Login ID')
# plt.ylabel('Average PPH (login time adjusted)')

# =======


# ======= Lists prepared for excel writing

list_operators = list(time_logged_in_dic.keys())
list_picks = list(pick_count_dic.values())
list_login_time = list(time_logged_in_dic.values())
list_avg_pph = list(average_time_dictionary.values())

writer = pd.ExcelWriter('output/output.xlsx', engine='openpyxl')
wb = writer.book
df = pd.DataFrame({'Operator': list_operators,
                   'Total Picks': list_picks,
                   'Total Login Time (minutes)': list_login_time,
                   'Average PPH (adjusted for login time)': list_avg_pph})

df.to_excel(writer, index=False)
wb.save('output/output.xlsx')

# =======

print('Full data processed from ',
      time_stamps_copy[0], ' to ', time_stamps_copy[-1])
print('\n')
print('Total time taken to process : ', round(
    time.time()-time_start, 2), ' seconds.')
