# Program to stitch multiple CSV files

import os
import glob
import pandas as pd
import time
# used to measure the total time the program was run for

time_start = time.time()

os.chdir("csv_files/")
extension = 'csv'

all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

print('Stitching ', len(all_filenames), ' raw CSV Files ..... ')
# combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
# export to csv (original combined CSV file)
combined_csv.to_csv("../combined_csv.csv", index=False, encoding='utf-8-sig')


# now reading back the combined file

file_name = "../combined_csv.csv"

# potential file to be saved with the duplicates removed

file_name_output = "../non_duplicate_csv.csv"

previously_combined_df = pd.read_csv(file_name)

# to calculate the shape of the pandas df
row_count, column_count = previously_combined_df.shape

print("Number of rows before removing duplicates ", row_count)
# Notes:
# - the `subset=None` means that every column is used
#    to determine if two rows are different; to change that specify
#    the columns as an array
# - the `inplace=True` means that the data structure is changed and
#   the duplicate rows are gone
new_non_duplicate_df = previously_combined_df.drop_duplicates(subset=None, inplace=False, keep='last')

row_count, column_count = new_non_duplicate_df.shape

print("Number of rows after removing duplicates", row_count)

# Write the results to a different file
new_non_duplicate_df.to_csv(file_name_output, index=False, encoding='utf-8-sig')
row_count, column_count = new_non_duplicate_df.shape

print("Number of rows after removing duplicates", row_count)

print('\n')
print('Total time taken to process the CSV files : ',
      time.time()-time_start, ' seconds.')

# print(previously_combined_df)
# print(new_non_duplicate_df)