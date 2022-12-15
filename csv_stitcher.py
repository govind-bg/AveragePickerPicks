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

# export to csv
combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8-sig')

print('Total time taken to combine the CSV files : ', time.time()-time_start)
