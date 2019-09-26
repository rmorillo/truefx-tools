import datetime as dt
import sys
import os
import zipfile
import csv
import struct
from tqdm import tqdm

currency_pair = sys.argv[1]
start_year_month = sys.argv[2]
end_year_month = sys.argv[3]

print(currency_pair)
print(start_year_month)
print(end_year_month)

start_year_month_split = start_year_month.split('-')
start_date = dt.datetime(int(start_year_month_split[0]), int(start_year_month_split[1]), 1)
end_year_month_split = end_year_month.split('-')
end_date = dt.datetime(int(end_year_month_split[0]), int(end_year_month_split[1]), 1)


zip_files = {}
day_counter = 0
next_month =  start_date

while (next_month<=end_date):
    target_dir = next_month.strftime("%Y-%m")
    if (not os.path.isdir(target_dir)):
        raise Exception("Folder '{}' does not exist!".format(target_dir))
    target_file = target_dir + '/' + currency_pair + '-' + target_dir + '.zip'    
    if (not os.path.exists(target_file)):
        raise Exception("File '{}' does not exist!".format(target_file))

    zip_files[target_dir] = target_file
    
    next_month = dt.datetime(next_month.year + int((next_month.month / 12)), ((next_month.month % 12) + 1), 1)
    day_counter += (next_month + dt.timedelta(days=-1)).day    


bidask_file_path = currency_pair + '-' + start_year_month + '-' + end_year_month + '.bidask'
bidask_file = open(bidask_file_path, 'wb')

next_day = start_date
timestamp = start_date
csv_file_path = None
csv_file = None
prev_target_dir = None
reader = None
loop_counter = 0
for day_index in tqdm(range(day_counter), ncols=70):
    target_dir = next_day.strftime("%Y-%m")
    
    if (prev_target_dir != target_dir):
        if (csv_file is not None and not csv_file.closed):
            csv_file.close()
            os.remove(csv_file_path)
        zip_ref = zipfile.ZipFile(zip_files[target_dir], 'r')
        zip_ref.extractall(".")
        zip_ref.close()
        
        csv_file_path = currency_pair + '-' + target_dir + '.csv'        
        csv_file= open(csv_file_path, 'r')
        reader = csv.reader(csv_file)        

    while (timestamp.date() == next_day.date()):
        try:
            row = reader.__next__()            
        except StopIteration:            
            break;        
                            
        val = row[1]
        bid = float(row[2])
        ask = float(row[3])
        
        year_part = int(val[0:4])
        month_part = int(val[4:6])
        day_part = int(val[6:8])
        hour_part = int(val[9:11])
        minute_part = int(val[12:14])
        second_part = int(val[15:17])
        microsecond_part = int(val[18:]) * 1000

        timestamp = dt.datetime(year_part, month_part, day_part, 0, 0, 0)
        
        date_part = (year_part * 10000) + (month_part * 100) + day_part
        time_part = (hour_part * 10000) + (minute_part * 100) + second_part
        row_data = struct.pack('iiiff', date_part, time_part,
                               microsecond_part, bid, ask)
        bidask_file.write(row_data)
            
    prev_target_dir = target_dir
    next_day = start_date + dt.timedelta(days=day_index + 1)
    timestamp = next_day

if (csv_file is not None and not csv_file.closed):
    csv_file.close()
    os.remove(csv_file_path)

bidask_file.close()
                      
