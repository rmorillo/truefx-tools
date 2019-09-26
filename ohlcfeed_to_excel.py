import sys
import struct
import pytz
import math
import datetime as dt
import os
from tqdm import tqdm
import pandas as pd

ohlcfeed_file_path = sys.argv[1]

ohlcfeed_file = open(ohlcfeed_file_path, 'rb')
struct_format = 'iiiffffiffff'
row_size = struct.calcsize(struct_format)

#get row count
current_position = ohlcfeed_file.tell()
ohlcfeed_file.seek(0, 2)
last_position = ohlcfeed_file.tell()
ohlcfeed_file.seek(current_position)
row_count = int((last_position - current_position) / row_size)

row_list = []
for row_index in tqdm(range(row_count), ncols=70):
    byte_data = ohlcfeed_file.read(row_size)
    if (len(byte_data) > 0):
        row_data = struct.unpack(struct_format, byte_data)

        date_part = row_data[0]        
        time_part = row_data[1]        
        timestamp = dt.datetime(math.floor(date_part // 10000),
                                math.floor((date_part // 100) % 100),
                                math.floor(date_part % 100),
                                int((time_part // 10000) % 100),
                                int((time_part // 100) % 100),
                                int(time_part % 100))       
        values = {"timestamp": timestamp}

        ind_currency_pair_id = row_data[2]
        values["open{}".format(ind_currency_pair_id)] = row_data[3]
        values["high{}".format(ind_currency_pair_id)] = row_data[4]
        values["low{}".format(ind_currency_pair_id)] = row_data[5]
        values["close{}".format(ind_currency_pair_id)] = row_data[6]

        dep_currency_pair_id = row_data[7]
        values["open{}".format(dep_currency_pair_id)] = row_data[8]
        values["high{}".format(dep_currency_pair_id)] = row_data[9]
        values["low{}".format(dep_currency_pair_id)] = row_data[10]
        values["close{}".format(dep_currency_pair_id)] = row_data[11]

        row_list.append(values)

base_file_name = os.path.basename(ohlcfeed_file_path)
file_name_only = os.path.splitext(base_file_name)[0]

xslx_file_path = file_name_only + '.xlsx'

writer = pd.ExcelWriter(xslx_file_path)

df = pd.DataFrame(row_list)

df.to_excel(writer, 'Sheet1')
writer.save()
