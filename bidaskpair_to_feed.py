import sys
import struct
import pytz
import math
import datetime as dt
import os
from tqdm import tqdm

def next_minute_datetime(year, month, day, hour, minute, minute_units=1):
    date_time = dt.datetime(year, month, day, hour, minute, 0, 0, tzinfo=pytz.utc)
    return date_time + dt.timedelta(minutes=minute_units)    

def next_hour_datetime(year, month, day, hour, hour_units=1):
    date_time = dt.datetime(year, month, day, hour, 0, 0, 0, tzinfo=pytz.utc)
    return date_time + dt.timedelta(hours=hour_units)    

def next_day_datetime(year, month, day, day_units=1):
    date_time = dt.datetime(year, month, day, 0, 0, 0, 0, tzinfo=pytz.utc)
    return date_time + dt.timedelta(days=day_units)    

def get_next_heartbeat_time_unit(heartbeat_time_unit, heartbeat_time_period, timestamp_value):
    if heartbeat_time_unit == "M":
        return next_minute_datetime(timestamp_value.year, timestamp_value.month,
                                                    timestamp_value.day, timestamp_value.hour,
                                                    timestamp_value.minute, heartbeat_time_period)
    elif heartbeat_time_unit == "H":
        return next_hour_datetime(timestamp_value.year, timestamp_value.month, timestamp_value.day,
                                                  timestamp_value.hour, heartbeat_time_period)
    elif heartbeat_time_unit == "D":
        return next_day_datetime(timestamp_value.year, timestamp_value.month, timestamp_value.day,
                                                 heartbeat_time_period)
    else:
        return None

def get_start_heartbeat_time_unit(heartbeat_time_unit, heartbeat_time_period, timestamp_value):
    #todo: should align to heartbeat_time_period
    if (heartbeat_time_unit == "M"):
        return dt.datetime(timestamp_value.year, timestamp_value.month, timestamp_value.day, timestamp_value.hour, 0, 0, 0, tzinfo=pytz.utc)
    else:
        return None

bidaskpair_file_path = sys.argv[1]
heartbeat_time_unit = sys.argv[2]
heartbeat_time_period = int(sys.argv[3])

bidaskpair_file = open(bidaskpair_file_path, 'rb')
struct_format = 'iiiiff'
row_size = struct.calcsize(struct_format)

#get row count
current_position = bidaskpair_file.tell()
bidaskpair_file.seek(0, 2)
last_position = bidaskpair_file.tell()
bidaskpair_file.seek(current_position)
row_count = int((last_position - current_position) / row_size)

is_init = True
next_ticks = None

#setup ohlc file
base_file_name = os.path.basename(bidaskpair_file_path)
file_name_only = os.path.splitext(base_file_name)[0]

bidaskfeed_file_path = file_name_only + '-' + heartbeat_time_unit + str(heartbeat_time_period) + '.bidaskfeed'
bidaskfeed_file = open(bidaskfeed_file_path, 'wb')

previous_date_part = None
previous_time_part = None
for row_index in tqdm(range(row_count), ncols=70):
    byte_data = bidaskpair_file.read(row_size)
    if (len(byte_data) > 0):
        row_data = struct.unpack(struct_format, byte_data)
        #get timestamp
        date_part = row_data[0]        
        time_part = row_data[1]
        microsecond_part = row_data[2]
        timestamp = dt.datetime(math.floor(date_part // 10000),
                                math.floor((date_part // 100) % 100),
                                math.floor(date_part % 100),
                                int((time_part // 10000) % 100),
                                int((time_part // 100) % 100),
                                int(time_part % 100),
                                microsecond_part, tzinfo=pytz.utc)                                    

        currency_pair_id = row_data[3]
        bid_price = row_data[4] # bid price
        ask_price = row_data[5] # bid price
        
        if (is_init):
            next_ticks = get_next_heartbeat_time_unit(heartbeat_time_unit, heartbeat_time_period, timestamp)            
            is_init = False
        else:            
            if timestamp >= next_ticks:                        
                ts_date_part = (next_ticks.year * 10000) + (next_ticks.month * 100) + next_ticks.day
                ts_time_part = (next_ticks.hour * 10000) + (next_ticks.minute * 100) + next_ticks.second                        

                #detect weekend gap
                if ((timestamp - next_ticks).days > 1):
                    next_ticks = get_start_heartbeat_time_unit(heartbeat_time_unit, heartbeat_time_period, dt.datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, 0))
                    row_data = struct.pack(struct_format, previous_date_part, previous_time_part, 0, 0, float('NaN'), float('NaN'))
                    bidaskfeed_file.write(row_data)
                else:
                    row_data = struct.pack(struct_format, ts_date_part, ts_time_part, 0, 0, float('NaN'), float('NaN'))
                    bidaskfeed_file.write(row_data)
                   
                next_ticks = get_next_heartbeat_time_unit(heartbeat_time_unit, heartbeat_time_period, next_ticks)

            row_data = struct.pack(struct_format, date_part, time_part, microsecond_part, currency_pair_id, bid_price, ask_price)
            bidaskfeed_file.write(row_data)

        previous_date_part = date_part
        previous_time_part = time_part

bidaskfeed_file.close()    
bidaskpair_file.close()
