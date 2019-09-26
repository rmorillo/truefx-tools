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

def get_next_time_unit(time_unit, time_period, timestamp_value):
    if time_unit == "M":
        return next_minute_datetime(timestamp_value.year, timestamp_value.month,
                                                    timestamp_value.day, timestamp_value.hour,
                                                    timestamp_value.minute, time_period)
    elif time_unit == "H":
        return next_hour_datetime(timestamp_value.year, timestamp_value.month, timestamp_value.day,
                                                  timestamp_value.hour, time_period)
    elif time_unit == "D":
        return next_day_datetime(timestamp_value.year, timestamp_value.month, timestamp_value.day,
                                                 time_period)
    else:
        return None

def get_start_time_unit(time_unit, time_period, timestamp_value):
    #todo: should align to time_period
    if (time_unit == "M"):
        return dt.datetime(timestamp_value.year, timestamp_value.month, timestamp_value.day, timestamp_value.hour, 0, 0, 0, tzinfo=pytz.utc)
    else:
        return None

class bidask_to_ohlc_aggregator:
    def __init__(self, current_price):        
        self.previous_price = current_price
        self.open_price = current_price
        self.high_price = current_price
        self.low_price = current_price

    def price_action(self, current_price):
        if current_price > self.high_price:
            self.high_price = current_price
        elif current_price < self.low_price:
            self.low_price = current_price

    def aggregate(self):
        ohlc = (self.open_price, self.high_price, self.low_price, self.previous_price)        
        return ohlc
    
bidask_file_path = sys.argv[1]
time_unit = sys.argv[2]
time_period = int(sys.argv[3])

bidask_file = open(bidask_file_path, 'rb')
struct_format = 'iiiff'
row_size = struct.calcsize(struct_format)

#get row count
current_position = bidask_file.tell()
bidask_file.seek(0, 2)
last_position = bidask_file.tell()
bidask_file.seek(current_position)
row_count = int((last_position - current_position) / row_size)

is_init = True
previous_price = None
open_price = None
high_price = None
low_price = None
next_ticks = None

#setup ohlc file
base_file_name = os.path.basename(bidask_file_path)
file_name_only = os.path.splitext(base_file_name)[0]

ohlc_file_path = file_name_only + '-' + time_unit + str(time_period) + '.ohlc'
ohlc_file = open(ohlc_file_path, 'wb')
        
for row_index in tqdm(range(row_count), ncols=70):
    byte_data = bidask_file.read(row_size)
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
          
        current_price = row_data[3] # bid price
        
        if (is_init):
            next_ticks = get_next_time_unit(time_unit, time_period, timestamp)

            previous_price = current_price
            open_price = current_price
            high_price = current_price
            low_price = current_price
            is_init = False
        else:        
            if timestamp < next_ticks:
                if current_price > high_price:
                    high_price = current_price
                elif current_price < low_price:
                    low_price = current_price
            else:
                ts_date_part = (next_ticks.year * 10000) + (next_ticks.month * 100) + next_ticks.day
                ts_time_part = (next_ticks.hour * 10000) + (next_ticks.minute * 100) + next_ticks.second
                
                row_data = struct.pack('iiffff', ts_date_part, ts_time_part,
                               open_price, high_price, low_price, previous_price)
                ohlc_file.write(row_data)
                
                open_price = previous_price
                high_price = current_price
                low_price = current_price

                #detect weekend gap
                if ((timestamp - next_ticks).days > 1):
                   next_ticks = get_start_time_unit(time_unit, time_period, dt.datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, 0))
                   open_price = current_price
                   
                next_ticks = get_next_time_unit(time_unit, time_period, next_ticks)

            previous_price = current_price

ohlc_file.close()    
bidask_file.close()
