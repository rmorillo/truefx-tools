import sys
import struct
import pytz
import math
import datetime as dt
import os
from tqdm import tqdm

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
        self.previous_price = current_price
        
    def aggregate(self):
        ohlc = (self.open_price, self.high_price, self.low_price, self.previous_price)
        self.open_price = self.previous_price
        self.high_price = self.previous_price
        self.low_price =  self.previous_price
        return ohlc
    
bidaskfeed_file_path = sys.argv[1]
bidaskfeed_file = open(bidaskfeed_file_path, 'rb')
struct_format = 'iiiiff'
row_size = struct.calcsize(struct_format)

#get row count
current_position = bidaskfeed_file.tell()
bidaskfeed_file.seek(0, 2)
last_position = bidaskfeed_file.tell()
bidaskfeed_file.seek(current_position)
row_count = int((last_position - current_position) / row_size)

is_init = True

#setup ohlc file
base_file_name = os.path.basename(bidaskfeed_file_path)
file_name_only = os.path.splitext(base_file_name)[0]

ohlcfeed_file_path = file_name_only + '.ohlcfeed'
ohlcfeed_file = open(ohlcfeed_file_path, 'wb')

ind_pair_ohlc = None
dep_pair_ohlc = None

for row_index in tqdm(range(row_count), ncols=70):
    byte_data = bidaskfeed_file.read(row_size)
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
        
        if (is_init):            
            if (currency_pair_id == 1):
                ind_pair_ohlc = bidask_to_ohlc_aggregator(bid_price)
            elif (currency_pair_id == 2):
                dep_pair_ohlc = bidask_to_ohlc_aggregator(bid_price)
                
            is_init = not (ind_pair_ohlc is not None and dep_pair_ohlc is not None)
        else:
            if (currency_pair_id == 0):                                        
                ind_ohlc = ind_pair_ohlc.aggregate()
                dep_ohlc = dep_pair_ohlc.aggregate()
                row_data = struct.pack('iiffffffff', date_part, time_part, ind_ohlc[0], ind_ohlc[1], ind_ohlc[2], ind_ohlc[3],
                                       dep_ohlc[0], dep_ohlc[1], dep_ohlc[2], dep_ohlc[3])                
                ohlcfeed_file.write(row_data)                                        
            else:
                if (currency_pair_id == 1):
                    ind_pair_ohlc.price_action(bid_price)
                elif (currency_pair_id == 2):
                    dep_pair_ohlc.price_action(bid_price)            

ohlcfeed_file.close()    
bidaskfeed_file.close()
