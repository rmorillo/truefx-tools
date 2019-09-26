import sys
import struct
import pytz
import math
import datetime as dt
import os
from tqdm import tqdm
from collections import namedtuple
from commontypes import FxMajor

def datetime_parts_to_datetime(date_part, time_part, microsecond_part):
    return dt.datetime(math.floor(date_part // 10000),
                                    math.floor((date_part // 100) % 100),
                                    math.floor(date_part % 100),
                                    int((time_part // 10000) % 100),
                                    int((time_part // 100) % 100),
                                    int(time_part % 100),
                                    microsecond_part, tzinfo=pytz.utc)

def next_minute_datetime(year, month, day, hour, minute, minute_units=1):
    date_time = dt.datetime(year, month, day, hour, minute, 0, 0, tzinfo=pytz.utc)
    return date_time + dt.timedelta(minutes=minute_units)
    
BidAskFileColumns = namedtuple('BidAskFileColumns', 'ts_date ts_time ts_microsecond bid ask')

class BidAskFileReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.format = 'iiiff'
        self.row_size = struct.calcsize(self.format)
        self.file = None
        self.row_count = 0
        
    def open(self):
        self.file = open(self.file_path, 'rb')
        current_position = self.file.tell()
        self.file.seek(0, 2)
        last_position = self.file.tell()
        self.file.seek(current_position)
        self.row_count = int((last_position - current_position) / self.row_size)

    def read(self):
        byte_data = self.file.read(self.row_size)
        if (len(byte_data) > 0):
            row_data = struct.unpack(self.format, byte_data)            
            date_part = row_data[0]        
            time_part = row_data[1]
            microsecond_part = row_data[2]
            bid_price = row_data[3]
            ask_price = row_data[4]
            return BidAskFileColumns(date_part, time_part, microsecond_part, bid_price, ask_price)
        else:
            return None

    def close(self):
        self.file.close()
        
class BidAskSetFileWriter:
    def __init__(self, file_path):
        self.file_path = file_path
        self.format = 'iiiiff'
        self.row_size = struct.calcsize(self.format)
        self.file = None
        self.row_count = 0

    def open(self):
        self.file = open(self.file_path, 'wb')

    def write(self, ts_date, ts_time, ts_microsecond, currency_pair_id, bid, ask):
        row_data = struct.pack(self.format, ts_date, ts_time, ts_microsecond, currency_pair_id, bid, ask)
        self.file.write(row_data)

    def close(self):
        self.file.close()

FxMajorBidAskColumns = namedtuple("FxMajorBidAskColumns", "ts_date ts_time ts_microsecond currency_pair_id bid ask")

class BidAskFileReaderItem:
    def __init__(self, file_name):
        self.file_name = file_name
        currency_pair_name = file_name[:6]
        currency_pair=  FxMajor.get_pair_by_symbols(currency_pair_name[:3],currency_pair_name[3:])
        self.currency_pair_id = currency_pair.value.currency_pair_id
        self.file_reader = BidAskFileReader(file_name)
        self.timestamp = None
        self.row_data = None
        self.bidask_data = None
        self.is_end_of_file = False

    def open(self):
        self.file_reader.open()

    def read(self):
        self.row_data = self.file_reader.read()
        if self.row_data is None:
            self.is_end_of_file = True
        else:
            self.timestamp = datetime_parts_to_datetime(self.row_data.ts_date, self.row_data.ts_time, self.row_data.ts_microsecond)
            self.bidask_data = FxMajorBidAskColumns(self.row_data.ts_date, self.row_data.ts_time, self.row_data.ts_microsecond, self.currency_pair_id, self.row_data.bid, self.row_data.ask)

    def close(self):
        self.file_reader.close()

class BidAskFileReaderCollection:
    def __init__(self, bidask_file_list):
        self.bidask_file_reader = []
        for file_name in bidask_file_list:
            self.bidask_file_reader.append(BidAskFileReaderItem(file_name))

    def open(self):
        for reader in self.bidask_file_reader:
            reader.open()
            reader.read()

    def get_next_timestamp(self):
        earliest_timestamp = dt.datetime(9999,12,31, tzinfo=pytz.utc)
        for reader in self.bidask_file_reader:
            if not reader.is_end_of_file and reader.timestamp < earliest_timestamp:
                earliest_timestamp = reader.timestamp
        return earliest_timestamp

    def fetch(self, timestamp):
        for reader in self.bidask_file_reader:
            if not reader.is_end_of_file and reader.timestamp <= timestamp:
                yield reader.bidask_data
                reader.read()

    @property
    def total_rows(self):
        row_counter = 0
        for reader in self.bidask_file_reader:
            row_counter += reader.file_reader.row_count
        return row_counter

    def close(self):
        for reader in self.bidask_file_reader:
            reader.close()

bidask_set = BidAskSetFileWriter(sys.argv[1])
bidask_file_readers = BidAskFileReaderCollection(sys.argv[2:])
bidask_file_readers.open()
bidask_set.open()

row_count = bidask_file_readers.total_rows

skip_read_ind = False
skip_read_dep = False
ind_bytes_read = 0
dep_bytes_read = 0
for row_index in tqdm(range(row_count), ncols=70):
    next_timestamp = bidask_file_readers.get_next_timestamp()
    bidask_rows = bidask_file_readers.fetch(next_timestamp)
    for bidask_row in bidask_rows:
        bidask_set.write(bidask_row.ts_date, bidask_row.ts_time, bidask_row.ts_microsecond, bidask_row.currency_pair_id,
                         bidask_row.bid, bidask_row.ask)
        
bidask_set.close()
bidask_file_readers.close()