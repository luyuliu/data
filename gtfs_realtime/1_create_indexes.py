# This script adds necessary indexes for GTFS data. It will improve the performance significantly.

import pymongo
from datetime import timedelta, date
import time
from tqdm import tqdm

''' !!!Parameters to change!!! '''
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs
db_tripupdate = client.cota_trip_update # Raw trip update database
trip_update_source_collection_name = "trip_update_09232025" # Raw trip update collection name


def convertSeconds(BTimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds

def sortArray(a):
    return a["time"]


# Find all GTFS schedule with unique timestamps
db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.list_collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()

# db_tripupdate[trip_update_source_collection_name].create_index([("start_date", 1)]) # Will take a long time

for each_time_stamp in tqdm(db_time_stamps):
    db_stops=db_GTFS[str(each_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(each_time_stamp)+"_stop_times"]
    db_trips=db_GTFS[str(each_time_stamp)+"_trips"]

    db_trips.create_index([("trip_id", pymongo.ASCENDING)])
    db_trips.create_index([("route_id", pymongo.ASCENDING), ("service_id", pymongo.ASCENDING)])
    db_trips.create_index([("trip_id", pymongo.ASCENDING), ("service_id", pymongo.ASCENDING)])

    db_stops.create_index([("stop_id", pymongo.ASCENDING)])
    db_stops.create_index([("stop_code", pymongo.ASCENDING)])

    db_stop_times.create_index([("trip_id", pymongo.ASCENDING), ("stop_id", pymongo.ASCENDING)])