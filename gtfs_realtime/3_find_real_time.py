# This script will find the real-time arrival/departure time from the GTFS real-time feed. 
# Need to first run 1_create_indexes.py and 2_create_trip_seq.py.
# The output (cota_real_time database) is daily collection of actual and scheduled arrival time in the COTA system, with trip_id, route_id, stop_id, and everything you will ever need.

import pymongo
from datetime import timedelta, date
import time
import multiprocessing
from tqdm import tqdm
import os, math
import sys
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import transfer_tools

''' !!!Parameters to change!!! '''
trip_update_source_collection_name = "all_trip_update_20211124" # Raw trip update collection name
start_date = date(2021, 1, 12)
end_date = date(2021, 11, 24)
cores = 30 # Paralleling process count. Go to task manager to find how many logical processors your machine have. I recommend to use 3/4 of all the cores you have. 
# For example, CURA workstation has 40 cores, I find 30 cores are a reasonable balance between speed and reliability. 35 is still okay but may risk crashing. 
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs # GTFS database
db_tripupdate = client.cota_trip_update # Raw trip update database
db_realtime = client.cota_real_time # Output. Processed actual arrival time database

def convertSeconds(BTimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds

# main loop
# enumerate every day in the range
def paralleling_transfers(single_date):
    # A dictionary: 1st layer: trip_id (each trip has a sequence of stops)
    time_matrix = {}
    #  2nd layer: stop_id (each stop in the sequence)
    start_time = time.time()
    today_date = single_date.strftime("%Y%m%d")  # date

    # The GTFS feed collection which has been divided by each day.
    # col_feed = db_tripupdate[today_date]
    col_feed = db_tripupdate[trip_update_source_collection_name] # The source trip update 

    # Find the corresponding GTFS set.
    that_time_stamp = transfer_tools.find_gtfs_time_stamp(single_date)

    # a=date.fromtimestamp(that_time_stamp)
    db_seq = db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_stops = db_GTFS[str(that_time_stamp)+"_stops"]
    db_stop_times = db_GTFS[str(that_time_stamp)+"_stop_times"]
    db_trips = db_GTFS[str(that_time_stamp)+"_trips"]

    # Collection to be added.
    col_real_time = db_realtime["R"+today_date]
    # The GTFS feed collection which has been divided by each day.
    rl_feeds = (col_feed.find({"start_date": today_date}, no_cursor_timeout=True))
    if rl_feeds.count() == 0:  # There is no feed this day, which shouldn't happen.
        return
    total_count = rl_feeds.count()  # Total length of this day's feed.

    count = 0
    for each_feed in rl_feeds:  # For each feed record.
        trip_id = each_feed["trip_id"]  # trip_id
        # Sequence's count. It records the sequence id of different records of the same trip at different time.
        seq_count = 0
        # For example, if a bus/trip run on a route, the system will send back several different GTFS real-time reports at each time.

        # We always assume the lastest record is the most accurate one. So, we have to store the seq_count to compare the current one and the new one
        # to determine whether we should update the records.

        # We will select the smallest count. This is because with the bus running, the seq in its GTFS real-time will become shorter. If there's a smaller count
        # then it means that the current record is after the best record we record before.

        for each_stop in each_feed["seq"]:  # The sequence of each feed record.
            stop_id = each_stop["stop"]  # stop_id of each sequential stop.
            try:
                # Create time_matrix's first layer: trip_id. Each trip should has a unique records in the first layer.
                time_matrix[trip_id]
            except:
                time_matrix[trip_id] = {}
            else:
                pass
            try:
                time_matrix[trip_id][stop_id]
            except:  # if the stop is not recorded, then create it and add it to the trip_id.
                time_matrix[trip_id][stop_id] = {}
                time_matrix[trip_id][stop_id] = {
                    "seq": seq_count, "time": each_stop["arr"]}
            else:
                # If the current seq_count is less, then update the records with the newest feed.
                if time_matrix[trip_id][stop_id]["seq"] > seq_count:
                    time_matrix[trip_id][stop_id] = {
                        "seq": seq_count, "time": each_stop["arr"]}
            seq_count += 1
        count += 1

    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = int(time.mktime(time.strptime(today_date, "%Y%m%d")))
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    stop_dic = {}
    trip_dic = {}
    count = 0

    insertionList = []
    for trip_id in time_matrix.keys():  # trip_id
        for stop_id in time_matrix[trip_id].keys():
            recordss = {}
            recordss["trip_id"] = trip_id
            recordss["stop_id"] = stop_id
            # This seq is different from the seq in the trip_seq.
            recordss["seq"] = time_matrix[trip_id][stop_id]["seq"]
            recordss["time"] = time_matrix[trip_id][stop_id]["time"]
            
            ################## Do join in advance ################
            try:
                stop_dic[stop_id]
            except:
                stop_dic[stop_id] = {}
                stop_query = (db_stops.find_one({"stop_id": stop_id}))
                if (stop_query) == None:
                    stop_dic[stop_id]["lat"] = "stop_error"
                    stop_dic[stop_id]["lon"] = "stop_error"
                    stop_dic[stop_id]["stop_name"] = "stop_error"
                    stop_dic[stop_id]["stop_code"] = "stop_error"
                else:
                    stop_dic[stop_id]["lat"] = stop_query["stop_lat"]
                    stop_dic[stop_id]["lon"] = stop_query["stop_lon"]
                    stop_dic[stop_id]["stop_name"] = stop_query["stop_name"]
                    stop_dic[stop_id]["stop_code"] = stop_query["stop_code"]

            try:
                trip_dic[trip_id]
            except:
                trip_dic[trip_id] = {}
                trip_query = (db_trips.find_one({"trip_id": trip_id}))
                if (trip_query) == None:
                    trip_dic[trip_id]['block_id'] = "trip_error"
                    trip_dic[trip_id]['shape_id'] = "trip_error"
                    trip_dic[trip_id]['trip_headsign'] = "trip_error"
                    trip_dic[trip_id]['direction_id'] = "trip_error"
                    trip_dic[trip_id]['route_id'] = "trip_error"
                else:
                    trip_dic[trip_id]['block_id'] = trip_query["block_id"]
                    trip_dic[trip_id]['shape_id'] = trip_query["shape_id"]
                    trip_dic[trip_id]['trip_headsign'] = trip_query["trip_headsign"]
                    trip_dic[trip_id]['direction_id'] = int(
                        trip_query["direction_id"])
                    trip_dic[trip_id]['route_id'] = int(
                        trip_query["route_id"])*(1-2*int(trip_query["direction_id"]))  # 0->1; 1->-1

            stop_times_query = (db_stop_times.find_one(
                {"trip_id": trip_id, "stop_id": stop_id}))
            if (stop_times_query) == None:
                stop_sequence = "stop_time_error"
            else:
                stop_sequence = stop_times_query["stop_sequence"]

            trip_seq_query = (db_seq.find_one(
                {"trip_id": trip_id, "stop_id": stop_id}))
            if (trip_seq_query) == None:
                trip_sequence = "GTFS_error"
                scheduled_time = "GTFS_error"
            else:
                trip_sequence = trip_seq_query["seq"]
                scheduled_time = int(trip_seq_query["time"]) + today_seconds

            recordss["lat"] = stop_dic[stop_id]["lat"]
            recordss["lon"] = stop_dic[stop_id]["lon"]
            recordss["stop_name"] = stop_dic[stop_id]["stop_name"]
            recordss["shape_id"] = trip_dic[trip_id]['shape_id']
            recordss["trip_headsign"] = trip_dic[trip_id]['trip_headsign']
            recordss["route_id"] = trip_dic[trip_id]['route_id']
            recordss["stop_sequence"] = stop_sequence
            recordss["trip_sequence"] = trip_sequence
            recordss["scheduled_time"] = scheduled_time
            recordss["location"] = {
                "type" : "Point", 
                "coordinates" : [recordss["lon"], recordss["lat"]]
            }

            count += 1

            insertionList.append(recordss)

            if len(insertionList) == 10000:
                col_real_time.insert_many(insertionList)
                insertionList = []

    col_real_time.insert_many(insertionList)
    insertionList = []

    end_time = time.time()

    col_real_time.create_index([("trip_id", 1), ("stop_id", 1)])
    col_real_time.create_index([("trip_sequence", 1), ("stop_id", 1)])


if __name__ == '__main__':
    date_range = list(transfer_tools.daterange(start_date, end_date))
    total_length = len(date_range)
    batch = math.ceil(total_length/cores)
    for i in tqdm(list(range(batch)), position=0, leave=True):
        pool = multiprocessing.Pool(processes=cores)
        sub_output = []
        try:
            sub_date_range = date_range[cores*i:cores*(i+1)]
        except:
            sub_date_range = date_range[cores*i:]
        sub_output = pool.map(paralleling_transfers, sub_date_range)
        pool.close()
        pool.join()
        print("----------------", date_range[cores*i], " to ", date_range[cores*(i+1)], " finished... ----------------")