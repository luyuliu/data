'''
This script will find the real-time arrival/departure time from the GTFS
real-time feed. 
'''



import pymongo
from datetime import timedelta, date
import time
import multiprocessing

def convertSeconds(BTimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds

# database setup
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs

db_tripupdate=client.cota_tripupdate

db_realtime=client.cota_real_time

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()


# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def find_gtfs_time_stamp(today_date,single_date): # Find the corresponding GTFS set.
    today_seconds=int((single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup=db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds> 86400:
            return backup
        backup=each_time_stamp
    return db_time_stamps[len(db_time_stamps)-1]



# main loop
# enumerate every day in the range
def paralleling_transfers(single_date):
    time_matrix={} # A dictionary: 1st layer: trip_id (each trip has a sequence of stops)
                                #  2nd layer: stop_id (each stop in the sequence)
    start_time=time.time()
    today_date = single_date.strftime("%Y%m%d")  # date
    
    db_feed_collection=db_tripupdate[today_date] # The GTFS feed collection which has been divided by each day.
    

    that_time_stamp=find_gtfs_time_stamp(today_date,single_date) # Find the corresponding GTFS set.
    print("-----------------------",today_date,": Start/ From",date.fromtimestamp(that_time_stamp)," -----------------------")

    # a=date.fromtimestamp(that_time_stamp)
    # print(a)
    db_seq=db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_stops=db_GTFS[str(that_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(that_time_stamp)+"_stop_times"]
    db_trips=db_GTFS[str(that_time_stamp)+"_trips"]

    db_realtime_collection=db_realtime["R"+today_date] # Collection to be added.
    db_feeds=(db_feed_collection.find({}, no_cursor_timeout=True)) # The GTFS feed collection which has been divided by each day.
    if db_feeds.count()==0: # There is no feed this day, which shouldn't happen.
        print("-----------------------",today_date," : Skip -----------------------")
        return
    print("-----------------------","FindDone","-----------------------")
    total_count=db_feeds.count() # Total length of this day's feed.

    count=0
    for each_feed in db_feeds: # For each feed record.
        trip_id=each_feed["trip_id"] # trip_id
        seq_count=0 # Sequence's count. It records the sequence id of different records of the same trip at different time.
        # For example, if a bus/trip run on a route, the system will send back several different GTFS real-time reports at each time. 

        # We always assume the lastest record is the most accurate one. So, we have to store the seq_count to compare the current one and the new one
        # to determine whether we should update the records.

        # We will select the smallest count. This is because with the bus running, the seq in its GTFS real-time will become shorter. If there's a smaller count 
        # then it means that the current record is after the best record we record before.

        for each_stop in each_feed["seq"]: # The sequence of each feed record.
            stop_id=each_stop["stop"] # stop_id of each sequential stop.
            try:
                time_matrix[trip_id] # Create time_matrix's first layer: trip_id. Each trip should has a unique records in the first layer.
            except:
                time_matrix[trip_id]={}
            else:
                pass
            try:
                time_matrix[trip_id][stop_id]
            except: # if the stop is not recorded, then create it and add it to the trip_id.
                time_matrix[trip_id][stop_id]={}
                time_matrix[trip_id][stop_id]={"seq":seq_count,"time":each_stop["arr"]}
            else:
                if time_matrix[trip_id][stop_id]["seq"]>seq_count: # If the current seq_count is less, then update the records with the newest feed.
                    time_matrix[trip_id][stop_id]={"seq":seq_count,"time":each_stop["arr"]}
            seq_count+=1
        
        if count%10000==0:
            print("-----------------------","QueryDoneBy:",count/total_count*100,today_date,"-----------------------")
        count+=1
    
    for trip_id in time_matrix.keys(): # trip_id
        for stop_id in time_matrix[trip_id].keys():
            recordss={}
            recordss["trip_id"]=trip_id
            recordss["stop_id"]=stop_id
            recordss["seq"]=time_matrix[trip_id][stop_id]["seq"] # This seq is different from the seq in the trip_seq.
            recordss["time"]=time_matrix[trip_id][stop_id]["time"]
            db_realtime_collection.insert_one(recordss)
    print("-----------------------","InsertDone:",today_date,"-----------------------")
    end_time=time.time()
    print(end_time-start_time)
    db_realtime_collection.create_index([("trip_id",1),("stop_id",1)])


if __name__ == '__main__':
    start_date = date(2018, 9, 3)
    end_date = date(2019, 1, 31)
    each_date = date(2018, 2, 1)
    paralleling_transfers(each_date)
    ''' for each_date in daterange(start_date, end_date):
        paralleling_transfers(each_date)'''

    # cores = multiprocessing.cpu_count()
    # pool = multiprocessing.Pool(processes=20)
    # date_range = daterange(start_date, end_date)
    # output=[]
    # output=pool.map(paralleling_transfers, date_range)
    # pool.close()
    # pool.join()