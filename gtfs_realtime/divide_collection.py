'''
This script will divide the whole GTFS database into several individual GTFS real-time 
trip-update database and add index.
'''

import pymongo
from datetime import timedelta, date
import time

client = pymongo.MongoClient('mongodb://localhost:27017/')
db_feed = client.trip_update
db_tripupdate = db_feed.full_trip_update

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2018, 9, 3)
end_date = date(2019, 1, 31)

for single_date in daterange(start_date, end_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    print(str(today_date))
    db_today_feeds=(db_tripupdate.find({"start_date": str(today_date)},no_cursor_timeout=True))
    print("---------------",today_date,": Query","---------------")
    for each_feed in db_today_feeds:
        db_feed[today_date].insert_one(each_feed)
    print("---------------",today_date,": Insert","---------------")
    db_feed[today_date].create_index([("trip_id",pymongo.ASCENDING)])
    print("---------------",today_date,": Index","---------------")