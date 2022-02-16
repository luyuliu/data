
import pymongo
from datetime import timedelta, date
import time

client = pymongo.MongoClient('mongodb://localhost:27017/')
db_feed = client.cota_trip_update
db_tripupdate = db_feed.full_trip_update_20211124

a = db_feed["20200209"]
b = db_feed["all_trip_update_20200717"]

t1 = time.time()
alist = list(a.find({}))
t2 = time.time()
blist = list(b.find({"start_date": "20200209"}))
t3 = time.time()

print(t3 - t2, t2 - t1, len(alist), len(blist))

# Conclusion: there is basically no difference between those two queries 