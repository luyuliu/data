# This script shows how to export and download the trip update and GTFS static schedules on the CURIO server. 

mongodump --gzip -d cota -c trip_update -q '{ts:{$gt: 1594785600}}' --out /var/cota/trip_updates_20211124 # export trip update records with query (only export the records after the timestamp). Change the timestamp and location.

mongodump -d cota_curio_gtfs_backup --out /var/cota/schedules_20211124 # export GTFS static schedules. Notice I did not make any queries. So you need to manually import those new collections.

# After export and downloading the two, import the two in cota_trip_update (or other databases, but remember to change the name in the third script) database and cota_gtfs database, respectively.

# I use mobaxterm to connect to the CURIO server and download the exported files. It will take times.

# I then use Studio 3T to import the exported trip update files (as one collection in cota_trip_update database) and GTFS schedule (as multiple collections, as what it was, do not modify the name or anything, just import them in cota_gtfs. Each collection has name like xxxxxxxxxxx_stops with xxxxxxxxxxx as the timestamp the GTFS files were caputured in CURIO)

# The scrape scripts on CURIO server are written by Jerry and Yongha. I do not know how they make it work. 

# After all things are ready, run the 1_create_indexes.py script.