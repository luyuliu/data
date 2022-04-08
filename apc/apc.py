# Transform monthly APC zip files into MongoDB daily collections. Translate field names and create indexes.

from zipfile import ZipFile
from io import TextIOWrapper
import csv
from math import sin, cos, sqrt, atan2, pi
from datetime import timedelta, date, datetime
from pymongo import MongoClient, ASCENDING
import BasicSolver
import sys
import os
import time
import multiprocessing
from tqdm import tqdm
sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
client = MongoClient('mongodb://localhost:27017/')


class APCTester(BasicSolver.BasicSolver):
    def __init__(self):
        BasicSolver.BasicSolver.__init__(self)
        # Parameters
        self.start_date = date(2021, 10, 1)
        self.end_date = date(2022, 3, 1)
        self.base_location = r"M:\COTA\APC_201905_202202\\"

        # Mongo GTFS setup
        # self.db_GTFS = client.cota_gtfs
        # self.db_real_time = client.cota_real_time
        self.db_apc = client.cota_apc
        # Merge GTFS and APC data into a db_real_time database wise.
        self.db_apc_real_time = client.cota_apc_real_time

        # self.col_stops = self.db_GTFS[str(self.curGTFSTimestamp) + "_stops"]
        # self.col_stop_times = self.db_GTFS[str(self.curGTFSTimestamp) + "_stop_times"]
        # self.col_real_times = self.db_real_time["R" + todayDate]
        self.col_apc_raw = []
        self.month_range = [date(int(m/12), int(m % 12+1), 1) for m in range(self.start_date.year * 12 + self.start_date.month - 1, self.end_date.year*12 + self.end_date.month)]
        for each_month in self.month_range:
            self.col_apc_raw.append(each_month.strftime("%Y%m"))

    def unzipCSVFiles(self):
        for each_date in tqdm(self.month_range):
            file_name = each_date.strftime("%Y-%m") + "_COTA_APC_data"
            with ZipFile(self.base_location + file_name + ".zip") as zf:
                zf.extractall(self.base_location)

    def normalizeAPC(self):
        for each_month_date in (self.month_range):
            file_name = each_month_date.strftime("%Y-%m") + "_COTA_APC_data"
            print("**********", file_name, ", start... **********")
            insert_dic = {}
            with open(self.base_location + file_name + '.csv', 'r') as infile:
                reader = csv.reader(infile, quoting=csv.QUOTE_NONE)
                count = -1
                fields = []
                for row in tqdm(reader):
                    count += 1
                    originDic = {}
                    if count == 0:
                        fields = row
                        continue
                    for e_index in range(len(row)):
                        originDic[fields[e_index]] = row[e_index]
                    eachRecordDic = self.translateFieldName(originDic)

                    # Find stop_id from GTFS.
                    today_date = eachRecordDic["start_date"]
                    today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
                    stop_code = str(eachRecordDic["stop_code"])
                    GTFSTimestamp = self.find_gtfs_time_stamp(today_seconds, isDate=False)
                    col_stops = client.cota_gtfs[str(GTFSTimestamp) + "_stops"]
                    rl_stop = col_stops.find_one({"stop_code": stop_code})
                    if rl_stop == None:
                        eachRecordDic["stop_id"] = "unknown"
                    else:
                        eachRecordDic["stop_id"] = rl_stop["stop_id"]

                    # Store in memory
                    try:
                        insert_dic[today_date]
                    except:
                        insert_dic[today_date] = []
                    insert_dic[today_date].append(eachRecordDic)
            
            # Insert to database
            for each_date, each_list in (insert_dic.items()):
                if int(each_date) < 20190505:
                    continue
                # self.db_apc[each_date].drop()
                self.db_apc[each_date].insert_many(each_list)
                self.db_apc[each_date].create_index([("trip_id", ASCENDING), ("stop_id", ASCENDING)])
                print("--------", each_date, "--------")

            print("**********", file_name, ", end... **********")
                
    def translateFieldName(self, originDic):
        eachRecordDic = {}
        eachRecordDic["load_num"] = int(originDic["LOAD_NUM"])
        eachRecordDic["stop_sequence"] = int(originDic["STOP_SEQ_ID"])
        eachRecordDic["stop_code"] = int(originDic["STOP_ID"])
        eachRecordDic["stop_name"] = originDic["STOP_NAME"]

        eachRecordDic["actual_stop_time"] = self.translateStr2Datetime(
            originDic["ACT_STOP_TIME"], False)
        eachRecordDic["actual_departure_time"] = self.translateStr2Datetime(
            originDic["ACT_DEP_TIME"], False)
        eachRecordDic["actual_move_time"] = self.translateStr2Datetime(
            originDic["ACT_MOVE_TIME"], False)
        eachRecordDic["start_date"] = self.translateStr2Datetime(
            originDic["TRIP_DATE"], True)['today_date'] # All times are in local time in the original APC data.

        eachRecordDic["passenger_on"] = int(originDic["PSGR_ON"])
        eachRecordDic["passenger_off"] = int(originDic["PSGR_OFF"])
        eachRecordDic["passenger_load"] = int(originDic["PSGR_LOAD"])

        eachRecordDic["route_id"] = int(originDic["ROUTE"])
        eachRecordDic["pattern"] = originDic["PATTERN"]
        eachRecordDic["block_id"] = originDic["BLOCK"]
        eachRecordDic["latitude"] = float(originDic["LATITUDE"])
        eachRecordDic["longitude"] = float(originDic["LONGITUDE"])
        eachRecordDic["act_trip_run_miles"] = float(originDic["ACT_TRIP_RUN_MILES"])

        eachRecordDic["trip"] = int(originDic["TRIP"])
        eachRecordDic["door_cycles"] = int(originDic["DOOR_CYCLES"])
        eachRecordDic["gps_error_ft"] = int(originDic["GPS_ERROR_FT"])
        eachRecordDic["direction"] = int(originDic["DIRECTION"])
        eachRecordDic["act_miles_since_last_stop"] = float(originDic["ACT_MILES_SINCE_LAST_STOP"])
        eachRecordDic["act_mins_since_last_stop"] = float(originDic["ACT_MINS_SINCE_LAST_STOP"])
        eachRecordDic["passenger_miles"] = float(originDic["PSGR_MILES"])
        eachRecordDic["passenger_hours"] = float(originDic["PSGR_HOURS"])
        eachRecordDic["bus_id"] = int(originDic["BUS"])
        try:
            eachRecordDic["trip_id"] = int(originDic["TRIP_ID"])
        except:
            eachRecordDic["trip_id"] = originDic["TRIP_ID"]

        eachRecordDic["num_wheelchair"] = int(originDic["NUM_WC_RECS"])
        eachRecordDic["num_bike1"] = int(originDic["NUM_SP1_RECS"])
        eachRecordDic["num_bike2"] = int(originDic["NUM_SP2_RECS"])

        eachRecordDic["max_volocity"] = int(originDic["MAX_VELOCITY"])
        eachRecordDic["moved_while_dwell"] = int(originDic["MOVED_WHILE_DWELL_FT"])
        eachRecordDic["run_number"] = int(originDic["RUN_NUMBER"])

        eachRecordDic["passenger_on_nobal"] = int(originDic["PSGR_ON_NOBAL"])
        eachRecordDic["passenger_off_nobal"] = int(originDic["PSGR_OFF_NOBAL"])
        eachRecordDic["rear_door_boardings"] = int(originDic["REAR_DOOR_BOARDINGS"])
        return eachRecordDic

def normalizeAPC():
    tester = APCTester()
    tester.normalizeAPC()

if __name__ == "__main__":
    tester = APCTester()
    # tester.unzipCSVFiles()
    tester.normalizeAPC()
    # pool = multiprocessing.Pool(processes=3)
    # month_range = ["May18", "Sep18", "Jan19"]
    # output = []
    # output = pool.map(normalizeAPC, month_range)
    # pool.close()
    # pool.join()
