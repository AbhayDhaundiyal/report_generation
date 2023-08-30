from datetime import datetime, timedelta
import pandas as pd
import pytz

from api.models import BusinessHours, StoreStatus, Timezone

def get_time_ranges(interval, old_start_time, old_end_time, date_format = "%H:%M:%S.%f", delta_period_type="minutes"):
    start_date = old_start_time
    end_date = old_end_time+ timedelta(seconds = 1)
    dates = []
    while start_date < end_date:
        delta_period_arg = {delta_period_type: interval}
        interval_end = start_date + timedelta(**delta_period_arg)
        if interval_end.time() < start_date.time():
            dates.append({"start_date" : start_date.time(), "end_date" : datetime.max.time()})
            if interval_end > end_date:
                interval_end = end_date
            interval_end = interval_end - timedelta(microseconds=1)
            dates.append({"start_date" : datetime.min.time(), "end_date" : interval_end.time()})
            start_date = start_date + timedelta(**delta_period_arg)
            continue
        if interval_end > end_date:
            interval_end = end_date
        interval_end = interval_end - timedelta(microseconds=1)
        dates.append({"start_date" : start_date.time(), "end_date" : interval_end.time()})
        start_date = start_date + timedelta(**delta_period_arg)
    return dates

def calculate_status_trend(store_status_row)-> list():
    top = datetime.now()
    filtered_status = store_status_row["status"].split("/") # filter out the status that corresponds to the store_id
    filtered_timestamp_utc = store_status_row["timestamp_utc"].split("/") # filter out timestamp column that corresponds to the store id
    filtered_timezone = Timezone.objects.get(store_id = store_status_row["store_id"]) # filter out the timezone of the store
    intervals = list()
    trends = list() * 7 # trend that corresponds to each business day
    local = None
    total_seconds = 0

    if not filtered_timezone:
        local = pytz.timezone("America/Chicago")
    else:
        local = pytz.timezone(filtered_timezone.timezone_str)
    print(f"filter took {(datetime.now() - top).total_seconds()}")
    seconds = 0
    for day in range(7):
        start = None
        end = None
        s1 = datetime.now()
        filtered_time_range = BusinessHours.objects.filter(store_id = store_status_row["store_id"], day = day) # fetching business hour of that store on a particular day
        s2 = datetime.now()
        seconds += (s2 - s1).total_seconds()

        trends.append([])
        intervals.append([])
        if not len(filtered_time_range): # if business hour is not present that means its operating all day
            start = datetime.combine(datetime.today(), datetime.min.time())
            end = datetime.combine(datetime.today(), datetime.max.time())

        else:
            start = datetime.strptime(filtered_time_range[0].start_time_local, "%H:%M:%S")
            end = datetime.strptime(filtered_time_range[0].end_time_local, "%H:%M:%S")



        # converting local time to UTC time
        new_start = start
        local_dt = local.localize(new_start, is_dst=None)
        start = local_dt.astimezone(pytz.utc)
        

        new_end = end
        local_dt = local.localize(new_end, is_dst=None)
        end = local_dt.astimezone(pytz.utc)   

        interval = get_time_ranges(15, start, end) # dividing the interval to 15 minute bits
        intervals.append(interval)
        
        total_seconds += (end - start).total_seconds()
        # calculating trend by checking if the status is active then incrementing that index by 1 else decrement by 1
        max_interval_trend = [None] * len(interval)
        for row in range(len(filtered_status)):
            for index in range(len(interval)):
                try:
                    curr_time = datetime.strptime(filtered_timestamp_utc[row], "%Y-%m-%d %H:%M:%S.%f UTC").time()
                except:
                    curr_time = datetime.strptime(filtered_timestamp_utc[row], "%Y-%m-%d %H:%M:%S UTC").time()
                if(interval[index]["start_date"] > interval[index]["end_date"]):
                    if(curr_time < interval[index]["start_date"] 
                        and curr_time > interval[index]["end_date"]):
                        continue
                    if filtered_status[row] == "active":
                        if max_interval_trend[index] == None:
                            max_interval_trend[index] = 0
                        max_interval_trend[index] += 1
                    else:
                        if max_interval_trend[index] == None:
                            max_interval_trend[index] = 0
                        max_interval_trend[index] -= 1
                    break
                else:
                    if(curr_time >= interval[index]["start_date"] 
                        and curr_time < interval[index]["end_date"]):
                        if filtered_status[row] == "active":
                            if max_interval_trend[index] == None:
                                max_interval_trend[index] = 0
                            max_interval_trend[index] += 1
                        elif filtered_status[row] == "inactive":
                            if max_interval_trend[index] == None:
                                max_interval_trend[index] = 0
                            max_interval_trend[index] -= 1
                        else:
                            continue
                        break
        
        for index in range(len(max_interval_trend)):
            if max_interval_trend[index] == None:
                if not index:
                    max_interval_trend[index] = 1
                else:
                    max_interval_trend[index] = max_interval_trend[index-1]
        trends[day] = (max_interval_trend)

    print(f"fetching took {seconds}")
    print(f"final {(datetime.now() - top).total_seconds()}")
    return trends, intervals, total_seconds


def generate_ranges(start, end):  # function is used to divide time interval into days
    ranges = list()
    while(start < end):
        new_end = datetime.combine(start, datetime.max.time())
        if new_end > end:
            new_end = end
        ranges.append({"start" : start, "end" : new_end})
        start = new_end + timedelta(microseconds= 1)
    return ranges

def calculate_uptime_and_downtime(ranges, trends, interval):  # this function is used to calculate uptime and downtime according to the trend and interval list
    uptime = 0
    downtime = 0
    for range in ranges:
        day = range["start"].weekday()
        index = 0
        for time in interval[day]:
            if range["start"].time() > time["end_date"]:
                continue
            if range["start"].time() <= time["start_date"]:
                range["start"] = datetime.combine(range["start"], time["start_date"])
            
            if range["start"].time() >= time["start_date"]:
                if range["end"].time() <= time["end_date"]:
                    if index < len(trends[day]) and trends[day][index] < 0:
                        downtime += (range["end"] - range["start"]).total_seconds()
                    else:
                        uptime += (range["end"] - range["start"]).total_seconds()
                    break
                else:
                    dt = datetime.combine(range["start"], time["end_date"])
                    if dt < range["start"]:
                        dt = dt + timedelta(days = 1)
                    if index < len(trends[day]) and trends[day][index] < 0:
                        downtime += (dt - range["start"]).total_seconds() 
                    else:
                        uptime += (dt - range["start"]).total_seconds()
                    if index < len(interval[day]) - 1:
                        range["start"] = datetime.combine(range["start"], interval[day][index+1]["start_date"])
            index += 1
    return uptime, downtime
