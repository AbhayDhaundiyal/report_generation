from django.db.models.functions import Concat
import threading
from .models import *
from datetime import datetime, timedelta
import pandas as pd
from .utils import *
from django.contrib.postgres.aggregates import StringAgg

class GenerateReportThread(threading.Thread):

    def __init__(self, report_id):
        self.report_id = report_id
        threading.Thread.__init__(self)
    
    def run(self):
        try:
            store_statuses = StoreStatus.objects.values("store_id").annotate(
                status = StringAgg("status",delimiter="/",),
                timestamp_utc = StringAgg("timestamp_utc",delimiter= "/",)
                ).values("store_id", "status", "timestamp_utc")
            report_obj = ReportStatus(report_id = self.report_id, status = "Running")
            report_obj.save()
            end = datetime.today().utcnow()
            ranges_week = generate_ranges(end - timedelta(days= 6), end)
            ranges_hour = generate_ranges(end - timedelta(hours= 1), end)
            ranges_day = generate_ranges(end - timedelta(days = 1), end)
            data = list()
            # timezone = Timezone.objects.all() # filter out the timezone of the store
            
            for row in store_statuses:
                # top = datetime.now()
                # s1 = datetime.now()
                trends, interval, tot = calculate_status_trend(row)
                # s2 = datetime.now()
                # print(f"calculate_status_trend {(s2 - s1).total_seconds()}")
                # s1 = datetime.now()
                uptime_week, downtime_week = calculate_uptime_and_downtime(ranges_week, trends, interval)
                # s2 = datetime.now()
                # print(f"weekly up and down {(s2 - s1).total_seconds()}")
                # s1 = datetime.now()
                uptime_day, downtime_day = calculate_uptime_and_downtime(ranges_day, trends, interval)
                # s2 = datetime.now()
                # print(f"daily up and down {(s2 - s1).total_seconds()}")
                # s1 = datetime.now()
                uptime_hour, downtime_hour = calculate_uptime_and_downtime(ranges_hour, trends, interval)
                # s2 = datetime.now()
                # print(f"hourly {(s2 - s1).total_seconds()}")
                # s1 = datetime.now()
                data.append({"store_id" : row["store_id"],
                              "uptime_last_hour(in minutes)" : (uptime_hour) / 60,
                              "uptime_last_day(in hours)" : (uptime_day)/3600,
                              "uptime_last_week(in hours)" : (uptime_week)/3600,
                              "downtime_last_hour(in minutes)" :  (downtime_hour)/60, 
                              "downtime_last_day(in hours)" : (downtime_day)/3600,
                                "downtime_last_week(in hours)" : (downtime_week)/3600})
                # s2 = datetime.now()
                # print(f"append {(s2 - s1).total_seconds()}")
                # print(f"total {(s2 - top).total_seconds()}")
                break
            # df = pd.DataFrame(data)
            # df.to_csv(f"{self.report_id}.csv")
            report_obj.status = "Complete"
            report_obj.save()
        except Exception as e:
            report_obj.status = "Failed"
            report_obj.save()
            print(str(e))