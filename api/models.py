from django.db import models

class StoreStatus(models.Model):
    id = models.AutoField(primary_key= True)
    store_id = models.TextField()
    status = models.CharField(max_length= 20)
    timestamp_utc = models.TextField(max_length= 40)


class BusinessHours(models.Model):
    id = models.AutoField(primary_key= True)
    store_id = models.TextField()
    day = models.PositiveIntegerField(max_length= 7)
    start_time_local = models.TextField(max_length= 40)
    end_time_local = models.TextField(max_length= 40)

class Timezone(models.Model):
    id = models.AutoField(primary_key= True)
    store_id = models.TextField()
    timezone_str = models.TextField()

class ReportStatus(models.Model):
    report_id = models.TextField(primary_key= True)
    status = models.CharField(max_length= 20)