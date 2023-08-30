import uuid
from rest_framework.response import Response
from rest_framework.views import APIView
from .thread import *

class TriggerReportGeneration(APIView):
    def get(self, request):
        report_id = str(uuid.uuid4())
        GenerateReportThread(report_id).start()
        return Response(report_id)
    
class GetReportStatus(APIView):
    def post(self, request):
        report_id = request.data.get("report_id", "")
        report_obj = ReportStatus.objects.filter(report_id = report_id)
        if not len(report_obj):
            response = Response("invalid report id")
            response.status_code = 400
            return response
        if report_obj[0].status == "Running":
            return Response("Running")
        else:
            file = open(f"{report_id}.csv", "r")
            return Response(file.write())