from django.urls import path, include
import api.views as views


urlpatterns = [
    path('trigger_report', views.TriggerReportGeneration.as_view(), name='TriggerReportGeneration'),
    path('get_report', views.GetReportStatus.as_view(), name='GetReportStatus'),
]