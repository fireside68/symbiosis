"""
raw_alerts - evidence. Deliberately thin, payload untouched JSONB.
JSONField on Postgres is jsonb under the hood; on sqlite it's a text
column that Django (de)serializes - fine for dev; real jsonb is better for production.
"""
from django.db import models

# Create your models here.
class RawAlert(models.Model):
    tenant = models.ForeignKey("events.Tenant", on_delete=models.PROTECT)
    source_id = models.CharField(max_length=200) # device-provided unique ID for this alert
    source_type = models.CharField(max_length=200) # device-provided type for this alert
    payload = models.JSONField()
    received_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        indexes = [
            models.Index(fields=["tenant", "-received_at"]),
        ]