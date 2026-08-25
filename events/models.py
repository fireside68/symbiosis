from django.db import models
from django.conf import settings

# Create your models here.

class Tenant(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name
    
class Device(models.Model):
    class Kind(models.TextChoices):
        IDS = "ids"
        FIREWALL = "firewall"
        CARD_READER = "card_reader"
        CAMERA = "camera"
        ROBOT = "robot"
        
    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT)
    kind = models.CharField(max_length=20, choices=Kind.choices)
    name = models.CharField(max_length=200)
    current_location = models.CharField(max_length=200)     # "current" - why events snapshot on their own
    
    def __str__(self):
        return f"{self.name} ({self.kind})"
    
class Event(models.Model):
    class Severity(models.TextChoices):
        LOW = "low"
        MEDIUM = "medium"
        HIGH = "high"
        CRITICAL = "critical"

    class Status(models.TextChoices):
        OPEN = "open"
        IN_PROGRESS = "in_progress"
        RESOLVED = "resolved"

    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT) # every query scoped by this
    occurred_at = models.DateTimeField() # what the device claims -- attacker-influenceable
    received_at = models.DateTimeField(auto_now_add=True) # what we know -- copied from the raw alert
    severity = models.CharField(max_length=10, choices=Severity.choices)
    status = models.CharField(max_length=15, choices=Status.choices, default=Status.OPEN)
    device = models.ForeignKey(Device, on_delete=models.PROTECT)
    location = models.CharField(max_length=200) # SNAPSHOTTED; robots move
    summary = models.CharField(max_length=500)
    claimed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.PROTECT
    )
    resolution_note = models.TextField(blank=True, default="")
    raw_alert = models.ForeignKey("ingest.RawAlert", on_delete=models.PROTECT)
    
    class Meta:
        indexes = [
            models.Index(fields=["tenant", "status", "severity", "-occurred_at"]),
        ]
        ordering = ["-occurred_at"]

class AnalystAction(models.Model):
    """
    Append-only audit trail. who, when, what, to which thing, plus the from/to
    pair that makes any point in time replayable. The append-only
    guarantee is enforced by a Postgres trigger at the database, not by asking
    the app nicely.
    """
    
    class Action(models.TextChoices):
        CLAIM = "claim"
        RESOLVE = "resolve"
    
    analyst = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    timestamp = models.DateTimeField(auto_now_add=True)
    event = models.ForeignKey(Event, on_delete=models.PROTECT)
    action = models.CharField(max_length=20, choices=Action.choices)
    from_status = models.CharField(max_length=15)
    to_status = models.CharField(max_length=15)
