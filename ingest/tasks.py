from asgiref.sync import async_to_sync
from celery import shared_task
from channels.layers import get_channel_layer
from django.utils.dateparse import parse_datetime

from events.models import Device, Event
from ingest.models import RawAlert

SEVERITY_BY_HINT = {
    "critical": Event.Severity.CRITICAL,
    "high": Event.Severity.HIGH,
    "medium": Event.Severity.MEDIUM,
    "low": Event.Severity.LOW,
}


def broadcast(event, intent: str):
    """Push an event change to every analyst watching this tenant's feed.
    Sync entry point (called from a Celery task and from on_commit callbacks
    inside sync GraphQL mutations) — group_send itself is async, so
    async_to_sync bridges it, mirroring listen_to_channel on the read side.
    """
    layer = get_channel_layer()
    if layer is None:  # no channel layer configured; nothing to notify
        return
    async_to_sync(layer.group_send)(
        f"tenant_{event.tenant_id}",
        {"type": "event.update", "event_id": event.id, "intent": intent},
    )


@shared_task
def process_raw_alert(raw_alert_id: int):
    raw = RawAlert.objects.select_related("tenant").get(id=raw_alert_id)
    
    # --- normalize
    p = raw.payload
    occurred_at = parse_datetime(p.get("timestamp", "")) or raw.received_at
    summary = p.get("message") or p.get("description") or f"{raw.source_type} alert"
    
    # --- enrich
    device, _ = Device.objects.get_or_create(
        tenant=raw.tenant,
        name=raw.source_id,
        defaults={"kind": raw.source_type, "current_location": "unknown"},
    )
    
    # --- score
    severity = SEVERITY_BY_HINT.get(str(p.get("severity", "")).lower(), "medium")
    
    event, created = Event.objects.get_or_create(
        raw_alert=raw,
        defaults=dict(
            tenant=raw.tenant,
            occurred_at=occurred_at,
            received_at=raw.received_at,
            severity=severity,
            device=device,
            location=device.current_location,
            summary=summary[:500],
            ),
    )
    if created:
        broadcast(event, intent="added")
    return event.id