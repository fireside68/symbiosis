import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from events.models import Device, Event, Tenant
from ingest.models import RawAlert
from ingest.tasks import broadcast, process_raw_alert

User = get_user_model()


class ReceiveAlertTests(TestCase):
    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")

    def post(self, body):
        return self.client.post(
            "/ingest/", data=json.dumps(body), content_type="application/json"
        )

    def test_happy_path_creates_raw_alert_and_event(self):
        # TestCase wraps the whole test in one uncommitted transaction, so
        # transaction.on_commit() in the view never fires on its own here —
        # captureOnCommitCallbacks runs it explicitly, standing in for the
        # real commit a live request would get.
        with self.captureOnCommitCallbacks(execute=True):
            resp = self.post(
                {
                    "tenant_id": self.tenant.id,
                    "source_id": "ids-01",
                    "source_type": "ids",
                    "payload": {"message": "port scan", "severity": "high"},
                }
            )
        self.assertEqual(resp.status_code, 202)
        raw_id = resp.json()["id"]

        raw = RawAlert.objects.get(id=raw_id)
        self.assertEqual(raw.tenant, self.tenant)
        self.assertEqual(raw.payload, {"message": "port scan", "severity": "high"})

        # CELERY_TASK_ALWAYS_EAGER in test settings (no REDIS_URL) means
        # process_raw_alert.delay() ran inline once on_commit fired above.
        event = Event.objects.get(raw_alert=raw)
        self.assertEqual(event.severity, Event.Severity.HIGH)
        self.assertEqual(event.summary, "port scan")
        self.assertEqual(event.status, Event.Status.OPEN)
        # location is snapshotted from the device at creation time, not a
        # live pointer to it — that's the whole point (events/models.py).
        self.assertEqual(event.location, event.device.current_location)

    def test_rejects_invalid_json(self):
        resp = self.client.post(
            "/ingest/", data="not json", content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)

    def test_rejects_non_object_body(self):
        resp = self.client.post(
            "/ingest/", data=json.dumps([1, 2, 3]), content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)

    def test_missing_field_names_the_actual_field(self):
        base = {
            "tenant_id": self.tenant.id,
            "source_id": "x",
            "source_type": "ids",
            "payload": {},
        }
        for missing in ("tenant_id", "source_id", "source_type", "payload"):
            body = {k: v for k, v in base.items() if k != missing}
            resp = self.post(body)
            self.assertEqual(resp.status_code, 400, missing)
            self.assertIn(missing, resp.content.decode())

    def test_rejects_non_object_payload(self):
        resp = self.post(
            {
                "tenant_id": self.tenant.id,
                "source_id": "x",
                "source_type": "ids",
                "payload": "not an object",
            }
        )
        self.assertEqual(resp.status_code, 400)

    def test_rejects_unknown_tenant(self):
        resp = self.post(
            {
                "tenant_id": self.tenant.id + 999,
                "source_id": "x",
                "source_type": "ids",
                "payload": {},
            }
        )
        self.assertEqual(resp.status_code, 400)

    def test_rejects_non_integer_tenant_id(self):
        resp = self.post(
            {
                "tenant_id": "not-an-id",
                "source_id": "x",
                "source_type": "ids",
                "payload": {},
            }
        )
        self.assertEqual(resp.status_code, 400)

    def test_get_not_allowed(self):
        resp = self.client.get("/ingest/")
        self.assertEqual(resp.status_code, 405)


class ProcessRawAlertTests(TestCase):
    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")

    def make_raw(self, source_id="ids-01", payload=None):
        return RawAlert.objects.create(
            tenant=self.tenant,
            source_id=source_id,
            source_type="ids",
            payload=payload or {},
        )

    def test_idempotent_on_repeated_processing(self):
        """get_or_create(raw_alert=raw) means re-processing the same raw
        alert (e.g. a retried task) must not create a second Event."""
        raw = self.make_raw(payload={"message": "dup"})
        id1 = process_raw_alert(raw.id)
        id2 = process_raw_alert(raw.id)
        self.assertEqual(id1, id2)
        self.assertEqual(Event.objects.filter(raw_alert=raw).count(), 1)

    def test_reuses_device_for_same_source_and_tenant(self):
        raw1 = self.make_raw(source_id="cam-14")
        raw2 = self.make_raw(source_id="cam-14")
        process_raw_alert(raw1.id)
        process_raw_alert(raw2.id)
        self.assertEqual(Device.objects.filter(tenant=self.tenant, name="cam-14").count(), 1)

    def test_unknown_severity_hint_defaults_to_medium(self):
        raw = self.make_raw(payload={"severity": "apocalyptic"})
        event = Event.objects.get(id=process_raw_alert(raw.id))
        self.assertEqual(event.severity, Event.Severity.MEDIUM)

    def test_missing_message_falls_back_to_source_type(self):
        raw = self.make_raw(payload={})
        event = Event.objects.get(id=process_raw_alert(raw.id))
        self.assertEqual(event.summary, "ids alert")

    def test_broadcasts_added_intent_only_when_created(self):
        raw = self.make_raw(payload={"message": "first"})
        with patch("ingest.tasks.broadcast") as mock_broadcast:
            process_raw_alert(raw.id)
            mock_broadcast.assert_called_once()
            self.assertEqual(mock_broadcast.call_args.kwargs.get("intent"), "added")
            process_raw_alert(raw.id)  # re-processing: no second "added"
            mock_broadcast.assert_called_once()


class BroadcastTests(TestCase):
    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")
        device = Device.objects.create(
            tenant=self.tenant, kind=Device.Kind.IDS, name="ids-01", current_location="loc"
        )
        raw = RawAlert.objects.create(
            tenant=self.tenant, source_id="ids-01", source_type="ids", payload={}
        )
        self.event = Event.objects.create(
            tenant=self.tenant,
            occurred_at=timezone.now(),
            severity=Event.Severity.HIGH,
            device=device,
            location="loc",
            summary="test",
            raw_alert=raw,
        )

    def test_noop_when_no_channel_layer_configured(self):
        with patch("ingest.tasks.get_channel_layer", return_value=None):
            broadcast(self.event, intent="updated")  # must not raise

    def test_sends_expected_group_and_payload(self):
        import asyncio

        async def fake_group_send(group, message):
            fake_group_send.calls.append((group, message))

        fake_group_send.calls = []

        with patch("ingest.tasks.get_channel_layer") as mock_get_layer:
            mock_get_layer.return_value.group_send = fake_group_send
            broadcast(self.event, intent="updated")

        self.assertEqual(len(fake_group_send.calls), 1)
        group, message = fake_group_send.calls[0]
        self.assertEqual(group, f"tenant_{self.tenant.id}")
        self.assertEqual(message, {
            "type": "event.update",
            "event_id": self.event.id,
            "intent": "updated",
        })


class RawAlertAdminReadOnlyTests(TestCase):
    """RawAlert is evidence (the untouched device payload) — see
    ingest/admin.py. Regression coverage for the same reason as
    events.tests.EventAdminReadOnlyTests: these permission overrides are
    the actual guarantee, not incidental, so they need a test that
    actually attempts the forbidden action."""

    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")
        self.superuser = User.objects.create_superuser("admin", "admin@example.com", "pw12345")
        self.raw = RawAlert.objects.create(
            tenant=self.tenant, source_id="dev-1", source_type="ids", payload={"message": "x"}
        )
        self.client.force_login(self.superuser)

    def test_cannot_add(self):
        resp = self.client.get("/admin/ingest/rawalert/add/")
        self.assertEqual(resp.status_code, 403)

    def test_change_view_is_viewable_but_rejects_a_save(self):
        self.assertEqual(
            self.client.get(f"/admin/ingest/rawalert/{self.raw.id}/change/").status_code, 200
        )
        resp = self.client.post(
            f"/admin/ingest/rawalert/{self.raw.id}/change/",
            data={"payload": '{"message": "tampered"}'},
        )
        self.assertEqual(resp.status_code, 403)
        self.raw.refresh_from_db()
        self.assertEqual(self.raw.payload, {"message": "x"})

    def test_cannot_delete(self):
        resp = self.client.post(f"/admin/ingest/rawalert/{self.raw.id}/delete/", data={"post": "yes"})
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(RawAlert.objects.filter(id=self.raw.id).exists())
