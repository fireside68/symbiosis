"""
Postgres-only tests: things that are genuinely about the database, not
application code, and that SQLite can't exercise at all.

Skipped automatically unless DATABASE_URL points at Postgres — run with:

    DATABASE_URL=postgres://symbiosis:symbiosis@localhost:5433/symbiosis \
        python manage.py test events.test_postgres
"""
import threading

from django.contrib.auth import get_user_model
from django.db import connection, transaction
from django.db.utils import DatabaseError
from django.test import TransactionTestCase
from django.test import TestCase
from django.utils import timezone
from unittest import skipUnless

from events.models import AnalystAction, Device, Event, Membership, Tenant
from ingest.models import RawAlert

User = get_user_model()

requires_postgres = skipUnless(
    connection.vendor == "postgresql", "requires a real Postgres connection (DATABASE_URL)"
)


class EventFixtureMixin:
    def make_event(self, tenant, status=Event.Status.OPEN, source_id="dev-1"):
        device = Device.objects.create(
            tenant=tenant, kind=Device.Kind.IDS, name=source_id, current_location="loc"
        )
        raw = RawAlert.objects.create(
            tenant=tenant, source_id=source_id, source_type="ids", payload={}
        )
        return Event.objects.create(
            tenant=tenant,
            occurred_at=timezone.now(),
            severity=Event.Severity.HIGH,
            status=status,
            device=device,
            location="loc",
            summary="test event",
            raw_alert=raw,
        )


@requires_postgres
class AppendOnlyTriggerTests(TestCase, EventFixtureMixin):
    """events/migrations/0003_append_only_trigger.py: the one thing that
    makes the audit trail an actual guarantee rather than a convention.
    Admin enforces this in Python (events/admin.py); this test proves the
    database enforces it independently of the app at all."""

    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")
        self.user = User.objects.create_user("analyst", password="pw12345")
        self.event = self.make_event(self.tenant)
        self.action = AnalystAction.objects.create(
            analyst=self.user,
            event=self.event,
            action=AnalystAction.Action.CLAIM,
            from_status=Event.Status.OPEN,
            to_status=Event.Status.IN_PROGRESS,
        )

    def test_update_is_rejected_by_the_database(self):
        with self.assertRaises(DatabaseError):
            with transaction.atomic():
                AnalystAction.objects.filter(id=self.action.id).update(
                    action=AnalystAction.Action.RESOLVE
                )
        # the trigger's exception aborted the transaction; the row is untouched
        self.action.refresh_from_db()
        self.assertEqual(self.action.action, AnalystAction.Action.CLAIM)

    def test_delete_is_rejected_by_the_database(self):
        with self.assertRaises(DatabaseError):
            with transaction.atomic():
                self.action.delete()
        self.assertTrue(AnalystAction.objects.filter(id=self.action.id).exists())


@requires_postgres
class ConcurrentClaimTests(TransactionTestCase, EventFixtureMixin):
    """The sequential two-calls-in-a-row version of this (events.tests.
    ClaimResolveTests.test_claim_already_claimed_fails_the_second_time)
    proves the logic is right but not that it's actually race-safe — two
    sequential calls in one process share one connection and never
    overlap. This fires N real threads with N separate connections at the
    same row at the same instant, which is what the conditional UPDATE is
    actually for. TransactionTestCase (not TestCase): TestCase wraps the
    whole test in one transaction and all queries would share that single
    connection, which would just serialize everything and prove nothing."""

    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")
        self.event = self.make_event(self.tenant)

    def test_exactly_one_of_many_concurrent_claims_wins(self):
        N = 10
        results = [None] * N
        start = threading.Barrier(N)

        def attempt_claim(i):
            # Each thread needs its own connection — Django connections
            # aren't thread-safe to share.
            start.wait()
            try:
                rows = Event.objects.filter(
                    id=self.event.id, status=Event.Status.OPEN
                ).update(status=Event.Status.IN_PROGRESS)
                results[i] = rows
            finally:
                connection.close()  # this thread's connection, not the main one

        threads = [threading.Thread(target=attempt_claim, args=(i,)) for i in range(N)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sum(results), 1, f"expected exactly 1 row updated total, got {results}")
        self.event.refresh_from_db()
        self.assertEqual(self.event.status, Event.Status.IN_PROGRESS)
