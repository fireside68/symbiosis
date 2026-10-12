import asyncio
import json

from channels.db import database_sync_to_async
from channels.testing import WebsocketCommunicator
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from strawberry.channels import GraphQLWSConsumer

from api.schema import schema
from events.models import AnalystAction, Device, Event, Membership, Tenant
from ingest.models import RawAlert
from ingest.tasks import broadcast

User = get_user_model()


class EventFixtureMixin:
    def make_event(
        self, tenant, status=Event.Status.OPEN, severity=Event.Severity.HIGH, source_id="dev-1"
    ):
        device = Device.objects.create(
            tenant=tenant, kind=Device.Kind.IDS, name=source_id, current_location="loc"
        )
        raw = RawAlert.objects.create(
            tenant=tenant, source_id=source_id, source_type="ids", payload={}
        )
        return Event.objects.create(
            tenant=tenant,
            occurred_at=timezone.now(),
            severity=severity,
            status=status,
            device=device,
            location="loc",
            summary="test event",
            raw_alert=raw,
        )


class GraphQLTestCase(TestCase):
    def gql(self, query, variables=None):
        resp = self.client.post(
            "/graphql/",
            data=json.dumps({"query": query, "variables": variables or {}}),
            content_type="application/json",
        )
        return json.loads(resp.content)

    def login(self, username, password):
        return self.gql(
            "mutation($u: String!, $p: String!) { login(username: $u, password: $p) { ok message } }",
            {"u": username, "p": password},
        )


class AuthMutationTests(GraphQLTestCase):
    def setUp(self):
        self.user = User.objects.create_user("analyst", password="pw12345")

    def test_me_is_null_when_logged_out(self):
        data = self.gql("{ me }")
        self.assertIsNone(data["data"]["me"])

    def test_login_wrong_password_rejected(self):
        data = self.login("analyst", "wrong")
        self.assertFalse(data["data"]["login"]["ok"])

    def test_login_unknown_user_rejected(self):
        data = self.login("ghost", "whatever")
        self.assertFalse(data["data"]["login"]["ok"])

    def test_login_then_me_reflects_session(self):
        login = self.login("analyst", "pw12345")
        self.assertTrue(login["data"]["login"]["ok"])
        data = self.gql("{ me }")
        self.assertEqual(data["data"]["me"], "analyst")

    def test_logout_clears_session(self):
        self.login("analyst", "pw12345")
        self.gql("mutation { logout { ok } }")
        data = self.gql("{ me }")
        self.assertIsNone(data["data"]["me"])


class MembershipScopingTests(GraphQLTestCase, EventFixtureMixin):
    def setUp(self):
        self.acme = Tenant.objects.create(name="Acme")
        self.globex = Tenant.objects.create(name="Globex")
        self.member = User.objects.create_user("member", password="pw12345")
        self.outsider = User.objects.create_user("outsider", password="pw12345")
        Membership.objects.create(user=self.member, tenant=self.acme)
        self.acme_event = self.make_event(self.acme, source_id="acme-dev")
        self.globex_event = self.make_event(self.globex, source_id="globex-dev")

    EVENTS_QUERY = "query($t: Int!) { events(tenantId: $t) { id } }"

    def test_events_requires_authentication(self):
        data = self.gql(self.EVENTS_QUERY, {"t": self.acme.id})
        self.assertIsNone(data["data"])
        self.assertIn("Authentication required", data["errors"][0]["message"])

    def test_my_tenants_scoped_to_membership(self):
        self.login("member", "pw12345")
        data = self.gql("{ myTenants { name } }")
        self.assertEqual([t["name"] for t in data["data"]["myTenants"]], ["Acme"])

    def test_outsider_sees_no_tenants(self):
        self.login("outsider", "pw12345")
        data = self.gql("{ myTenants { name } }")
        self.assertEqual(data["data"]["myTenants"], [])

    def test_events_rejects_logged_in_non_member(self):
        self.login("outsider", "pw12345")
        data = self.gql(self.EVENTS_QUERY, {"t": self.acme.id})
        self.assertIsNone(data["data"])
        self.assertIn("Not a member of this tenant", data["errors"][0]["message"])

    def test_events_returns_data_for_member(self):
        self.login("member", "pw12345")
        data = self.gql(self.EVENTS_QUERY, {"t": self.acme.id})
        self.assertEqual([e["id"] for e in data["data"]["events"]], [str(self.acme_event.id)])

    def test_member_of_one_tenant_cannot_query_the_other(self):
        self.login("member", "pw12345")
        data = self.gql(self.EVENTS_QUERY, {"t": self.globex.id})
        self.assertIsNone(data["data"])
        self.assertIn("Not a member of this tenant", data["errors"][0]["message"])


class EventsFilterTests(GraphQLTestCase, EventFixtureMixin):
    def setUp(self):
        self.acme = Tenant.objects.create(name="Acme")
        self.member = User.objects.create_user("member", password="pw12345")
        Membership.objects.create(user=self.member, tenant=self.acme)
        # Deliberately distinct status/severity so each filter test can only
        # pass if the query actually applied that filter, not just returned
        # everything in the tenant.
        self.open_high = self.make_event(self.acme, source_id="dev-1")
        self.resolved_low = self.make_event(
            self.acme, status=Event.Status.RESOLVED, severity=Event.Severity.LOW, source_id="dev-2"
        )
        self.open_critical = self.make_event(
            self.acme, severity=Event.Severity.CRITICAL, source_id="dev-3"
        )
        self.login("member", "pw12345")

    def test_filters_by_status(self):
        data = self.gql(
            "query($t: Int!, $s: String) { events(tenantId: $t, status: $s) { id } }",
            {"t": self.acme.id, "s": "resolved"},
        )
        self.assertEqual([e["id"] for e in data["data"]["events"]], [str(self.resolved_low.id)])

    def test_filters_by_severity(self):
        data = self.gql(
            "query($t: Int!, $sev: String) { events(tenantId: $t, severity: $sev) { id } }",
            {"t": self.acme.id, "sev": "critical"},
        )
        self.assertEqual([e["id"] for e in data["data"]["events"]], [str(self.open_critical.id)])

    def test_after_cursor_excludes_ids_at_or_above_it(self):
        # order_by("-id"): "after" means "older than this cursor", so the
        # highest-id fixture used as the cursor must exclude itself.
        data = self.gql(
            "query($t: Int!, $after: ID) { events(tenantId: $t, after: $after) { id } }",
            {"t": self.acme.id, "after": str(self.open_critical.id)},
        )
        returned_ids = {int(e["id"]) for e in data["data"]["events"]}
        self.assertNotIn(self.open_critical.id, returned_ids)
        self.assertTrue(all(i < self.open_critical.id for i in returned_ids))
        self.assertIn(self.resolved_low.id, returned_ids)


class ClaimResolveTests(GraphQLTestCase, EventFixtureMixin):
    def setUp(self):
        self.acme = Tenant.objects.create(name="Acme")
        self.globex = Tenant.objects.create(name="Globex")
        self.member = User.objects.create_user("member", password="pw12345")
        Membership.objects.create(user=self.member, tenant=self.acme)
        self.event = self.make_event(self.acme, source_id="acme-dev")
        self.globex_event = self.make_event(self.globex, source_id="globex-dev")

    def claim(self, event_id):
        return self.gql(
            "mutation($id: ID!) { claimEvent(id: $id) { ok message event { status } } }",
            {"id": str(event_id)},
        )

    def resolve(self, event_id, note=""):
        return self.gql(
            "mutation($id: ID!, $note: String!) { "
            "resolveEvent(id: $id, note: $note) { ok message event { status } } }",
            {"id": str(event_id), "note": note},
        )

    def test_claim_unauthenticated_rejected(self):
        data = self.claim(self.event.id)
        self.assertEqual(
            data["data"]["claimEvent"], {"ok": False, "message": "Authentication required", "event": None}
        )

    def test_claim_happy_path_writes_audit_row(self):
        self.login("member", "pw12345")
        data = self.claim(self.event.id)
        self.assertTrue(data["data"]["claimEvent"]["ok"])
        self.event.refresh_from_db()
        self.assertEqual(self.event.status, Event.Status.IN_PROGRESS)
        action = AnalystAction.objects.get(event=self.event)
        self.assertEqual(action.action, AnalystAction.Action.CLAIM)
        self.assertEqual(action.from_status, Event.Status.OPEN)
        self.assertEqual(action.to_status, Event.Status.IN_PROGRESS)

    def test_claim_already_claimed_fails_the_second_time(self):
        """THE conditional UPDATE: two claims on the same event, exactly one wins."""
        self.login("member", "pw12345")
        first = self.claim(self.event.id)
        second = self.claim(self.event.id)
        self.assertTrue(first["data"]["claimEvent"]["ok"])
        self.assertFalse(second["data"]["claimEvent"]["ok"])
        self.assertEqual(second["data"]["claimEvent"]["message"], "Already claimed")
        self.assertEqual(AnalystAction.objects.filter(event=self.event).count(), 1)

    def test_claim_cross_tenant_rejected_without_revealing_existence(self):
        self.login("member", "pw12345")
        data = self.claim(self.globex_event.id)
        self.assertFalse(data["data"]["claimEvent"]["ok"])
        # Same message as "already claimed" — no separate error path that
        # would confirm this event ID exists in a tenant the caller isn't in.
        self.assertEqual(data["data"]["claimEvent"]["message"], "Already claimed")
        self.globex_event.refresh_from_db()
        self.assertEqual(self.globex_event.status, Event.Status.OPEN)

    def test_resolve_unauthenticated_rejected(self):
        data = self.resolve(self.event.id)
        self.assertEqual(
            data["data"]["resolveEvent"],
            {"ok": False, "message": "Authentication required", "event": None},
        )

    def test_resolve_cross_tenant_rejected_without_revealing_existence(self):
        self.globex_event.status = Event.Status.IN_PROGRESS
        self.globex_event.save(update_fields=["status"])
        self.login("member", "pw12345")
        data = self.resolve(self.globex_event.id, "n/a")
        self.assertFalse(data["data"]["resolveEvent"]["ok"])
        # Same message as "not in progress" — doesn't confirm the event
        # exists and *is* in_progress in a tenant the caller can't see.
        self.assertEqual(data["data"]["resolveEvent"]["message"], "Not in progress")
        self.globex_event.refresh_from_db()
        self.assertEqual(self.globex_event.status, Event.Status.IN_PROGRESS)

    def test_resolve_requires_in_progress(self):
        self.login("member", "pw12345")
        data = self.resolve(self.event.id, "too early")
        self.assertFalse(data["data"]["resolveEvent"]["ok"])
        self.assertEqual(data["data"]["resolveEvent"]["message"], "Not in progress")

    def test_resolve_happy_path_writes_audit_row(self):
        self.login("member", "pw12345")
        self.claim(self.event.id)
        data = self.resolve(self.event.id, "false positive")
        self.assertTrue(data["data"]["resolveEvent"]["ok"])
        self.event.refresh_from_db()
        self.assertEqual(self.event.status, Event.Status.RESOLVED)
        self.assertEqual(self.event.resolution_note, "false positive")
        action = AnalystAction.objects.get(event=self.event, action=AnalystAction.Action.RESOLVE)
        self.assertEqual(action.from_status, Event.Status.IN_PROGRESS)
        self.assertEqual(action.to_status, Event.Status.RESOLVED)


class DashboardViewTests(TestCase):
    def test_renders_shell(self):
        resp = self.client.get("/dashboard/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Symbiosis", resp.content)


class EventAdminReadOnlyTests(TestCase, EventFixtureMixin):
    """Regression coverage for events/admin.py's permission overrides —
    the whole point of making Event/AnalystAction read-only in admin is
    that the DB-level guarantees (conditional UPDATE, the Postgres
    append-only trigger) can't be bypassed through a side door. A test
    that never actually POSTs to these views wouldn't catch a future edit
    that accidentally re-enabled them."""

    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")
        self.superuser = User.objects.create_superuser("admin", "admin@example.com", "pw12345")
        self.event = self.make_event(self.tenant, source_id="dev-1")
        self.client.force_login(self.superuser)

    def test_cannot_add(self):
        resp = self.client.get("/admin/events/event/add/")
        self.assertEqual(resp.status_code, 403)

    def test_change_view_is_viewable_but_rejects_a_save(self):
        self.assertEqual(self.client.get(f"/admin/events/event/{self.event.id}/change/").status_code, 200)
        resp = self.client.post(
            f"/admin/events/event/{self.event.id}/change/", data={"status": "resolved"}
        )
        self.assertEqual(resp.status_code, 403)
        self.event.refresh_from_db()
        self.assertEqual(self.event.status, Event.Status.OPEN)

    def test_cannot_delete(self):
        resp = self.client.post(f"/admin/events/event/{self.event.id}/delete/", data={"post": "yes"})
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(Event.objects.filter(id=self.event.id).exists())

    def test_analyst_action_cannot_be_edited_or_deleted(self):
        user = User.objects.create_user("analyst", password="pw12345")
        action = AnalystAction.objects.create(
            analyst=user,
            event=self.event,
            action=AnalystAction.Action.CLAIM,
            from_status=Event.Status.OPEN,
            to_status=Event.Status.IN_PROGRESS,
        )
        resp = self.client.post(
            f"/admin/events/analystaction/{action.id}/change/", data={"action": "resolve"}
        )
        self.assertEqual(resp.status_code, 403)
        action.refresh_from_db()
        self.assertEqual(action.action, AnalystAction.Action.CLAIM)
        resp = self.client.post(f"/admin/events/analystaction/{action.id}/delete/", data={"post": "yes"})
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(AnalystAction.objects.filter(id=action.id).exists())


class EventSubscriptionTests(TransactionTestCase, EventFixtureMixin):
    """The subscription's auth/membership check is separate code from the
    HTTP resolvers' (ws.scope["user"] vs info.context.request.user), so it
    needs its own coverage, not just an extension of the HTTP tests above.

    TransactionTestCase, not TestCase, and fixtures built via
    database_sync_to_async from inside each async test rather than a plain
    synchronous setUp(): under SQLite this class worked either way, but
    under real Postgres a synchronous setUp() reliably raised "the
    connection is closed" once an async test method followed it — psycopg3
    is stricter than SQLite about a connection opened on one thread/loop
    being touched from another. This is the pattern Channels' own docs use
    for exactly this reason, not an arbitrary choice.
    """

    async def _setup_fixtures(self):
        def _create():
            tenant = Tenant.objects.create(name="Acme")
            member = User.objects.create_user("member", password="pw12345")
            Membership.objects.create(user=member, tenant=tenant)
            event = self.make_event(tenant, source_id="acme-dev")
            return tenant, member, event

        self.tenant, self.member, self.event = await database_sync_to_async(_create)()

    async def _connected_communicator(self, user):
        application = GraphQLWSConsumer.as_asgi(schema=schema)
        communicator = WebsocketCommunicator(
            application, "/graphql/", subprotocols=["graphql-transport-ws"]
        )
        # Standing in for AuthMiddlewareStack (config/asgi.py), which would
        # normally populate scope["user"] from the session cookie — tested
        # separately by Channels itself; here we test our resolver's use of it.
        communicator.scope["user"] = user
        connected, _ = await communicator.connect()
        self.assertTrue(connected)
        await communicator.send_json_to({"type": "connection_init"})
        ack = await communicator.receive_json_from(timeout=5)
        self.assertEqual(ack["type"], "connection_ack")
        return communicator

    async def _subscribe(self, communicator, tenant_id):
        await communicator.send_json_to(
            {
                "id": "1",
                "type": "subscribe",
                "payload": {
                    "query": "subscription($t: Int!) { eventUpdates(tenantId: $t) "
                    "{ intent event { id status } } }",
                    "variables": {"t": tenant_id},
                },
            }
        )

    async def test_unauthenticated_subscribe_is_rejected(self):
        await self._setup_fixtures()
        communicator = await self._connected_communicator(AnonymousUser())
        await self._subscribe(communicator, self.tenant.id)
        # A resolver error raised before the first yield comes back as a
        # "next" message carrying payload.errors, not a transport-level
        # "error" message (that's reserved for protocol/validation failures).
        msg = await communicator.receive_json_from(timeout=5)
        self.assertEqual(msg["type"], "next")
        self.assertIsNone(msg["payload"]["data"])
        self.assertIn("Authentication required", msg["payload"]["errors"][0]["message"])
        await communicator.disconnect()

    async def test_non_member_subscribe_is_rejected(self):
        await self._setup_fixtures()
        outsider = await database_sync_to_async(User.objects.create_user)(
            "outsider", password="pw12345"
        )
        communicator = await self._connected_communicator(outsider)
        await self._subscribe(communicator, self.tenant.id)
        msg = await communicator.receive_json_from(timeout=5)
        self.assertEqual(msg["type"], "next")
        self.assertIsNone(msg["payload"]["data"])
        self.assertIn("Not a member of this tenant", msg["payload"]["errors"][0]["message"])
        await communicator.disconnect()

    async def test_member_receives_broadcast_live(self):
        await self._setup_fixtures()
        communicator = await self._connected_communicator(self.member)
        await self._subscribe(communicator, self.tenant.id)

        # No "ready" ack exists between "subscribe" and the resolver actually
        # joining the channel group — it awaits a DB membership check first —
        # so broadcasting immediately can race ahead of the join. A retry
        # loop doesn't work around this: ApplicationCommunicator.receive_output
        # cancels (and permanently kills) the consumer's task the first time
        # a receive times out, so this is a one-shot wait, not a poll. The
        # gap is a few in-process event-loop ticks (an .exists() query on an
        # in-memory sqlite db), not real I/O, so this is as deterministic as
        # a sleep gets here — the live demo already proved the real thing
        # works end-to-end over an actual network connection.
        await asyncio.sleep(0.2)
        await database_sync_to_async(broadcast)(self.event, intent="updated")

        msg = await communicator.receive_json_from(timeout=5)
        self.assertEqual(msg["type"], "next")
        payload = msg["payload"]["data"]["eventUpdates"]
        self.assertEqual(payload["intent"], "updated")
        self.assertEqual(payload["event"]["id"], str(self.event.id))
        await communicator.disconnect()
