import asyncio
import json

from channels.db import database_sync_to_async
from channels.testing import WebsocketCommunicator
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.test import TestCase
from django.utils import timezone
from strawberry.channels import GraphQLWSConsumer

from api.schema import schema
from events.models import AnalystAction, Device, Event, Membership, Tenant
from ingest.models import RawAlert
from ingest.tasks import broadcast

User = get_user_model()


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


class EventSubscriptionTests(TestCase, EventFixtureMixin):
    """The subscription's auth/membership check is separate code from the
    HTTP resolvers' (ws.scope["user"] vs info.context.request.user), so it
    needs its own coverage, not just an extension of the HTTP tests above."""

    def setUp(self):
        self.tenant = Tenant.objects.create(name="Acme")
        self.member = User.objects.create_user("member", password="pw12345")
        Membership.objects.create(user=self.member, tenant=self.tenant)
        self.event = self.make_event(self.tenant, source_id="acme-dev")

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
