"""GraphQL via Strawberry: query · mutation · subscription — the three operations.
Strawberry types are Python type hints (closer to Absinthe-with-typespecs than
Graphene's class soup — it's why your notes picked it)."""
from typing import AsyncGenerator, Optional

import strawberry
import strawberry_django
from asgiref.sync import sync_to_async
from django.contrib.auth import authenticate, login as django_login, logout as django_logout
from django.db import transaction
from graphql import GraphQLError

from events.models import AnalystAction, Event
from ingest.tasks import broadcast


@strawberry_django.type(Event)
class EventType:
    id: strawberry.auto
    occurred_at: strawberry.auto
    received_at: strawberry.auto
    severity: strawberry.auto
    status: strawberry.auto
    location: strawberry.auto
    summary: strawberry.auto


@strawberry.type
class ClaimResult:
    ok: bool
    message: str
    event: Optional[EventType]


@strawberry.type
class AuthResult:
    ok: bool
    message: str


def _current_user(info: strawberry.Info):
    """The authenticated analyst for this request, or None. Django session
    auth: AuthenticationMiddleware already populates request.user from the
    sessionid cookie, the same way it does for admin/ — login/logout below
    just drive that same session via the GraphQL endpoint.

    HTTP only: info.context is a StrawberryDjangoContext with `.request`
    for GraphQLView. The WS consumer's context is a plain dict (see
    event_updates below), which reads the user off `ws.scope` instead,
    populated by the AuthMiddlewareStack wrapping it in config/asgi.py.
    """
    request = getattr(info.context, "request", None)
    user = getattr(request, "user", None)
    if user and user.is_authenticated:
        return user
    return None


def _require_user(info: strawberry.Info):
    user = _current_user(info)
    if user is None:
        raise GraphQLError("Authentication required")
    return user


@strawberry.type
class Query:
    @strawberry_django.field
    def events(
        self,
        info: strawberry.Info,
        # tenant_id is still an explicit arg, not derived from the analyst:
        # there's no User<->Tenant membership model yet, so this only proves
        # *someone* is logged in, not that they're allowed to see this tenant.
        tenant_id: int,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        first: int = 50,
        after: Optional[strawberry.ID] = None,  # cursor = last-seen id; offset breaks on a live feed
    ) -> list[EventType]:
        _require_user(info)
        qs = Event.objects.select_related("device").filter(tenant_id=tenant_id)
        if status:
            qs = qs.filter(status=status)
        if severity:
            qs = qs.filter(severity=severity)
        if after:
            qs = qs.filter(id__lt=int(after))
        return qs.order_by("-id")[: min(first, 200)]


@strawberry.type
class Mutation:
    @strawberry_django.mutation
    def login(self, info: strawberry.Info, username: str, password: str) -> AuthResult:
        """Establishes the Django session the rest of the schema reads
        request.user from. Runs over the same csrf_exempt GraphQLView as
        everything else — fine for a token-less internal API, but it means
        there's no CSRF protection on this endpoint; revisit if it's ever
        exposed to a browser session shared with untrusted pages."""
        request = info.context.request
        user = authenticate(request, username=username, password=password)
        if user is None:
            return AuthResult(ok=False, message="Invalid credentials")
        django_login(request, user)
        return AuthResult(ok=True, message=f"Logged in as {user.username}")

    @strawberry_django.mutation
    def logout(self, info: strawberry.Info) -> AuthResult:
        django_logout(info.context.request)
        return AuthResult(ok=True, message="Logged out")

    @strawberry_django.mutation
    def claim_event(self, info: strawberry.Info, id: strawberry.ID) -> ClaimResult:
        """THE conditional UPDATE. The database decides, once."""
        user = _current_user(info)
        if user is None:
            return ClaimResult(ok=False, message="Authentication required", event=None)
        with transaction.atomic():
            rows = Event.objects.filter(id=id, status=Event.Status.OPEN).update(
                status=Event.Status.IN_PROGRESS, claimed_by=user
            )
            if rows == 0:
                return ClaimResult(ok=False, message="Already claimed", event=None)
            event = Event.objects.get(id=id)
            # same transaction as the status write — otherwise status and log drift
            AnalystAction.objects.create(
                analyst=user, action=AnalystAction.Action.CLAIM, event=event,
                from_status=Event.Status.OPEN, to_status=Event.Status.IN_PROGRESS,
            )
            transaction.on_commit(lambda: broadcast(event, intent="updated"))
        return ClaimResult(ok=True, message="Claimed", event=event)

    @strawberry_django.mutation
    def resolve_event(self, info: strawberry.Info, id: strawberry.ID, note: str = "") -> ClaimResult:
        user = _current_user(info)
        if user is None:
            return ClaimResult(ok=False, message="Authentication required", event=None)
        with transaction.atomic():
            rows = Event.objects.filter(id=id, status=Event.Status.IN_PROGRESS).update(
                status=Event.Status.RESOLVED, resolution_note=note
            )
            if rows == 0:
                return ClaimResult(ok=False, message="Not in progress", event=None)
            event = Event.objects.get(id=id)
            AnalystAction.objects.create(
                analyst=user, action=AnalystAction.Action.RESOLVE, event=event,
                from_status=Event.Status.IN_PROGRESS, to_status=Event.Status.RESOLVED,
            )
            # 'removed' intent: a resolved event LEAVES an open-events filter
            transaction.on_commit(lambda: broadcast(event, intent="removed"))
        return ClaimResult(ok=True, message="Resolved", event=event)


@strawberry.type
class EventUpdate:
    intent: str  # added | updated | removed
    event: EventType


@strawberry.type
class Subscription:
    @strawberry.subscription
    async def event_updates(
        self, info: strawberry.Info, tenant_id: int
    ) -> AsyncGenerator[EventUpdate, None]:
        """Rides the channel layer. listen_to_channel is a context manager
        (it joins the group on enter, discards on exit) that hands you the
        generator of messages — hence `async with` wrapping `async for`."""
        ws = info.context["ws"]
        # scope["user"] comes from AuthMiddlewareStack (config/asgi.py) reading
        # the same session cookie login/logout above write to over HTTP.
        user = ws.scope.get("user")
        if not (user and user.is_authenticated):
            raise GraphQLError("Authentication required")
        async with ws.listen_to_channel(
            "event.update", groups=[f"tenant_{tenant_id}"]
        ) as messages:
            async for message in messages:
                event = await sync_to_async(
                    Event.objects.select_related("device").get
                )(id=message["event_id"])
                yield EventUpdate(intent=message["intent"], event=event)


schema = strawberry.Schema(query=Query, mutation=Mutation, subscription=Subscription)
