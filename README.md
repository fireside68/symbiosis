# Symbiosis

A multi-tenant security-event triage backend. Physical/network security
devices (IDS, firewalls, card readers, cameras, robots) report alerts →
they're normalized into actionable events → analysts triage them in real
time over a GraphQL API with live subscriptions.

## The invariants this is built around

1. **Append-only audit trail, enforced in the database, not the app.**
   `AnalystAction` rows can't be `UPDATE`d or `DELETE`d — a Postgres trigger
   (`events/migrations/0003_append_only_trigger.py`) raises if you try.
   The app can have bugs; the audit log still can't be falsified from it.
   This only exists under Postgres (SQLite has no trigger support), which
   is why `events/admin.py` also makes `AnalystAction` read-only in Django
   admin — otherwise local dev's admin would silently allow what
   production's database refuses.
2. **Race-safe claiming via conditional `UPDATE`.** `claimEvent` runs
   `UPDATE events SET status='in_progress' WHERE id=X AND status='open'`
   and checks the row count. Two analysts claiming the same event at once —
   the database decides exactly one winner; no app-level locking.
3. **Evidence and interpretation are separate tables.** `RawAlert.payload`
   is the untouched device payload; `Event` is what a Celery task derives
   from it. `occurred_at` (device-claimed, attacker-influenceable) is kept
   separate from `received_at` (what we actually know). `Event.location`
   is snapshotted at creation, not a live pointer to the device — the
   audit trail shouldn't shift under you if a robot moves later.
4. **Tenant isolation is enforced at every entry point, not just at the
   query filter.** `Membership(user, tenant)` is checked by the `events`
   query, both mutations (folded into the same conditional `UPDATE`, so a
   cross-tenant claim fails identically to "already claimed" — no separate
   error that would confirm an event ID exists in a tenant you can't see),
   and the subscription (checked against `ws.scope["user"]`, since that's
   a different code path from the HTTP resolvers' `request.user`).

## Quickstart (SQLite, zero infra)

```bash
pip install -r requirements.txt
python manage.py migrate
python manage.py seed_demo      # two tenants, a scoped analyst, five alerts
uvicorn config.asgi:application --port 8000
```

Open **http://localhost:8000/dashboard/** — log in as `analyst` /
`analystpw1` (seeded; a member of Acme Robotics only — see the command's
output for the full account list). Claim an event and watch it update
live; open `/admin/` as `admin` / `adminpw123` for the Django admin.

**Must run under an ASGI server (`uvicorn`/`daphne`), not
`manage.py runserver`.** Channels dropped its `runserver` override in 4.x,
and plain `runserver` only speaks WSGI/HTTP — the subscription's WebSocket
upgrade would never reach the consumer.

## Running with Postgres + Redis

```bash
docker compose up -d          # Postgres on :5433, Redis on :6379
cp .env.example .env           # then uncomment DATABASE_URL and REDIS_URL
python manage.py migrate
uvicorn config.asgi:application --port 8000
celery -A config worker -l info   # separate process, for real async task processing
```

`.env` is loaded automatically (`python-dotenv`, see `config/settings.py`)
and real environment variables always win over it. Without `REDIS_URL` set,
Celery runs tasks eagerly in-process and Channels uses an in-memory layer —
convenient for local dev, but only correct for a single process (won't fan
out across a separate worker, or multiple web processes).

## API

- `POST /ingest/` — `{tenant_id, source_id, source_type, payload}` → `202`
  with the new `RawAlert`'s id. `payload` must be a JSON object.
- `/graphql/` — Strawberry GraphQL, query + mutation + subscription on one
  endpoint (HTTP for query/mutation, WebSocket `graphql-transport-ws` for
  subscription).
  - `login(username, password)` / `logout` — Django session auth.
  - `me` — current username or `null`; lets a client check for an existing
    session without forcing a re-login after a refresh.
  - `myTenants` — tenants the current analyst is a member of.
  - `events(tenantId, status?, severity?, first?, after?)` — id-based
    cursor (`after`), not offset, since offset breaks against a live feed.
  - `claimEvent(id)` / `resolveEvent(id, note?)` — the conditional updates.
  - `eventUpdates(tenantId)` subscription — `intent` is `added` / `updated`
    / `removed` (`removed` meaning "left an open-events board", since a
    resolved event still exists, just not as something to triage).
- `/dashboard/` — a small reference client (inline HTML/CSS/JS, no build
  step) exercising the schema the way a real client would: `fetch()` for
  query/mutation, `graphql-ws` (CDN) for the subscription.
- `/admin/` — Django admin (Unfold theme). `Event`, `AnalystAction`, and
  `RawAlert` are read-only here by design (see invariants above); use the
  GraphQL API to actually change state.

## Testing

```bash
pip install -r requirements-dev.txt   # adds daphne + coverage; see below
python manage.py test                 # SQLite: 48 tests, 3 skipped (Postgres-only)

coverage run manage.py test && coverage report   # 100% on measured app code, see .coveragerc
```

`requirements-dev.txt` adds `daphne` on top of `requirements.txt`. It's
not a runtime dependency — the app is served by `uvicorn` — but
`channels.testing`'s `__init__` unconditionally imports
`daphne.testing.DaphneProcess` even if you only want `WebsocketCommunicator`,
so it's a hard dependency of testing the subscription at all.

Coverage: the conditional-update race (`claimEvent` called twice on the
same event — see below for the *real*, multi-thread version of this),
cross-tenant claim/resolve rejection, the `events` query's filters and
its and the subscription's independent auth/membership checks, every
`events`/`ingest` admin page's read-only enforcement (an actual
add/change/delete attempt, not just checking a method's return value),
ingest validation (missing/invalid fields, non-object payload, unknown
tenant), `process_raw_alert`'s idempotency and severity mapping, and
`broadcast()`'s group/payload shape and no-channel-layer no-op. The
subscription test confirms live delivery over an actual Channels
`WebsocketCommunicator` — i.e. the same mechanism `/dashboard/` depends
on, not a mocked stand-in.

### Postgres-only tests

`events/test_postgres.py` covers two things SQLite genuinely cannot:
the append-only trigger (`events/migrations/0003`) actually rejecting
an `UPDATE`/`DELETE` at the database level, and a real-concurrency
version of the claim race — 10 actual threads with 10 separate
connections hitting the same conditional `UPDATE` at once via a
`threading.Barrier`, not two sequential calls in one process. Both are
skipped automatically unless `DATABASE_URL` points at Postgres:

```bash
docker compose up -d
DATABASE_URL=postgres://symbiosis:symbiosis@localhost:5433/symbiosis \
    python manage.py test
```

### CI

`.github/workflows/ci.yml` runs on every push/PR: a SQLite job (fast,
no services, `coverage report --fail-under=90`) and a Postgres job (a
real `postgres:16` service container, runs the *full* suite against
it — so the trigger and concurrency tests actually execute instead of
skipping, and everything else gets exercised against the real engine
too, not only SQLite).

## Known gaps

- No password reset / user self-service; accounts are created via
  `createsuperuser` or `seed_demo`. Fine for an internal analyst tool,
  not for anything self-serve.
- `login`/`logout` run over the same CSRF-exempt GraphQL endpoint as
  everything else — fine for a token-less API with no browser clients
  sharing cookies with untrusted pages; revisit if that changes.
- No rate limiting on `/ingest/` or `login`.
- `Membership` proves an analyst can see a tenant, not finer-grained
  authorization within one (e.g. by device kind or severity) — `role`
  exists on the model but nothing branches on it yet.
- No pagination on `myTenants` or past ~200 events per `events` query page.
