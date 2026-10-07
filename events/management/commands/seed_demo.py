"""
Idempotent demo fixtures: two tenants, two analysts (one a member of each,
one a member of neither), a handful of raw alerts run through the real
ingest pipeline so Events exist with real severities/locations.

Usage: python manage.py seed_demo
"""
import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.test import Client

from events.models import Membership, Tenant

ALERTS = [
    # (tenant, source_id, source_type, payload)
    ("Acme Robotics", "ids-01", "ids", {"message": "Port scan detected from 10.0.4.17", "severity": "high"}),
    ("Acme Robotics", "cam-14", "camera", {"message": "Motion in restricted bay 3", "severity": "medium"}),
    ("Acme Robotics", "robot-07", "robot", {"message": "E-stop triggered on floor 2", "severity": "critical"}),
    ("Globex Industrial", "fw-02", "firewall", {"message": "Outbound connection to known C2 IP", "severity": "critical"}),
    ("Globex Industrial", "reader-09", "card_reader", {"message": "Badge used outside shift hours", "severity": "low"}),
]


class Command(BaseCommand):
    help = "Seed demo tenants, users, memberships, and events for a live walkthrough."

    def handle(self, *args, **options):
        User = get_user_model()

        tenants = {}
        for name in ("Acme Robotics", "Globex Industrial"):
            t, created = Tenant.objects.get_or_create(name=name)
            tenants[name] = t
            self.stdout.write(f"{'Created' if created else 'Found'} tenant: {name} (id={t.id})")

        analyst, created = User.objects.get_or_create(
            username="analyst", defaults={"is_staff": True}
        )
        analyst.set_password("analystpw1")
        analyst.save()
        self.stdout.write(f"{'Created' if created else 'Reset'} user: analyst / analystpw1 (member of Acme only)")

        outsider, created = User.objects.get_or_create(username="outsider")
        outsider.set_password("outsiderpw1")
        outsider.save()
        self.stdout.write(f"{'Created' if created else 'Reset'} user: outsider / outsiderpw1 (member of nothing)")

        if not User.objects.filter(is_superuser=True).exists():
            User.objects.create_superuser("admin", "admin@example.com", "adminpw123")
            self.stdout.write("Created superuser: admin / adminpw123")

        Membership.objects.get_or_create(
            user=analyst, tenant=tenants["Acme Robotics"], defaults={"role": Membership.Role.ANALYST}
        )
        self.stdout.write("Granted analyst membership on Acme Robotics")

        client = Client()
        for tenant_name, source_id, source_type, payload in ALERTS:
            resp = client.post(
                "/ingest/",
                data=json.dumps(
                    {
                        "tenant_id": tenants[tenant_name].id,
                        "source_id": source_id,
                        "source_type": source_type,
                        "payload": payload,
                    }
                ),
                content_type="application/json",
            )
            self.stdout.write(f"Ingested {source_id} -> {tenant_name}: HTTP {resp.status_code}")

        self.stdout.write(self.style.SUCCESS("Demo data ready."))
        self.stdout.write(
            "\nLogin as: analyst / analystpw1  (sees only Acme Robotics, tenant id "
            f"{tenants['Acme Robotics'].id})"
        )
        self.stdout.write(
            f"Globex Industrial tenant id: {tenants['Globex Industrial'].id} "
            "(analyst should be REJECTED here)"
        )
