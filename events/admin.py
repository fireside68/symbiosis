from django.contrib import admin
from unfold.admin import ModelAdmin, TabularInline

from events.models import AnalystAction, Device, Event, Membership, Tenant


class MembershipInline(TabularInline):
    model = Membership
    extra = 1
    autocomplete_fields = ["user"]


@admin.register(Tenant)
class TenantAdmin(ModelAdmin):
    list_display = ["name", "created_at"]
    inlines = [MembershipInline]


@admin.register(Membership)
class MembershipAdmin(ModelAdmin):
    list_display = ["user", "tenant", "role", "created_at"]
    list_filter = ["role", "tenant"]
    autocomplete_fields = ["user"]


@admin.register(Device)
class DeviceAdmin(ModelAdmin):
    list_display = ["name", "kind", "tenant", "current_location"]
    list_filter = ["kind", "tenant"]
    search_fields = ["name", "current_location"]


@admin.register(Event)
class EventAdmin(ModelAdmin):
    list_display = ["id", "tenant", "severity", "status", "device", "occurred_at", "claimed_by"]
    list_filter = ["tenant", "status", "severity"]
    search_fields = ["summary", "location"]
    autocomplete_fields = ["claimed_by"]
    # occurred_at is device-claimed, received_at/raw_alert are evidence —
    # editing any of this from admin would undercut the audit trail the
    # same way bypassing claimEvent/resolveEvent would. Admin is for
    # visibility here, not for mutating state outside the GraphQL API.
    readonly_fields = [f.name for f in Event._meta.fields]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(AnalystAction)
class AnalystActionAdmin(ModelAdmin):
    """Read-only in admin, mirroring the Postgres trigger
    (events/migrations/0003) that makes this table append-only at the DB
    level. That trigger doesn't exist under SQLite, so without this,
    local dev's admin would silently let you falsify the audit trail."""

    list_display = ["timestamp", "analyst", "action", "event", "from_status", "to_status"]
    list_filter = ["action"]
    readonly_fields = [f.name for f in AnalystAction._meta.fields]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
