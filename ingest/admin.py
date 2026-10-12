from django.contrib import admin
from unfold.admin import ModelAdmin

from ingest.models import RawAlert


@admin.register(RawAlert)
class RawAlertAdmin(ModelAdmin):
    """Read-only: this is evidence (the untouched device payload), not a
    record to edit from admin. If a raw alert is wrong, that's a fact about
    what the device sent, not something to silently correct here."""

    list_display = ["id", "tenant", "source_id", "source_type", "received_at"]
    list_filter = ["tenant", "source_type"]
    search_fields = ["source_id"]
    readonly_fields = [f.name for f in RawAlert._meta.fields]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
