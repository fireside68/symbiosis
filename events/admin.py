from django.contrib import admin
from unfold.admin import ModelAdmin, TabularInline

from events.models import Membership, Tenant


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
