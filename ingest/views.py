import json

from django.db import transaction
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.shortcuts import render

from events.models import Tenant
from ingest.models import RawAlert
from ingest.tasks import process_raw_alert

# Create your views here.
@csrf_exempt
@require_POST
def receive_alert(request):
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return HttpResponseBadRequest("Invalid JSON")

    try:
        tenant = Tenant.objects.get(id=body["tenant_id"])
        source_id = body["source_id"]
        source_type = body["source_type"]
        payload = body["payload"]
    except KeyError:
        return HttpResponseBadRequest("missing field: tenant_id")
    except Tenant.DoesNotExist:
        return HttpResponseBadRequest("invalid tenant_id; we don't know her")

    with transaction.atomic():
        raw_alert = RawAlert.objects.create(
            tenant=tenant,
            source_id=source_id,
            source_type=source_type,
            payload=payload
        )
        transaction.on_commit(lambda: process_raw_alert.delay(raw_alert.id))

    return JsonResponse({"id": raw_alert.id}, status=202)
