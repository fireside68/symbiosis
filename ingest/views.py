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

    if not isinstance(body, dict):
        return HttpResponseBadRequest("body must be a JSON object")

    for field in ("tenant_id", "source_id", "source_type", "payload"):
        if field not in body:
            return HttpResponseBadRequest(f"missing field: {field}")

    if not isinstance(body["payload"], dict):
        # process_raw_alert does payload.get(...) downstream; a list/string/
        # number here would pass this view, then raise inside the task.
        return HttpResponseBadRequest("payload must be a JSON object")

    try:
        tenant = Tenant.objects.get(id=body["tenant_id"])
    except (Tenant.DoesNotExist, ValueError, TypeError):
        # ValueError/TypeError: tenant_id wasn't an int-like value at all
        return HttpResponseBadRequest("invalid tenant_id; we don't know her")

    source_id = body["source_id"]
    source_type = body["source_type"]
    payload = body["payload"]

    with transaction.atomic():
        raw_alert = RawAlert.objects.create(
            tenant=tenant,
            source_id=source_id,
            source_type=source_type,
            payload=payload
        )
        transaction.on_commit(lambda: process_raw_alert.delay(raw_alert.id))

    return JsonResponse({"id": raw_alert.id}, status=202)
