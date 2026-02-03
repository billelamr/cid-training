import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["CONTACTS_TABLE"]

REQUIRED_FIELDS = ["name", "email", "message"]

def _resp(status: int, payload: Dict[str, Any]):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }

def _parse_json_body(event) -> Dict[str, Any]:
    body = event.get("body") or ""
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {}

def handler(event, context):
    payload = _parse_json_body(event)

    missing = [f for f in REQUIRED_FIELDS if not payload.get(f)]
    if missing:
        return _resp(400, {"error": "validation_error", "missing_fields": missing})

    if "@" not in payload["email"]:
        return _resp(400, {"error": "validation_error", "field": "email", "message": "invalid email"})

    table = dynamodb.Table(TABLE_NAME)

    contact_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    item = {
        "contact_id": contact_id,
        "created_at": now,
        "name": payload["name"].strip(),
        "email": payload["email"].strip().lower(),
        "phone": (payload.get("phone") or "").strip(),
        "subject": (payload.get("subject") or "").strip(),
        "message": payload["message"].strip(),
        "product_id": (payload.get("product_id") or "").strip(),
        "status": "new",
        "source": (payload.get("source") or "website").strip(),
    }

    table.put_item(Item=item)
    return _resp(201, {"ok": True, "contact_id": contact_id})