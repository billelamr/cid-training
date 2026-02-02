import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["CONTACTS_TABLE"]

REQUIRED_FIELDS = ["name", "email", "message"]

def _response(status_code: int, payload: Dict[str, Any]):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }

def _parse_body(event) -> Dict[str, Any]:
    body = event.get("body") or ""
    # En proxy integration, body est une string JSON
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {}

def _validate(payload: Dict[str, Any]):
    missing = [f for f in REQUIRED_FIELDS if not payload.get(f)]
    if missing:
        return False, {"error": "validation_error", "missing_fields": missing}

    # validation simple email
    if "@" not in payload["email"]:
        return False, {"error": "validation_error", "field": "email", "message": "invalid email"}

    return True, {}

def handler(event, context):
    payload = _parse_body(event)

    ok, err = _validate(payload)
    if not ok:
        return _response(400, err)

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
        "source": (payload.get("source") or "website").strip(),
        "status": "new",
    }

    table.put_item(Item=item)

    return _response(201, {"ok": True, "contact_id": contact_id})
