import json
import os
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["PRODUCTS_TABLE"]


def _json_default(o):
    if isinstance(o, Decimal):
        # si entier -> int, sinon -> float
        if o % 1 == 0:
            return int(o)
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _resp(status: int, payload, headers: Optional[Dict[str, str]] = None):
    base_headers = {"Content-Type": "application/json"}
    if headers:
        base_headers.update(headers)
    return {
        "statusCode": status,
        "headers": base_headers,
        "body": json.dumps(payload, default=_json_default),
    }


def _get_qs(event) -> Dict[str, str]:
    # API Gateway REST: queryStringParameters peut être None
    return event.get("queryStringParameters") or {}


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    # 1) Détail: /products/{product_id}
    path_params = event.get("pathParameters") or {}
    product_id = path_params.get("product_id")
    if product_id:
        res = table.get_item(Key={"product_id": product_id})
        item = res.get("Item")
        if not item:
            return _resp(404, {"error": "product_not_found", "product_id": product_id})
        return _resp(200, item)

    # 2) Liste: /products + filtres
    qs = _get_qs(event)

    typ = qs.get("type")          # "category" | "product"
    parent_id = qs.get("parent_id")
    category = qs.get("category")  # ex: "engrais" (optionnel)

    # FilterExpression (scan)
    filt = None

    if typ:
        if typ not in ("category", "product"):
            return _resp(400, {"error": "invalid_type", "expected": ["category", "product"], "got": typ})
        filt = Attr("type").eq(typ)

    if parent_id:
        cond = Attr("parent_id").eq(parent_id)
        filt = cond if filt is None else (filt & cond)

    if category:
        cond = Attr("category").eq(category)
        filt = cond if filt is None else (filt & cond)

    # (optionnel) pagination basique
    limit = qs.get("limit")
    next_token = qs.get("next_token")

    scan_kwargs: Dict[str, Any] = {}
    if filt is not None:
        scan_kwargs["FilterExpression"] = filt
    if limit:
        try:
            scan_kwargs["Limit"] = int(limit)
        except ValueError:
            return _resp(400, {"error": "invalid_limit", "got": limit})
    if next_token:
        # next_token = JSON de LastEvaluatedKey encodé en base64-url (simple)
        try:
            import base64
            lek_json = base64.urlsafe_b64decode(next_token.encode("utf-8")).decode("utf-8")
            scan_kwargs["ExclusiveStartKey"] = json.loads(lek_json)
        except Exception:
            return _resp(400, {"error": "invalid_next_token"})

    res = table.scan(**scan_kwargs)
    items: List[Dict[str, Any]] = res.get("Items", [])

    # renvoyer next_token si pagination
    lek = res.get("LastEvaluatedKey")
    if lek:
        import base64
        token = base64.urlsafe_b64encode(json.dumps(lek).encode("utf-8")).decode("utf-8")
        return _resp(200, {"items": items, "next_token": token})

    return _resp(200, {"items": items})