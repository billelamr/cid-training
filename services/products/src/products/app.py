import json
import os
from typing import Any, Dict, List, Optional
from decimal import Decimal

import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["PRODUCTS_TABLE"]

def _json_default(o):
    if isinstance(o, Decimal):
        # si c'est un entier (ex: Decimal('4')) -> int
        if o % 1 == 0:
            return int(o)
        # sinon -> float
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

def _resp(status: int, payload):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, default=_json_default),
    }

def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    path_params = event.get("pathParameters") or {}
    product_id = path_params.get("product_id")

    if product_id:
        res = table.get_item(Key={"product_id": product_id})
        item = res.get("Item")
        if not item:
            return _resp(404, {"error": "product_not_found", "product_id": product_id})
        return _resp(200, item)

    res = table.scan()
    items: List[Dict[str, Any]] = res.get("Items", [])
    return _resp(200, items)