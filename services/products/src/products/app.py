import json
import os
from typing import Any, Dict, List, Optional

import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["PRODUCTS_TABLE"]

def _resp(status: int, payload: Any):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
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