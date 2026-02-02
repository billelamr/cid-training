import json
import os
from typing import Any, Dict, List, Union

import boto3

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["PRODUCTS_TABLE"]

JsonPayload = Union[Dict[str, Any], List[Any]]

def _response(status_code: int, payload: JsonPayload):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }

def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    path_params = event.get("pathParameters") or {}
    product_id = path_params.get("product_id")

    # Détail: /products/{product_id}
    if product_id:
        resp = table.get_item(Key={"product_id": product_id})
        item = resp.get("Item")
        if not item:
            return _response(404, {"error": "product_not_found", "product_id": product_id})
        return _response(200, item)

    # Liste: /products
    resp = table.scan()
    items = resp.get("Items", [])
    return _response(200, items)
