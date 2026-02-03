import json
import os
import urllib.request
import urllib.error
from typing import Any, Dict, Optional

PRODUCTS_BASE = os.environ["PRODUCTS_BASE_URL"].rstrip("/")
CONTACT_BASE = os.environ["CONTACT_BASE_URL"].rstrip("/")

def _resp(status: int, payload: Any):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }

def _http_json(method: str, url: str, body: Optional[Dict[str, Any]] = None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8") if e.fp else ""
        try:
            payload = json.loads(raw) if raw else {"error": "upstream_error"}
        except json.JSONDecodeError:
            payload = {"error": "upstream_error", "raw": raw}
        return e.code, payload

def handler(event, context):
    path = event.get("rawPath") or event.get("path") or ""
    method = (event.get("httpMethod") or "").upper()

    path_params = event.get("pathParameters") or {}
    product_id = path_params.get("product_id")

    # 1) GET /api/products -> GET products-service /products
    if method == "GET" and path.endswith("/api/products"):
        status, data = _http_json("GET", f"{PRODUCTS_BASE}/products")
        return _resp(status, data)

    # 2) GET /api/products/{id} -> GET products-service /products/{id}
    if method == "GET" and product_id:
        status, data = _http_json("GET", f"{PRODUCTS_BASE}/products/{product_id}")
        return _resp(status, data)

    # 3) POST /api/contact -> POST contact-service /contacts
    if method == "POST" and path.endswith("/api/contact"):
        body_str = event.get("body") or ""
        try:
            payload = json.loads(body_str) if body_str else {}
        except json.JSONDecodeError:
            return _resp(400, {"error": "invalid_json"})

        status, data = _http_json("POST", f"{CONTACT_BASE}/contacts", payload)
        return _resp(status, data)

    return _resp(404, {"error": "route_not_found"})