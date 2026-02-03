import json
import os
import urllib.request
import urllib.error
from typing import Any, Dict, Optional, List

PRODUCTS_BASE = os.environ["PRODUCTS_BASE_URL"].rstrip("/")
CONTACT_BASE = os.environ["CONTACT_BASE_URL"].rstrip("/")


def _resp(status: int, payload: Any):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        # accents lisibles
        "body": json.dumps(payload, ensure_ascii=False),
    }


def _http_json(method: str, url: str, body: Optional[Dict[str, Any]] = None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else None

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8") if e.fp else ""
        try:
            payload = json.loads(raw) if raw else {"error": "upstream_error"}
        except json.JSONDecodeError:
            payload = {"error": "upstream_error", "raw": raw}
        return e.code, payload


def _as_list_payload(data: Any) -> List[Dict[str, Any]]:
    """
    products-service renvoie maintenant:
      {"items": [...], "next_token": "..."} ou {"items": [...]}
    mais au début il renvoyait parfois directement une liste.
    On rend ça robuste.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("items", [])
    return []


def handler(event, context):
    path = event.get("rawPath") or event.get("path") or ""
    method = (event.get("httpMethod") or "").upper()
    path_params = event.get("pathParameters") or {}
    product_id = path_params.get("product_id")

    # -----------------------------
    # 0) GET /api/catalog
    # -----------------------------
    # But: réponse "prête front" (menu + catégories + produits groupés)
    #
    # Stratégie:
    # - GET products?type=category  -> catégories + sous-catégories
    # - GET products?type=product   -> produits
    # - Assemble:
    #   level=1 => top categories
    #   level=2 => sub categories (parent_id = top category "engrais", "produits-chimiques", ...)
    #   products => attach to matching parent_id (ex: engrais__idha, produits-chimiques__chlorure, ...)
    if method == "GET" and path.endswith("/api/catalog"):
        s1, cats_data = _http_json("GET", f"{PRODUCTS_BASE}/products?type=category")
        if s1 >= 400:
            return _resp(s1, {"error": "products_categories_failed", "details": cats_data})

        s2, prods_data = _http_json("GET", f"{PRODUCTS_BASE}/products?type=product")
        if s2 >= 400:
            return _resp(s2, {"error": "products_list_failed", "details": prods_data})

        categories = _as_list_payload(cats_data)
        products = _as_list_payload(prods_data)

        # Index des catégories par id
        cat_by_id: Dict[str, Dict[str, Any]] = {}
        top: List[Dict[str, Any]] = []

        # 1) Préparer les catégories
        for c in categories:
            cid = c.get("product_id")
            if not cid:
                continue

            node = {
                "id": cid,
                "name": c.get("name"),
                "url": c.get("source_url"),
                "category": c.get("category"),
                "level": c.get("level"),
                # pour le menu:
                "children": [],   # sous-catégories
                "products": [],   # produits directement rattachés (rare)
            }
            cat_by_id[cid] = node

        # 2) Trouver les top categories (level=1 ou pas de parent_id)
        #    + rattacher les sous-catégories (level=2)
        for c in categories:
            cid = c.get("product_id")
            if not cid or cid not in cat_by_id:
                continue

            node = cat_by_id[cid]
            parent = c.get("parent_id")

            if not parent:
                # pas de parent => top
                top.append(node)
            else:
                # parent_id de niveau 2 ressemble à "engrais" ou "produits-chimiques"
                # Mais nos top categories ont des IDs type "engrais__xxxx".
                # On rattache donc par match sur "category" + level, ou par recherche:
                #
                # - si parent correspond à un "category slug" (engrais/produits-chimiques)
                #   on cherche le top node dont node["category"] == parent et level==1
                parent_slug = parent
                parent_top = None
                for t in cat_by_id.values():
                    if t.get("level") == 1 and t.get("category") == parent_slug:
                        parent_top = t
                        break
                if parent_top:
                    parent_top["children"].append(node)
                else:
                    # fallback: si le parent_id est un vrai ID (au cas où)
                    if parent in cat_by_id:
                        cat_by_id[parent]["children"].append(node)

        # 3) Rattacher les produits à leur parent_id (souvent une sous-catégorie id "engrais__idha", etc.)
        for p in products:
            pid = p.get("product_id")
            if not pid:
                continue
            parent_id = p.get("parent_id")
            prod = {
                "id": pid,
                "name": p.get("name"),
                "url": p.get("source_url"),
                "category": p.get("category"),
                "level": p.get("level"),
                "type": p.get("type"),
            }
            if parent_id and parent_id in cat_by_id:
                cat_by_id[parent_id]["products"].append(prod)
            else:
                # fallback: si pas de parent match, on peut le ranger sous la top category via "category"
                placed = False
                for t in top:
                    if t.get("category") == p.get("category"):
                        t["products"].append(prod)
                        placed = True
                        break
                if not placed:
                    # sinon on le met dans un "orphans"
                    pass

        # Option: trier par nom
        def by_name(x):
            return (x.get("name") or "").lower()

        for t in top:
            t["children"].sort(key=by_name)
            t["products"].sort(key=by_name)
            for ch in t["children"]:
                ch["products"].sort(key=by_name)

        top.sort(key=by_name)

        return _resp(200, {"categories": top})

    # -----------------------------
    # 1) GET /api/products -> products-service /products
    # -----------------------------
    if method == "GET" and path.endswith("/api/products"):
        status, data = _http_json("GET", f"{PRODUCTS_BASE}/products")
        return _resp(status, data)

    # -----------------------------
    # 2) GET /api/products/{id} -> products-service /products/{id}
    # -----------------------------
    if method == "GET" and product_id:
        status, data = _http_json("GET", f"{PRODUCTS_BASE}/products/{product_id}")
        return _resp(status, data)

    # -----------------------------
    # 3) POST /api/contact -> contact-service /contacts
    # -----------------------------
    if method == "POST" and path.endswith("/api/contact"):
        body_str = event.get("body") or ""
        try:
            payload = json.loads(body_str) if body_str else {}
        except json.JSONDecodeError:
            return _resp(400, {"error": "invalid_json"})

        status, data = _http_json("POST", f"{CONTACT_BASE}/contacts", payload)
        return _resp(status, data)

    return _resp(404, {"error": "route_not_found"})