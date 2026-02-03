import json
import re
import subprocess
import hashlib
from collections import deque
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
import html
import re

DOMAIN = "cidgroupe.com"
TABLE = "cid-ms-products"

START_URLS = [
    "https://cidgroupe.com/adblue",
    "https://cidgroupe.com/engrais",
    "https://cidgroupe.com/granules-de-bois",
    "https://cidgroupe.com/produits-chimiques",
]

SKIP_CONTAINS = [
    "/contact", "/nous-contacter", "/a-propos-de-nous", "/a-propos",
    "/mentions-legales", "/politique", "/conditions", "/cgv", "/cg",
    "/actualites",
]

ALLOWED_PREFIXES = (
    "/adblue", "/engrais", "/granules-de-bois", "/produits-chimiques"
)

def fetch(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return urlopen(req, timeout=20).read().decode("utf-8", errors="ignore")

def normalize(url: str) -> str:
    # decode &lt; &gt; etc.
    url = html.unescape(url)

    url = url.replace("https://www.cidgroupe.com", "https://cidgroupe.com")
    url = url.replace("http://cidgroupe.com", "https://cidgroupe.com")
    url = url.replace("http://www.cidgroupe.com", "https://cidgroupe.com")

    # remove fragments
    url = url.split("#")[0].strip()

    # remove trailing slash (sauf racine)
    if url.endswith("/") and len(url) > len("https://cidgroupe.com/"):
        url = url.rstrip("/")

    return url

def is_internal(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return host in ("cidgroupe.com", "www.cidgroupe.com")
    except Exception:
        return False

ASSET_EXT = re.compile(r"\.(css|js|png|jpg|jpeg|webp|svg|gif|ico|woff2?|ttf|eot|map)(\?.*)?$", re.IGNORECASE)

def should_skip(url: str) -> bool:
    u = url.lower()

    # mauvaises “URLs” HTML / placeholders
    if "<" in u or ">" in u or "nolink" in u:
        return True

    # on ne garde que le domaine exact
    if not is_internal(url):
        return True

    p = urlparse(url)

    # fichiers statiques
    if ASSET_EXT.search(p.path):
        return True

    # pages hors scope produits
    if any(x in u for x in SKIP_CONTAINS):
        return True
    
    if "fonts.gstatic.com" in u:
        return True
    
    p = urlparse(url)
    path_low = p.path.lower()

# évite les faux liens du type ".../fonts.gstatic.com"
    if any(x in path_low for x in ("gstatic.com", "fonts.gstatic.com")):
        return True

# si un "domaine" apparaît dans le path, c'est quasi toujours un asset/CDN
    if re.search(r"\b[a-z0-9\-]+\.(com|net|org|io|cdn)\b", path_low):
        return True

    return False

def extract_links(html_str: str, base_url: str):
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html_str, flags=re.IGNORECASE)
    out = []
    for h in hrefs:
        if not h:
            continue

        raw = h.strip()

        # ignore protocol-relative and external font/cdn links early
        low = raw.lower()
        if low.startswith(("mailto:", "tel:", "javascript:")):
            continue
        if low.startswith("//"):             # ex: //fonts.gstatic.com/...
            continue
        if "gstatic.com" in low or "fonts." in low:
            continue

        link = normalize(urljoin(base_url, raw))

        if should_skip(link):
            continue

        out.append(link)
    return out

def get_title(url: str) -> str:
    try:
        html = fetch(url)
    except Exception:
        return url
    m = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return url
    title = re.sub(r"\s+", " ", m.group(1)).strip()
    title = re.sub(r"\s*\|\s*CID Groupe.*$", "", title).strip()
    return title or url

def path_parts(url: str):
    p = urlparse(url).path.strip("/")
    return [x for x in p.split("/") if x]

def slug_from_url(url: str) -> str:
    parts = path_parts(url)
    if not parts:
        return "home"
    slug = parts[-1].lower()
    slug = re.sub(r"[^a-z0-9\-]+", "-", slug).strip("-")
    return (slug or "item")[:80]

def category_from_url(url: str) -> str:
    parts = path_parts(url)
    return parts[0] if parts else "other"

def classify(url: str) -> str:
    parts = path_parts(url)
    if not parts:
        return "other"
    if not urlparse(url).path.startswith(ALLOWED_PREFIXES):
        return "other"
    if len(parts) <= 2:
        return "category"
    return "product"

def parent_id_for(url: str) -> str:
    parts = path_parts(url)
    if len(parts) <= 1:
        return ""
    parent_slug = parts[-2].lower()
    parent_slug = re.sub(r"[^a-z0-9\-]+", "-", parent_slug).strip("-")
    top = parts[0].lower()
    return f"{top}__{parent_slug}"

def short_hash(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]

def make_id(url: str) -> str:
    """
    ID stable + unique :
    - base lisible : <top>__<leaf>
    - + suffixe hash pour éviter collisions
    """
    parts = path_parts(url)
    if not parts:
        return "home__" + short_hash(url)
    top = parts[0].lower()
    leaf = slug_from_url(url)
    base = f"{top}__{leaf}"

    # catégories top-level
    if classify(url) == "category" and len(parts) == 1:
        base = f"{top}"
    # catégories level 2
    if classify(url) == "category" and len(parts) == 2:
        mid = re.sub(r"[^a-z0-9\-]+", "-", parts[1].lower()).strip("-")
        base = f"{top}__{mid}"

    return f"{base}__{short_hash(url)}"

def batch_write(items):
    for i in range(0, len(items), 25):
        chunk = items[i:i+25]
        payload = {TABLE: [{"PutRequest": {"Item": it}} for it in chunk]}
        p = subprocess.run(
            ["aws", "dynamodb", "batch-write-item", "--request-items", json.dumps(payload)],
            capture_output=True, text=True
        )
        if p.returncode != 0:
            print("Batch error:", p.stderr.strip())
            raise SystemExit(1)

def main():
    max_pages = 600
    max_depth = 4

    seen = set()
    q = deque([(normalize(u), 0) for u in START_URLS])
    urls = set()

    while q and len(seen) < max_pages:
        url, depth = q.popleft()
        if url in seen:
            continue
        seen.add(url)

        if not is_internal(url) or should_skip(url):
            continue

        path = urlparse(url).path
        if not path.startswith(ALLOWED_PREFIXES):
            continue

        urls.add(url)

        if depth >= max_depth:
            continue

        try:
            html = fetch(url)
        except Exception:
            continue

        for link in extract_links(html, url):
            if link not in seen and is_internal(link) and not should_skip(link):
                pth = urlparse(link).path
                if pth.startswith(ALLOWED_PREFIXES):
                    q.append((link, depth + 1))

    # Build items + déduplication par product_id
    by_id = {}

    for u in sorted(urls):
        c = classify(u)
        if c == "other":
            continue

        parts = path_parts(u)
        level = len(parts)
        item_id = make_id(u)
        title = get_title(u)
        cat = category_from_url(u)

        parent = ""
        if level == 2:
            parent = parts[0].lower()
        elif level >= 3:
            parent = parent_id_for(u)

        item = {
            "product_id": {"S": item_id},
            "type": {"S": c},
            "level": {"N": str(level)},
            "name": {"S": title},
            "category": {"S": cat},
            "source_url": {"S": u},
            "active": {"BOOL": True},
        }
        if parent:
            item["parent_id"] = {"S": parent}

        # garde le premier si collision (normalement très rare avec hash)
        by_id.setdefault(item_id, item)

    items = list(by_id.values())

    print(f"Crawled {len(seen)} pages; inserting {len(items)} unique catalog items into {TABLE} ...")
    if not items:
        print("No items found.")
        return

    batch_write(items)
    print("Done.")

if __name__ == "__main__":
    main()
