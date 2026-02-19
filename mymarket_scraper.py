import asyncio
import json
import random
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm.asyncio import tqdm

BASE = "https://www.mymarket.gr/"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Be polite: keep concurrency low unless the site explicitly permits more.
CONCURRENCY = 5
REQUEST_DELAY_RANGE = (0.2, 0.8)  # jitter between requests
TIMEOUT = 30.0

# Common sitemap locations to try
SITEMAP_CANDIDATES = [
    urljoin(BASE, "sitemap.xml"),
    urljoin(BASE, "sitemap_index.xml"),
    urljoin(BASE, "sitemap/sitemap.xml"),
]

# ---------------------------------------------------------------------

@dataclass
class Product:
    url: str
    name: Optional[str] = None
    sku: Optional[str] = None
    brand: Optional[str] = None
    image: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    availability: Optional[str] = None
    category: Optional[str] = None

# ---------------------------------------------------------------------

class FetchError(Exception):
    pass

def jitter_delay():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def is_same_site(url: str) -> bool:
    return urlparse(url).netloc == urlparse(BASE).netloc

def normalize_url(u: str) -> str:
    # strip fragments, keep query (sometimes important)
    p = urlparse(u)
    return p._replace(fragment="").geturl()

def extract_json_ld(tree: HTMLParser) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for node in tree.css("script[type='application/ld+json']"):
        txt = node.text(strip=True)
        if not txt:
            continue
        try:
            data = json.loads(txt)
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict):
                        items.append(d)
            elif isinstance(data, dict):
                items.append(data)
        except json.JSONDecodeError:
            # sometimes multiple JSON objects are concatenated; try to salvage
            pass
    return items

def parse_product_from_ldjson(url: str, ld: Dict[str, Any]) -> Optional[Product]:
    # JSON-LD commonly uses @type: "Product" with offers
    t = ld.get("@type")
    if isinstance(t, list):
        t = next((x for x in t if isinstance(x, str)), None)

    if t != "Product":
        return None

    p = Product(url=url)
    p.name = ld.get("name")
    p.sku = ld.get("sku") or ld.get("mpn")

    brand = ld.get("brand")
    if isinstance(brand, dict):
        p.brand = brand.get("name")
    elif isinstance(brand, str):
        p.brand = brand

    image = ld.get("image")
    if isinstance(image, list) and image:
        p.image = image[0]
    elif isinstance(image, str):
        p.image = image

    offers = ld.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    if isinstance(offers, dict):
        price = offers.get("price")
        try:
            p.price = float(price) if price is not None else None
        except (ValueError, TypeError):
            p.price = None
        p.currency = offers.get("priceCurrency")
        p.availability = offers.get("availability")
        p.sku = p.sku or offers.get("sku")

    # category sometimes present as "category"
    p.category = ld.get("category")
    return p

def looks_like_product_url(url: str) -> bool:
    # Heuristic: you should adjust after observing site structure.
    # Often product pages include /product/ or /p/ or a SKU-like slug.
    path = urlparse(url).path.lower()
    return any(seg in path for seg in ("/product", "/products", "/p/"))

# ---------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type((httpx.TransportError, httpx.ReadTimeout, FetchError)),
    wait=wait_exponential(min=1, max=20),
    stop=stop_after_attempt(5),
)
async def fetch(client: httpx.AsyncClient, url: str) -> str:
    r = await client.get(url, follow_redirects=True)
    if r.status_code >= 400:
        raise FetchError(f"HTTP {r.status_code} for {url}")
    return r.text

async def fetch_binary(client: httpx.AsyncClient, url: str) -> bytes:
    r = await client.get(url, follow_redirects=True)
    r.raise_for_status()
    return r.content

# ---------------------------------------------------------------------

async def try_get_sitemaps(client: httpx.AsyncClient) -> List[str]:
    found: List[str] = []
    for s in SITEMAP_CANDIDATES:
        try:
            xml = await fetch(client, s)
        except Exception:
            continue
        if "<sitemapindex" in xml or "<urlset" in xml:
            found.append(s)
    return found

def parse_sitemap_xml(xml: str) -> Tuple[List[str], List[str]]:
    """
    Returns (urls, nested_sitemaps)
    """
    urls: List[str] = []
    nested: List[str] = []

    # very small/simple XML parsing via regex (good enough for sitemaps)
    if "<sitemapindex" in xml:
        nested = re.findall(r"<loc>\s*([^<]+)\s*</loc>", xml, flags=re.IGNORECASE)
    else:
        urls = re.findall(r"<loc>\s*([^<]+)\s*</loc>", xml, flags=re.IGNORECASE)

    urls = [normalize_url(u.strip()) for u in urls]
    nested = [normalize_url(u.strip()) for u in nested]
    return urls, nested

async def collect_urls_from_sitemaps(client: httpx.AsyncClient) -> Set[str]:
    seeds = await try_get_sitemaps(client)
    all_urls: Set[str] = set()
    queue: List[str] = list(seeds)
    seen_maps: Set[str] = set()

    while queue:
        sm = queue.pop()
        if sm in seen_maps:
            continue
        seen_maps.add(sm)
        try:
            xml = await fetch(client, sm)
        except Exception:
            continue
        urls, nested = parse_sitemap_xml(xml)
        all_urls.update(u for u in urls if is_same_site(u))
        for n in nested:
            if is_same_site(n) and n not in seen_maps:
                queue.append(n)

    return all_urls

# ---------------------------------------------------------------------

def extract_links(html: str, base_url: str) -> List[str]:
    tree = HTMLParser(html)
    out: List[str] = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href")
        if not href:
            continue
        u = urljoin(base_url, href)
        u = normalize_url(u)
        if is_same_site(u):
            out.append(u)
    return out

async def crawl_categories_for_products(
    client: httpx.AsyncClient, start_urls: List[str], max_pages: int = 2000
) -> Set[str]:
    """
    Fallback discovery if no sitemap: BFS crawl within same site, collecting product-like URLs.
    Keep max_pages to avoid crawling the whole web presence.
    """
    q: List[str] = list(dict.fromkeys(start_urls))
    seen: Set[str] = set()
    product_urls: Set[str] = set()

    while q and len(seen) < max_pages:
        url = q.pop(0)
        if url in seen:
            continue
        seen.add(url)
        try:
            jitter_delay()
            html = await fetch(client, url)
        except Exception:
            continue

        if looks_like_product_url(url):
            product_urls.add(url)

        for link in extract_links(html, url):
            if link not in seen and link not in q:
                # avoid obvious non-content routes
                path = urlparse(link).path.lower()
                if any(x in path for x in ("/account", "/login", "/cart", "/checkout")):
                    continue
                q.append(link)

    return product_urls

# ---------------------------------------------------------------------

def parse_product_page(url: str, html: str) -> Optional[Product]:
    tree = HTMLParser(html)

    # 1) Try JSON-LD
    for ld in extract_json_ld(tree):
        p = parse_product_from_ldjson(url, ld)
        if p and p.name:
            return p

    # 2) Fallback: simple HTML heuristics (you will likely tweak selectors)
    p = Product(url=url)

    title = tree.css_first("h1")
    if title:
        p.name = title.text(strip=True)

    # Try to find price with common patterns
    price_node = tree.css_first("[class*='price'], [data-testid*='price']")
    if price_node:
        txt = price_node.text(strip=True)
        # extract number like 1,23 or 1.23
        m = re.search(r"(\d+[.,]\d+)", txt)
        if m:
            p.price = float(m.group(1).replace(",", "."))
    if p.name:
        return p

    return None

async def scrape_products(client: httpx.AsyncClient, product_urls: List[str]) -> List[Product]:
    sem = asyncio.Semaphore(CONCURRENCY)
    results: List[Product] = []

    async def worker(u: str):
        async with sem:
            try:
                jitter_delay()
                html = await fetch(client, u)
                p = parse_product_page(u, html)
                if p:
                    results.append(p)
            except Exception:
                return

    for f in tqdm(asyncio.as_completed([worker(u) for u in product_urls]), total=len(product_urls)):
        await f

    return results

# ---------------------------------------------------------------------

async def main():
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "el-GR,el;q=0.9,en;q=0.8"}
    async with httpx.AsyncClient(headers=headers, timeout=TIMEOUT) as client:
        # 1) Prefer sitemap discovery
        urls = await collect_urls_from_sitemaps(client)

        product_urls = {u for u in urls if looks_like_product_url(u)}
        if not product_urls:
            # 2) Fallback crawl from homepage (limited)
            product_urls = await crawl_categories_for_products(client, [BASE], max_pages=1500)

        product_list = sorted(product_urls)
        print(f"Discovered {len(product_list)} product-like URLs")

        products = await scrape_products(client, product_list)
        print(f"Parsed {len(products)} products")

        # Save
        with open("products.json", "w", encoding="utf-8") as f:
            json.dump([asdict(p) for p in products], f, ensure_ascii=False, indent=2)

        # Also save CSV
        import csv
        with open("products.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(asdict(Product(url="")).keys()))
            w.writeheader()
            for p in products:
                w.writerow(asdict(p))

if __name__ == "__main__":
    asyncio.run(main())
