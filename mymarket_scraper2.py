import asyncio
import csv
import re
from dataclasses import dataclass, asdict
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm

BASE = "https://www.mymarket.gr"
CATEGORIES_TREE = f"{BASE}/sitemap/categories-tree"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

CONCURRENCY = 6
TIMEOUT = 30

# --- helpers ------------------------------------------------------------

def norm(url: str) -> str:
    p = urlparse(url)
    return p._replace(fragment="").geturl()

def same_site(url: str) -> bool:
    return urlparse(url).netloc == urlparse(BASE).netloc

def looks_like_product_url(url: str) -> bool:
    # On mymarket.gr products are often single-slug pages (not /product/),
    # so we use a heuristic: exclude known non-product paths and keep same-site.
    path = urlparse(url).path.strip("/").lower()
    if not path:
        return False
    # exclude obvious non-product sections
    banned_prefixes = (
        "sitemap", "company", "career", "contact", "login", "register",
        "terms", "privacy", "payment", "app", "search"
    )
    if any(path.startswith(x) for x in banned_prefixes):
        return False
    # categories are also slugs — so we’ll only accept URLs discovered *from category listings*
    return True

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5))
async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    r = await client.get(url, follow_redirects=True)
    r.raise_for_status()
    return r.text

# --- data model ---------------------------------------------------------

@dataclass
class Product:
    url: str
    name: Optional[str] = None
    code: Optional[str] = None
    unit_text: Optional[str] = None   # e.g. "~1 τεμάχιο / 100g"
    price: Optional[float] = None     # e.g. 2.19
    price_unit: Optional[str] = None  # e.g. "Τιμή κιλού"
    breadcrumbs: Optional[str] = None # "Φρούτα & Λαχανικά > Φρούτα > ..."

# --- parsing ------------------------------------------------------------

def parse_category_urls(html: str) -> List[str]:
    tree = HTMLParser(html)
    urls: List[str] = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        u = norm(urljoin(BASE, href))
        if same_site(u) and "/sitemap/" not in u:
            # category tree page contains many category links; keep those
            urls.append(u)
    # de-dupe while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def parse_product_links_from_category(html: str, category_url: str) -> Set[str]:
    tree = HTMLParser(html)
    product_urls: Set[str] = set()

    # The category page in text form shows product title links as anchors.
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        u = norm(urljoin(BASE, href))
        if same_site(u) and looks_like_product_url(u):
            # Filter out the category page itself and obvious nav links by requiring the anchor to have some text
            txt = (a.text() or "").strip()
            if txt and u != category_url:
                product_urls.add(u)

    # Conservative: keep only URLs that are not the category itself and not other sitemap links.
    product_urls = {u for u in product_urls if "/sitemap/" not in u}

    return product_urls

_price_re = re.compile(r"(\d+[.,]\d+)\s*€\s*(.*)$")
_code_re = re.compile(r"(Κωδ(?:ικός)?\s*[:：]\s*)(\d+)")

def parse_product_page(html: str, url: str) -> Product:
    tree = HTMLParser(html)
    p = Product(url=url)

    h1 = tree.css_first("h1")
    if h1:
        p.name = h1.text(strip=True)

    text = tree.text(separator="\n", strip=True)

    # Code: "Κωδικός: 198120"
    m = _code_re.search(text)
    if m:
        p.code = m.group(2)

    # Unit line often like "~1 τεμάχιο / 100g"
    # Grab first line starting with "~"
    for line in text.splitlines():
        if line.strip().startswith("~"):
            p.unit_text = line.strip()
            break

    # Price line: "2,19€ Τιμή κιλού"
    for line in text.splitlines():
        line = line.strip()
        if "€" in line and ("Τιμή" in line or "τιμή" in line):
            m2 = _price_re.search(line.replace("\xa0", " "))
            if m2:
                p.price = float(m2.group(1).replace(".", "").replace(",", "."))
                p.price_unit = m2.group(2).strip() or None
                break

    # Breadcrumbs appear as a numbered list in the page text; easiest is:
    # collect anchors that look like breadcrumb categories near the top.
    # (Fallback: use first few category-like links)
    crumbs = []
    for a in tree.css("a[href]"):
        t = (a.text() or "").strip()
        if t and len(t) <= 60 and t not in ("mymarket.gr", "Προϊόντα"):
            # heuristic: these include "Φρούτα & Λαχανικά", "Φρούτα", etc.
            if " " in t or "&" in t:
                crumbs.append(t)
        if len(crumbs) >= 6:
            break
    if crumbs:
        p.breadcrumbs = " > ".join(dict.fromkeys(crumbs))

    return p

# --- async workflow -----------------------------------------------------

async def main():
    async with httpx.AsyncClient(headers=HEADERS, timeout=TIMEOUT) as client:
        # 1) categories
        cat_html = await fetch_text(client, CATEGORIES_TREE)
        categories = parse_category_urls(cat_html)
        print(f"Found {len(categories)} category URLs from {CATEGORIES_TREE}")

        # 2) collect product URLs from categories
        sem = asyncio.Semaphore(CONCURRENCY)
        product_urls: Set[str] = set()

        async def crawl_category(cu: str):
            async with sem:
                try:
                    html = await fetch_text(client, cu)
                    urls = parse_product_links_from_category(html, cu)
                    product_urls.update(urls)
                except Exception:
                    pass

        await tqdm.gather(*(crawl_category(c) for c in categories))

        # Heuristic clean-up: category crawling will pick up some non-product links
        # Remove the category set itself
        product_urls.difference_update(set(categories))
        print(f"Collected {len(product_urls)} candidate product URLs")

        # 3) scrape products
        products: List[Product] = []

        async def scrape_one(pu: str):
            async with sem:
                try:
                    html = await fetch_text(client, pu)
                    prod = parse_product_page(html, pu)
                    if prod.name and prod.code:
                        products.append(prod)
                except Exception:
                    pass

        await tqdm.gather(*(scrape_one(u) for u in sorted(product_urls)))

        print(f"Parsed {len(products)} products with name+code")

        # 4) save
        with open("mymarket_products.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(asdict(Product(url="")).keys()))
            w.writeheader()
            for pr in products:
                w.writerow(asdict(pr))

        print("Saved: mymarket_products.csv")

if __name__ == "__main__":
    asyncio.run(main())
