import csv
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional, List, Set
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.mymarket.gr"
ROOT_LISTING = f"{BASE}/frouta-lachanika"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

# -----------------------------
# Data model
# -----------------------------

@dataclass
class ProductRow:
    url: str
    name: Optional[str] = None
    code: Optional[str] = None
    price: Optional[float] = None
    price_unit: Optional[str] = None
    unit_text: Optional[str] = None
    root_category: Optional[str] = None
    breadcrumbs: Optional[str] = None


# -----------------------------
# Helpers
# -----------------------------

def same_site(u: str) -> bool:
    return urlparse(u).netloc == urlparse(BASE).netloc

def normalize(u: str) -> str:
    p = urlparse(u)
    return p._replace(fragment="").geturl()

def looks_like_product_url(u: str) -> bool:
    """
    Products and categories are both slugs on this site.
    We ONLY consider URLs as products if they are discovered from the root listing pages.
    Here we just exclude obvious non-product sections.
    """
    path = urlparse(u).path.lower()
    if not path or path == "/":
        return False

    banned_prefixes = (
        "/sitemap", "/company", "/career", "/contact", "/login", "/register",
        "/terms", "/privacy", "/payment", "/search", "/cart", "/checkout"
    )
    return not any(path.startswith(b) for b in banned_prefixes)

def listing_has_products(html: str) -> bool:
    # Listing pages typically contain many "Κωδ:" occurrences.
    return "Κωδ:" in html


# -----------------------------
# Parsing: category listing pages
# -----------------------------

def extract_product_links_from_listing(html: str) -> Set[str]:
    t = HTMLParser(html)
    out: Set[str] = set()

    # Find elements containing "Κωδ:" and then grab the nearest link in that product tile.
    # We do a tolerant approach:
    # - locate nodes with text that includes "Κωδ:"
    # - walk up a few levels and look for the first <a href> that points to a single-slug page
    code_nodes = t.css("*")

    for node in code_nodes:
        txt = node.text(strip=True)
        if not txt or "Κωδ:" not in txt:
            continue

        # climb up to find an anchor that looks like a product link
        cur = node
        for _ in range(6):  # up to 6 parents
            # find any <a href> inside this container
            a = cur.css_first("a[href]") if cur else None
            if a:
                href = a.attributes.get("href", "")
                if href:
                    u = normalize(urljoin(BASE, href))
                    if same_site(u) and looks_like_product_url(u):
                        p = urlparse(u).path
                        if p.count("/") == 1 and len(p) > 2:
                            out.add(u)
                            break
            cur = cur.parent

    out.discard(ROOT_LISTING)
    return out


# -----------------------------
# Parsing: product pages
# -----------------------------

_code_re = re.compile(r"(Κωδ(?:ικός)?\s*[:：]\s*)(\d+)")
_price_line_re = re.compile(r"(\d+[.,]\d+)\s*€\s*(.+)?$")

def extract_name(t: HTMLParser) -> Optional[str]:
    h1 = t.css_first("h1")
    return h1.text(strip=True) if h1 else None

def extract_code(full_text: str) -> Optional[str]:
    m = _code_re.search(full_text)
    return m.group(2) if m else None

def extract_unit_text(full_text: str) -> Optional[str]:
    # Often appears like "~1 τεμάχιο / 100g"
    for line in full_text.splitlines():
        s = line.strip()
        if s.startswith("~"):
            return s
    return None

def extract_price_and_unit(full_text: str) -> tuple[Optional[float], Optional[str]]:
    """
    Find a line like: "2,19€ Τιμή κιλού"
    """
    for line in full_text.splitlines():
        s = line.strip().replace("\xa0", " ")
        if "€" in s and ("Τιμή" in s or "τιμή" in s):
            m = _price_line_re.search(s)
            if m:
                num = m.group(1).replace(".", "").replace(",", ".")
                try:
                    price = float(num)
                except ValueError:
                    price = None
                unit = (m.group(2) or "").strip() or None
                return price, unit
    return None, None

def extract_breadcrumbs(t: HTMLParser) -> tuple[Optional[str], Optional[str]]:
    """
    Robust-ish heuristic:
    - Collect anchor texts early in the DOM that look like breadcrumb category labels.
    - Deduplicate while preserving order.
    Returns (root_category, breadcrumb_string)
    """
    # Grab anchor texts; on product pages breadcrumb links appear early.
    texts: List[str] = []
    for a in t.css("a[href]"):
        txt = (a.text(strip=True) or "").strip()
        if not txt:
            continue
        # avoid very long stuff and obvious navigation noise
        if len(txt) > 60:
            continue
        if txt in ("", "mymarket.gr", "MY market"):
            continue
        texts.append(txt)
        if len(texts) >= 40:  # we only need early part
            break

    # De-dupe preserving order
    seen = set()
    dedup = []
    for x in texts:
        if x not in seen:
            seen.add(x)
            dedup.append(x)

    # Heuristic: root category is usually a known top-level label present in crumbs.
    # If you want to be strict, set ROOT_LABEL = "Φρούτα & Λαχανικά" and require it.
    root = None
    for x in dedup:
        if x in ("Φρούτα & Λαχανικά", "Φρέσκο Κρέας & Ψάρι", "Παντοπωλείο", "Κατεψυγμένα", "Ποτά"):
            root = x
            break

    breadcrumb_str = " > ".join(dedup[:10]) if dedup else None
    return root, breadcrumb_str

def parse_product_page(html: str, url: str) -> ProductRow:
    t = HTMLParser(html)
    full_text = t.text(separator="\n", strip=True)

    name = extract_name(t)
    code = extract_code(full_text)
    unit_text = extract_unit_text(full_text)
    price, price_unit = extract_price_and_unit(full_text)
    root_cat, breadcrumbs = extract_breadcrumbs(t)

    return ProductRow(
        url=url,
        name=name,
        code=code,
        price=price,
        price_unit=price_unit,
        unit_text=unit_text,
        root_category=root_cat,
        breadcrumbs=breadcrumbs,
    )


# -----------------------------
# Crawl + verify + save
# -----------------------------

def crawl_frouta_lachanika(max_pages: int = 500) -> List[str]:
    product_urls: Set[str] = set()

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as c:
        for page in range(1, max_pages + 1):
            url = ROOT_LISTING if page == 1 else f"{ROOT_LISTING}?page={page}"
            r = c.get(url)

            if r.status_code == 404:
                print(f"page={page} -> 404, stopping pagination.")
                break

            r.raise_for_status()
            html = r.text

            new_links = extract_product_links_from_listing(html)
            if not new_links:
                print(f"page={page} -> 0 products, stopping.")
                break

            before = len(product_urls)
            product_urls |= new_links
            print(f"page={page} +{len(product_urls)-before} total={len(product_urls)}")

            time.sleep(0.5)

    return sorted(product_urls)


def scrape_and_filter(urls: List[str], expected_root: str = "Φρούτα & Λαχανικά") -> List[ProductRow]:
    rows: List[ProductRow] = []

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as c:
        for i, u in enumerate(urls, start=1):
            try:
                r = c.get(u)
                if r.status_code != 200:
                    continue
                row = parse_product_page(r.text, u)

                # Keep only those really under the expected root category
                if row.root_category == expected_root:
                    rows.append(row)

                if i % 50 == 0:
                    print(f"scraped {i}/{len(urls)} -> kept {len(rows)}")
                time.sleep(0.3)  # polite
            except Exception:
                continue

    return rows

def save_to_csv(rows: List[ProductRow], filename: str = "frouta_lachanika_products.csv") -> None:
    if not rows:
        print("No rows to save.")
        return

    fieldnames = list(asdict(rows[0]).keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(asdict(row))

    print(f"Saved {len(rows)} rows to {filename}")


if __name__ == "__main__":
    urls = crawl_frouta_lachanika()
    print("candidates:", len(urls))

    rows = scrape_and_filter(urls, expected_root="Φρούτα & Λαχανικά")
    print("verified under Φρούτα & Λαχανικά:", len(rows))

    save_to_csv(rows, "frouta_lachanika_products.csv")
