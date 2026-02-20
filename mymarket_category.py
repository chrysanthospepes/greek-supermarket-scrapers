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

    final_price: Optional[float] = None
    currency: Optional[str] = None

    unit_price: Optional[float] = None
    unit_price_unit: Optional[str] = None

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

    # Find nodes that include "Κωδ:" (product code line on tiles)
    for node in t.css("*"):
        txt = node.text(strip=True)
        if not txt or "Κωδ:" not in txt:
            continue

        cur = node
        for _ in range(8):  # walk up ancestors
            if cur is None:
                break

            container_text = cur.text(strip=True)

            # Stop at a "small" container that likely represents ONE product tile:
            # it should contain exactly one Κωδ:
            if container_text.count("Κωδ:") == 1:
                anchors = cur.css("a[href]")
                best = None

                for a in anchors:
                    a_text = (a.text(strip=True) or "").strip()
                    href = a.attributes.get("href", "")

                    # skip banner / CTA links
                    if "αγόρασε" in a_text.lower():
                        continue

                    u = normalize(urljoin(BASE, href))
                    if not same_site(u) or not looks_like_product_url(u):
                        continue

                    p = urlparse(u).path
                    if p.count("/") != 1 or len(p) <= 2:
                        continue

                    # Prefer anchors that look like product title/image links:
                    # (usually have an <img> inside or non-trivial text)
                    has_img = a.css_first("img") is not None
                    if has_img or len(a_text) >= 3:
                        best = u
                        break

                if best:
                    out.add(best)
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

def extract_unit_text_dom(t: HTMLParser) -> Optional[str]:
    """
    Extract unit description like: "~1 τεμάχιο / 300g"
    from the small rounded info box on the product page.
    """
    # Look for a span that is bold and contains "~"
    for bold in t.css("span.font-bold"):
        left = (bold.text(strip=True) or "").strip()
        if not left.startswith("~"):
            continue

        parent = bold.parent
        if not parent:
            continue

        # Find the sibling/other span that contains the rest (unit + / grams)
        # In your HTML it's the second <span> in the same parent.
        spans = parent.css("span")
        if len(spans) >= 2:
            right = spans[1].text(separator=" ", strip=True)
            right = re.sub(r"\s+", " ", (right or "").strip())
            left = re.sub(r"\s+", " ", left)
            combo = f"{left} {right}".strip()
            return combo

    return None


# def extract_price_and_unit(full_text: str) -> tuple[Optional[float], Optional[str]]:
#     """
#     Find a line like: "2,19€ Τιμή κιλού"
#     """
#     for line in full_text.splitlines():
#         s = line.strip().replace("\xa0", " ")
#         if "€" in s and ("Τιμή" in s or "τιμή" in s):
#             m = _price_line_re.search(s)
#             if m:
#                 num = m.group(1).replace(".", "").replace(",", ".")
#                 try:
#                     price = float(num)
#                 except ValueError:
#                     price = None
#                 unit = (m.group(2) or "").strip() or None
#                 return price, unit
#     return None, None

def extract_prices(t: HTMLParser) -> tuple[Optional[float], Optional[str], Optional[float], Optional[str]]:
    """
    Returns:
      (final_price, final_price_currency, unit_price, unit_price_unit)
    """
    def to_float(s: str) -> Optional[float]:
        s = (s or "").strip().replace("\xa0", " ")
        s = s.replace("€", "").strip()
        # greek format: 12,38
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    # Final price (selling unit) like: <span class="product-full--final-price ...">0,99€</span>
    final_price = None
    final_node = t.css_first(".product-full--price-per-selling-unit .product-full--final-price")
    if final_node:
        final_price = to_float(final_node.text(strip=True))

    # Unit price block like:
    # <div class="... text-center ...">
    #   <span class="font-bold text-base">12,38€</span>
    #   <span class="text-[7px] ...">Τιμή κιλού</span>
    # </div>
    unit_price = None
    unit_label = None

    # Find a block that contains a "Τιμή ..." label and then read the number above it
    for label in t.css("span"):
        label_text = label.text(strip=True)
        if not label_text:
            continue
        if label_text.startswith("Τιμή "):  # "Τιμή κιλού", "Τιμή λίτρου", etc.
            unit_label = label_text
            parent = label.parent
            if parent:
                # price is typically the first bold/base span in the same parent
                price_span = parent.css_first("span.font-bold")
                if price_span:
                    unit_price = to_float(price_span.text(strip=True))
            break

    return final_price, "EUR", unit_price, unit_label

def extract_breadcrumbs_dom(t: HTMLParser):
    """
    Find the breadcrumb <ol> by locating an <a> that links to /frouta-lachanika,
    then walk up to its ancestor <ol>. Works even if classes change.
    """
    a = t.css_first('ol a[href*="/frouta-lachanika"]')
    if not a:
        # fallback: any ol with several breadcrumb-like links near top
        for ol in t.css("ol"):
            links = [x.text(strip=True) for x in ol.css("a[href]") if x.text(strip=True)]
            if 2 <= len(links) <= 10:
                return links[0], " > ".join(links)
        return None, None

    # walk up to the <ol>
    cur = a
    while cur and cur.tag != "ol":
        cur = cur.parent

    if not cur:
        return None, None

    links = []
    for link in cur.css("a[href]"):
        txt = (link.text(strip=True) or "").strip()
        if txt:
            links.append(txt)

    if not links:
        return None, None

    return links[0], " > ".join(links)

def parse_product_page(html: str, url: str) -> ProductRow:
    t = HTMLParser(html)
    full_text = t.text(separator="\n", strip=True)

    name = extract_name(t)
    code = extract_code(full_text)
    unit_text = extract_unit_text_dom(t)

    final_price, currency, unit_price, unit_label = extract_prices(t)

    root_cat, breadcrumbs = extract_breadcrumbs_dom(t)

    return ProductRow(
        url=url,
        name=name,
        code=code,
        final_price=final_price,
        currency=currency,
        unit_price=unit_price,
        unit_price_unit=unit_label,
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
    return product_urls


def scrape_and_filter(urls: List[str], expected_root: str = "Φρούτα & Λαχανικά") -> List[ProductRow]:
    rows: List[ProductRow] = []

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as c:
        for i, u in enumerate(urls, start=1):
            try:
                r = c.get(u)
                if r.status_code != 200:
                    continue
                row = parse_product_page(r.text, u)
                
                if not row.name or not row.code:
                    continue

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
