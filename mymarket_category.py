import asyncio
import csv
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional, List, Set
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.mymarket.gr"
ROOT_CATEGORIES = [
    # "frouta-lachanika",
    # "fresko-kreas-psari",
    # "galaktokomika-eidi-psygeiou",
    # "tyria-allantika-deli",
    "katepsygmena-trofima",
    # "mpyres-anapsyktika-krasia-pota",
    # "proino-rofimata-kafes",
    # "artozacharoplasteio-snacks",
    # "trofima",
    # "frontida-gia-to-moro-sas",
    # "prosopiki-frontida",
    # "oikiaki-frontida-chartika",
    # "kouzina-mikrosyskeves-spiti",
    # "frontida-gia-to-katoikidio-sas",
    # "epochiaka",
]
MAX_PAGES_PER_CATEGORY = 500
PAGE_SLEEP_SECONDS = 0.3
PRODUCT_SLEEP_SECONDS = 0
PRODUCT_FETCH_CONCURRENCY = 20
# True: deterministic sort before CSV write. False: keep parser discovery order.
SORT_PRODUCTS_FOR_CSV = True

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

    # current selling-unit final price (e.g. 0.99€ for 1 τεμ)
    final_price: Optional[float] = None
    currency: Optional[str] = None

    # final unit price (e.g. final €/kg or €/lt)
    unit_price: Optional[float] = None
    unit_price_unit: Optional[str] = None

    # NEW: original prices when discounted
    original_price: Optional[float] = None                # selling-unit original (if available)
    original_unit_price: Optional[float] = None           # original €/kg
    original_unit_price_unit: Optional[str] = None
    
    # NEW: set/bundle prices for 1+1 / 2+1 etc (σετ)
    final_set_price: Optional[float] = None
    original_set_price: Optional[float] = None

    discount_percent: Optional[int] = None
    promo_text: Optional[str] = None
    gift_buy_qty: Optional[int] = None
    gift_free_qty: Optional[int] = None

    unit_text: Optional[str] = None
    root_category_slug: Optional[str] = None
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

def to_category_slug(category: str) -> str:
    parsed = urlparse(category)
    if parsed.scheme and parsed.netloc:
        slug = parsed.path.strip("/")
    else:
        slug = category.strip("/")

    if not slug:
        raise ValueError(f"Invalid category '{category}'")

    return slug

def to_category_url(category: str) -> str:
    slug = to_category_slug(category)
    return f"{BASE}/{slug}"

def csv_filename_for_category(category: str) -> str:
    slug = to_category_slug(category).replace("/", "_")
    safe_slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug).strip("_")
    if not safe_slug:
        safe_slug = "category"
    return f"{safe_slug}-products.csv"

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

def extract_pagination_state(html: str):
    """
    Return (current_page, max_page, has_next) from listing HTML.
    """
    t = HTMLParser(html)

    page_numbers: Set[int] = set()
    current_page: Optional[int] = None

    has_next = (
        t.css_first("link[rel='next']") is not None
        or t.css_first("a[rel='next']") is not None
    )

    current_node = t.css_first("[aria-current='page']")
    if current_node:
        m = re.search(r"\d+", current_node.text(strip=True) or "")
        if m:
            try:
                current_page = int(m.group(0))
                page_numbers.add(current_page)
            except ValueError:
                pass

    for a in t.css("a[href]"):
        href = (a.attributes.get("href") or "").strip()
        if href:
            m = _page_param_re.search(href)
            if m:
                try:
                    page_numbers.add(int(m.group(1)))
                except ValueError:
                    pass

        if (a.attributes.get("data-mkey") or "").strip().lower() == "next":
            has_next = True

    max_page = max(page_numbers) if page_numbers else None
    return current_page, max_page, has_next


# -----------------------------
# Parsing: category listing pages
# -----------------------------

def extract_product_links_from_listing(html: str, root_listing: str) -> List[str]:
    t = HTMLParser(html)
    out: List[str] = []
    seen: Set[str] = set()
    root_listing = normalize(root_listing)

    # Product cards are consistently rendered as article.product--teaser.
    # Pick one best product URL per card to avoid accidental misses from deep DOM walks.
    for article in t.css("article.product--teaser"):
        best_url = None
        best_score = -1

        for a in article.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href:
                continue

            u = normalize(urljoin(BASE, href))
            if not same_site(u) or not looks_like_product_url(u):
                continue

            p = urlparse(u).path
            if p.count("/") != 1 or len(p) <= 2:
                continue

            a_text = (a.text(strip=True) or "").strip()
            rel_attr = (a.attributes.get("rel") or "").lower()
            has_img = a.css_first("img") is not None

            # Prefer canonical product anchors (bookmark + title/image signals).
            score = 0
            if "bookmark" in rel_attr:
                score += 2
            if has_img:
                score += 1
            if len(a_text) >= 3:
                score += 1

            if score > best_score:
                best_score = score
                best_url = u

        if best_url and best_url != root_listing and best_url not in seen:
            seen.add(best_url)
            out.append(best_url)

    return out


# -----------------------------
# Parsing: product pages
# -----------------------------

_code_re = re.compile(r"(Κωδ(?:ικός)?\s*[:：]\s*)(\d+)")
_price_line_re = re.compile(r"(\d+[.,]\d+)\s*€\s*(.+)?$")
_gift_offer_re = re.compile(r"(\d+)\s*\+\s*(\d+)")
_page_param_re = re.compile(r"[?&]page=(\d+)", re.IGNORECASE)

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

def extract_prices(t: HTMLParser):
    def to_float(s: str) -> Optional[float]:
        s = (s or "").strip().replace("\xa0", " ")
        s = s.replace("€", "").strip()
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    # selling-unit prices (your existing logic)
    final_price = None
    original_price = None

    final_node = t.css_first(".product-full--price-per-selling-unit .product-full--final-price")
    if final_node:
        final_price = to_float(final_node.text(strip=True))

    old_node = (
        t.css_first(".product-full--price-per-selling-unit .product-full--old-price")
        or t.css_first(".product-full--price-per-selling-unit .line-through")
        or t.css_first(".product-full--price-per-selling-unit .diagonal-line")
    )
    if old_node:
        original_price = to_float(old_node.text(strip=True))

    # -------------------------
    # NEW: parse the "tag boxes"
    # -------------------------
    unit_price = None
    unit_price_unit = None
    original_unit_price = None
    original_unit_price_unit = None

    final_set_price = None
    original_set_price = None

    for box in t.css(".product-full--product-tags .rounded"):
        price_span = box.css_first("span.font-bold")
        if not price_span:
            continue

        price_val = to_float(price_span.text(strip=True))
        if price_val is None:
            continue

        # Find a label that contains "τιμή" (avoid the "(5,80€ X 2)" line)
        label = None
        for s in box.css("span"):
            txt = (s.text(strip=True) or "").strip()
            if not txt:
                continue
            low = txt.lower()
            if "τιμή" in low and "x" not in low:
                label = txt
                break
        if not label:
            continue

        is_old = "diagonal-line" in (price_span.attributes.get("class") or "")

        low_label = label.lower()
        is_set = "σετ" in low_label
        is_unit = any(
            k in low_label
            for k in ["κιλ", "λίτρ", "lt", "kg", "ml", "gr", "γρ", "τεμαχ", "τεμ", "τμχ"]
        )

        if is_set:
            if is_old:
                original_set_price = price_val
            else:
                final_set_price = price_val
        elif is_unit:
            if is_old:
                original_unit_price = price_val
                original_unit_price_unit = label
            else:
                # prefer "Τελική τιμή ..." if multiple appear
                if unit_price is None or low_label.startswith("τελική"):
                    unit_price = price_val
                    unit_price_unit = label
        else:
            # Fallback for selling-unit prices rendered as plain
            # "Αρχική τιμή" / "Τελική τιμή" tag boxes.
            is_initial_label = low_label.startswith("αρχική")
            is_final_label = low_label.startswith("τελική")
            if is_old or is_initial_label:
                if original_price is None or is_initial_label:
                    original_price = price_val
            else:
                if final_price is None or is_final_label:
                    final_price = price_val

    # discount percent (your existing logic)
    discount_percent = None
    disc = t.css_first(".product-discount-tag")
    if disc:
        m = re.search(r"(-?\s*\d+)\s*%", disc.text(strip=True) or "")
        if m:
            try:
                discount_percent = int(m.group(1).replace(" ", ""))
            except ValueError:
                pass

    return (
        final_price, "EUR",
        unit_price, unit_price_unit,
        original_price,
        original_unit_price, original_unit_price_unit,
        discount_percent,
        final_set_price, original_set_price,
    )

def _normalized_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def extract_gift_offer(t: HTMLParser, full_text: str):
    """
    Detect offers like '1+1 Δώρο' or '2+1 Δώρο'.
    Returns (promo_text, buy_qty, free_qty).
    """
    gift_keyword_re = re.compile(r"\bδ(?:ώ|ω)ρο\b", re.IGNORECASE)
    candidate_texts: List[str] = []

    promo_selectors = [
        ".product-discount-tag",
        ".product-label",
        ".product-tag",
        ".product-badge",
        "[class*='discount']",
        "[class*='offer']",
        "[class*='promo']",
        "[class*='badge']",
        "[class*='tag']",
    ]

    seen: Set[str] = set()
    for selector in promo_selectors:
        for node in t.css(selector):
            txt = _normalized_space(node.text(separator=" ", strip=True))
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            candidate_texts.append(txt)

    # Fallback for cases where promo text is not under known promo classes.
    for line in full_text.splitlines():
        txt = _normalized_space(line)
        if not txt:
            continue
        if not gift_keyword_re.search(txt):
            continue
        key = txt.lower()
        if key in seen:
            continue
        seen.add(key)
        candidate_texts.append(txt)

    for txt in candidate_texts:
        if not gift_keyword_re.search(txt):
            continue
        match = _gift_offer_re.search(txt)
        if match:
            try:
                buy_qty = int(match.group(1))
                free_qty = int(match.group(2))
                return txt, buy_qty, free_qty
            except ValueError:
                continue

    return None, None, None

def extract_breadcrumbs_dom(t: HTMLParser):
    """
    Return (root_category_slug, root_category_name, breadcrumbs_text).
    Works across all category roots by reading breadcrumb href paths.
    """
    candidates: List[List[tuple[str, str]]] = []

    for ol in t.css("ol"):
        links: List[tuple[str, str]] = []
        for link in ol.css("a[href]"):
            txt = (link.text(strip=True) or "").strip()
            href = (link.attributes.get("href") or "").strip()
            if not txt or not href:
                continue

            u = normalize(urljoin(BASE, href))
            if not same_site(u):
                continue

            path = urlparse(u).path.strip("/")
            if not path:
                continue

            links.append((path, txt))

        if 1 <= len(links) <= 12:
            candidates.append(links)

    if not candidates:
        return None, None, None

    breadcrumb_links = max(candidates, key=len)

    root_slug = None
    root_name = None
    for path, txt in breadcrumb_links:
        if path.count("/") == 0:
            root_slug = path
            root_name = txt
            break

    if root_slug is None:
        root_slug, root_name = breadcrumb_links[0]

    breadcrumb_text = " > ".join(txt for _, txt in breadcrumb_links)
    return root_slug, root_name, breadcrumb_text

def parse_product_page(html: str, url: str) -> ProductRow:
    t = HTMLParser(html)
    full_text = t.text(separator="\n", strip=True)

    name = extract_name(t)
    code = extract_code(full_text)
    unit_text = extract_unit_text_dom(t)

    (
        final_price, currency,
        unit_price, unit_label,
        original_price,
        original_unit_price, original_unit_label,
        discount_percent,
        final_set_price, original_set_price,
    ) = extract_prices(t)
    promo_text, gift_buy_qty, gift_free_qty = extract_gift_offer(t, full_text)

    root_slug, root_cat, breadcrumbs = extract_breadcrumbs_dom(t)

    return ProductRow(
        url=url,
        name=name,
        code=code,
        final_price=final_price,
        currency=currency,
        unit_price=unit_price,
        unit_price_unit=unit_label,
        original_price=original_price,
        original_unit_price=original_unit_price,
        original_unit_price_unit=original_unit_label,
        discount_percent=discount_percent,
        promo_text=promo_text,
        gift_buy_qty=gift_buy_qty,
        gift_free_qty=gift_free_qty,
        final_set_price=final_set_price,
        original_set_price=original_set_price,
        unit_text=unit_text,
        root_category_slug=root_slug,
        root_category=root_cat,
        breadcrumbs=breadcrumbs,
    )

# -----------------------------
# Crawl + verify + save
# -----------------------------

def crawl_category(root_listing: str, max_pages: int = 500) -> List[str]:
    root_listing = normalize(root_listing.rstrip("/"))
    product_urls: List[str] = []
    seen_urls: Set[str] = set()

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as c:
        for page in range(1, max_pages + 1):
            url = root_listing if page == 1 else f"{root_listing}?page={page}"
            r = c.get(url)

            if r.status_code == 404:
                print(f"page={page} -> 404, stopping pagination.")
                break

            r.raise_for_status()
            html = r.text
            current_page, max_page, has_next = extract_pagination_state(html)

            # Out-of-range requests can return a remapped page while still containing products.
            if current_page is not None and current_page != page:
                print(
                    f"page={page} -> server current_page={current_page}, "
                    "stopping pagination."
                )
                break

            new_links = extract_product_links_from_listing(html, root_listing=root_listing)
            if not new_links:
                print(f"page={page} -> 0 products, stopping.")
                break

            added = 0
            for u in new_links:
                if u in seen_urls:
                    continue
                seen_urls.add(u)
                product_urls.append(u)
                added += 1
            print(f"page={page} +{added} total={len(product_urls)}")

            # Some category endpoints may keep returning non-empty duplicate pages
            # past the real last page; stop when no new unique products are discovered.
            if added == 0:
                print(f"page={page} -> 0 NEW unique products, stopping.")
                break

            if max_page is not None and page >= max_page:
                print(f"page={page} -> reached max_page={max_page}, stopping.")
                break

            if not has_next:
                print(f"page={page} -> no next page in pagination, stopping.")
                break

            time.sleep(PAGE_SLEEP_SECONDS)

    return product_urls


async def _fetch_one_product(
    client: httpx.AsyncClient,
    url: str,
    expected_root_slug: Optional[str],
    semaphore: asyncio.Semaphore,
) -> Optional[ProductRow]:
    async with semaphore:
        try:
            r = await client.get(url)
            if r.status_code != 200:
                return None

            row = parse_product_page(r.text, url)
            if not row.name or not row.code:
                return None

            # Keep only those really under the expected root category slug.
            if expected_root_slug and row.root_category_slug != expected_root_slug:
                return None

            if PRODUCT_SLEEP_SECONDS > 0:
                await asyncio.sleep(PRODUCT_SLEEP_SECONDS)  # polite pacing
            return row
        except Exception as e:
            print("ERROR on", url, "->", repr(e))
            return None


async def scrape_and_filter_async(
    urls: List[str],
    expected_root_slug: Optional[str] = None,
    concurrency: int = PRODUCT_FETCH_CONCURRENCY,
) -> List[ProductRow]:
    ordered_rows: List[Optional[ProductRow]] = [None] * len(urls)
    kept = 0
    semaphore = asyncio.Semaphore(max(1, concurrency))
    limits = httpx.Limits(
        max_connections=max(1, concurrency),
        max_keepalive_connections=max(1, concurrency),
    )

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=30,
        follow_redirects=True,
        limits=limits,
    ) as client:
        async def fetch_with_index(index: int, product_url: str):
            row = await _fetch_one_product(client, product_url, expected_root_slug, semaphore)
            return index, row

        tasks = [
            asyncio.create_task(
                fetch_with_index(i, u)
            )
            for i, u in enumerate(urls)
        ]

        for i, task in enumerate(asyncio.as_completed(tasks), start=1):
            idx, row = await task
            if row:
                ordered_rows[idx] = row
                kept += 1
            if i % 50 == 0:
                print(f"scraped {i}/{len(urls)} -> kept {kept}")

    return [row for row in ordered_rows if row]

def save_to_csv(rows: List[ProductRow], filename: str) -> None:
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
    for category in ROOT_CATEGORIES:
        try:
            root_slug = to_category_slug(category)
        except ValueError as exc:
            print(exc)
            continue

        root_listing = to_category_url(root_slug)
        print(f"\n=== category={root_slug} ({root_listing}) ===")

        urls = crawl_category(root_listing, max_pages=MAX_PAGES_PER_CATEGORY)
        print("candidates:", len(urls))

        rows = asyncio.run(
            scrape_and_filter_async(
                urls,
                expected_root_slug=root_slug,
                concurrency=PRODUCT_FETCH_CONCURRENCY,
            )
        )
        print(f"verified under {root_slug}:", len(rows))

        if SORT_PRODUCTS_FOR_CSV:
            rows.sort(key=lambda row: (row.url or "").lower())

        save_to_csv(rows, csv_filename_for_category(root_slug))
