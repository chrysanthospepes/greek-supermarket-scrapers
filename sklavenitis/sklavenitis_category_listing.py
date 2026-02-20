import csv
import html
import json
import re
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.sklavenitis.gr"
ROOT_CATEGORIES = [
    "eidi-artozacharoplasteioy",
    # "freska-kai-psychomena",
    # "kreas-poulerika-kynigi",
    # "galaktokomika-tyria",
]
MAX_PAGES_PER_CATEGORY = 500
PAGE_SLEEP_SECONDS = 0.3
# True: deterministic sort before CSV write. False: keep parser discovery order.
SORT_PRODUCTS_FOR_CSV = True

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

_gift_offer_re = re.compile(r"(\d+)\s*\+\s*(\d+)")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_current_total_re = re.compile(r"(\d+)\s+.+?\s+(\d+)\s+.+", re.IGNORECASE)


@dataclass
class ListingProductRow:
    url: Optional[str] = None
    name: Optional[str] = None
    code: Optional[str] = None
    analytics_id: Optional[str] = None
    variant_id: Optional[str] = None
    brand: Optional[str] = None

    final_price: Optional[float] = None
    currency: Optional[str] = None
    unit_price: Optional[float] = None
    unit_price_unit: Optional[str] = None
    original_price: Optional[float] = None
    original_unit_price: Optional[float] = None
    original_unit_price_unit: Optional[str] = None
    final_set_price: Optional[float] = None
    original_set_price: Optional[float] = None

    discount_percent: Optional[int] = None
    promo_text: Optional[str] = None
    gift_buy_qty: Optional[int] = None
    gift_free_qty: Optional[int] = None

    image_url: Optional[str] = None

    analytics_category: Optional[str] = None
    analytics_category2: Optional[str] = None
    analytics_category3: Optional[str] = None
    root_category_slug: Optional[str] = None
    root_category: Optional[str] = None

    listing_page: Optional[int] = None
    listing_page_url: Optional[str] = None


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def same_site(url: str) -> bool:
    return urlparse(url).netloc == urlparse(BASE).netloc


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def parse_price_number(text: str) -> Optional[float]:
    s = normalize_spaces(text)
    if not s:
        return None

    s = s.replace("EUR", "")
    s = s.replace("€", "")
    s = re.sub(r"[^0-9,.\-]", "", s)
    if not s:
        return None

    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    elif s.count(".") > 1:
        s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


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
    return f"{BASE}/{slug}/"


def csv_filename_for_category(category: str) -> str:
    slug = to_category_slug(category).replace("/", "_")
    safe_slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug).strip("_")
    if not safe_slug:
        safe_slug = "category"
    return f"{safe_slug}-listing-products.csv"


def parse_json_attr(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(html.unescape(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_analytics_item(article) -> Dict[str, Any]:
    payload = parse_json_attr(article.attributes.get("data-plugin-analyticsimpressions"))
    call = payload.get("Call")
    if not isinstance(call, dict):
        return {}
    ecommerce = call.get("ecommerce")
    if not isinstance(ecommerce, dict):
        return {}
    items = ecommerce.get("items")
    if not isinstance(items, list) or not items:
        return {}
    first = items[0]
    return first if isinstance(first, dict) else {}


def parse_product_meta(article) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    plugin_product = parse_json_attr(article.attributes.get("data-plugin-product"))
    data_item = parse_json_attr(article.attributes.get("data-item"))

    sku = plugin_product.get("sku") or data_item.get("ProductSKU")
    if sku is not None:
        out["sku"] = str(sku).strip()

    product_id = data_item.get("ProductID")
    if product_id is not None:
        out["product_id"] = str(product_id).strip()

    return out


def parse_product_url(article) -> Optional[str]:
    abs_link = article.css_first("a.absLink[href]")
    if abs_link:
        href = (abs_link.attributes.get("href") or "").strip()
        if href:
            url = normalize_url(urljoin(BASE, href))
            if same_site(url):
                return url

    for a in article.css("a[href]"):
        href = (a.attributes.get("href") or "").strip()
        if not href:
            continue
        url = normalize_url(urljoin(BASE, href))
        if same_site(url):
            return url
    return None


def parse_name(article, analytics_item: Dict[str, Any]) -> Optional[str]:
    analytics_name = analytics_item.get("item_name")
    if isinstance(analytics_name, str) and analytics_name.strip():
        return normalize_spaces(html.unescape(analytics_name))

    node = article.css_first("h4.product__title a")
    if node:
        txt = normalize_spaces(node.text(separator=" ", strip=True))
        if txt:
            return txt
    return None


def parse_image_url(article) -> Optional[str]:
    node = article.css_first("figure.product__figure img[src]")
    if not node:
        return None
    src = (node.attributes.get("src") or "").strip()
    if not src:
        return None
    return normalize_url(urljoin(BASE, src))


def parse_unit_price(article) -> Tuple[Optional[float], Optional[str]]:
    node = article.css_first(".priceWrp .priceKil")
    if not node:
        return None, None

    full = normalize_spaces(node.text(separator=" ", strip=True))
    value = parse_price_number(full)

    unit = None
    span = node.css_first("span")
    if span:
        unit = normalize_spaces(span.text(separator=" ", strip=True))
    if not unit and "/" in full:
        unit = full.split("/", 1)[1].strip()
        unit = f"/{unit}" if unit and not unit.startswith("/") else unit

    return value, unit or None


def parse_main_prices(article, analytics_price: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    entries: List[Tuple[float, str]] = []
    for node in article.css(".priceWrp .main-price .price"):
        raw = (node.attributes.get("data-price") or "").strip()
        value = parse_price_number(raw) if raw else None
        if value is None:
            value = parse_price_number(node.text(separator=" ", strip=True))
        if value is None:
            continue

        parent = node.parent
        parent_class = ""
        if parent is not None:
            parent_class = (parent.attributes.get("class") or "").lower()
        entries.append((value, parent_class))

    if not entries and analytics_price is None:
        return None, None

    final_price = analytics_price
    if final_price is None and entries:
        preferred = next((v for v, cls in entries if "previous" not in cls), None)
        final_price = preferred if preferred is not None else entries[0][0]

    original_price = None
    if final_price is not None and entries:
        higher = [v for v, _ in entries if v > final_price + 1e-9]
        if higher:
            original_price = max(higher)
    elif len(entries) >= 2:
        values = sorted({v for v, _ in entries})
        if len(values) >= 2:
            final_price = values[0]
            original_price = values[-1]

    return final_price, original_price


def parse_promo(article) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[int]]:
    candidates: List[str] = []
    seen: Set[str] = set()

    def add_candidate(text: Optional[str]) -> None:
        txt = normalize_spaces(text or "")
        if not txt:
            return
        key = txt.lower()
        if key in seen:
            return
        seen.add(key)
        candidates.append(txt)

    for selector in (
        ".product-note-tag",
        ".product-discount-tag",
        ".product-label",
        ".product-tag",
        ".product-badge",
        ".product-flags_figure",
        ".sign-new_figure",
        "[class*='offer']",
        "[class*='discount']",
        "[class*='promo']",
        "[class*='badge']",
        "[class*='tag']",
        "[class*='flag']",
        "[class*='sign']",
    ):
        for node in article.css(selector):
            add_candidate(node.text(separator=" ", strip=True))
            add_candidate(node.attributes.get("title"))
            add_candidate(node.attributes.get("aria-label"))
            for img in node.css("img"):
                add_candidate(img.attributes.get("alt"))
                add_candidate(img.attributes.get("title"))

    promo_text = None
    for txt in candidates:
        low = txt.lower()
        if "%" in txt or "+" in txt or "dor" in low or "δώρ" in low or "δωρ" in low:
            promo_text = txt
            break
    if promo_text is None and candidates:
        # Keep non-discount badges (e.g. "Νέο", "Vegan") in existing promo_text column.
        promo_text = candidates[0]

    discount_percent = None
    if promo_text:
        m = _discount_re.search(promo_text)
        if m:
            try:
                discount_percent = int(m.group(1).replace(" ", ""))
            except ValueError:
                pass

    buy_qty = None
    free_qty = None
    if promo_text:
        m = _gift_offer_re.search(promo_text)
        if m:
            try:
                buy_qty = int(m.group(1))
                free_qty = int(m.group(2))
            except ValueError:
                pass

    return promo_text, discount_percent, buy_qty, free_qty


def parse_listing_article(article, root_slug: str, page: int, page_url: str) -> Optional[ListingProductRow]:
    analytics_item = parse_analytics_item(article)
    product_meta = parse_product_meta(article)

    analytics_id = None
    item_id = analytics_item.get("item_id")
    if item_id is not None:
        analytics_id = str(item_id).strip()

    analytics_price = None
    if analytics_item.get("price") is not None:
        analytics_price = parse_price_number(str(analytics_item.get("price")))

    name = parse_name(article, analytics_item=analytics_item)
    url = parse_product_url(article)

    unit_price, unit_price_unit = parse_unit_price(article)
    final_price, original_price = parse_main_prices(article, analytics_price=analytics_price)
    promo_text, discount_percent, gift_buy_qty, gift_free_qty = parse_promo(article)

    # Fallback discount if list price data is available but no explicit percentage text.
    if discount_percent is None and final_price and original_price and original_price > final_price:
        discount_percent = int(round(((original_price - final_price) / original_price) * 100))

    code = analytics_id or product_meta.get("sku")
    variant_id = product_meta.get("product_id") or product_meta.get("sku")

    analytics_category = analytics_item.get("item_category")
    if analytics_category is not None:
        analytics_category = normalize_spaces(str(analytics_category))
    else:
        analytics_category = None

    root_category = analytics_item.get("item_list_name")
    if root_category is not None:
        root_category = normalize_spaces(str(root_category))
    if not root_category:
        root_category = root_slug

    brand = analytics_item.get("item_brand")
    if brand is not None:
        brand = normalize_spaces(html.unescape(str(brand))) or None

    row = ListingProductRow(
        url=url,
        name=name,
        code=code,
        analytics_id=analytics_id,
        variant_id=variant_id,
        brand=brand,
        final_price=final_price,
        currency="EUR",
        unit_price=unit_price,
        unit_price_unit=unit_price_unit,
        original_price=original_price,
        original_unit_price=None,
        original_unit_price_unit=None,
        final_set_price=None,
        original_set_price=None,
        discount_percent=discount_percent,
        promo_text=promo_text,
        gift_buy_qty=gift_buy_qty,
        gift_free_qty=gift_free_qty,
        image_url=parse_image_url(article),
        analytics_category=analytics_category,
        analytics_category2=None,
        analytics_category3=None,
        root_category_slug=root_slug,
        root_category=root_category,
        listing_page=page,
        listing_page_url=page_url,
    )

    if not row.url and not row.name and not row.code:
        return None
    return row


def extract_next_page_info(t: HTMLParser) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Returns:
    - next page number if pagination marker exists
    - current displayed product count (from text like '24 ... 282 ...')
    - total products
    """
    next_page = None
    current_count = None
    total_count = None

    pg_node = t.css_first("section.pagination.go-next")
    if pg_node:
        raw = (pg_node.attributes.get("data-pg") or "").strip()
        if raw.isdigit():
            next_page = int(raw)

    count_node = t.css_first("section.pagination .current-page")
    if count_node:
        txt = normalize_spaces(count_node.text(separator=" ", strip=True))
        m = _current_total_re.search(txt)
        if m:
            try:
                current_count = int(m.group(1))
                total_count = int(m.group(2))
            except ValueError:
                pass

    return next_page, current_count, total_count


def build_page_url(root_listing: str, page: int) -> str:
    if page <= 1:
        return root_listing
    sep = "&" if "?" in root_listing else "?"
    return f"{root_listing}{sep}pg={page}"


def crawl_category_listing(root_listing: str, root_slug: str, max_pages: int = 500) -> List[ListingProductRow]:
    root_listing = normalize_url(root_listing.rstrip("/") + "/")
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()

    page = 1
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        while page <= max_pages:
            url = build_page_url(root_listing, page)
            response = client.get(url)

            if response.status_code == 404:
                print(f"page={page} -> 404, stopping pagination.")
                break

            response.raise_for_status()
            html_text = response.text
            t = HTMLParser(html_text)

            articles = t.css("section.productList div[data-plugin-product]")
            if not articles:
                print(f"page={page} -> 0 products, stopping.")
                break

            added = 0
            for article in articles:
                row = parse_listing_article(article, root_slug=root_slug, page=page, page_url=url)
                if not row:
                    continue

                key = row.url or f"{row.analytics_id or ''}|{row.code or ''}|{row.name or ''}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rows.append(row)
                added += 1

            next_page, current_count, total_count = extract_next_page_info(t)
            count_part = ""
            if current_count is not None and total_count is not None:
                count_part = f" current={current_count}/{total_count}"
            print(f"page={page} +{added} total={len(rows)} cards={len(articles)}{count_part}")

            if added == 0:
                print(f"page={page} -> 0 NEW unique products, stopping.")
                break

            if next_page is None:
                print(f"page={page} -> no next page marker, stopping.")
                break

            if next_page <= page:
                print(f"page={page} -> invalid next page ({next_page}), stopping.")
                break

            page = next_page
            time.sleep(PAGE_SLEEP_SECONDS)

    return rows


def save_to_csv(rows: List[ListingProductRow], filename: str) -> None:
    if not rows:
        print("No rows to save.")
        return

    fieldnames = list(asdict(rows[0]).keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))

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

        rows = crawl_category_listing(
            root_listing=root_listing,
            root_slug=root_slug,
            max_pages=MAX_PAGES_PER_CATEGORY,
        )
        print(f"parsed from listings under {root_slug}: {len(rows)}")

        if SORT_PRODUCTS_FOR_CSV:
            rows.sort(key=lambda row: ((row.url or "").lower(), row.code or "", row.name or ""))

        save_to_csv(rows, csv_filename_for_category(root_slug))
