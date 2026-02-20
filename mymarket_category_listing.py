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
# True: deterministic sort before CSV write. False: keep parser discovery order.
SORT_PRODUCTS_FOR_CSV = True

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

_code_re = re.compile(r"(Κωδ(?:ικός)?\s*[:：]\s*)(\d+)")
_gift_offer_re = re.compile(r"(\d+)\s*\+\s*(\d+)")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_page_param_re = re.compile(r"[?&]page=(\d+)", re.IGNORECASE)


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


def normalize(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def looks_like_product_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    if not path or path == "/":
        return False

    banned_prefixes = (
        "/sitemap", "/company", "/career", "/contact", "/login", "/register",
        "/terms", "/privacy", "/payment", "/search", "/cart", "/checkout",
    )
    return not any(path.startswith(prefix) for prefix in banned_prefixes)


def parse_price_number(text: str) -> Optional[float]:
    s = normalize_spaces(text)
    if not s:
        return None

    s = s.replace("€", "")
    s = re.sub(r"[^0-9,.\-]", "", s)
    if not s:
        return None

    # Typical Greek format: 1.234,56
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
    return f"{BASE}/{to_category_slug(category)}"


def csv_filename_for_category(category: str) -> str:
    slug = to_category_slug(category).replace("/", "_")
    safe_slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", slug).strip("_")
    if not safe_slug:
        safe_slug = "category"
    return f"{safe_slug}-listing-products.csv"


def extract_pagination_state(html_text: str) -> Tuple[Optional[int], Optional[int], bool]:
    t = HTMLParser(html_text)
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


def parse_analytics_payload(article) -> Dict[str, Any]:
    raw = (
        article.attributes.get("data-google-analytics-item-value")
        or article.attributes.get("data-google-analytics-item-param")
    )
    if not raw:
        return {}

    decoded = html.unescape(raw)
    try:
        data = json.loads(decoded)
    except json.JSONDecodeError:
        return {}

    return data if isinstance(data, dict) else {}


def parse_code(article, analytics_id: Optional[str]) -> Optional[str]:
    sku = article.css_first(".sku")
    if sku:
        m = _code_re.search(sku.text(separator=" ", strip=True) or "")
        if m:
            return m.group(2)

    if analytics_id and analytics_id.isdigit():
        return analytics_id
    return None


def parse_promo(article) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[int]]:
    candidates: List[str] = []
    seen: Set[str] = set()
    for selector in (
        ".product-discount-tag",
        ".product-note-tag",
        ".product-label",
        ".product-tag",
        ".product-badge",
        "[class*='discount']",
        "[class*='offer']",
        "[class*='promo']",
        "[class*='badge']",
        "[class*='tag']",
    ):
        for node in article.css(selector):
            txt = normalize_spaces(node.text(separator=" ", strip=True))
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(txt)

    promo_text = None
    for txt in candidates:
        low = txt.lower()
        if "%" in txt or re.search(r"\bδ(?:ώ|ω)ρο\b", low, re.IGNORECASE):
            promo_text = txt
            break
    if promo_text is None and candidates:
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
    if promo_text and re.search(r"\bδ(?:ώ|ω)ρο\b", promo_text, re.IGNORECASE):
        m = _gift_offer_re.search(promo_text)
        if m:
            try:
                buy_qty = int(m.group(1))
                free_qty = int(m.group(2))
            except ValueError:
                pass

    return promo_text, discount_percent, buy_qty, free_qty


def parse_product_url(article) -> Optional[str]:
    for selector in (
        ".tooltip a[rel='bookmark'][href]",
        ".teaser-image-container a[rel='bookmark'][href]",
        "a[rel='bookmark'][href]",
    ):
        a = article.css_first(selector)
        if not a:
            continue
        href = (a.attributes.get("href") or "").strip()
        if not href:
            continue
        url = normalize(urljoin(BASE, href))
        if same_site(url) and looks_like_product_url(url):
            return url
    return None


def parse_image_url(article) -> Optional[str]:
    img = article.css_first("img[data-main-image]") or article.css_first("img[src]")
    if not img:
        return None

    src = (img.attributes.get("src") or "").strip()
    if src:
        return normalize(urljoin(BASE, src))
    return None


def parse_price_labels(article):
    final_price = None
    original_price = None
    unit_price = None
    unit_price_unit = None
    original_unit_price = None
    original_unit_price_unit = None
    final_set_price = None
    original_set_price = None

    blocks = list(article.css(".measure-label-wrapper"))
    blocks.extend(article.css(".product-full--product-tags .rounded"))

    for block in blocks:
        price_span = None
        price_val = None
        label = None

        for span in block.css("span"):
            txt = normalize_spaces(span.text(strip=True) or "")
            if not txt:
                continue

            if price_span is None and "€" in txt:
                maybe_price = parse_price_number(txt)
                if maybe_price is not None:
                    price_span = span
                    price_val = maybe_price

            if label is None:
                low = txt.lower()
                if "τιμή" in low and "x" not in low:
                    label = txt

        if price_span is None or price_val is None or not label:
            continue

        low_label = label.lower()
        is_old = "diagonal-line" in (price_span.attributes.get("class") or "").lower()
        is_old = is_old or low_label.startswith("αρχική")
        is_set = "σετ" in low_label
        is_unit = any(
            token in low_label
            for token in ("κιλ", "λίτρ", "lt", "kg", "ml", "gr", "γρ", "τεμαχ", "τεμ", "τμχ")
        )
        is_initial_label = low_label.startswith("αρχική")
        is_final_label = low_label.startswith("τελική")

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
                if unit_price is None or is_final_label:
                    unit_price = price_val
                    unit_price_unit = label
        else:
            if is_old or is_initial_label:
                if original_price is None or is_initial_label:
                    original_price = price_val
            else:
                if final_price is None or is_final_label:
                    final_price = price_val

    # Fallback for listings that only expose the compact selling-unit row.
    selling_price = article.css_first(".selling-unit-row .price")
    if final_price is None and selling_price:
        final_price = parse_price_number(selling_price.text(strip=True))

    return (
        final_price,
        unit_price,
        unit_price_unit,
        original_price,
        original_unit_price,
        original_unit_price_unit,
        final_set_price,
        original_set_price,
    )


def parse_listing_article(article, root_slug: str, page: int, page_url: str) -> Optional[ListingProductRow]:
    analytics = parse_analytics_payload(article)

    analytics_id_raw = analytics.get("id")
    analytics_id = str(analytics_id_raw) if analytics_id_raw is not None else None

    name = normalize_spaces(str(analytics.get("name") or ""))
    if not name:
        name_node = article.css_first(".tooltip p")
        if name_node:
            name = normalize_spaces(name_node.text(separator=" ", strip=True))
    name = name or None

    url = parse_product_url(article)
    code = parse_code(article, analytics_id=analytics_id)
    promo_text, discount_percent, gift_buy_qty, gift_free_qty = parse_promo(article)

    (
        final_price,
        unit_price,
        unit_price_unit,
        original_price,
        original_unit_price,
        original_unit_price_unit,
        final_set_price,
        original_set_price,
    ) = parse_price_labels(article)

    if final_price is None and analytics.get("price") is not None:
        final_price = parse_price_number(str(analytics.get("price")))

    currency = analytics.get("currency")
    if isinstance(currency, str):
        currency = currency.strip().upper() or None
    else:
        currency = None
    if currency is None:
        currency = "EUR"

    variant_id_raw = article.attributes.get("data-id")
    variant_id = variant_id_raw.strip() if isinstance(variant_id_raw, str) else None

    root_category_name = analytics.get("category")
    if not isinstance(root_category_name, str) or not root_category_name.strip():
        root_category_name = root_slug
    else:
        root_category_name = root_category_name.strip()

    row = ListingProductRow(
        url=url,
        name=name,
        code=code,
        analytics_id=analytics_id,
        variant_id=variant_id,
        brand=normalize_spaces(str(analytics.get("brand") or "")) or None,
        final_price=final_price,
        currency=currency,
        unit_price=unit_price,
        unit_price_unit=unit_price_unit,
        original_price=original_price,
        original_unit_price=original_unit_price,
        original_unit_price_unit=original_unit_price_unit,
        final_set_price=final_set_price,
        original_set_price=original_set_price,
        discount_percent=discount_percent,
        promo_text=promo_text,
        gift_buy_qty=gift_buy_qty,
        gift_free_qty=gift_free_qty,
        image_url=parse_image_url(article),
        analytics_category=(analytics.get("category") or None),
        analytics_category2=(analytics.get("category2") or None),
        analytics_category3=(analytics.get("category3") or None),
        root_category_slug=root_slug,
        root_category=root_category_name,
        listing_page=page,
        listing_page_url=page_url,
    )

    if not row.url and not row.name and not row.code:
        return None
    return row


def crawl_category_listing(root_listing: str, root_slug: str, max_pages: int = 500) -> List[ListingProductRow]:
    root_listing = normalize(root_listing.rstrip("/"))
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = root_listing if page == 1 else f"{root_listing}?page={page}"
            response = client.get(url)

            if response.status_code == 404:
                print(f"page={page} -> 404, stopping pagination.")
                break

            response.raise_for_status()
            html_text = response.text
            current_page, max_page, has_next = extract_pagination_state(html_text)

            if current_page is not None and current_page != page:
                print(
                    f"page={page} -> server current_page={current_page}, "
                    "stopping pagination."
                )
                break

            t = HTMLParser(html_text)
            articles = t.css("article.product--teaser")
            if not articles:
                print(f"page={page} -> 0 products, stopping.")
                break

            added = 0
            for article in articles:
                row = parse_listing_article(article, root_slug=root_slug, page=page, page_url=url)
                if not row:
                    continue

                # URL is the best dedupe key; fallback to analytics/code identity.
                key = row.url or f"{row.analytics_id or ''}|{row.code or ''}|{row.name or ''}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rows.append(row)
                added += 1

            print(f"page={page} +{added} total={len(rows)} cards={len(articles)}")

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
