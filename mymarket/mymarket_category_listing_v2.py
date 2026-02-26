import csv
import html
import json
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.mymarket.gr"
ROOT_CATEGORIES = [
    "frouta-lachanika",
    # "fresko-kreas-psari",
    # "galaktokomika-eidi-psygeiou",
    # "tyria-allantika-deli",
    # "katepsygmena-trofima",
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
PAGE_SLEEP_SECONDS = 0.1
# True: deterministic sort before CSV write. False: keep parser discovery order.
SORT_PRODUCTS_FOR_CSV = True
REQUEST_RETRY_ATTEMPTS = 3
REQUEST_RETRY_BACKOFF_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

_code_re = re.compile(r"(Κωδ(?:ικός)?\s*[:：]\s*)(\d+)")
_one_plus_one_re = re.compile(r"\b1\s*\+\s*1\b")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_page_param_re = re.compile(r"[?&]page=(\d+)", re.IGNORECASE)


@dataclass
class ListingProductRow:
    url: Optional[str] = None
    name: Optional[str] = None
    sku: Optional[str] = None
    brand: Optional[str] = None

    final_price: Optional[float] = None
    final_unit_price: Optional[float] = None
    original_price: Optional[float] = None
    original_unit_price: Optional[float] = None
    unit_of_measure: Optional[str] = None
    final_set_price: Optional[float] = None
    original_set_price: Optional[float] = None

    discount_percent: Optional[int] = None
    offer: bool = False
    one_plus_one: bool = False

    image_url: Optional[str] = None

    root_category: Optional[str] = None

def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_text_no_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", normalize_spaces(text).lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def detect_unit_of_measure(label: str) -> Optional[str]:
    low = normalize_text_no_accents(label)
    if any(token in low for token in ("κιλου", "κιλα", "κιλο")):
        return "kilos"
    if any(token in low for token in ("λιτρου", "λιτρα", "λιτρο")):
        return "liters"
    if any(
        token in low
        for token in (
            "τεμαχ",
            "τεμ",
            "τμχ",
            "/pc",
            "pcs",
            "piece",
            "/ea",
            "each",
        )
    ):
        return "pieces"
    return None


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


def parse_sku(article) -> Optional[str]:
    sku = article.css_first(".sku")
    if sku:
        m = _code_re.search(sku.text(separator=" ", strip=True) or "")
        if m:
            return m.group(2)

    return None


def parse_promo(article) -> Tuple[Optional[int], bool]:
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

    one_plus_one = any(_one_plus_one_re.search(txt) for txt in candidates)

    discount_percent = None
    for txt in candidates:
        m = _discount_re.search(txt)
        if m:
            try:
                # Keep discount as a positive integer (e.g. "-15%" -> 15).
                discount_percent = abs(int(m.group(1).replace(" ", "")))
                break
            except ValueError:
                pass

    return discount_percent, one_plus_one


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
    final_unit_price = None
    original_unit_price = None
    unit_of_measure = None
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
            unit_guess = detect_unit_of_measure(label)
            if unit_guess and unit_of_measure is None:
                unit_of_measure = unit_guess

            if is_old:
                original_unit_price = price_val
            else:
                if final_unit_price is None or is_final_label:
                    final_unit_price = price_val
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
        final_unit_price,
        original_price,
        original_unit_price,
        unit_of_measure,
        final_set_price,
        original_set_price,
    )


def parse_listing_article(
    article,
    root_category: str,
) -> Optional[ListingProductRow]:
    analytics = parse_analytics_payload(article)

    name = normalize_spaces(str(analytics.get("name") or ""))
    if not name:
        name_node = article.css_first(".tooltip p")
        if name_node:
            name = normalize_spaces(name_node.text(separator=" ", strip=True))
    name = name or None

    url = parse_product_url(article)
    sku = parse_sku(article)
    discount_percent, one_plus_one = parse_promo(article)

    (
        final_price,
        final_unit_price,
        original_price,
        original_unit_price,
        unit_of_measure,
        final_set_price,
        original_set_price,
    ) = parse_price_labels(article)

    if final_price is None and analytics.get("price") is not None:
        final_price = parse_price_number(str(analytics.get("price")))

    if (
        final_unit_price is None
        and original_unit_price is None
        and final_price is not None
    ):
        final_unit_price = final_price

    has_price_discount = (
        (original_price is not None and final_price is not None and original_price > final_price)
        or (
            original_unit_price is not None
            and final_unit_price is not None
            and original_unit_price > final_unit_price
        )
        or (
            original_set_price is not None
            and final_set_price is not None
            and original_set_price > final_set_price
        )
    )
    offer = discount_percent is not None or has_price_discount

    row = ListingProductRow(
        url=url,
        name=name,
        sku=sku,
        brand=normalize_spaces(str(analytics.get("brand") or "")) or None,
        final_price=final_price,
        final_unit_price=final_unit_price,
        original_price=original_price,
        original_unit_price=original_unit_price,
        unit_of_measure=unit_of_measure,
        final_set_price=final_set_price,
        original_set_price=original_set_price,
        discount_percent=discount_percent,
        offer=offer,
        one_plus_one=one_plus_one,
        image_url=parse_image_url(article),
        root_category=root_category,
    )

    if not row.url and not row.name and not row.sku:
        return None
    return row


def fetch_listing_page(client: httpx.Client, url: str, page: int) -> httpx.Response:
    last_error: Optional[Exception] = None
    for attempt in range(1, REQUEST_RETRY_ATTEMPTS + 1):
        try:
            response = client.get(url)
        except httpx.RequestError as exc:
            last_error = exc
            if attempt >= REQUEST_RETRY_ATTEMPTS:
                raise

            wait_seconds = REQUEST_RETRY_BACKOFF_SECONDS * attempt
            print(
                f"page={page} -> request error ({exc}), "
                f"retrying in {wait_seconds:.1f}s "
                f"({attempt}/{REQUEST_RETRY_ATTEMPTS})."
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code == 404:
            return response

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < REQUEST_RETRY_ATTEMPTS:
            wait_seconds = REQUEST_RETRY_BACKOFF_SECONDS * attempt
            print(
                f"page={page} -> status={response.status_code}, "
                f"retrying in {wait_seconds:.1f}s "
                f"({attempt}/{REQUEST_RETRY_ATTEMPTS})."
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        return response

    if last_error is not None:
        raise RuntimeError(f"page={page} -> exhausted retries") from last_error
    raise RuntimeError(f"page={page} -> exhausted retries")


def crawl_category_listing(
    root_listing: str,
    root_category: str,
    max_pages: int = 500,
) -> List[ListingProductRow]:
    root_listing = normalize(root_listing.rstrip("/"))
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = root_listing if page == 1 else f"{root_listing}?page={page}"
            response = fetch_listing_page(client=client, url=url, page=page)

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
                row = parse_listing_article(
                    article,
                    root_category=root_category,
                )
                if not row:
                    continue

                # URL is the best dedupe key; fallback to sku/name identity.
                key = row.url or f"{row.sku or ''}|{row.name or ''}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rows.append(row)
                added += 1

            print(f"page={page} +{added} total={len(rows)} cards={len(articles)}")

            if added == 0:
                print(f"page={page} -> 0 NEW unique products, continuing.")

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
            root_category=root_slug,
            max_pages=MAX_PAGES_PER_CATEGORY,
        )
        print(f"parsed from listings under {root_slug}: {len(rows)}")

        if SORT_PRODUCTS_FOR_CSV:
            rows.sort(key=lambda row: ((row.url or "").lower(), row.sku or "", row.name or ""))

        save_to_csv(rows, csv_filename_for_category(root_slug))
