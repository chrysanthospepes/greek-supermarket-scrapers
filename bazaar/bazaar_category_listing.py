import csv
import argparse
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.bazaar-online.gr"
BAZAAR_DIR = Path(__file__).resolve().parent
KNOWN_ROOT_CATEGORIES = [
    "froyta-lachanika",
    # "allantika-delicatessen",
    # "artozacharoplasteio",
    # "vrefika",
    # "galaktokomika-eidi-rygeioy",
    # "glyka-almyra-snak-zacharodi",
    # "kava",
    # "kathariotita-oikiaka-eidi",
    # "kataryxi",
    # "kreas-poylerika",
    # "pantopoleio",
    # "proino-kafes-rofimata",
    # "tyria-tyrokomika",
    # "ygeia-and-omorfia",
    # "fytika",
    # "pet-shop",
]
ROOT_CATEGORIES = [
    "froyta-lachanika",
    # "allantika-delicatessen",
    # "artozacharoplasteio",
    # "vrefika",
    # "galaktokomika-eidi-rygeioy",
    # "glyka-almyra-snak-zacharodi",
    # "kava",
    # "kathariotita-oikiaka-eidi",
    # "kataryxi",
    # "kreas-poylerika",
    # "pantopoleio",
    # "proino-kafes-rofimata",
    # "tyria-tyrokomika",
    # "ygeia-and-omorfia",
    # "fytika",
    # "pet-shop",
]
MAX_PAGES_PER_CATEGORY = 500
PAGE_SLEEP_SECONDS = 0.1
SORT_PRODUCTS_FOR_CSV = True
REQUEST_RETRY_ATTEMPTS = 3
REQUEST_RETRY_BACKOFF_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

BASE_NETLOC = urlparse(BASE).netloc
_spaces_re = re.compile(r"\s+")
_non_price_chars_re = re.compile(r"[^0-9,.\-]")
_one_plus_one_re = re.compile(r"\b1\s*\+\s*1\b")
_two_plus_one_re = re.compile(r"\b2\s*\+\s*1\b")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_page_param_re = re.compile(r"[?&]page=(\d+)", re.IGNORECASE)
_price_before_currency_re = re.compile(r"(-?[0-9][0-9\.,]*)\s*€", re.IGNORECASE)
BRAND_ALLOWLIST_PATH = BAZAAR_DIR / "bazaar_brand_allowlist.txt"
BRAND_DENYLIST_PATH = BAZAAR_DIR / "bazaar_brand_denylist.txt"
BRAND_CANDIDATES_PATH = BAZAAR_DIR / "bazaar_brand_candidates.csv"
_hidden_price_quantum = Decimal("0.01")
_hidden_price_fields = ("hidden_price", "hidden_unit_price")


@dataclass
class ListingProductRow:
    url: Optional[str] = None
    name: Optional[str] = None
    sku: Optional[str] = None
    brand: Optional[str] = None

    final_price: Optional[float] = None
    final_unit_price: Optional[float] = None
    hidden_price: Optional[float] = None
    hidden_unit_price: Optional[float] = None
    original_price: Optional[float] = None
    original_unit_price: Optional[float] = None
    unit_of_measure: Optional[str] = None

    discount_percent: Optional[int] = None
    offer: bool = False
    one_plus_one: bool = False
    two_plus_one: bool = False
    promo_text: Optional[str] = None

    image_url: Optional[str] = None

    root_category: Optional[str] = None

    def refresh_hidden_prices(self) -> None:
        multiplier = 1.0
        if self.two_plus_one:
            multiplier = 2.0 / 3.0
        elif self.one_plus_one:
            multiplier = 0.5

        self.hidden_price = round_hidden_price(self.final_price, multiplier)
        self.hidden_unit_price = round_hidden_price(self.final_unit_price, multiplier)

    def __post_init__(self) -> None:
        self.refresh_hidden_prices()


def round_hidden_price(value: Optional[float], multiplier: float) -> Optional[float]:
    if value is None:
        return None
    amount = Decimal(str(value)) * Decimal(str(multiplier))
    return float(amount.quantize(_hidden_price_quantum, rounding=ROUND_CEILING))


def serialize_row_for_csv(row: ListingProductRow) -> Dict[str, Any]:
    data = asdict(row)
    for field_name in _hidden_price_fields:
        value = data.get(field_name)
        if value is not None:
            data[field_name] = f"{value:.2f}"
    return data


@dataclass
class BrandCandidateStat:
    raw_brand: str
    count: int = 0
    categories: Set[str] = None
    sample_names: List[str] = None
    sample_urls: List[str] = None

    def __post_init__(self) -> None:
        if self.categories is None:
            self.categories = set()
        if self.sample_names is None:
            self.sample_names = []
        if self.sample_urls is None:
            self.sample_urls = []


def normalize_spaces(text: str) -> str:
    return _spaces_re.sub(" ", (text or "").replace("\xa0", " ")).strip()


@lru_cache(maxsize=512)
def normalize_text_no_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", normalize_spaces(text).lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


@lru_cache(maxsize=512)
def detect_unit_of_measure(label: str) -> Optional[str]:
    low = normalize_text_no_accents(label)
    if any(
        token in low
        for token in ("κιλου", "κιλα", "κιλο", "/κιλο", "/κιλου", "/κιλα", "/kg", "kg")
    ):
        return "kilos"
    if any(
        token in low
        for token in ("λιτρου", "λιτρα", "λιτρο", "/λιτρο", "/λιτρα", "/lt", "/l", "lt", "ml")
    ):
        return "liters"
    if any(
        token in low
        for token in ("τεμαχ", "τεμ", "τμχ", "/τεμαχιο", "/τεμ", "/pc", "pcs", "piece", "each")
    ):
        return "piece"
    return None


def same_site(url: str) -> bool:
    return urlparse(url).netloc == BASE_NETLOC


def normalize(url: str, *, drop_query: bool = False) -> str:
    parsed = urlparse(url)
    if drop_query:
        parsed = parsed._replace(query="")
    return parsed._replace(fragment="").geturl()


def parse_price_number(text: str) -> Optional[float]:
    s = normalize_spaces(text)
    if not s:
        return None

    s = s.replace("EUR", "")
    s = s.replace("€", "")
    s = _non_price_chars_re.sub("", s)
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


def parse_first_price_value(text: str) -> Optional[float]:
    match = _price_before_currency_re.search(text or "")
    if match:
        value = parse_price_number(match.group(1))
        if value is not None:
            return value
    return parse_price_number(text)


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


def parse_list_file(path: Path) -> Set[str]:
    if not path.exists():
        return set()

    values: Set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = normalize_spaces(raw_line)
        if not line or line.startswith("#"):
            continue
        values.add(normalize_text_no_accents(line))
    return values


@lru_cache(maxsize=1)
def load_brand_allowlist() -> Set[str]:
    return parse_list_file(BRAND_ALLOWLIST_PATH)


@lru_cache(maxsize=1)
def load_brand_denylist() -> Set[str]:
    return parse_list_file(BRAND_DENYLIST_PATH)


def ensure_brand_review_files() -> None:
    defaults = {
        BRAND_ALLOWLIST_PATH: (
            "# One Bazaar brand per line.\n"
            "# Lines starting with # are ignored.\n"
            "# Example:\n"
            "# ΒΕΖΥΡΟΓΛΟΥ\n"
            "# FARMER\n"
        ),
        BRAND_DENYLIST_PATH: (
            "# One Bazaar non-brand label per line.\n"
            "# Use this for commodity/product-type labels from .manufacturer_link a.\n"
            "# Example:\n"
            "# ΓΛΥΚΟΠΑΤΑΤΕΣ\n"
            "# ΑΒΟΚΑΝΤΟ\n"
        ),
    }

    for path, content in defaults.items():
        if not path.exists():
            path.write_text(content, encoding="utf-8")


def reset_brand_list_caches() -> None:
    load_brand_allowlist.cache_clear()
    load_brand_denylist.cache_clear()


def extract_pagination_state(
    tree: HTMLParser,
) -> Tuple[Optional[int], Optional[int], bool]:
    page_numbers: Set[int] = set()
    current_page: Optional[int] = None
    has_next = (
        tree.css_first("link[rel='next']") is not None
        or tree.css_first("a[rel='next']") is not None
    )

    current_node = tree.css_first(".pagination li.active span")
    if current_node:
        try:
            current_page = int(normalize_spaces(current_node.text(separator=" ", strip=True)))
            page_numbers.add(current_page)
        except ValueError:
            pass

    for link in tree.css("link[rel='next'], link[rel='prev']"):
        href = normalize_spaces(link.attributes.get("href") or "")
        if not href:
            continue
        m = _page_param_re.search(href)
        if not m:
            continue
        try:
            page_numbers.add(int(m.group(1)))
        except ValueError:
            pass

    for a in tree.css(".pagination a[href]"):
        href = normalize_spaces(a.attributes.get("href") or "")
        if not href:
            continue
        m = _page_param_re.search(href)
        if m:
            try:
                page_numbers.add(int(m.group(1)))
            except ValueError:
                pass
        else:
            normalized_href = normalize(urljoin(BASE, href))
            if normalized_href.rstrip("/") == normalize(BASE + "/" + to_category_slug(normalized_href)).rstrip("/"):
                page_numbers.add(1)

    max_page = max(page_numbers) if page_numbers else None
    return current_page, max_page, has_next


def parse_sku(article) -> Optional[str]:
    node = article.css_first(".knns-model-value")
    if node:
        value = normalize_spaces(node.text(separator=" ", strip=True))
        if value:
            return value

    value = normalize_spaces(article.attributes.get("data-product-id") or "")
    return value or None


def parse_brand(article) -> Optional[str]:
    node = article.css_first(".manufacturer_link a")
    if not node:
        return None
    value = normalize_spaces(node.text(separator=" ", strip=True))
    if not value or value == "-":
        return None

    key = normalize_text_no_accents(value)
    denylist = load_brand_denylist()
    if key in denylist:
        return None

    return value


def parse_name(article) -> Optional[str]:
    node = article.css_first("h4 a")
    if node:
        value = normalize_spaces(node.text(separator=" ", strip=True))
        if value:
            return value

    img = article.css_first(".image img")
    if img:
        for attr in ("title", "alt"):
            value = normalize_spaces(img.attributes.get(attr) or "")
            if value:
                return value
    return None


def parse_product_url(article) -> Optional[str]:
    for selector in ("h4 a[href]", ".image a[href]"):
        node = article.css_first(selector)
        if not node:
            continue
        href = normalize_spaces(node.attributes.get("href") or "")
        if not href:
            continue
        url = normalize(urljoin(BASE, href), drop_query=True)
        if same_site(url):
            return url
    return None


def parse_image_url(article) -> Optional[str]:
    img = article.css_first(".image img")
    if not img:
        return None

    for attr in ("src", "data-src"):
        src = normalize_spaces(img.attributes.get(attr) or "")
        if src:
            return normalize(urljoin(BASE, src))
    return None


def is_unit_price_label(label: str) -> bool:
    low = normalize_text_no_accents(label)
    if "τελικη τιμη" in low:
        return False
    return detect_unit_of_measure(label) is not None


def normalize_discount_text(text: str) -> Optional[str]:
    value = normalize_spaces(text)
    if not value:
        return None
    return re.sub(r"\s*%\s*", "%", value)


def parse_promo(article) -> Tuple[Optional[int], bool, bool, Optional[str]]:
    candidates: List[str] = []
    seen: Set[str] = set()

    def add_candidate(text: Optional[str]) -> None:
        value = normalize_spaces(text or "")
        if not value:
            return
        key = normalize_text_no_accents(value)
        if key in seen:
            return
        seen.add(key)
        candidates.append(value)

    for node in article.css(".labels_container [class*='label']"):
        add_candidate(node.text(separator=" ", strip=True))
        add_candidate(node.attributes.get("data-tag"))
        add_candidate(node.attributes.get("title"))

    one_plus_one = any(_one_plus_one_re.search(text) for text in candidates)
    two_plus_one = any(_two_plus_one_re.search(text) for text in candidates)

    discount_percent: Optional[int] = None
    promo_text: Optional[str] = None
    for text in candidates:
        match = _discount_re.search(text)
        if not match:
            continue
        try:
            discount_percent = abs(int(match.group(1).replace(" ", "")))
        except ValueError:
            continue
        promo_text = normalize_discount_text(text)
        break

    return discount_percent, one_plus_one, two_plus_one, promo_text


def parse_pricing(
    article,
) -> Tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[str],
]:
    price_wrapper = article.css_first(".price .price_wrapper")
    final_primary = None
    original_primary = None
    if price_wrapper:
        final_primary = parse_price_number(
            price_wrapper.css_first(".price-new").text(separator=" ", strip=True)
            if price_wrapper.css_first(".price-new")
            else ""
        )
        original_primary = parse_price_number(
            price_wrapper.css_first(".price-old").text(separator=" ", strip=True)
            if price_wrapper.css_first(".price-old")
            else ""
        )
        if final_primary is None:
            final_primary = parse_first_price_value(price_wrapper.text(separator=" ", strip=True))

    item_price_text = normalize_spaces(
        article.css_first(".price .item_price_text").text(separator=" ", strip=True)
        if article.css_first(".price .item_price_text")
        else ""
    )
    secondary_text = normalize_spaces(
        article.css_first(".priceperkg").text(separator=" ", strip=True)
        if article.css_first(".priceperkg")
        else ""
    )
    secondary_price = parse_first_price_value(secondary_text)

    final_price = None
    final_unit_price = None
    original_price = None
    original_unit_price = None

    primary_is_unit = is_unit_price_label(item_price_text)
    if primary_is_unit:
        final_unit_price = final_primary
        original_unit_price = original_primary
        final_price = secondary_price
    else:
        final_price = final_primary
        original_price = original_primary
        final_unit_price = secondary_price

    if final_price is not None and original_price is not None and original_price <= final_price + 1e-9:
        original_price = None
    if (
        final_unit_price is not None
        and original_unit_price is not None
        and original_unit_price <= final_unit_price + 1e-9
    ):
        original_unit_price = None

    if final_price is None and final_unit_price is not None:
        final_price = final_unit_price
    if final_unit_price is None and final_price is not None:
        final_unit_price = final_price

    unit_of_measure = detect_unit_of_measure(" ".join((item_price_text, secondary_text)))
    return final_price, final_unit_price, original_price, original_unit_price, unit_of_measure


def parse_listing_article(
    article,
    root_category: str,
) -> Optional[ListingProductRow]:
    name = parse_name(article)
    url = parse_product_url(article)
    sku = parse_sku(article)
    brand = parse_brand(article)

    (
        final_price,
        final_unit_price,
        original_price,
        original_unit_price,
        unit_of_measure,
    ) = parse_pricing(article)
    discount_percent, one_plus_one, two_plus_one, promo_text = parse_promo(article)

    if discount_percent is None:
        if final_price and original_price and original_price > final_price:
            discount_percent = int(round(((original_price - final_price) / original_price) * 100))
        elif (
            final_unit_price
            and original_unit_price
            and original_unit_price > final_unit_price
        ):
            discount_percent = int(
                round(((original_unit_price - final_unit_price) / original_unit_price) * 100)
            )

    has_price_discount = (
        original_price is not None
        and final_price is not None
        and original_price > final_price
    ) or (
        original_unit_price is not None
        and final_unit_price is not None
        and original_unit_price > final_unit_price
    )

    if discount_percent is None:
        promo_text = None

    offer = one_plus_one or two_plus_one or discount_percent is not None or has_price_discount
    unit_of_measure = unit_of_measure or "piece"

    row = ListingProductRow(
        url=url,
        name=name,
        sku=sku,
        brand=brand,
        final_price=final_price,
        final_unit_price=final_unit_price,
        original_price=original_price,
        original_unit_price=original_unit_price,
        unit_of_measure=unit_of_measure,
        discount_percent=discount_percent,
        offer=offer,
        one_plus_one=one_plus_one,
        two_plus_one=two_plus_one,
        promo_text=promo_text,
        image_url=parse_image_url(article),
        root_category=root_category,
    )

    if not row.url and not row.name and not row.sku:
        return None
    return row


def collect_brand_candidates_from_category(
    root_listing: str,
    root_category: str,
    max_pages: int = 500,
) -> Dict[str, BrandCandidateStat]:
    root_listing = normalize(root_listing.rstrip("/"))
    out: Dict[str, BrandCandidateStat] = {}
    seen_keys: Set[str] = set()

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            url = root_listing if page == 1 else f"{root_listing}?page={page}"
            response = fetch_listing_page(client=client, url=url, page=page)

            if response.status_code == 404:
                print(f"brand-scan page={page} -> 404, stopping pagination.")
                break

            response.raise_for_status()
            tree = HTMLParser(response.text)
            current_page, max_page, has_next = extract_pagination_state(tree)

            if current_page is not None and current_page != page:
                print(
                    f"brand-scan page={page} -> server current_page={current_page}, "
                    "stopping pagination."
                )
                break

            articles = tree.css("#mfilter-content-container .product-thumb[data-product-id]")
            if not articles:
                articles = tree.css(".product-thumb[data-product-id]")
            if not articles:
                print(f"brand-scan page={page} -> 0 products, stopping.")
                break

            added = 0
            for article in articles:
                sku = parse_sku(article)
                name = parse_name(article)
                url_value = parse_product_url(article)
                key = url_value or f"{sku or ''}|{name or ''}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                raw_brand = parse_raw_brand(article)
                if not raw_brand:
                    continue

                stat = out.setdefault(raw_brand, BrandCandidateStat(raw_brand=raw_brand))
                stat.count += 1
                stat.categories.add(root_category)
                if name and name not in stat.sample_names and len(stat.sample_names) < 5:
                    stat.sample_names.append(name)
                if url_value and url_value not in stat.sample_urls and len(stat.sample_urls) < 5:
                    stat.sample_urls.append(url_value)
                added += 1

            print(
                f"brand-scan page={page} +{added} brand-rows "
                f"unique-brands={len(out)} cards={len(articles)}"
            )

            if max_page is not None and page >= max_page:
                print(f"brand-scan page={page} -> reached max_page={max_page}, stopping.")
                break

            if not has_next:
                print(f"brand-scan page={page} -> no next page in pagination, stopping.")
                break

            time.sleep(PAGE_SLEEP_SECONDS)

    return out


def merge_brand_candidate_stats(
    target: Dict[str, BrandCandidateStat],
    source: Dict[str, BrandCandidateStat],
) -> None:
    for raw_brand, incoming in source.items():
        current = target.setdefault(raw_brand, BrandCandidateStat(raw_brand=raw_brand))
        current.count += incoming.count
        current.categories.update(incoming.categories)
        for name in incoming.sample_names:
            if name not in current.sample_names and len(current.sample_names) < 5:
                current.sample_names.append(name)
        for url in incoming.sample_urls:
            if url not in current.sample_urls and len(current.sample_urls) < 5:
                current.sample_urls.append(url)


def save_brand_candidates_csv(stats: Dict[str, BrandCandidateStat], filename: Path) -> None:
    rows = sorted(
        stats.values(),
        key=lambda row: (-row.count, normalize_text_no_accents(row.raw_brand), row.raw_brand),
    )
    with filename.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "raw_brand",
                "normalized_brand",
                "count",
                "categories",
                "sample_names",
                "sample_urls",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "raw_brand": row.raw_brand,
                    "normalized_brand": normalize_text_no_accents(row.raw_brand),
                    "count": row.count,
                    "categories": " | ".join(sorted(row.categories)),
                    "sample_names": " | ".join(row.sample_names),
                    "sample_urls": " | ".join(row.sample_urls),
                }
            )

    print(f"Saved {len(rows)} brand candidates to {filename}")


def dump_brand_candidates(categories: List[str], max_pages: int = 500) -> None:
    ensure_brand_review_files()

    all_stats: Dict[str, BrandCandidateStat] = {}
    for category in categories:
        try:
            root_slug = to_category_slug(category)
        except ValueError as exc:
            print(exc)
            continue

        root_listing = to_category_url(root_slug)
        print(f"\n=== brand scan category={root_slug} ({root_listing}) ===")
        stats = collect_brand_candidates_from_category(
            root_listing=root_listing,
            root_category=root_slug,
            max_pages=max_pages,
        )
        merge_brand_candidate_stats(all_stats, stats)

    save_brand_candidates_csv(all_stats, BRAND_CANDIDATES_PATH)


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
            tree = HTMLParser(response.text)
            current_page, max_page, has_next = extract_pagination_state(tree)

            if current_page is not None and current_page != page:
                print(
                    f"page={page} -> server current_page={current_page}, "
                    "stopping pagination."
                )
                break

            articles = tree.css("#mfilter-content-container .product-thumb[data-product-id]")
            if not articles:
                articles = tree.css(".product-thumb[data-product-id]")
            if not articles:
                print(f"page={page} -> 0 products, stopping.")
                break

            added = 0
            for article in articles:
                row = parse_listing_article(article, root_category=root_category)
                if not row:
                    continue

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
            writer.writerow(serialize_row_for_csv(row))

    print(f"Saved {len(rows)} rows to {filename}")


def parse_raw_brand(article) -> Optional[str]:
    node = article.css_first(".manufacturer_link a")
    if not node:
        return None
    value = normalize_spaces(node.text(separator=" ", strip=True))
    if not value or value == "-":
        return None
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dump-brands",
        action="store_true",
        help="Crawl categories and write raw .manufacturer_link brand candidates to CSV.",
    )
    parser.add_argument(
        "--brand-categories",
        choices=("root", "known"),
        default="known",
        help="Which category list to use with --dump-brands.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.dump_brands:
        categories = ROOT_CATEGORIES if args.brand_categories == "root" else KNOWN_ROOT_CATEGORIES
        dump_brand_candidates(categories=categories, max_pages=MAX_PAGES_PER_CATEGORY)
    else:
        reset_brand_list_caches()
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
