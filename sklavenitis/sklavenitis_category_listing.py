import csv
import html
import json
import re
import time
import unicodedata
from dataclasses import asdict, dataclass, fields
from decimal import ROUND_CEILING, Decimal
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.sklavenitis.gr"
ROOT_CATEGORIES = [
    # "eidi-artozacharoplasteioy",
    "freska-froyta-lachanika",
    # "fresko-psari-thalassina",
    # "fresko-kreas",
    # "galata-rofimata-chymoi-psygeioy",
    # "giaoyrtia-kremes-galaktos-epidorpia-psygeioy",
    # "turokomika-futika-anapliromata",
    # "ayga-voytyro-nopes-zymes-zomoi",
    # "allantika",
    # "orektika-delicatessen",
    # "etoima-geymata",
    # "katepsygmena",
    # "kava",
    # "anapsyktika-nera-chymoi",
    # "xiroi-karpoi-snak",
    # "mpiskota-sokolates-zacharodi",
    # "eidi-proinoy-rofimata",
    # "vrefikes-paidikes-trofes",
    # "trofima-pantopoleioy",
    # "trofes-eidi-gia-katoikidia",
    # "eidi-mias-chrisis-eidi-parti",
    # "chartika-panes-servietes",
    # "kallyntika-eidi-prosopikis-ygieinis",
    # "aporrypantika-eidi-katharismoy",
    # "eidi-oikiakis-chrisis",
    # "chartopoleio",
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

BASE_NETLOC = urlparse(BASE).netloc
PROMO_SELECTORS = (
    ".sign-badges .badge",
    ".offer-span",
    ".product-discount-tag",
    ".product-note-tag",
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
)
_spaces_re = re.compile(r"\s+")
_non_price_chars_re = re.compile(r"[^0-9,.\-]")
_one_plus_one_re = re.compile(r"\b1\s*\+\s*1\b")
_two_plus_one_re = re.compile(r"\b2\s*\+\s*1\b")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_page_param_re = re.compile(r"[?&](?:pg|page)=(\d+)", re.IGNORECASE)
_price_before_currency_re = re.compile(r"([0-9][0-9\.,]*)\s*(?:€|EUR)", re.IGNORECASE)
_max_price_mismatch_ratio = 1.8
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


def normalize_spaces(text: str) -> str:
    return _spaces_re.sub(" ", (text or "").replace("\xa0", " ")).strip()


@lru_cache(maxsize=512)
def normalize_text_no_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", normalize_spaces(text).lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


@lru_cache(maxsize=512)
def detect_unit_of_measure(label: str) -> Optional[str]:
    low = normalize_text_no_accents(label)
    if any(token in low for token in ("κιλου", "κιλα", "κιλο", "/kg")):
        return "kilos"
    if any(token in low for token in ("λιτρου", "λιτρα", "λιτρο", "/lt", "/l")):
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
        return "piece"
    return None


def same_site(url: str) -> bool:
    return urlparse(url).netloc == BASE_NETLOC


def normalize(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def looks_like_product_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    if not path or path == "/":
        return False

    banned_prefixes = (
        "/sitemap",
        "/company",
        "/career",
        "/contact",
        "/login",
        "/register",
        "/terms",
        "/privacy",
        "/payment",
        "/search",
        "/cart",
        "/checkout",
    )
    return not any(path.startswith(prefix) for prefix in banned_prefixes)


def parse_price_number(text: str) -> Optional[float]:
    s = normalize_spaces(text)
    if not s:
        return None

    s = s.replace("EUR", "")
    s = s.replace("€", "")
    s = _non_price_chars_re.sub("", s)
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


def parse_first_price_before_currency(text: str) -> Optional[float]:
    for match in _price_before_currency_re.finditer(text or ""):
        value = parse_price_number(match.group(1))
        if value is not None:
            return value
    return None


def parse_analytics_price(analytics_item: Dict[str, Any]) -> Optional[float]:
    if analytics_item.get("price") is None:
        return None
    value = parse_price_number(str(analytics_item.get("price")))
    if value is None or value <= 0:
        return None
    return value


def reconcile_prices(
    final_price: Optional[float],
    final_unit_price: Optional[float],
    original_price: Optional[float],
    original_unit_price: Optional[float],
    analytics_price: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if final_price is None and analytics_price is not None:
        final_price = analytics_price

    # Trust analytics only for obvious parsing outliers.
    if (
        analytics_price is not None
        and final_price is not None
        and original_price is None
        and analytics_price > 0
        and final_price > 0
    ):
        higher = max(analytics_price, final_price)
        lower = min(analytics_price, final_price)
        if higher / lower >= _max_price_mismatch_ratio and (
            final_unit_price is None or abs(final_unit_price - final_price) <= 1e-9
        ):
            final_price = analytics_price
            final_unit_price = analytics_price

    if (
        final_price is not None
        and original_price is not None
        and original_price <= final_price + 1e-9
    ):
        original_price = None
    if (
        final_unit_price is not None
        and original_unit_price is not None
        and original_unit_price <= final_unit_price + 1e-9
    ):
        original_unit_price = None

    if (
        final_unit_price is None
        and original_unit_price is None
        and final_price is not None
    ):
        final_unit_price = final_price

    return final_price, final_unit_price, original_price, original_unit_price


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
    return f"{BASE}/{to_category_slug(category)}/"


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

    return out


def extract_pagination_state(
    tree: HTMLParser,
    requested_page: int,
) -> Tuple[Optional[int], Optional[int], bool, Optional[int], Optional[int]]:
    page_numbers: Set[int] = set()
    next_page: Optional[int] = None
    has_next = (
        tree.css_first("link[rel='next']") is not None
        or tree.css_first("a[rel='next']") is not None
    )

    next_node = tree.css_first("section.pagination.go-next")
    if next_node:
        raw = (next_node.attributes.get("data-pg") or "").strip()
        if raw.isdigit():
            next_page = int(raw)
            page_numbers.add(next_page)
            has_next = True

    for a in tree.css("section.pagination a[href], a[rel='next'][href]"):
        href = (a.attributes.get("href") or "").strip()
        if href:
            m = _page_param_re.search(href)
            if m:
                try:
                    page_numbers.add(int(m.group(1)))
                except ValueError:
                    pass

        rel = (a.attributes.get("rel") or "").strip().lower()
        cls = (a.attributes.get("class") or "").strip().lower()
        if rel == "next" or "next" in cls:
            has_next = True

    if next_page is None and page_numbers:
        candidate = min((p for p in page_numbers if p > requested_page), default=None)
        if candidate is not None:
            next_page = candidate
            has_next = True

    max_page = max(page_numbers) if page_numbers else None

    current_count = None
    total_count = None
    count_node = tree.css_first("section.pagination .current-page")
    if count_node:
        txt = normalize_spaces(count_node.text(separator=" ", strip=True))
        nums = [int(n) for n in re.findall(r"\d+", txt)]
        if len(nums) >= 2:
            current_count = nums[-2]
            total_count = nums[-1]

    return next_page, max_page, has_next, current_count, total_count


def parse_sku(analytics_item: Dict[str, Any], product_meta: Dict[str, Any]) -> Optional[str]:
    sku = product_meta.get("sku")
    if sku:
        return str(sku).strip()

    item_id = analytics_item.get("item_id")
    if item_id is not None:
        item_id_str = str(item_id).strip()
        if item_id_str:
            return item_id_str
    return None


def parse_promo(article) -> Tuple[Optional[int], bool, bool, Optional[str]]:
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

    for selector in PROMO_SELECTORS:
        for node in article.css(selector):
            add_candidate(node.text(separator=" ", strip=True))
            add_candidate(node.attributes.get("title"))
            add_candidate(node.attributes.get("aria-label"))
            for img in node.css("img"):
                add_candidate(img.attributes.get("alt"))
                add_candidate(img.attributes.get("title"))

    one_plus_one = any(_one_plus_one_re.search(txt) for txt in candidates)
    two_plus_one = any(_two_plus_one_re.search(txt) for txt in candidates)

    discount_percent = None
    for txt in candidates:
        m = _discount_re.search(txt)
        if m:
            try:
                discount_percent = abs(int(m.group(1).replace(" ", "")))
                break
            except ValueError:
                pass

    promo_text = candidates[0] if candidates else None
    if discount_percent is not None or one_plus_one or two_plus_one:
        promo_text = None

    return discount_percent, one_plus_one, two_plus_one, promo_text


def parse_product_url(article) -> Optional[str]:
    abs_link = article.css_first("a.absLink[href]")
    if abs_link:
        href = (abs_link.attributes.get("href") or "").strip()
        if href:
            url = normalize(urljoin(BASE, href))
            if same_site(url) and looks_like_product_url(url):
                return url

    for a in article.css("a[href]"):
        href = (a.attributes.get("href") or "").strip()
        if not href:
            continue
        url = normalize(urljoin(BASE, href))
        if same_site(url) and looks_like_product_url(url):
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
    node = article.css_first("figure.product__figure img")
    if not node:
        node = article.css_first("img[src]") or article.css_first("img[data-src]")
    if not node:
        return None

    src = (
        (node.attributes.get("src") or "").strip()
        or (node.attributes.get("data-src") or "").strip()
    )
    if not src:
        return None
    return normalize(urljoin(BASE, src))


def parse_unit_prices(article, main_price_block=None) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    def parse_unit_block(block) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        if block is None:
            return None, None, None

        unit_label = None
        for span in block.css("span"):
            txt = normalize_spaces(span.text(separator=" ", strip=True))
            if "/" in txt:
                unit_label = txt
                break

        original_unit_price = None
        for node in block.css(".deleted__price"):
            original_unit_price = parse_price_number(node.text(separator=" ", strip=True))
            if original_unit_price is not None:
                break

        final_unit_price = None
        for node in block.css(".price"):
            final_unit_price = parse_price_number(node.text(separator=" ", strip=True))
            if final_unit_price is not None:
                break

        if final_unit_price is None:
            highlight = block.css_first(".hightlight")
            if highlight:
                final_unit_price = parse_price_number(highlight.text(separator=" ", strip=True))

        if final_unit_price is None:
            final_unit_price = parse_first_price_before_currency(block.text(separator=" ", strip=True))

        return final_unit_price, original_unit_price, unit_label

    final_unit_price, original_unit_price, unit_label = parse_unit_block(
        article.css_first(".priceWrp .priceKil")
    )

    unit_of_measure = detect_unit_of_measure(unit_label or "")

    # Some cards (especially kilo-only/liter-only) keep unit pricing in main-price.
    if final_unit_price is None and unit_of_measure is None:
        main_final, main_original, main_unit_label = parse_unit_block(
            main_price_block if main_price_block is not None else article.css_first(".priceWrp .main-price")
        )
        main_uom = detect_unit_of_measure(main_unit_label or "")
        if main_uom is not None:
            final_unit_price = main_final
            original_unit_price = main_original
            unit_of_measure = main_uom

    if final_unit_price is None:
        unit_of_measure = None

    if (
        original_unit_price is not None
        and final_unit_price is not None
        and original_unit_price <= final_unit_price + 1e-9
    ):
        original_unit_price = None

    return final_unit_price, original_unit_price, unit_of_measure


def parse_main_prices(main_price_block, analytics_price: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    block = main_price_block
    if not block:
        return analytics_price, None

    original_price = None
    for node in block.css(".deleted__price"):
        original_price = parse_price_number(node.text(separator=" ", strip=True))
        if original_price is not None:
            break

    final_price = None
    values: List[float] = []
    for node in block.css(".price"):
        value = parse_price_number(node.text(separator=" ", strip=True))
        if value is None:
            continue
        values.append(value)
        if final_price is None:
            final_price = value

    if final_price is None:
        final_price = parse_first_price_before_currency(block.text(separator=" ", strip=True))

    if final_price is None:
        final_price = analytics_price

    if original_price is None:
        unique_values = sorted(set(values))
        if len(unique_values) >= 2:
            final_price = final_price if final_price is not None else unique_values[0]
            original_price = unique_values[-1]

    if (
        original_price is not None
        and final_price is not None
        and original_price <= final_price + 1e-9
    ):
        original_price = None

    return final_price, original_price


def parse_listing_article(
    article,
    root_category: str,
) -> Optional[ListingProductRow]:
    analytics_item = parse_analytics_item(article)
    product_meta = parse_product_meta(article)

    analytics_price = parse_analytics_price(analytics_item)

    name = parse_name(article, analytics_item=analytics_item)
    url = parse_product_url(article)
    sku = parse_sku(analytics_item=analytics_item, product_meta=product_meta)
    discount_percent, one_plus_one, two_plus_one, promo_text = parse_promo(article)

    main_price_block = article.css_first(".priceWrp .main-price")
    final_unit_price, original_unit_price, unit_of_measure = parse_unit_prices(
        article,
        main_price_block=main_price_block,
    )
    final_price, original_price = parse_main_prices(
        main_price_block,
        analytics_price=analytics_price,
    )

    final_price, final_unit_price, original_price, original_unit_price = reconcile_prices(
        final_price=final_price,
        final_unit_price=final_unit_price,
        original_price=original_price,
        original_unit_price=original_unit_price,
        analytics_price=analytics_price,
    )

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

    offer = one_plus_one or two_plus_one or discount_percent is not None or has_price_discount
    unit_of_measure = unit_of_measure or "piece"

    brand = analytics_item.get("item_brand")
    if brand is not None:
        brand = normalize_spaces(html.unescape(str(brand))) or None

    if not url and not name and not sku:
        return None

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


def build_page_url(root_listing: str, page: int) -> str:
    if page <= 1:
        return root_listing
    sep = "&" if "?" in root_listing else "?"
    return f"{root_listing}{sep}pg={page}"


def crawl_category_listing(
    root_listing: str,
    root_category: str,
    max_pages: int = 500,
) -> List[ListingProductRow]:
    root_listing = normalize(root_listing.rstrip("/") + "/")
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()

    page = 1
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        while page <= max_pages:
            url = build_page_url(root_listing, page)
            response = fetch_listing_page(client=client, url=url, page=page)

            if response.status_code == 404:
                print(f"page={page} -> 404, stopping pagination.")
                break

            response.raise_for_status()
            html_text = response.text
            t = HTMLParser(html_text)
            next_page, max_page, has_next, current_count, total_count = extract_pagination_state(
                tree=t,
                requested_page=page,
            )

            articles = t.css("section.productList div[data-plugin-product]")
            if not articles:
                articles = t.css("div[data-plugin-product]")
            if not articles:
                articles = t.css("div[data-plugin-analyticsimpressions]")
                if articles:
                    print(
                        f"page={page} -> using fallback card selector (analytics impressions)."
                    )
            if not articles:
                print(f"page={page} -> 0 products, stopping.")
                break

            added = 0
            for article in articles:
                row = parse_listing_article(article, root_category=root_category)
                if not row:
                    continue

                # URL is the best dedupe key; fallback to sku/name identity.
                key = row.url or f"{row.sku or ''}|{row.name or ''}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rows.append(row)
                added += 1

            count_part = ""
            if current_count is not None and total_count is not None:
                count_part = f" current={current_count}/{total_count}"
            print(f"page={page} +{added} total={len(rows)} cards={len(articles)}{count_part}")

            if added == 0:
                print(f"page={page} -> 0 NEW unique products, continuing.")

            if max_page is not None and page >= max_page:
                print(f"page={page} -> reached max_page={max_page}, stopping.")
                break

            if next_page is None:
                if not has_next:
                    print(f"page={page} -> no next page marker, stopping.")
                    break
                next_page = page + 1

            if next_page <= page:
                print(f"page={page} -> invalid next page ({next_page}), stopping.")
                break

            page = next_page
            time.sleep(PAGE_SLEEP_SECONDS)

    return rows


def save_to_csv(rows: List[ListingProductRow], filename: str) -> None:
    fieldnames = [field.name for field in fields(ListingProductRow)]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(serialize_row_for_csv(row) for row in rows)

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
