import csv
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://www.ab.gr"
ROOT_CATEGORIES = [
    # "el/eshop/Oporopoleio/c/001",
    # "el/eshop/Fresko-Kreas-and-Psaria/c/002",
    # "el/eshop/Galaktokomika-Fytika-Rofimata-and-Eidi-Psygeioy/c/003",
    # "el/eshop/Tyria-Fytika-Anapliromata-and-Allantika/c/004",
    # "el/eshop/Katepsygmena-trofima/c/005",
    # "el/eshop/Artos-Zacharoplasteio/c/006",
    # "el/eshop/Etoima-Geymata/c/007",
    # "el/eshop/Kava-anapsyktika-nera-xiroi-karpoi/c/008",
    "el/eshop/Proino-snacking-and-rofimata/c/009",
    # "el/eshop/Vasika-typopoiimena-trofima/c/010",
    # "el/eshop/Ola-gia-to-moro/c/011",
    # "el/eshop/Eidi-prosopikis-peripoiisis/c/012",
    # "el/eshop/Katharistika-Chartika-and-eidi-spitioy/c/013",
    # "el/eshop/Gia-katoikidia/c/014",
]
MAX_PAGES_PER_CATEGORY = 500
PAGE_SLEEP_SECONDS = 0.05
SORT_PRODUCTS_FOR_CSV = True
REQUEST_RETRY_ATTEMPTS = 3
REQUEST_RETRY_BACKOFF_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
}

GRAPHQL_ENDPOINT = f"{BASE}/api/v1/"
GRAPHQL_OPERATION_NAME = "GetCategoryProductSearch"
GRAPHQL_LANG = "gr"
GRAPHQL_FIELDS = "PRODUCT_TILE"
GRAPHQL_PAGE_SIZE = 20

_spaces_re = re.compile(r"\s+")
_price_cleanup_re = re.compile(r"[^0-9,.\-]")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_one_plus_one_re = re.compile(r"\b1\s*\+\s*1\b")
_two_plus_one_re = re.compile(r"\b2\s*\+\s*1\b")
_page_param_re = re.compile(
    r"[?&](?:page|pg|p|currentPage|currentpage)=(\d+)",
    re.IGNORECASE,
)
_euros_cents_re = re.compile(r"(\d+)\s+ευρ(?:ώ|ω)\s+και\s+(\d+)\s+λεπτ(?:ά|α)", re.IGNORECASE)
_split_price_re = re.compile(r"(?:~?\s*€\s*)?(\d+)\s+(\d{1,2})(?:\s|$)")
_category_code_re = re.compile(r"/c/([^/?#]+)", re.IGNORECASE)


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
    two_plus_one: bool = False
    promo_text: Optional[str] = None

    image_url: Optional[str] = None

    root_category: Optional[str] = None


def normalize_spaces(text: str) -> str:
    return _spaces_re.sub(" ", (text or "").replace("\xa0", " ")).strip()


def normalize_text_no_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", normalize_spaces(text).lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def detect_unit_of_measure(label: str) -> Optional[str]:
    low = normalize_text_no_accents(label)
    if any(token in low for token in ("/κιλ", "κιλο", "κιλου", "κιλα", "/kg")):
        return "kilos"
    if any(token in low for token in ("/λιτ", "λιτρο", "λιτρου", "λιτρα", "/lt", "/l")):
        return "liters"
    return None


def same_site(url: str) -> bool:
    return urlparse(url).netloc == urlparse(BASE).netloc


def normalize(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def parse_price_number(text: str) -> Optional[float]:
    s = normalize_spaces(text)
    if not s:
        return None

    s = s.replace("EUR", "")
    s = s.replace("€", "")
    s = _price_cleanup_re.sub("", s)
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


def parse_price_node(node) -> Optional[float]:
    if node is None:
        return None

    aria_label = normalize_spaces(node.attributes.get("aria-label") or "")
    if aria_label:
        for candidate in (aria_label, normalize_text_no_accents(aria_label)):
            m = _euros_cents_re.search(candidate)
            if m:
                try:
                    return float(f"{int(m.group(1))}.{int(m.group(2)):02d}")
                except ValueError:
                    pass

    whole_node = node.css_first("[class*='dqia0p-8'], [class*='dqia0p-4']")
    cents_node = node.css_first("sup")
    if whole_node and cents_node:
        whole = re.sub(r"\D+", "", whole_node.text(separator="", strip=True) or "")
        cents = re.sub(r"\D+", "", cents_node.text(separator="", strip=True) or "")
        if whole and cents:
            try:
                cents_value = int(cents)
                if len(cents) == 1:
                    cents_value *= 10
                return float(f"{int(whole)}.{cents_value:02d}")
            except ValueError:
                pass

    split_text = normalize_spaces(node.text(separator=" ", strip=True))
    m = _split_price_re.search(split_text)
    if m:
        try:
            cents_value = int(m.group(2))
            if len(m.group(2)) == 1:
                cents_value *= 10
            return float(f"{int(m.group(1))}.{cents_value:02d}")
        except ValueError:
            pass

    return parse_price_number(node.text(separator=" ", strip=True))


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


def to_root_category(category: str) -> str:
    slug = to_category_slug(category)
    parts = [p for p in slug.split("/") if p]
    lowered = [p.lower() for p in parts]

    if "eshop" in lowered:
        idx = lowered.index("eshop")
        if idx + 1 < len(parts):
            return normalize_text_no_accents(parts[idx + 1])

    if "c" in lowered:
        idx = lowered.index("c")
        if idx > 0:
            return normalize_text_no_accents(parts[idx - 1])

    if parts:
        return normalize_text_no_accents(parts[0])

    raise ValueError(f"Unable to derive root_category from '{category}'")


def to_category_code(category: str) -> Optional[str]:
    slug = to_category_slug(category)
    m = _category_code_re.search(f"/{slug}")
    if m:
        code = normalize_spaces(m.group(1))
        return code or None

    parts = [p for p in slug.split("/") if p]
    if parts and parts[-1]:
        return normalize_spaces(parts[-1]) or None

    return None


def csv_filename_for_root_category(root_category: str) -> str:
    safe_root = re.sub(r"[^a-zA-Z0-9_-]+", "_", normalize_spaces(root_category)).strip("_")
    if not safe_root:
        safe_root = "category"
    return f"{safe_root}-listing-products.csv"


CATEGORY_SEARCH_QUERY = (
    "query GetCategoryProductSearch("
    "$lang:String,$searchQuery:String,$pageSize:Int,$pageNumber:Int,$category:String,"
    "$sort:String,$filterFlag:Boolean,$customerSegment:String,$plainChildCategories:Boolean,"
    "$facetsOnly:Boolean,$fields:String){"
    "categoryProductSearch:categoryProductSearchV2("
    "lang:$lang searchQuery:$searchQuery pageSize:$pageSize pageNumber:$pageNumber "
    "category:$category sort:$sort filterFlag:$filterFlag customerSegment:$customerSegment "
    "plainChildCategories:$plainChildCategories facetsOnly:$facetsOnly fields:$fields){"
    "products{"
    "code name manufacturerName manufacturerSubBrandName url "
    "images{url format imageType} "
    "price{approximatePriceSymbol currencySymbol formattedValue priceType supplementaryPriceLabel1 "
    "supplementaryPriceLabel2 showStrikethroughPrice discountedPriceFormatted "
    "discountedUnitPriceFormatted unit unitPriceFormatted unitCode unitPrice value wasPrice} "
    "potentialPromotions{promotionTypeCode promotionType offerType title description "
    "simplePromotionMessage percentageDiscount points toDisplay} "
    "potentialActivatablePromotions{promotionTypeCode promotionType offerType title description "
    "simplePromotionMessage percentageDiscount points toDisplay}"
    "} pagination{currentPage totalResults totalPages sort}"
    "}}"
)


def parse_sku(article) -> Optional[str]:
    node = article.css_first("[data-testid='product-id']")
    if not node:
        return None
    sku = normalize_spaces(node.text(separator=" ", strip=True))
    return sku or None


def parse_product_url(article) -> Optional[str]:
    for selector in (
        "a[data-testid='product-block-name-link'][href]",
        "a[data-testid='product-block-image-link'][href]",
        "a[href*='/p/'][href]",
    ):
        a = article.css_first(selector)
        if not a:
            continue
        href = (a.attributes.get("href") or "").strip()
        if not href:
            continue
        url = normalize(urljoin(BASE, href))
        if same_site(url):
            return url
    return None


def parse_name(article) -> Optional[str]:
    node = article.css_first("[data-testid='product-name']")
    if node:
        value = normalize_spaces(node.text(separator=" ", strip=True))
        if value:
            return value

    node = article.css_first("a[data-testid='product-block-name-link'][title]")
    if node:
        value = normalize_spaces(node.attributes.get("title") or "")
        if value:
            return value

    return None


def parse_brand(article) -> Optional[str]:
    node = article.css_first("[data-testid='product-brand']")
    if not node:
        return None
    value = normalize_spaces(node.text(separator=" ", strip=True))
    if not value or value == "-":
        return None
    return value


def parse_image_url(article) -> Optional[str]:
    img = article.css_first("img[data-testid='product-block-image']")
    if not img:
        return None

    src = (img.attributes.get("src") or "").strip()
    if not src:
        return None
    return normalize(urljoin(BASE, src))


def parse_discount_percent(article) -> Optional[int]:
    label = article.css_first("[data-testid='tag-promo-label']")
    if not label:
        return None
    txt = normalize_spaces(label.text(separator=" ", strip=True))
    if not txt:
        return None
    m = _discount_re.search(txt)
    if not m:
        return None
    try:
        return abs(int(m.group(1).replace(" ", "")))
    except ValueError:
        return None


def parse_unit_prices(article) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    unit_node = article.css_first("[data-testid='product-block-price-per-unit']")
    old_unit_node = article.css_first("[data-testid='product-block-old-ppu']")

    final_unit = parse_price_node(unit_node)
    original_unit = parse_price_number(
        old_unit_node.text(separator=" ", strip=True) if old_unit_node else ""
    )

    unit_label = ""
    if unit_node:
        unit_label = normalize_spaces(unit_node.text(separator=" ", strip=True))
        if not unit_label:
            unit_label = normalize_spaces(unit_node.attributes.get("aria-label") or "")
    unit_of_measure = detect_unit_of_measure(unit_label)

    if (
        final_unit is not None
        and original_unit is not None
        and original_unit <= final_unit + 1e-9
    ):
        original_unit = None

    return final_unit, original_unit, unit_of_measure


def parse_main_prices(article) -> Tuple[Optional[float], Optional[float]]:
    final_price = parse_price_node(article.css_first("[data-testid='product-block-price']"))
    original_price = parse_price_number(
        article.css_first("[data-testid='product-block-old-price']").text(separator=" ", strip=True)
        if article.css_first("[data-testid='product-block-old-price']")
        else ""
    )

    if (
        final_price is not None
        and original_price is not None
        and original_price <= final_price + 1e-9
    ):
        original_price = None

    return final_price, original_price


def parse_one_plus_one(article) -> bool:
    promo_node = article.css_first("[data-testid='tag-promo-label']")
    if not promo_node:
        return False
    txt = normalize_spaces(promo_node.text(separator=" ", strip=True))
    return bool(_one_plus_one_re.search(txt))


def parse_two_plus_one(article) -> bool:
    promo_node = article.css_first("[data-testid='tag-promo-label']")
    if not promo_node:
        return False
    txt = normalize_spaces(promo_node.text(separator=" ", strip=True))
    return bool(_two_plus_one_re.search(txt))


def parse_promo_text(article) -> Optional[str]:
    promo_node = article.css_first("[data-testid='tag-promo-label']")
    if not promo_node:
        return None
    txt = normalize_spaces(promo_node.text(separator=" ", strip=True))
    return txt or None


def parse_listing_article(article, root_category: str) -> Optional[ListingProductRow]:
    name = parse_name(article)
    url = parse_product_url(article)
    sku = parse_sku(article)
    brand = parse_brand(article)

    final_price, original_price = parse_main_prices(article)
    final_unit_price, original_unit_price, unit_of_measure = parse_unit_prices(article)

    if final_unit_price is None and original_unit_price is None and final_price is not None:
        final_unit_price = final_price

    discount_percent = parse_discount_percent(article)
    one_plus_one = parse_one_plus_one(article)
    two_plus_one = parse_two_plus_one(article)
    promo_text = parse_promo_text(article)

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
    if discount_percent is not None or one_plus_one or two_plus_one:
        promo_text = None

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
        final_set_price=None,
        original_set_price=None,
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


def detect_unit_of_measure_from_code(unit_code: Optional[str], label: str = "") -> Optional[str]:
    code = normalize_spaces((unit_code or "").lower())
    if code in {"kilogram", "kg"}:
        return "kilos"
    if code in {"liter", "litre", "l"}:
        return "liters"
    return detect_unit_of_measure(label)


def parse_api_image_url(images: Any) -> Optional[str]:
    if not isinstance(images, list) or not images:
        return None

    format_rank = {"respListGrid": 0, "small": 1, "xlarge": 2, "zoom": 3}
    best_url: Optional[str] = None
    best_rank = 10_000

    for idx, image in enumerate(images):
        if not isinstance(image, dict):
            continue

        raw = normalize_spaces(image.get("url") or "")
        if not raw:
            continue

        fmt = normalize_spaces(image.get("format") or "")
        rank = format_rank.get(fmt, 100) * 1000 + idx
        if rank < best_rank:
            best_rank = rank
            best_url = raw

    if not best_url:
        return None

    return normalize(urljoin(BASE, best_url))


def parse_promotions_info(
    product: Dict[str, Any]
) -> Tuple[Optional[int], bool, bool, bool, Optional[str]]:
    promotions: List[Dict[str, Any]] = []
    for key in ("potentialPromotions", "potentialActivatablePromotions"):
        values = product.get(key)
        if isinstance(values, list):
            promotions.extend(p for p in values if isinstance(p, dict))

    discount_percent: Optional[int] = None
    one_plus_one = False
    two_plus_one = False
    promo_text: Optional[str] = None

    for promo in promotions:
        pct = promo.get("percentageDiscount")
        if isinstance(pct, (int, float)) and pct > 0 and discount_percent is None:
            discount_percent = int(round(float(pct)))

        texts = [
            normalize_spaces(str(promo.get("title") or "")),
            normalize_spaces(str(promo.get("description") or "")),
            normalize_spaces(str(promo.get("simplePromotionMessage") or "")),
        ]
        if promo_text is None:
            for txt in texts:
                if txt:
                    promo_text = txt
                    break
        if any(_one_plus_one_re.search(txt) for txt in texts if txt):
            one_plus_one = True
        if any(_two_plus_one_re.search(txt) for txt in texts if txt):
            two_plus_one = True

    return discount_percent, one_plus_one, two_plus_one, bool(promotions), promo_text


def parse_api_listing_product(
    product: Dict[str, Any],
    root_category: str,
) -> Optional[ListingProductRow]:
    if not isinstance(product, dict):
        return None

    sku = normalize_spaces(str(product.get("code") or ""))
    name = normalize_spaces(str(product.get("name") or ""))

    raw_url = normalize_spaces(str(product.get("url") or ""))
    product_url = normalize(urljoin(BASE, raw_url)) if raw_url else None
    if product_url and not same_site(product_url):
        product_url = None

    raw_brand = product.get("manufacturerName") or product.get("manufacturerSubBrandName") or ""
    brand = normalize_spaces(str(raw_brand))
    if not brand or brand == "-":
        brand = None

    price = product.get("price") if isinstance(product.get("price"), dict) else {}
    show_strikethrough = bool(price.get("showStrikethroughPrice"))

    original_price = parse_price_number(str(price.get("formattedValue") or ""))
    discounted_price = parse_price_number(str(price.get("discountedPriceFormatted") or ""))
    final_price = discounted_price if show_strikethrough and discounted_price is not None else original_price
    if final_price is not None and original_price is not None and original_price <= final_price + 1e-9:
        original_price = None

    supplementary_unit_price = parse_price_number(str(price.get("supplementaryPriceLabel1") or ""))
    discounted_unit_price = parse_price_number(str(price.get("discountedUnitPriceFormatted") or ""))
    unit_price_formatted = parse_price_number(str(price.get("unitPriceFormatted") or ""))

    if show_strikethrough:
        final_unit_price = discounted_unit_price
        original_unit_price = supplementary_unit_price
    else:
        final_unit_price = supplementary_unit_price
        original_unit_price = None

    if final_unit_price is None:
        final_unit_price = unit_price_formatted

    if show_strikethrough and original_unit_price is None:
        original_unit_price = unit_price_formatted

    if final_unit_price is None and isinstance(price.get("unitPrice"), (int, float)):
        final_unit_price = float(price["unitPrice"])

    if show_strikethrough and original_unit_price is None and isinstance(price.get("unitPrice"), (int, float)):
        original_unit_price = float(price["unitPrice"])

    if (
        final_unit_price is not None
        and original_unit_price is not None
        and original_unit_price <= final_unit_price + 1e-9
    ):
        original_unit_price = None

    if final_unit_price is None and final_price is not None:
        final_unit_price = final_price

    unit_label = " ".join(
        [
            normalize_spaces(str(price.get("supplementaryPriceLabel1") or "")),
            normalize_spaces(str(price.get("supplementaryPriceLabel2") or "")),
        ]
    ).strip()
    unit_of_measure = detect_unit_of_measure_from_code(price.get("unitCode"), unit_label)

    (
        discount_percent,
        one_plus_one,
        two_plus_one,
        has_promotions,
        promo_text,
    ) = parse_promotions_info(product)
    if discount_percent is None and final_price and original_price and original_price > final_price:
        discount_percent = int(round(((original_price - final_price) / original_price) * 100))
    if (
        discount_percent is None
        and final_unit_price
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
    offer = (
        one_plus_one
        or two_plus_one
        or discount_percent is not None
        or has_price_discount
        or has_promotions
    )
    if discount_percent is not None or one_plus_one or two_plus_one:
        promo_text = None

    row = ListingProductRow(
        url=product_url,
        name=name or None,
        sku=sku or None,
        brand=brand,
        final_price=final_price,
        final_unit_price=final_unit_price,
        original_price=original_price,
        original_unit_price=original_unit_price,
        unit_of_measure=unit_of_measure,
        final_set_price=None,
        original_set_price=None,
        discount_percent=discount_percent,
        offer=offer,
        one_plus_one=one_plus_one,
        two_plus_one=two_plus_one,
        promo_text=promo_text,
        image_url=parse_api_image_url(product.get("images")),
        root_category=root_category,
    )

    if not row.url and not row.name and not row.sku:
        return None
    return row


def fetch_category_search_page_api(
    client: httpx.Client,
    category_code: str,
    page: int,
) -> Dict[str, Any]:
    if page < 1:
        raise ValueError("page must be >= 1")

    payload = {
        "operationName": GRAPHQL_OPERATION_NAME,
        "query": CATEGORY_SEARCH_QUERY,
        "variables": {
            "lang": GRAPHQL_LANG,
            "searchQuery": None,
            "pageSize": GRAPHQL_PAGE_SIZE,
            "pageNumber": page - 1,
            "category": category_code,
            "sort": None,
            "filterFlag": False,
            "customerSegment": None,
            "plainChildCategories": False,
            "facetsOnly": False,
            "fields": GRAPHQL_FIELDS,
        },
    }

    last_error: Optional[Exception] = None
    for attempt in range(1, REQUEST_RETRY_ATTEMPTS + 1):
        try:
            response = client.post(
                GRAPHQL_ENDPOINT,
                headers={"X-Apollo-Operation-Name": GRAPHQL_OPERATION_NAME},
                json=payload,
            )
        except httpx.RequestError as exc:
            last_error = exc
            if attempt >= REQUEST_RETRY_ATTEMPTS:
                raise

            wait_seconds = REQUEST_RETRY_BACKOFF_SECONDS * attempt
            print(
                f"page={page} -> API request error ({exc}), "
                f"retrying in {wait_seconds:.1f}s "
                f"({attempt}/{REQUEST_RETRY_ATTEMPTS})."
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < REQUEST_RETRY_ATTEMPTS:
            wait_seconds = REQUEST_RETRY_BACKOFF_SECONDS * attempt
            print(
                f"page={page} -> API status={response.status_code}, "
                f"retrying in {wait_seconds:.1f}s "
                f"({attempt}/{REQUEST_RETRY_ATTEMPTS})."
            )
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        data = response.json()
        errors = data.get("errors")
        if errors:
            raise RuntimeError(f"page={page} -> GraphQL errors: {errors}")

        result = data.get("data", {}).get("categoryProductSearch")
        if isinstance(result, dict):
            return result
        return {}

    if last_error is not None:
        raise RuntimeError(f"page={page} -> exhausted API retries") from last_error
    raise RuntimeError(f"page={page} -> exhausted API retries")


def extract_pagination_state(
    tree: HTMLParser,
    requested_page: int,
) -> Tuple[Optional[int], Optional[int], bool]:
    page_numbers: Set[int] = set()
    next_page: Optional[int] = None

    has_next = tree.css_first("link[rel='next']") is not None

    for node in tree.css("a[href], link[href], button[data-page], [data-page-number]"):
        href = (node.attributes.get("href") or "").strip()
        if href:
            m = _page_param_re.search(href)
            if m:
                try:
                    page_numbers.add(int(m.group(1)))
                except ValueError:
                    pass

            parsed = urlparse(href)
            for key in ("page", "pg", "p", "currentPage", "currentpage"):
                values = parse_qs(parsed.query).get(key)
                if not values:
                    continue
                try:
                    page_numbers.add(int(values[0]))
                except ValueError:
                    pass

        data_page = (node.attributes.get("data-page") or "").strip()
        if data_page.isdigit():
            page_numbers.add(int(data_page))

        data_page_number = (node.attributes.get("data-page-number") or "").strip()
        if data_page_number.isdigit():
            page_numbers.add(int(data_page_number))

        aria_label = normalize_text_no_accents(node.attributes.get("aria-label") or "")
        rel = normalize_spaces(node.attributes.get("rel") or "").lower()
        cls = normalize_spaces(node.attributes.get("class") or "").lower()
        disabled = (
            node.attributes.get("disabled") is not None
            or node.attributes.get("aria-disabled") == "true"
            or "disabled" in cls
        )
        if not disabled and ("next" in rel or "next" in cls or "επομεν" in aria_label):
            has_next = True

    if next_page is None and page_numbers:
        higher = [p for p in page_numbers if p > requested_page]
        if higher:
            next_page = min(higher)
            has_next = True

    max_page = max(page_numbers) if page_numbers else None
    return next_page, max_page, has_next


def build_page_url(root_listing: str, page: int) -> str:
    if page <= 1:
        return root_listing
    sep = "&" if "?" in root_listing else "?"
    return f"{root_listing}{sep}page={page}"


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


def crawl_category_listing_from_html(
    client: httpx.Client,
    root_listing: str,
    root_category: str,
    max_pages: int = 500,
) -> List[ListingProductRow]:
    root_listing = normalize(root_listing.rstrip("/"))
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()

    page = 1
    while page <= max_pages:
        url = build_page_url(root_listing, page)
        response = fetch_listing_page(client=client, url=url, page=page)

        if response.status_code == 404:
            print(f"page={page} -> 404, stopping pagination.")
            break

        response.raise_for_status()
        t = HTMLParser(response.text)
        next_page, max_page, has_next = extract_pagination_state(
            tree=t,
            requested_page=page,
        )

        articles = t.css("li.product-item [data-testid='product-block']")
        if not articles:
            articles = t.css("[data-testid='product-block']")
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
            print(f"page={page} -> 0 NEW unique products, stopping.")
            break

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


def crawl_category_listing(
    root_listing: str,
    root_category: str,
    max_pages: int = 500,
) -> List[ListingProductRow]:
    root_listing = normalize(root_listing.rstrip("/"))
    category_code = to_category_code(root_listing)

    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()

    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        if category_code:
            page = 1
            while page <= max_pages:
                try:
                    result = fetch_category_search_page_api(
                        client=client,
                        category_code=category_code,
                        page=page,
                    )
                except Exception as exc:
                    print(f"page={page} -> API failed ({exc}), switching to HTML fallback.")
                    break
                products = result.get("products")
                if not isinstance(products, list) or not products:
                    print(f"page={page} -> API returned 0 products.")
                    break

                added = 0
                for product in products:
                    row = parse_api_listing_product(
                        product=product,
                        root_category=root_category,
                    )
                    if not row:
                        continue

                    key = row.url or f"{row.sku or ''}|{row.name or ''}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    rows.append(row)
                    added += 1

                pagination = result.get("pagination") if isinstance(result, dict) else {}
                total_pages: Optional[int] = None
                if isinstance(pagination, dict):
                    raw_total_pages = pagination.get("totalPages")
                    if isinstance(raw_total_pages, int):
                        total_pages = raw_total_pages

                print(
                    f"page={page} +{added} total={len(rows)} "
                    f"api_products={len(products)} total_pages={total_pages}"
                )

                if added == 0:
                    print(f"page={page} -> 0 NEW unique products, stopping API pagination.")
                    break

                if total_pages is not None and page >= total_pages:
                    print(f"page={page} -> reached API total_pages={total_pages}, stopping.")
                    break

                page += 1
                time.sleep(PAGE_SLEEP_SECONDS)

        if rows:
            return rows

        print("API yielded no rows. Falling back to HTML parsing.")
        return crawl_category_listing_from_html(
            client=client,
            root_listing=root_listing,
            root_category=root_category,
            max_pages=max_pages,
        )


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
            root_category = to_root_category(category)
        except ValueError as exc:
            print(exc)
            continue

        root_listing = to_category_url(root_slug)
        print(f"\n=== category={root_slug} ({root_listing}) root_category={root_category} ===")

        rows = crawl_category_listing(
            root_listing=root_listing,
            root_category=root_category,
            max_pages=MAX_PAGES_PER_CATEGORY,
        )
        print(f"parsed from listings under {root_slug}: {len(rows)}")

        if SORT_PRODUCTS_FOR_CSV:
            rows.sort(key=lambda row: ((row.url or "").lower(), row.sku or "", row.name or ""))

        save_to_csv(rows, csv_filename_for_root_category(root_category))
