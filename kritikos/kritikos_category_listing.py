import csv
import json
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

BASE = "https://kritikos-sm.gr"
ROOT_CATEGORIES = [
    # "categories/manabikh",
    # "categories/fresko-kreas",
    # "categories/allantika",
    # "categories/turokomika",
    # "categories/galaktokomika",
    # "categories/eidh-psugeiou",
    # "categories/katapsuxh",
    # "categories/pantopwleio",
    # "categories/kaba",
    # "categories/proswpikh-frontida",
    # "categories/brefika",
    # "categories/kathariothta",
    # "categories/oikiakh-xrhsh",
    "categories/pet-shop",
    # "categories/biologikaleitourgika",
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

_spaces_re = re.compile(r"\s+")
_non_price_chars_re = re.compile(r"[^0-9,.\-]")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_one_plus_one_re = re.compile(r"\b1\s*\+\s*1\b")
_two_plus_one_re = re.compile(r"\b2\s*\+\s*1\b")
_sku_from_url_re = re.compile(r"-([0-9]{3,})/?$")
_page_param_re = re.compile(r"[?&](?:page|pg|p)=(\d+)", re.IGNORECASE)
_unit_price_slash_re = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*€?\s*/\s*"
    r"(κιλ(?:ό|ου)?|λίτρ(?:ο|ου)?|λιτρ(?:ο|ου)?|kg|l|lt|τεμ(?:άχιο)?|τμχ|τεμάχιο)",
    re.IGNORECASE,
)
_unit_price_to_re = re.compile(
    r"€\s*(\d+(?:[.,]\d+)?)\s*το\s*"
    r"(κιλ(?:ό|ου)?|λίτρ(?:ο|ου)?|λιτρ(?:ο|ου)?|kg|l|lt|τεμ(?:άχιο)?|τμχ|τεμάχιο)",
    re.IGNORECASE,
)
_money_off_re = re.compile(r"-\s*\d+(?:[.,]\d+)?\s*€", re.IGNORECASE)
_brand_token_letters_re = re.compile(r"[^A-Za-zΑ-ΩΆ-ΏΪΫΈΉΊΌΎΏά-ώϊϋΐΰ]")
_identity_token_cleanup_re = re.compile(r"[^0-9a-zα-ω]+")
_image_sku_hint_re = re.compile(r"\d{3,8}")
_unit_desc_cleanup_re = re.compile(
    r"(?:\s*-\s*ανά\s+\d+[.,]?\d*\s*(?:γρ|gr|ml|lt|l)\.?)*$",
    re.IGNORECASE,
)
_pack_token_re = re.compile(
    r"\b\d+\s*(?:[*xX×])\s*\d+(?:[.,]\d+)?\s*"
    r"(?:kg|kgr|g|gr|γρ|ml|cl|l|lt)\b\.?"
    r"|\b\d+(?:[.,]\d+)?\s*"
    r"(?:kg|kgr|g|gr|γρ|ml|cl|l|lt)\b\.?",
    re.IGNORECASE,
)
_pack_token_parts_re = re.compile(
    r"^\s*(?:(?P<count>\d+)\s*(?:[*xX×])\s*)?"
    r"(?P<amount>\d+(?:[.,]\d+)?)\s*"
    r"(?P<unit>kg|kgr|g|gr|γρ|ml|cl|l|lt)\s*\.?\s*$",
    re.IGNORECASE,
)
_brand_connector_tokens = {"&", "+", "/"}


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
    final_set_price: Optional[float] = None
    original_set_price: Optional[float] = None

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

        self.hidden_price = (
            self.final_price * multiplier if self.final_price is not None else None
        )
        self.hidden_unit_price = (
            self.final_unit_price * multiplier if self.final_unit_price is not None else None
        )

    def __post_init__(self) -> None:
        self.refresh_hidden_prices()


def normalize_spaces(text: str) -> str:
    return _spaces_re.sub(" ", (text or "").replace("\xa0", " ")).strip()


def normalize_text_no_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", normalize_spaces(text).lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def detect_unit_of_measure(label: str) -> Optional[str]:
    low = normalize_text_no_accents(label)
    if any(token in low for token in ("κιλο", "κιλ", "kg", "kilogram")):
        return "kilos"
    if any(token in low for token in ("λιτρο", "λιτρ", "/l", "/lt", "liter", "litre")):
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
    return urlparse(url).netloc == urlparse(BASE).netloc


def normalize(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def looks_like_product_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    if not path or path == "/":
        return False
    return "/products/" in path or "/offers/" in path


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


def to_category_slug(category: str) -> str:
    parsed = urlparse(category)
    if parsed.scheme and parsed.netloc:
        slug = parsed.path.strip("/")
    else:
        slug = category.strip("/")

    if not slug:
        raise ValueError(f"Invalid category '{category}'")
    return slug


def to_root_category(category: str) -> str:
    slug = to_category_slug(category)
    parts = [p for p in slug.split("/") if p]
    lowered = [p.lower() for p in parts]

    if "categories" in lowered:
        idx = lowered.index("categories")
        if idx + 1 < len(parts):
            return normalize_spaces(parts[idx + 1].lower())
    if parts:
        return normalize_spaces(parts[-1].lower())
    raise ValueError(f"Unable to derive root_category from '{category}'")


def to_category_url(category: str) -> str:
    return f"{BASE}/{to_category_slug(category)}"


def csv_filename_for_root_category(root_category: str) -> str:
    safe_root = re.sub(r"[^a-zA-Z0-9_-]+", "_", normalize_spaces(root_category)).strip("_")
    if not safe_root:
        safe_root = "category"
    return f"{safe_root}-listing-products.csv"


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
    return f"{root_listing}{sep}page={page}"


def extract_pagination_state(tree: HTMLParser, requested_page: int) -> Tuple[Optional[int], Optional[int], bool]:
    page_numbers: Set[int] = set()
    next_page: Optional[int] = None
    has_next = tree.css_first("link[rel='next']") is not None

    for node in tree.css("a[href], button[data-page], [data-page-number], [data-page]"):
        href = (node.attributes.get("href") or "").strip()
        if href:
            m = _page_param_re.search(href)
            if m:
                try:
                    page_numbers.add(int(m.group(1)))
                except ValueError:
                    pass

            parsed = urlparse(href)
            for key in ("page", "pg", "p"):
                values = parse_qs(parsed.query).get(key)
                if not values:
                    continue
                try:
                    page_numbers.add(int(values[0]))
                except ValueError:
                    pass

        for key in ("data-page", "data-page-number"):
            raw = (node.attributes.get(key) or "").strip()
            if raw.isdigit():
                page_numbers.add(int(raw))

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

    if page_numbers:
        higher = [p for p in page_numbers if p > requested_page]
        if higher:
            next_page = min(higher)
            has_next = True

    max_page = max(page_numbers) if page_numbers else None
    return next_page, max_page, has_next


def parse_discount_percent(text: str) -> Optional[int]:
    m = _discount_re.search(normalize_spaces(text or ""))
    if not m:
        return None
    try:
        return abs(int(m.group(1).replace(" ", "")))
    except ValueError:
        return None


def detect_combo_offers(*texts: str) -> Tuple[bool, bool]:
    merged = " ".join(normalize_spaces(t or "") for t in texts if t)
    low = normalize_text_no_accents(unquote(merged))
    low = (
        low.replace("＋", "+")
        .replace("×", "x")
        .replace("χ", "x")
        .replace("Χ", "x")
        .replace("*", "x")
        .replace("%2b", "+")
        .replace("%2B", "+")
        .replace("plus", "+")
        .replace("συν", "+")
    )

    one_plus_one = bool(re.search(r"(?<!\d)1\s*(?:\+|x)\s*1(?!\d)", low))
    two_plus_one = bool(re.search(r"(?<!\d)2\s*(?:\+|x)\s*1(?!\d)", low))
    return one_plus_one, two_plus_one


def detect_combo_from_badge(badge_text: str) -> Tuple[bool, bool]:
    low = normalize_text_no_accents(unquote(normalize_spaces(badge_text)))
    if not low:
        return False, False

    nums = re.findall(r"\d+", low)
    one_plus_one = False
    two_plus_one = False

    if nums:
        if len(nums) >= 2:
            if nums[0] == "1" and nums[1] == "1":
                one_plus_one = True
            if nums[0] == "2" and nums[1] == "1":
                two_plus_one = True
        elif nums[0] == "11":
            one_plus_one = True
        elif nums[0] == "21":
            two_plus_one = True

    return one_plus_one, two_plus_one


def parse_unit_price(desc_text: str) -> Tuple[Optional[float], Optional[str]]:
    txt = normalize_spaces(desc_text)
    if not txt:
        return None, None
    txt = _unit_desc_cleanup_re.sub("", txt).strip()

    m = _unit_price_slash_re.search(txt)
    if not m:
        m = _unit_price_to_re.search(txt)
    if not m:
        return None, None

    unit_price = parse_price_number(m.group(1))
    unit_of_measure = detect_unit_of_measure(m.group(2))
    if unit_price is not None and unit_price <= 0:
        unit_price = None
    return unit_price, unit_of_measure


def parse_price_node(node) -> Optional[float]:
    if node is None:
        return None
    return parse_price_number(node.text(separator=" ", strip=True))


def looks_like_brand_token(token: str) -> bool:
    cleaned = _brand_token_letters_re.sub("", token or "")
    if len(cleaned) < 2:
        return False
    return cleaned.upper() == cleaned


def extract_pack_tokens(desc_text: str) -> List[str]:
    txt = normalize_spaces(desc_text)
    if not txt:
        return []

    matches = [normalize_spaces(m.group(0)) for m in _pack_token_re.finditer(txt)]
    out: List[str] = []
    seen: Set[str] = set()
    for token in matches:
        normalized_token = normalize_pack_token_display(token)
        if not normalized_token:
            continue
        key = canonical_pack_token_key(normalized_token)
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized_token)
    return out


def format_pack_amount(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def normalize_pack_token_display(token: str) -> Optional[str]:
    raw = normalize_spaces(token).rstrip(".,;:")
    if not raw:
        return None

    m = _pack_token_parts_re.match(raw)
    if not m:
        return None

    count_text = m.group("count")
    amount_text = (m.group("amount") or "").replace(",", ".")
    unit_text = normalize_text_no_accents(m.group("unit") or "").lower()
    unit_text = unit_text.replace("γρ", "gr")
    if unit_text == "kgr":
        unit_text = "kg"
    if unit_text == "lt":
        unit_text = "l"

    try:
        amount_value = float(amount_text)
    except ValueError:
        return None

    if unit_text == "cl":
        amount_value *= 10.0
        unit_out = "mL"
    elif unit_text == "ml":
        unit_out = "mL"
    elif unit_text == "l":
        unit_out = "L"
    elif unit_text in {"g", "gr"}:
        unit_out = "g"
    elif unit_text == "kg":
        unit_out = "kg"
    else:
        return None

    amount_out = format_pack_amount(amount_value)
    if count_text:
        try:
            count_value = int(count_text)
        except ValueError:
            return None
        return f"{count_value}*{amount_out}{unit_out}"
    return f"{amount_out}{unit_out}"


def canonical_pack_token_key(token: str) -> str:
    display = normalize_pack_token_display(token)
    if display:
        return display.lower()
    fallback = normalize_text_no_accents(token).lower()
    fallback = fallback.replace("×", "x").replace("*", "x")
    fallback = fallback.replace(",", ".")
    return re.sub(r"\s+", "", fallback)


def append_pack_tokens_to_name(name: Optional[str], *token_sources: str) -> Optional[str]:
    base_name = normalize_spaces(name or "")
    if not base_name:
        return name

    out_name = base_name
    seen_tokens: Set[str] = {
        canonical_pack_token_key(token) for token in extract_pack_tokens(out_name)
    }
    for source in token_sources:
        for pack_token in extract_pack_tokens(source):
            key = canonical_pack_token_key(pack_token)
            if not key or key in seen_tokens:
                continue
            seen_tokens.add(key)
            out_name = normalize_spaces(f"{out_name} {pack_token}")

    return out_name or None


def parse_brand_and_name(title_text: str, desc_text: str) -> Tuple[Optional[str], Optional[str]]:
    title = normalize_spaces(title_text)
    desc = normalize_spaces(desc_text)
    if not title and not desc:
        return None, None

    tokens = title.split()
    brand_tokens: List[str] = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if looks_like_brand_token(token):
            brand_tokens.append(token)
            i += 1
            continue

        if (
            brand_tokens
            and token in _brand_connector_tokens
            and i + 1 < len(tokens)
            and looks_like_brand_token(tokens[i + 1])
        ):
            brand_tokens.append(token)
            brand_tokens.append(tokens[i + 1])
            i += 2
            continue
        break

    brand = " ".join(brand_tokens) if brand_tokens else None

    name_title = title
    if brand and title.startswith(brand):
        stripped = normalize_spaces(title[len(brand) :])
        if stripped:
            name_title = stripped

    name = name_title or desc
    if name and desc:
        name = append_pack_tokens_to_name(name, desc)
    elif not name:
        name = desc

    return brand, name or None


def parse_product_url(anchor) -> Optional[str]:
    href = (anchor.attributes.get("href") or "").strip()
    if not href:
        return None
    url = normalize(urljoin(BASE, href))
    if not same_site(url) or not looks_like_product_url(url):
        return None
    return url


def parse_sku_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    path = urlparse(url).path
    m = _sku_from_url_re.search(path)
    if not m:
        return None
    return m.group(1)


def parse_image_url(card) -> Optional[str]:
    img = card.css_first("img[class*='ProductListItem_productImage']")
    if not img:
        img = card.css_first("img[src]") or card.css_first("img[data-src]")
    if not img:
        return None
    src = (
        (img.attributes.get("src") or "").strip()
        or (img.attributes.get("data-src") or "").strip()
    )
    if not src:
        srcset = (img.attributes.get("srcset") or "").strip()
        if srcset:
            src = srcset.split(",")[0].strip().split(" ")[0].strip()
    if not src or src.startswith("data:"):
        return None
    return normalize(urljoin(BASE, src))


def parse_listing_card(card, root_category: str) -> Optional[ListingProductRow]:
    anchor = card.css_first("a[class*='ProductListItem_productLink'][href]") or card.css_first("a[href]")
    if not anchor:
        return None
    url = parse_product_url(anchor)
    if not url:
        return None

    title_node = card.css_first("p[class*='ProductListItem_title__']")
    desc_node = card.css_first("p[class*='ProductListItem_titleDesc__']")
    unit_node = card.css_first("p[class*='ProductListItem_description__'] strong")
    if not unit_node:
        unit_node = card.css_first("p[class*='ProductListItem_description__']")
    final_node = card.css_first("p[class*='ProductListItem_finalPrice__']")
    original_node = card.css_first("p[class*='ProductListItem_beginPrice__']")
    badge_node = (
        anchor.css_first("div[class*='ProductListItem_badgeOffer__']")
        or anchor.css_first("div[class*='ProductListItem_badge__']")
        or anchor.css_first("[class*='badgeOffer']")
        or anchor.css_first("[class*='badge']")
        or card.css_first("div[class*='ProductListItem_badgeOffer__']")
        or card.css_first("div[class*='ProductListItem_badge__']")
        or card.css_first("[class*='badgeOffer']")
        or card.css_first("[class*='badge']")
    )

    title_text = normalize_spaces(title_node.text(separator=" ", strip=True) if title_node else "")
    desc_text = normalize_spaces(desc_node.text(separator=" ", strip=True) if desc_node else "")
    desc_title_text = normalize_spaces((desc_node.attributes.get("title") if desc_node else "") or "")
    unit_text = normalize_spaces(unit_node.text(separator=" ", strip=True) if unit_node else "")
    badge_text = normalize_spaces(badge_node.text(separator=" ", strip=True) if badge_node else "")
    badge_candidates = [
        normalize_spaces(node.text(separator=" ", strip=True))
        for node in card.css("[class*='badge']")
        if normalize_spaces(node.text(separator=" ", strip=True))
    ]
    if badge_text:
        badge_candidates.insert(0, badge_text)
    badge_text = " | ".join(dict.fromkeys(badge_candidates))

    brand, name = parse_brand_and_name(title_text=title_text, desc_text=desc_text)
    name = append_pack_tokens_to_name(
        name,
        desc_text,
        desc_title_text,
        unit_text,
    )
    final_price = parse_price_node(final_node)
    original_price = parse_price_node(original_node)
    if (
        final_price is not None
        and original_price is not None
        and original_price <= final_price + 1e-9
    ):
        original_price = None

    unit_price, unit_of_measure = parse_unit_price(unit_text)
    final_unit_price = unit_price
    if final_unit_price is None and final_price is not None:
        final_unit_price = final_price

    # Some listings expose an invalid "0.00 €/τεμάχιο". Fall back to final price.
    if final_unit_price is not None and final_unit_price <= 0 and final_price is not None:
        final_unit_price = final_price
    if final_unit_price is None and final_price is not None:
        final_unit_price = final_price

    original_unit_price = None
    unit_of_measure = (
        unit_of_measure
        or detect_unit_of_measure(f"{unit_text} {desc_text} {desc_title_text}")
        or "piece"
    )

    full_card_text = normalize_spaces(card.text(separator=" ", strip=True))
    href_text = unquote(anchor.attributes.get("href") or "")
    card_html = card.html or ""
    anchor_html = anchor.html or ""

    combined_promo_text = normalize_spaces(
        " ".join(
            part
            for part in (
                badge_text,
                title_text,
                desc_text,
                desc_title_text,
                unit_text,
                full_card_text,
                href_text,
                card_html,
                anchor_html,
            )
            if part
        )
    )
    one_plus_one, two_plus_one = detect_combo_offers(combined_promo_text)
    badge_one_plus_one, badge_two_plus_one = detect_combo_from_badge(badge_text)
    one_plus_one = one_plus_one or badge_one_plus_one
    two_plus_one = two_plus_one or badge_two_plus_one

    if "/offers/" in (urlparse(url).path.lower()) and not one_plus_one and not two_plus_one:
        loose = normalize_text_no_accents(unquote(f"{badge_text} {combined_promo_text}"))
        if re.search(r"2\D{0,4}1", loose):
            two_plus_one = True
        elif re.search(r"1\D{0,4}1", loose):
            one_plus_one = True

    if not one_plus_one:
        one_plus_one = bool(_one_plus_one_re.search(combined_promo_text))
    if not two_plus_one:
        two_plus_one = bool(_two_plus_one_re.search(combined_promo_text))
    discount_percent = parse_discount_percent(badge_text)
    if discount_percent is None and final_price and original_price and original_price > final_price:
        discount_percent = int(round(((original_price - final_price) / original_price) * 100))

    has_price_discount = (
        original_price is not None
        and final_price is not None
        and original_price > final_price
    )
    is_smart_buy = "εξυπνη αγορα" in normalize_text_no_accents(badge_text)

    promo_text = None
    if badge_text and not is_smart_buy:
        if (
            not _discount_re.search(badge_text)
            and not _money_off_re.search(badge_text)
            and not _one_plus_one_re.search(badge_text)
            and not _two_plus_one_re.search(badge_text)
        ):
            promo_text = badge_text

    offer = (
        one_plus_one
        or two_plus_one
        or discount_percent is not None
        or has_price_discount
        or (promo_text is not None)
    )

    row = ListingProductRow(
        url=url,
        name=name,
        sku=parse_sku_from_url(url),
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
        image_url=parse_image_url(card),
        root_category=root_category,
    )

    if not row.url and not row.name and not row.sku:
        return None
    return row


def is_offer_product_url(url: Optional[str]) -> bool:
    if not url:
        return False
    return "/offers/" in urlparse(url).path.lower()


def build_product_identity_key(row: ListingProductRow) -> Optional[str]:
    brand = normalize_spaces(row.brand or "")
    name = normalize_spaces(row.name or "")
    if not brand and not name:
        return None

    base = normalize_text_no_accents(f"{brand} {name}")
    base = _identity_token_cleanup_re.sub(" ", base)
    key = normalize_spaces(base)
    return key or None


def extract_image_sku_hints(image_url: Optional[str]) -> List[str]:
    if not image_url:
        return []
    path = urlparse(image_url).path
    filename = path.rsplit("/", 1)[-1]
    if not filename:
        return []

    hints: List[str] = []
    seen: Set[str] = set()
    for token in reversed(_image_sku_hint_re.findall(filename)):
        token = token.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        hints.append(token)
    return hints


def apply_offer_overlay_to_base(base: ListingProductRow, offer_rows: List[ListingProductRow]) -> None:
    has_one_plus_one = any(r.one_plus_one for r in offer_rows)
    has_two_plus_one = any(r.two_plus_one for r in offer_rows)

    base.one_plus_one = base.one_plus_one or has_one_plus_one
    base.two_plus_one = base.two_plus_one or has_two_plus_one

    if base.one_plus_one or base.two_plus_one:
        base.promo_text = None

    offer_name_sources = [r.name for r in offer_rows if r.name]
    if offer_name_sources:
        base.name = append_pack_tokens_to_name(base.name, *offer_name_sources)

    has_price_discount = (
        base.original_price is not None
        and base.final_price is not None
        and base.original_price > base.final_price
    )
    base.offer = (
        base.one_plus_one
        or base.two_plus_one
        or base.discount_percent is not None
        or has_price_discount
        or (base.promo_text is not None)
    )
    base.refresh_hidden_prices()


def extract_offer_overlay_map_from_next_data(tree: HTMLParser) -> Dict[str, Dict[str, Any]]:
    script = tree.css_first("script#__NEXT_DATA__")
    if not script:
        return {}

    raw_json = script.text(deep=True) or script.text() or ""
    if not raw_json:
        return {}

    try:
        payload = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        return {}

    offers = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("offers")
    ) or []

    out: Dict[str, Dict[str, Any]] = {}
    for offer in offers:
        if not isinstance(offer, dict):
            continue

        promo_blob = normalize_spaces(
            " ".join(
                str(part)
                for part in (
                    offer.get("mobileSticker"),
                    offer.get("webSticker"),
                    offer.get("name"),
                    offer.get("description"),
                    offer.get("shortDescription"),
                )
                if part
            )
        )
        one_plus_one, two_plus_one = detect_combo_offers(promo_blob)
        badge_one_plus_one, badge_two_plus_one = detect_combo_from_badge(promo_blob)
        one_plus_one = one_plus_one or badge_one_plus_one
        two_plus_one = two_plus_one or badge_two_plus_one
        if not one_plus_one:
            one_plus_one = bool(_one_plus_one_re.search(promo_blob))
        if not two_plus_one:
            two_plus_one = bool(_two_plus_one_re.search(promo_blob))

        pack_tokens = extract_pack_tokens(
            normalize_spaces(
                " ".join(
                    str(part)
                    for part in (
                        offer.get("name"),
                        offer.get("description"),
                        offer.get("shortDescription"),
                    )
                    if part
                )
            )
        )
        products = offer.get("products") or []
        for item in products:
            if not isinstance(item, dict):
                continue
            sku = normalize_spaces(str(item.get("sku") or ""))
            if not sku:
                continue

            entry = out.setdefault(
                sku,
                {
                    "one_plus_one": False,
                    "two_plus_one": False,
                    "final_set_price": None,
                    "pack_tokens": [],
                },
            )
            entry["one_plus_one"] = bool(entry["one_plus_one"]) or one_plus_one
            entry["two_plus_one"] = bool(entry["two_plus_one"]) or two_plus_one

            existing_tokens = [str(token) for token in entry.get("pack_tokens") or []]
            existing_keys = {normalize_text_no_accents(token) for token in existing_tokens}
            for token in pack_tokens:
                token_key = normalize_text_no_accents(token)
                if token_key in existing_keys:
                    continue
                existing_tokens.append(token)
                existing_keys.add(token_key)
            entry["pack_tokens"] = existing_tokens

    return out


def extract_static_pack_tokens_map_from_next_data(tree: HTMLParser) -> Dict[str, List[str]]:
    script = tree.css_first("script#__NEXT_DATA__")
    if not script:
        return {}

    raw_json = script.text(deep=True) or script.text() or ""
    if not raw_json:
        return {}

    try:
        payload = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        return {}

    static_products = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("staticProducts")
    ) or {}
    if not isinstance(static_products, dict):
        return {}

    out: Dict[str, List[str]] = {}
    for group in static_products.values():
        if not isinstance(group, list):
            continue
        for product in group:
            if not isinstance(product, dict):
                continue

            sku = normalize_spaces(str(product.get("sku") or ""))
            if not sku:
                continue

            token_source_parts: List[str] = []
            for field in ("name", "displayName", "details"):
                value = product.get(field)
                if isinstance(value, str) and value.strip():
                    token_source_parts.append(value)
            token_source = normalize_spaces(" ".join(token_source_parts))
            if not token_source:
                continue

            tokens = extract_pack_tokens(token_source)
            if not tokens:
                continue

            existing = out.setdefault(sku, [])
            existing_keys = {canonical_pack_token_key(token) for token in existing}
            for token in tokens:
                token_key = canonical_pack_token_key(token)
                if token_key in existing_keys:
                    continue
                existing.append(token)
                existing_keys.add(token_key)

    return out


def overlay_offer_map_on_rows(
    rows: List[ListingProductRow],
    offer_map_by_sku: Dict[str, Dict[str, Any]],
) -> List[ListingProductRow]:
    if not offer_map_by_sku:
        return rows

    base_indices_by_sku: Dict[str, List[int]] = {}
    for idx, row in enumerate(rows):
        if is_offer_product_url(row.url):
            continue
        if row.sku:
            base_indices_by_sku.setdefault(row.sku, []).append(idx)

    updated_rows = 0
    for sku, payload in offer_map_by_sku.items():
        candidate_indices = base_indices_by_sku.get(sku) or []
        if not candidate_indices:
            continue

        one_plus_one = bool(payload.get("one_plus_one"))
        two_plus_one = bool(payload.get("two_plus_one"))
        pack_tokens = [str(token) for token in (payload.get("pack_tokens") or [])]
        for idx in candidate_indices:
            row = rows[idx]
            changed = False

            if one_plus_one and not row.one_plus_one:
                row.one_plus_one = True
                changed = True
            if two_plus_one and not row.two_plus_one:
                row.two_plus_one = True
                changed = True
            if pack_tokens:
                new_name = append_pack_tokens_to_name(row.name, " ".join(pack_tokens))
                if new_name != row.name:
                    row.name = new_name
                    changed = True

            if row.one_plus_one or row.two_plus_one:
                row.promo_text = None

            has_price_discount = (
                row.original_price is not None
                and row.final_price is not None
                and row.original_price > row.final_price
            )
            row.offer = (
                row.one_plus_one
                or row.two_plus_one
                or row.discount_percent is not None
                or has_price_discount
                or (row.promo_text is not None)
            )
            row.refresh_hidden_prices()
            if changed:
                updated_rows += 1

    if updated_rows:
        print(f"post-process next_data overlay: updated_rows={updated_rows}")
    return rows


def overlay_offer_section_rows(rows: List[ListingProductRow]) -> List[ListingProductRow]:
    grouped_indices: Dict[str, List[int]] = {}
    for idx, row in enumerate(rows):
        key = build_product_identity_key(row)
        if not key:
            continue
        grouped_indices.setdefault(key, []).append(idx)

    to_drop: Set[int] = set()
    merged_groups = 0
    dropped_offer_rows = 0
    matched_offer_indices: Set[int] = set()

    for indices in grouped_indices.values():
        if len(indices) < 2:
            continue

        base_indices = [idx for idx in indices if not is_offer_product_url(rows[idx].url)]
        offer_indices = [idx for idx in indices if is_offer_product_url(rows[idx].url)]
        if not base_indices or not offer_indices:
            continue

        merged_groups += 1
        offer_rows = [rows[idx] for idx in offer_indices]

        for idx in base_indices:
            apply_offer_overlay_to_base(rows[idx], offer_rows)

        for idx in offer_indices:
            if idx not in to_drop:
                to_drop.add(idx)
                dropped_offer_rows += 1
            matched_offer_indices.add(idx)

    # Fallback overlay: match /offers cards to /products cards by image-derived SKU hints.
    base_indices_by_sku: Dict[str, List[int]] = {}
    for idx, row in enumerate(rows):
        if is_offer_product_url(row.url):
            continue

        base_skus = []
        if row.sku:
            base_skus.append(row.sku.strip())
        base_skus.extend(extract_image_sku_hints(row.image_url))
        for sku in base_skus:
            if not sku:
                continue
            base_indices_by_sku.setdefault(sku, []).append(idx)

    for offer_idx, offer_row in enumerate(rows):
        if not is_offer_product_url(offer_row.url):
            continue
        if offer_idx in matched_offer_indices:
            continue

        offer_skus = []
        if offer_row.sku:
            offer_skus.append(offer_row.sku.strip())
        offer_skus.extend(extract_image_sku_hints(offer_row.image_url))

        candidate_indices: Set[int] = set()
        for sku in offer_skus:
            for base_idx in base_indices_by_sku.get(sku, []):
                candidate_indices.add(base_idx)
        if not candidate_indices:
            continue

        offer_key_tokens = set((build_product_identity_key(offer_row) or "").split())
        scored: List[Tuple[int, int]] = []
        for base_idx in candidate_indices:
            base_row = rows[base_idx]
            base_key_tokens = set((build_product_identity_key(base_row) or "").split())
            score = len(offer_key_tokens.intersection(base_key_tokens))

            if offer_row.brand and base_row.brand:
                if normalize_text_no_accents(offer_row.brand) == normalize_text_no_accents(base_row.brand):
                    score += 2
            scored.append((score, base_idx))

        if len(scored) == 1:
            best_indices = [scored[0][1]]
        else:
            best_score = max(score for score, _ in scored)
            if best_score <= 0:
                continue
            best_indices = [idx for score, idx in scored if score == best_score]

        apply_offer_overlay_to_base(
            rows[best_indices[0]],
            [offer_row],
        )
        if offer_idx not in to_drop:
            to_drop.add(offer_idx)
            dropped_offer_rows += 1

    if merged_groups or dropped_offer_rows:
        print(
            f"post-process overlay: merged_groups={merged_groups} "
            f"dropped_offer_rows={dropped_offer_rows}"
        )

    return [row for idx, row in enumerate(rows) if idx not in to_drop]


def crawl_category_listing(
    root_listing: str,
    root_category: str,
    max_pages: int = 500,
) -> List[ListingProductRow]:
    root_listing = normalize(root_listing.rstrip("/"))
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()
    offer_overlay_map_by_sku: Dict[str, Dict[str, Any]] = {}

    page = 1
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        while page <= max_pages:
            url = build_page_url(root_listing, page)
            response = fetch_listing_page(client=client, url=url, page=page)

            if response.status_code == 404:
                print(f"page={page} -> 404, stopping pagination.")
                break

            response.raise_for_status()
            t = HTMLParser(response.text)
            page_offer_map = extract_offer_overlay_map_from_next_data(t)
            page_static_pack_map = extract_static_pack_tokens_map_from_next_data(t)
            for sku, payload in page_offer_map.items():
                existing = offer_overlay_map_by_sku.setdefault(
                    sku,
                    {
                        "one_plus_one": False,
                        "two_plus_one": False,
                        "final_set_price": None,
                        "pack_tokens": [],
                    },
                )
                existing["one_plus_one"] = bool(existing["one_plus_one"]) or bool(payload.get("one_plus_one"))
                existing["two_plus_one"] = bool(existing["two_plus_one"]) or bool(payload.get("two_plus_one"))

                existing_tokens = [str(token) for token in existing.get("pack_tokens") or []]
                existing_keys = {canonical_pack_token_key(token) for token in existing_tokens}
                for token in payload.get("pack_tokens") or []:
                    token_text = normalize_spaces(str(token))
                    if not token_text:
                        continue
                    token_key = canonical_pack_token_key(token_text)
                    if token_key in existing_keys:
                        continue
                    existing_tokens.append(token_text)
                    existing_keys.add(token_key)
                existing["pack_tokens"] = existing_tokens

            for sku, pack_tokens in page_static_pack_map.items():
                existing = offer_overlay_map_by_sku.setdefault(
                    sku,
                    {
                        "one_plus_one": False,
                        "two_plus_one": False,
                        "final_set_price": None,
                        "pack_tokens": [],
                    },
                )
                existing_tokens = [str(token) for token in existing.get("pack_tokens") or []]
                existing_keys = {canonical_pack_token_key(token) for token in existing_tokens}
                for token in pack_tokens:
                    token_text = normalize_spaces(str(token))
                    if not token_text:
                        continue
                    token_key = canonical_pack_token_key(token_text)
                    if token_key in existing_keys:
                        continue
                    existing_tokens.append(token_text)
                    existing_keys.add(token_key)
                existing["pack_tokens"] = existing_tokens
            next_page, max_page, has_next = extract_pagination_state(
                tree=t,
                requested_page=page,
            )

            cards = t.css("div[class*='ProductListItem_productItem__']")
            if not cards:
                cards = t.css("div[class*='ProductListItem_productItem']")
            if not cards:
                print(f"page={page} -> 0 product cards, stopping.")
                break

            added = 0
            for card in cards:
                row = parse_listing_card(card, root_category=root_category)
                if not row:
                    continue

                key = row.url or f"{row.sku or ''}|{row.name or ''}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                rows.append(row)
                added += 1

            print(f"page={page} +{added} total={len(rows)} cards={len(cards)}")

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

    rows = overlay_offer_map_on_rows(rows, offer_overlay_map_by_sku)
    return overlay_offer_section_rows(rows)


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
