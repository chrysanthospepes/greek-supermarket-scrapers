import builtins
import csv
import math
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from decimal import ROUND_CEILING, Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx

BASE = "https://www.masoutis.gr"
MASOUTIS_DIR = Path(__file__).resolve().parent
ROOT_CATEGORIES = [
    "categories/index/manabiko?item=566",
    "categories/index/kreopwleio?item=565",
    "categories/index/eidh-psugeiou?item=568",
    "categories/index/eidh-katapsukshs?item=573",
    "categories/index/kaba?item=574",
    "categories/index/snack-kshroi-karpoi?item=579",
    "categories/index/prwina?item=544",
    "categories/index/artozaxaroplasteio?item=575",
    "categories/index/zaxarwdh-mpiskota?item=571",
    "categories/index/eidh-pantopwleiou?item=562",
    "categories/index/zumarika-ospria?item=577",
    "categories/index/dressing?item=563",
    "categories/index/konserboeidh?item=578",
    "categories/index/brefikh-frontida?item=545",
    "categories/index/proswpikh-peripoihsh?item=570",
    "categories/index/ugieinh-xartika?item=576",
    "categories/index/eidh-katharismou?item=572",
    "categories/index/eidh-oikiakhs?item=727",
    "categories/index/katoikidia?item=567",
    "categories/index/ugieinh-diatrofh?item=564",
]
MAX_PAGES_PER_CATEGORY = 500
SORT_PRODUCTS_FOR_CSV = True
REQUEST_RETRY_ATTEMPTS = 3
REQUEST_RETRY_BACKOFF_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}
DEFAULT_PAGE_SLEEP_SECONDS = 0.02
DEFAULT_CATEGORY_WORKERS = 4
CLIENT_TIMEOUT_SECONDS = 30.0
PASSKEY = "Sc@NnSh0p"
PAGE_SIZE = 50
API_HEADERS_TTL_SECONDS = 20 * 60
TOKEN = ""
ZIP_CODE = ""
FILL_MISSING_BRANDS_FROM_DETAIL = True
BRAND_DENYLIST_PATH = MASOUTIS_DIR / "masoutis_brand_denylist.txt"

try:
    PAGE_SLEEP_SECONDS = max(
        0.0,
        float(os.getenv("CRAWLER_PAGE_SLEEP_SECONDS", str(DEFAULT_PAGE_SLEEP_SECONDS))),
    )
except ValueError:
    PAGE_SLEEP_SECONDS = DEFAULT_PAGE_SLEEP_SECONDS

try:
    CATEGORY_WORKERS = max(
        1,
        int(os.getenv("CRAWLER_CATEGORY_WORKERS", str(DEFAULT_CATEGORY_WORKERS))),
    )
except ValueError:
    CATEGORY_WORKERS = DEFAULT_CATEGORY_WORKERS

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
    "Origin": BASE,
    "Referer": f"{BASE}/",
}
HTTPX_LIMITS = httpx.Limits(
    max_connections=max(8, CATEGORY_WORKERS * 4),
    max_keepalive_connections=max(4, CATEGORY_WORKERS * 2),
)

_spaces_re = re.compile(r"\s+")
_non_price_chars_re = re.compile(r"[^0-9,.\-]")
_one_plus_one_re = re.compile(r"\b1\s*\+\s*1\b")
_two_plus_one_re = re.compile(r"\b2\s*\+\s*1\b")
_discount_re = re.compile(r"(-?\s*\d+)\s*%")
_plain_percent_re = re.compile(r"^\s*-?\s*\d+\s*%\s*$")
_hidden_price_quantum = Decimal("0.01")
_hidden_price_fields = ("hidden_price", "hidden_unit_price")

console_print = builtins.print


def print(*args: Any, **kwargs: Any) -> None:
    return None


def make_http_client() -> httpx.Client:
    client_kwargs = dict(
        headers=HEADERS,
        timeout=CLIENT_TIMEOUT_SECONDS,
        follow_redirects=True,
        limits=HTTPX_LIMITS,
    )
    try:
        return httpx.Client(http2=True, **client_kwargs)
    except ImportError:
        return httpx.Client(**client_kwargs)


@dataclass(frozen=True)
class RootCategory:
    item: str
    slug: str
    name: str


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


class MasoutisApiClient:
    def __init__(self) -> None:
        self.client = make_http_client()
        self._api_headers: Dict[str, str] = {}
        self._api_headers_refreshed_at = 0.0
        self._detail_brand_cache: Dict[str, Optional[str]] = {}

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "MasoutisApiClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def ensure_api_headers(self, force: bool = False) -> Dict[str, str]:
        now = time.time()
        if (
            not force
            and self._api_headers
            and now - self._api_headers_refreshed_at < API_HEADERS_TTL_SECONDS
        ):
            return self._api_headers

        headers = {"Content-Type": "application/json"}
        if TOKEN:
            headers["Authorization"] = f"Bearer {TOKEN}"

        response = self.client.get(f"{BASE}/api/eshop/GetCred", headers=headers)
        response.raise_for_status()
        payload = response.json()

        self._api_headers = {
            "Content-Type": "application/json",
            "Uid": str(payload.get("Uid") or "").strip(),
            "Usl": str(payload.get("Usl") or "").strip(),
            "Key": str(payload.get("Key") or "").strip(),
        }
        if TOKEN:
            self._api_headers["Authorization"] = f"Bearer {TOKEN}"

        self._api_headers_refreshed_at = now
        return self._api_headers

    def post_json(self, path: str, payload: Dict[str, Any]) -> Any:
        last_error: Optional[Exception] = None
        force_refresh = False

        for attempt in range(1, REQUEST_RETRY_ATTEMPTS + 1):
            headers = self.ensure_api_headers(force=force_refresh)
            force_refresh = False

            try:
                response = self.client.post(
                    urljoin(BASE, path),
                    json=payload,
                    headers=headers,
                )
            except httpx.RequestError as exc:
                last_error = exc
                if attempt >= REQUEST_RETRY_ATTEMPTS:
                    raise

                wait_seconds = REQUEST_RETRY_BACKOFF_SECONDS * attempt
                print(
                    f"{path} -> request error ({exc}), "
                    f"retrying in {wait_seconds:.1f}s "
                    f"({attempt}/{REQUEST_RETRY_ATTEMPTS})."
                )
                time.sleep(wait_seconds)
                continue

            if response.status_code == 403 and attempt < REQUEST_RETRY_ATTEMPTS:
                print(f"{path} -> 403, refreshing API headers and retrying.")
                force_refresh = True
                time.sleep(0.2)
                continue

            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and response.status_code != 403
                and attempt < REQUEST_RETRY_ATTEMPTS
            ):
                wait_seconds = REQUEST_RETRY_BACKOFF_SECONDS * attempt
                print(
                    f"{path} -> status={response.status_code}, "
                    f"retrying in {wait_seconds:.1f}s "
                    f"({attempt}/{REQUEST_RETRY_ATTEMPTS})."
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response.json()

        if last_error is not None:
            raise RuntimeError(f"{path} -> exhausted retries") from last_error
        raise RuntimeError(f"{path} -> exhausted retries")

    def fetch_menu(self) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"PassKey": PASSKEY}
        if TOKEN:
            payload["Token"] = TOKEN
        elif ZIP_CODE:
            payload["Zip"] = ZIP_CODE

        data = self.post_json("/api/eshop/GetScanNShopMenuAllLevelsAutoScheduler", payload)
        return data if isinstance(data, list) else []

    def fetch_listing_page(
        self,
        root_category: RootCategory,
        page: int,
    ) -> List[Dict[str, Any]]:
        payload = {
            "PassKey": PASSKEY,
            "Itemcode": root_category.item,
            "ItemDescr": "0",
            "IfWeight": str(page),
            "ServiceResponse": "",
            "Token": TOKEN,
            "Zip": ZIP_CODE,
            "BrandName": "",
            "TeamId": "",
            "ExtraFilter": "",
        }
        data = self.post_json(
            "/api/eshop/GetPromoItemWithListCouponsSubCategoriesAutoPromosv2",
            payload,
        )
        return data if isinstance(data, list) else []

    def fetch_detail_brand(self, sku: str) -> Optional[str]:
        raw_brand = self.fetch_detail_raw_brand(sku)
        if not raw_brand:
            return None
        return filter_brand(raw_brand)

    def fetch_detail_raw_brand(self, sku: str) -> Optional[str]:
        sku = normalize_spaces(sku)
        if not sku:
            return None
        if sku in self._detail_brand_cache:
            return self._detail_brand_cache[sku]

        payload = {
            "PassKey": PASSKEY,
            "Itemcode": sku,
            "Token": TOKEN,
            "Zip": ZIP_CODE,
        }
        data = self.post_json("/api/eshop/GetOfferItemCustWithCoupons", payload)
        brand = None
        if isinstance(data, dict):
            brand = clean_raw_brand(data.get("BrandNameDesciption"))
        self._detail_brand_cache[sku] = brand
        return brand


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


@lru_cache(maxsize=2048)
def normalize_text_no_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", normalize_spaces(text).lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


@lru_cache(maxsize=1)
def load_brand_denylist() -> Set[str]:
    return parse_list_file(BRAND_DENYLIST_PATH)


def reset_brand_list_caches() -> None:
    load_brand_denylist.cache_clear()


@lru_cache(maxsize=2048)
def detect_unit_of_measure(label: str, if_weight: bool = False) -> Optional[str]:
    low = normalize_text_no_accents(label).replace("o", "ο")
    if any(token in low for token in ("κιλου", "κιλα", "κιλο", "κιλ", "/kg")):
        return "kilos"
    if any(token in low for token in ("λιτρου", "λιτρα", "λιτρο", "λιτ", "/lt", "/l")):
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


def normalize_url(url: str) -> Optional[str]:
    raw = normalize_spaces(url)
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme:
        raw = urljoin(BASE, raw)
        parsed = urlparse(raw)
    return parsed._replace(fragment="").geturl()


def parse_price_number(value: Any) -> Optional[float]:
    s = normalize_spaces(str(value or ""))
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


def parse_unit_price_label(label: Any) -> Tuple[Optional[float], Optional[str]]:
    text = normalize_spaces(str(label or ""))
    if not text:
        return None, None
    return parse_price_number(text), text


def clean_raw_brand(value: Any) -> Optional[str]:
    brand = normalize_spaces(str(value or ""))
    if not brand:
        return None
    if normalize_text_no_accents(brand) in {"no brand", "-"}:
        return None
    return brand


def filter_brand(brand: Optional[str]) -> Optional[str]:
    if not brand:
        return None
    if normalize_text_no_accents(brand) in load_brand_denylist():
        return None
    return brand


def clean_brand(value: Any) -> Optional[str]:
    return filter_brand(clean_raw_brand(value))


def should_try_detail_brand(value: Any) -> bool:
    brand = normalize_spaces(str(value or ""))
    if not brand:
        return True
    return normalize_text_no_accents(brand) not in {"no brand", "-"}


def parse_discount_percent(*values: str) -> Optional[int]:
    for value in values:
        m = _discount_re.search(value or "")
        if not m:
            continue
        try:
            return abs(int(m.group(1).replace(" ", "")))
        except ValueError:
            continue
    return None


def is_mono_text(text: str) -> bool:
    low = normalize_text_no_accents(text).replace("o", "ο")
    return "μονο" in low


def is_generic_price_promo(text: str) -> bool:
    txt = normalize_spaces(text)
    if not txt:
        return False
    return is_mono_text(txt)


def choose_promo_text(candidates: Sequence[str]) -> Optional[str]:
    for text in candidates:
        txt = normalize_spaces(text)
        if not txt:
            continue
        if is_generic_price_promo(txt):
            continue
        if _plain_percent_re.fullmatch(txt):
            continue
        return txt

    for text in candidates:
        txt = normalize_spaces(text)
        if not txt:
            continue
        if _one_plus_one_re.search(txt) or _two_plus_one_re.search(txt):
            return txt

    return None


def parse_promo(product: Dict[str, Any]) -> Tuple[Optional[int], bool, bool, Optional[str]]:
    candidates: List[str] = []
    seen: Set[str] = set()

    for key in ("Discount", "OfferDescr", "CouponDescr", "CouponCondition"):
        text = normalize_spaces(str(product.get(key) or ""))
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        candidates.append(text)

    one_plus_one = any(_one_plus_one_re.search(text) for text in candidates)
    two_plus_one = any(_two_plus_one_re.search(text) for text in candidates)
    discount_percent = parse_discount_percent(*candidates)
    promo_text = choose_promo_text(candidates)
    return discount_percent, one_plus_one, two_plus_one, promo_text


def parse_api_image_url(product: Dict[str, Any]) -> Optional[str]:
    for key in ("PhotoData", "PhotoLink", "PhotoLink2", "PhotoLink3", "PhotoLink4", "PhotoLink5", "PhotoLink6"):
        url = normalize_url(str(product.get(key) or ""))
        if url:
            return url
    return None


def parse_root_categories_from_menu(
    menu_rows: Sequence[Dict[str, Any]],
    selected_slugs: Optional[Set[str]] = None,
) -> List[RootCategory]:
    roots: List[RootCategory] = []
    seen_items: Set[str] = set()

    for row in menu_rows:
        if not isinstance(row, dict):
            continue

        item = normalize_spaces(str(row.get("HeaderMenuItem") or ""))
        slug = normalize_spaces(str(row.get("HeaderMenuItemLinkDescr") or ""))
        name = normalize_spaces(str(row.get("HeaderMenuItemDescr") or ""))
        if not item or not slug or not name:
            continue
        if item in seen_items:
            continue
        if selected_slugs and slug not in selected_slugs:
            continue

        seen_items.add(item)
        roots.append(RootCategory(item=item, slug=slug, name=name))

    return roots


def to_category_slug(category: str) -> str:
    parsed = urlparse(category)
    path = parsed.path.strip("/") or category.strip("/")
    slug = path.split("/")[-1]

    if not slug:
        raise ValueError(f"Invalid category '{category}'")
    return slug


def category_url(root_category: RootCategory) -> str:
    return f"{BASE}/categories/index/{root_category.slug}?item={root_category.item}"


def csv_filename_for_root_category(root_category: RootCategory) -> str:
    safe_slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", root_category.slug).strip("_")
    if not safe_slug:
        safe_slug = "category"
    return f"{safe_slug}-listing-products.csv"


def parse_api_listing_product(
    product: Dict[str, Any],
    root_category: str,
) -> Optional[ListingProductRow]:
    if not isinstance(product, dict):
        return None

    sku = normalize_spaces(str(product.get("Itemcode") or ""))
    name = normalize_spaces(str(product.get("ItemDescr") or ""))
    url = normalize_url(str(product.get("ItemDescrLink") or ""))
    brand = clean_brand(product.get("BrandNameDesciption"))

    final_price = parse_price_number(product.get("PosPrice"))
    original_price = parse_price_number(product.get("StartPrice"))
    if (
        final_price is not None
        and original_price is not None
        and original_price <= final_price + 1e-9
    ):
        original_price = None

    final_unit_price, final_unit_label = parse_unit_price_label(product.get("ItemVolume"))
    original_unit_price, original_unit_label = parse_unit_price_label(product.get("StartPrItemVolume"))
    if (
        final_unit_price is not None
        and original_unit_price is not None
        and original_unit_price <= final_unit_price + 1e-9
    ):
        original_unit_price = None

    if_weight = bool(product.get("IfWeight"))
    unit_of_measure = (
        detect_unit_of_measure(final_unit_label or "", if_weight=if_weight)
        or detect_unit_of_measure(original_unit_label or "", if_weight=if_weight)
        or "piece"
    )

    if final_unit_price is None and final_price is not None:
        final_unit_price = final_price
    if original_unit_price is None and original_price is not None and unit_of_measure == "piece":
        original_unit_price = original_price

    discount_percent, one_plus_one, two_plus_one, promo_text = parse_promo(product)
    has_price_discount = (
        original_price is not None
        and final_price is not None
        and original_price > final_price
    ) or (
        original_unit_price is not None
        and final_unit_price is not None
        and original_unit_price > final_unit_price
    )
    offer = one_plus_one or two_plus_one or discount_percent is not None or has_price_discount or promo_text is not None

    row = ListingProductRow(
        url=url,
        name=name or None,
        sku=sku or None,
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
        image_url=parse_api_image_url(product),
        root_category=root_category,
    )

    if not row.url and not row.name and not row.sku:
        return None
    return row


def crawl_root_category(
    api: MasoutisApiClient,
    root_category: RootCategory,
    max_pages: int = MAX_PAGES_PER_CATEGORY,
) -> List[ListingProductRow]:
    rows: List[ListingProductRow] = []
    seen_keys: Set[str] = set()
    expected_pages: Optional[int] = None

    for page in range(1, max_pages + 1):
        products = api.fetch_listing_page(root_category=root_category, page=page)
        if not products:
            print(f"page={page} -> 0 products, stopping.")
            break

        if expected_pages is None:
            total_items = parse_price_number(products[0].get("PassKey"))
            if total_items is not None and total_items > 0:
                expected_pages = max(1, math.ceil(total_items / PAGE_SIZE))

        added = 0
        for product in products:
            row = parse_api_listing_product(product, root_category=root_category.slug)
            if not row:
                continue

            if (
                FILL_MISSING_BRANDS_FROM_DETAIL
                and not row.brand
                and row.sku
                and should_try_detail_brand(product.get("BrandNameDesciption"))
            ):
                row.brand = api.fetch_detail_brand(row.sku)

            key = row.url or f"{row.sku or ''}|{row.name or ''}"
            if key in seen_keys:
                continue
            seen_keys.add(key)

            rows.append(row)
            added += 1

        total_hint = ""
        if expected_pages is not None:
            total_hint = f" max_page={expected_pages}"
        print(f"page={page} +{added} total={len(rows)} cards={len(products)}{total_hint}")

        if expected_pages is not None and page >= expected_pages:
            print(f"page={page} -> reached max_page={expected_pages}, stopping.")
            break

        if len(products) < PAGE_SIZE:
            print(f"page={page} -> short page ({len(products)} < {PAGE_SIZE}), stopping.")
            break

        time.sleep(PAGE_SLEEP_SECONDS)

    return rows


def save_to_csv(rows: List[ListingProductRow], filename: str) -> None:
    if not rows:
        print("No rows to save.")
        return

    fieldnames = [field.name for field in fields(ListingProductRow)]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(serialize_row_for_csv(row) for row in rows)

    return


def main() -> None:
    requested_slugs = {to_category_slug(category) for category in ROOT_CATEGORIES if category.strip()}
    reset_brand_list_caches()

    with MasoutisApiClient() as api:
        root_categories = parse_root_categories_from_menu(
            api.fetch_menu(),
            selected_slugs=requested_slugs or None,
        )

        found_slugs = {root.slug for root in root_categories}
        missing_slugs = sorted(requested_slugs - found_slugs)
        for slug in missing_slugs:
            print(f"Menu did not return requested root category '{slug}'.")

        if not root_categories:
            print("No root categories selected.")
            return

    def process_root_category(root_category: RootCategory) -> None:
        console_print(f"category={root_category.slug} -> start")

        with MasoutisApiClient() as category_api:
            rows = crawl_root_category(
                api=category_api,
                root_category=root_category,
                max_pages=MAX_PAGES_PER_CATEGORY,
            )
        console_print(f"category={root_category.slug} -> done products={len(rows)}")

        if SORT_PRODUCTS_FOR_CSV:
            rows.sort(key=lambda row: ((row.url or "").lower(), row.sku or "", row.name or ""))

        save_to_csv(rows, csv_filename_for_root_category(root_category))

    if CATEGORY_WORKERS <= 1 or len(root_categories) <= 1:
        for root_category in root_categories:
            process_root_category(root_category)
    else:
        with ThreadPoolExecutor(max_workers=min(CATEGORY_WORKERS, len(root_categories))) as executor:
            futures = {
                executor.submit(process_root_category, root_category): root_category.slug
                for root_category in root_categories
            }
            for future in as_completed(futures):
                root_slug = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    console_print(f"category={root_slug} -> failed ({exc})")


if __name__ == "__main__":
    main()
