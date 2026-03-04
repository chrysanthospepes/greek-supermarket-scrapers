# Supermarkets

Python scrapers for category listing pages on Greek supermarket e-shops. Each script crawls one retailer, normalizes product card data, and exports a CSV with a shared schema for prices, promo flags, unit pricing, images, and category metadata.

## Supported stores

- AB Vassilopoulos: `ab/ab_category_listing.py`
- Bazaar: `bazaar/bazaar_category_listing.py`
- Kritikos: `kritikos/kritikos_category_listing.py`
- Masoutis: `masoutis/masoutis_category_listing.py`
- Sklavenitis: `sklavenitis/sklavenitis_category_listing.py`
- My market: `mymarket/mymarket_category_listing.py`

Each scraper is store-specific. There is no shared framework layer in this repo because each site exposes different HTML structures, pagination rules, and pricing quirks.

## Repository layout

- `ab/`, `bazaar/`, `kritikos/`, `masoutis/`, `sklavenitis/`, `mymarket/`: standalone scrapers and parser notes for each retailer
- `*_cards.md`: captured HTML snippets and edge-case notes used while building the parsers
- `requirements.txt`: Python dependencies

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Each script is configured directly in code. The main knobs live near the top of every scraper:

- `ROOT_CATEGORIES`: category paths to crawl
- `MAX_PAGES_PER_CATEGORY`: pagination limit
- `PAGE_SLEEP_SECONDS`: delay between page requests
- `CATEGORY_WORKERS`: how many root categories to crawl in parallel
- `SORT_PRODUCTS_FOR_CSV`: whether output is sorted before writing

The crawl pacing and root-category concurrency can also be overridden with environment variables:

- `CRAWLER_PAGE_SLEEP_SECONDS`
- `CRAWLER_CATEGORY_WORKERS`

Run a scraper directly:

```bash
python3 ab/ab_category_listing.py
python3 bazaar/bazaar_category_listing.py
python3 kritikos/kritikos_category_listing.py
python3 masoutis/masoutis_category_listing.py
python3 sklavenitis/sklavenitis_category_listing.py
python3 mymarket/mymarket_category_listing.py
```

Brand denylist files:

- `bazaar/bazaar_brand_denylist.txt`
- `masoutis/masoutis_brand_denylist.txt`

If a parsed brand matches the normalized contents of the store's denylist file, the scraper writes `brand = None` for that product.

The scripts write CSV files to the repository root. Filenames are derived from the configured category slug or root category, depending on the retailer.

Console output is intentionally minimal. During a run, each scraper prints only when a category starts and when it finishes, together with the number of products written for that category.

## CSV schema

All scrapers write the same logical fields:

| Column | Meaning |
| --- | --- |
| `url` | Product URL |
| `name` | Product name as parsed from the listing |
| `sku` | Store SKU or product identifier when available |
| `brand` | Parsed brand |
| `final_price` | Current selling price |
| `final_unit_price` | Current per-unit price |
| `original_price` | Previous price when a discount is present |
| `original_unit_price` | Previous per-unit price when available |
| `unit_of_measure` | Normalized unit, usually `kilos`, `liters`, or `piece` |
| `discount_percent` | Parsed percentage discount |
| `offer` | Generic promo flag |
| `one_plus_one` | `1+1` offer flag |
| `two_plus_one` | `2+1` offer flag |
| `promo_text` | Raw promotion text when relevant |
| `image_url` | Product image URL |
| `root_category` | Normalized root category used for the crawl |

## Notes

- These scrapers make live requests to retailer websites. Markup and APIs can change without notice.
- The `*_cards.md` files are working notes, not tests.
- Some stores expose misleading promo badges or inconsistent unit prices; the scripts contain store-specific cleanup logic to normalize those cases.
- Use the scrapers responsibly and in line with the target sites' terms and rate limits.
