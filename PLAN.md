# Supermarket Comparison App Plan (Django)

## Goal
Group equivalent products from different stores under one canonical item, while keeping each store's own listing and price.

Example you gave:
- `freska-froyta-lachanika-listing-products.csv:58`
- `frouta-lachanika-listing-products.csv:34`

These should map to one shared product and keep both store prices.

## 1. Data Model (Django)
Create these core models:

1. `Store`
- `name` (e.g. `sklavenitis`, `mymarket`)

2. `Category`
- `name`
- `slug`

3. `Product` (canonical/shared product)
- `canonical_name`
- `brand_normalized`
- `quantity_value` (e.g. `425`)
- `quantity_unit` (e.g. `g`, `kg`, `ml`, `l`, `temaxio`)
- `normalized_key` (indexed, unique when possible)
- `category` (FK)

4. `StoreListing` (one row per store product)
- `store` (FK)
- `store_sku`
- `store_name`
- `store_brand`
- `url`
- `image_url`
- `final_price`
- `final_unit_price`
- `original_price`
- `original_unit_price`
- `unit_of_measure`
- `offer`
- `snapshot_at` (datetime of crawl/import)
- `product` (FK to canonical `Product`, nullable initially)

5. `PriceHistory` (optional now, useful later)
- `store_listing` (FK)
- `price`
- `unit_price`
- `captured_at`

6. `MatchReview` (manual review queue)
- `store_listing` (FK)
- `candidate_product` (FK)
- `score`
- `status` (`pending/approved/rejected`)
- `notes`

## 2. Import Pipeline
Build management command:
- `python manage.py import_store_csv --store sklavenitis --file freska-froyta-lachanika-listing-products.csv`
- `python manage.py import_store_csv --store mymarket --file frouta-lachanika-listing-products.csv`

Rules:
- Always insert/update `StoreListing`.
- Keep raw store fields unchanged.
- Link to `Product` only after matching pipeline runs.

## 3. Name Normalization + Attribute Extraction
Create a shared matcher utility (`matching/normalizer.py`) that:

1. Normalizes text:
- lowercase
- remove Greek accents
- remove punctuation
- unify `gr`, `g`, `γραμ`, `γρ` -> `g`
- unify `κιλού`, `τιμή κιλού`, `timi kilou` noise tokens
- normalize brand variants (`Φρεσκούλης`/`ΦΡΕΣΚΟΥΛΗΣ`)

2. Extracts pack size:
- regex for `(\d+(?:[.,]\d+)?)\s?(g|gr|kg|ml|l|τεμ|tem)`
- convert to standard unit (e.g. `425gr` -> `425 g`)

3. Produces tokens:
- remove stopwords (`σαλάτα`, `ποιότητα`, `ελληνικά`) where needed
- keep discriminating tokens (`ιταλική`, `οικογενειακή`, brand, quantity)

## 4. Matching Strategy (Tiered)
Use deterministic first, fuzzy second.

1. Tier A (auto-match, high confidence):
- same normalized brand
- same normalized quantity/unit
- token similarity above strict threshold (e.g. `>= 0.92`)

2. Tier B (auto-match if very strong):
- quantity exact
- high text similarity (e.g. trigram/rapidfuzz `>= 0.95`)
- category compatible

3. Tier C (manual review):
- score in middle range (e.g. `0.80 - 0.92`)
- create `MatchReview` row for approval UI/admin

4. Tier D (new canonical):
- no acceptable candidate => create new `Product`

Suggested score weights:
- brand exact: `0.35`
- quantity exact: `0.30`
- normalized name similarity: `0.35`

## 5. Apply to Your Example
For lines `58` and `34`:
- brand normalized: `freskoulis` (match)
- quantity: `425 g` (match)
- core descriptor: `italiki` (match)
- similarity should be high enough for Tier A auto-match

Result:
- one `Product` row
- two `StoreListing` rows (one per store) linked to that product
- comparison page shows both prices side by side

## 6. Admin/Review Workflow
In Django admin:
- List unlinked `StoreListing`
- List `MatchReview` pending items sorted by score desc
- Approve action links listing -> candidate product
- Reject action forces new product creation

This avoids wrong merges while still automating most matches.

## 7. Comparison API/UI (first version)
Minimal endpoint:
- `GET /api/products/<id>/offers`
- returns canonical product + all linked store listings with current prices and unit prices

UI page:
- product title
- one card per store
- show `final_price`, `final_unit_price`, `offer`, `last_updated`

## 8. Tests (must-have)
1. Normalization tests:
- Greek accents/uppercase handling
- unit parsing (`425gr`, `0.425kg`)

2. Matcher tests:
- positive match for your known pair (58 vs 34)
- non-match for similarly named but different pack size

3. Import tests:
- importing same CSV twice updates listing without duplicates

## 9. Rollout Steps
1. Implement models + migrations.
2. Implement CSV import command.
3. Implement normalizer + matcher.
4. Run matcher on current 2 CSVs.
5. Review ambiguous matches in admin.
6. Build first comparison view/API.
7. Add more categories/stores.

## 10. Practical Rule of Thumb
Never merge two listings into one `Product` unless quantity and category align.  
Most bad matches happen from name similarity alone without pack-size validation.
