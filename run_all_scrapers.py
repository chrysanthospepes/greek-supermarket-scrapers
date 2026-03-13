import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parent
SCRAPERS: Tuple[Tuple[str, Path], ...] = (
    ("ab", REPO_ROOT / "ab" / "ab_category_listing.py"),
    ("bazaar", REPO_ROOT / "bazaar" / "bazaar_category_listing.py"),
    ("kritikos", REPO_ROOT / "kritikos" / "kritikos_category_listing.py"),
    ("masoutis", REPO_ROOT / "masoutis" / "masoutis_category_listing.py"),
    ("sklavenitis", REPO_ROOT / "sklavenitis" / "sklavenitis_category_listing.py"),
    ("mymarket", REPO_ROOT / "mymarket" / "mymarket_category_listing.py"),
)
SCRAPER_MAP = {store: path for store, path in SCRAPERS}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run supermarket scrapers sequentially with the current Python interpreter.",
    )
    parser.add_argument(
        "stores",
        nargs="*",
        choices=tuple(SCRAPER_MAP),
        help="Optional store subset. Defaults to all stores.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop after the first scraper failure instead of attempting the remaining stores.",
    )
    return parser.parse_args()


def selected_scrapers(requested_stores: Sequence[str]) -> List[Tuple[str, Path]]:
    if not requested_stores:
        return list(SCRAPERS)

    ordered_unique_stores: List[str] = []
    for store in requested_stores:
        if store not in ordered_unique_stores:
            ordered_unique_stores.append(store)

    return [(store, SCRAPER_MAP[store]) for store in ordered_unique_stores]


def run_scraper(store: str, script_path: Path) -> int:
    print(f"[{store}] start -> {script_path.relative_to(REPO_ROOT)}")
    completed = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=REPO_ROOT,
        check=False,
    )
    print(f"[{store}] done exit_code={completed.returncode}")
    return completed.returncode


def main() -> int:
    args = parse_args()
    failures: List[str] = []
    scrapers_to_run = selected_scrapers(args.stores)

    for store, script_path in scrapers_to_run:
        exit_code = run_scraper(store, script_path)
        if exit_code == 0:
            continue

        failures.append(f"{store}({exit_code})")
        if args.stop_on_error:
            break

    if failures:
        print("Failed scrapers: " + ", ".join(failures), file=sys.stderr)
        return 1

    print(f"Completed {len(scrapers_to_run)} scraper(s) successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
