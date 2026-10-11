"""
IKEA India - Full Catalog Extractor
"""
import csv
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIGURATION
# ============================================================

BASE_URL = "https://www.ikea.com"
INDIA_HOME = "https://www.ikea.com/in/en/"
PRODUCTS_PAGE = "https://www.ikea.com/in/en/cat/products-products/"
PRODUCT_API = "https://sik.search.blue.cdtapps.com/in/en/product-list-page"

# The category endpoint accepts a large size window. 1000 is kept as a
# safety limit; the code also retries with smaller sizes if necessary.
PAGE_SIZE = 1000
MAX_WORKERS = 8
MAX_RETRIES = 4
TIMEOUT = 40

OUTPUT_DIR = Path("ikea_output")
CSV_FILE = OUTPUT_DIR / "ikea_products.csv"
RAW_FILE = OUTPUT_DIR / "products_raw.json"
CATEGORY_FILE = OUTPUT_DIR / "categories.json"
PROGRESS_FILE = OUTPUT_DIR / "scraper_progress.json"


# ============================================================
# FINAL CSV COLUMNS
# ============================================================

CSV_FIELDS = [
    "category",
    "productID",
    "variant_id",
    "parent_product_id",
    "is_variant",
    "product_name",
    "price",
    "offer_price",
    "discount",
    "rating",
    "rating_count",
    "product_url",
    "image_urls",
    "badge",
    "colors",
    "size",
    "pack_size",
    "product_type",
    "description",
]

# ============================================================
# HTTP
# ============================================================

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/153.0.0.0 Safari/537.36 Edg/153.0.0.0"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-IN,en;q=0.9",
        "Origin": BASE_URL,
        "Referer": INDIA_HOME,
    })
    return session


def request_with_retry(
    session: requests.Session,
    url: str,
) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=TIMEOUT)

            if response.status_code == 200:
                return response

            if response.status_code in (429, 500, 502, 503, 504):
                wait = min(2 * attempt, 10)
                print(
                    f"HTTP {response.status_code} | retry "
                    f"{attempt}/{MAX_RETRIES} | wait {wait}s"
                )
                time.sleep(wait)
                continue

            print(f"HTTP {response.status_code}: {url}")
            return None

        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                print(f"Request failed: {exc}")
                return None

            wait = min(2 * attempt, 10)
            time.sleep(wait)

    return None


# ============================================================
# CATEGORY DISCOVERY
# ============================================================

def extract_category_id(url: str) -> str:
    path = urlparse(url).path.rstrip("/")

    if "/in/en/cat/" not in path:
        return ""

    last_part = path.split("/")[-1]
    match = re.search(r"-([a-z]{2}\d+|\d+)$", last_part, re.I)
    return match.group(1) if match else ""


def discover_category_ids() -> List[Dict[str, str]]:
    print("1/4 Discovering IKEA category IDs...")

    session = make_session()
    response = request_with_retry(session, PRODUCTS_PAGE)

    if not response:
        raise RuntimeError("Could not open IKEA Products page.")

    soup = BeautifulSoup(response.text, "html.parser")
    categories: Dict[str, Dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = urljoin(BASE_URL, anchor["href"]).split("#")[0]
        category_id = extract_category_id(href)

        if not category_id:
            continue

        name = re.sub(r"\s+", " ", anchor.get_text(" ", strip=True)).strip()
        if not name:
            continue

        categories[category_id] = {
            "category_id": category_id,
            "category_url": href,
            "category_link_text": name,
        }

    if not categories:
        raise RuntimeError("No IKEA category IDs were found.")

    result = sorted(categories.values(), key=lambda x: x["category_id"])
    print(f"Category IDs discovered: {len(result)}")
    return result


# ============================================================
# PRODUCT API
# ============================================================

def get_product_window(
    session: requests.Session,
    category_id: str,
    size: int,
) -> List[Dict[str, Any]]:
    url = f"{PRODUCT_API}?category={category_id}&size={size}"
    response = request_with_retry(session, url)

    if not response:
        return []

    try:
        data = response.json()
    except ValueError:
        print(f"Invalid JSON for category {category_id}")
        return []

    page = data.get("productListPage") or {}
    window = page.get("productWindow") or []

    if not isinstance(window, list):
        return []

    return [item for item in window if isinstance(item, dict)]


def fetch_category_products(
    category: Dict[str, str],
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    category_id = category["category_id"]
    session = make_session()

    probe_url = f"{PRODUCT_API}?category={category_id}&size=1"
    response = request_with_retry(session, probe_url)

    if not response:
        return category, []

    try:
        probe = response.json()
    except ValueError:
        return category, []

    page = probe.get("productListPage") or {}
    product_count = int(page.get("productCount") or 0)

    if product_count <= 0:
        return category, []

    # The endpoint uses one large window rather than offset pagination.
    # Try the exact count first when reasonable, then safe fallbacks.
    sizes: List[int] = []
    for value in (product_count, PAGE_SIZE, 500, 250, 100):
        value = max(1, min(int(value), max(product_count, 1)))
        if value not in sizes:
            sizes.append(value)

    products: List[Dict[str, Any]] = []

    for size in sizes:
        products = get_product_window(session, category_id, size)
        if products:
            break

    print(
        f"  {category_id} | website cards={product_count} "
        f"| received={len(products)}"
    )

    return category, products


# ============================================================
# GENERAL HELPERS
# ============================================================

def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    return str(value).strip()


def first_value(product: Dict[str, Any], keys: Iterable[str]) -> str:
    wanted = {key.lower() for key in keys}

    def walk(value: Any) -> str:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).lower() in wanted and isinstance(child, (str, int, float)):
                    result = clean_text(child)
                    if result:
                        return result
                result = walk(child)
                if result:
                    return result

        elif isinstance(value, list):
            for child in value:
                result = walk(child)
                if result:
                    return result

        return ""

    return walk(product)


def product_id(product: Dict[str, Any]) -> str:
    # Only actual IKEA article/product ID fields belong here.
    for key in (
        "itemNo",
        "itemNumber",
        "item_no",
        "productID",
        "productId",
        "id",
    ):
        value = product.get(key)
        if value is not None and clean_text(value):
            return clean_text(value)
    return ""


# ============================================================
# VARIANT / ARTICLE FLATTENING  <-- IMPORTANT FIX
# ============================================================

def flatten_product_variants(
    product: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Convert one IKEA product card into article-level records.

    A category card can contain alternate article records inside
    gprDescription.variants. Every article gets its own CSV row.

    Output metadata added to each flattened record:
      _variant_id       = article ID for a nested variant
      _parent_product_id = ID of the top-level/category-card article
      _is_variant       = TRUE/FALSE
    """
    output: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    root_id = product_id(product)

    def walk(
        current: Dict[str, Any],
        inherited_category_path: Optional[List[Any]],
        inherited_colors: Optional[List[Any]],
        is_variant: bool,
    ) -> None:
        record = dict(current)

        category_path = record.get("categoryPath")
        if not isinstance(category_path, list) and isinstance(inherited_category_path, list):
            record["categoryPath"] = inherited_category_path

        colors = record.get("colors")
        if not isinstance(colors, list) and isinstance(inherited_colors, list):
            record["colors"] = inherited_colors

        pid = product_id(record)
        if pid and pid not in seen:
            seen.add(pid)
            record["_variant_id"] = pid if is_variant else ""
            record["_parent_product_id"] = root_id if is_variant else ""
            record["_is_variant"] = "TRUE" if is_variant else "FALSE"
            output.append(record)

        gpr = current.get("gprDescription")
        variants = gpr.get("variants") if isinstance(gpr, dict) else None
        if not isinstance(variants, list):
            return

        parent_path = record.get("categoryPath")
        parent_colors = record.get("colors")

        for variant in variants:
            if not isinstance(variant, dict):
                continue
            walk(variant, parent_path, parent_colors, True)

    walk(product, None, None, False)
    return output


def flatten_all_cards(
    cards: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for card in cards:
        for record in flatten_product_variants(card):
            pid = product_id(record)
            if pid and pid not in seen:
                seen.add(pid)
                output.append(record)

    return output


# ============================================================
# PRICE / OFFER / DISCOUNT
# ============================================================

def money_from_price_part(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, dict):
        whole = value.get("wholeNumber")
        if whole is not None:
            prefix = clean_text(value.get("prefix")) or "Rs."
            decimals = clean_text(value.get("decimals"))
            result = f"{prefix}{whole}"
            if decimals:
                result += f".{decimals}"
            return result

        numeral = value.get("numeral")
        if numeral is not None:
            try:
                number = float(numeral)
                if number.is_integer():
                    return f"Rs.{int(number):,}"
                return f"Rs.{number:,.2f}".rstrip("0").rstrip(".")
            except (ValueError, TypeError):
                pass

    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return f"Rs.{int(value):,}"
        return f"Rs.{float(value):,.2f}".rstrip("0").rstrip(".")

    return clean_text(value)


def extract_price_fields(product: Dict[str, Any]) -> Tuple[str, str, str]:
    sales_price = product.get("salesPrice")
    if not isinstance(sales_price, dict):
        return "", "", ""

    current_text = money_from_price_part(sales_price.get("current"))
    previous_text = money_from_price_part(sales_price.get("previous"))
    discount_value = sales_price.get("discountPercentage")

    has_offer = bool(
        previous_text and current_text and previous_text != current_text
    )

    if not has_offer:
        return current_text or previous_text, "", ""

    discount = ""
    if discount_value not in (None, ""):
        try:
            number = float(discount_value)
            discount = f"{int(number)}%" if number.is_integer() else f"{number:g}%"
        except (ValueError, TypeError):
            discount = clean_text(discount_value)

    return previous_text, current_text, discount


# ============================================================
# FULL IKEA CATEGORY BREADCRUMB
# ============================================================

def extract_category_names(product: Dict[str, Any]) -> List[str]:
    """Return categoryPath names in IKEA hierarchy order."""
    path = product.get("categoryPath")

    if not isinstance(path, list):
        return []

    names: List[str] = []
    for item in path:
        if not isinstance(item, dict):
            continue
        name = clean_text(item.get("name"))
        if name and name not in names:
            names.append(name)

    return names


def extract_category_path(product: Dict[str, Any]) -> str:
    """
    Build ONE complete category value.

    Example:
    Products > Baby & children > Children > Children's textiles >
    Children's bed linen > BARNDRÖM Duvet cover and pillowcase
    """
    parts = ["Products"]
    parts.extend(extract_category_names(product))

    product_name = clean_text(product.get("name"))
    if product_name:
        parts.append(product_name)

    return " > ".join(parts)


# ============================================================
# PRODUCT FIELDS
# ============================================================

def extract_product_url(product: Dict[str, Any]) -> str:
    direct = clean_text(product.get("pipUrl"))
    if direct and "/in/en/p/" in direct:
        return direct

    candidates: List[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_lower = str(key).lower()
                if key_lower in {
                    "pipurl",
                    "producturl",
                    "product_url",
                    "canonicalurl",
                    "url",
                } and isinstance(child, str):
                    candidates.append(child)
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(product)

    for candidate in candidates:
        candidate = urljoin(BASE_URL, candidate)
        path = urlparse(candidate).path.lower()
        if "/in/en/p/" not in path:
            continue
        if any(ext in path for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg")):
            continue
        return candidate

    return ""


def extract_images(product: Dict[str, Any]) -> str:
    images: List[str] = []
    seen: Set[str] = set()

    all_images = product.get("allProductImage", [])
    if isinstance(all_images, list):
        for item in all_images:
            if not isinstance(item, dict):
                continue
            url = clean_text(item.get("url"))
            if url and url not in seen:
                seen.add(url)
                images.append(url)

    main_image = clean_text(product.get("mainImageUrl"))
    if main_image and main_image not in seen:
        images.insert(0, main_image)

    return json.dumps(images, ensure_ascii=False)


def extract_colors(product: Dict[str, Any]) -> str:
    colors = product.get("colors", [])
    result: List[str] = []

    if isinstance(colors, list):
        for item in colors:
            if isinstance(item, dict):
                name = clean_text(item.get("name"))
            else:
                name = clean_text(item)
            if name and name not in result:
                result.append(name)

    # Some variant objects do not repeat the colors array. For those,
    # validDesignText is useful as a variant-specific fallback.
    if not result:
        design = clean_text(product.get("validDesignText"))
        if design:
            result.append(design)

    return ", ".join(result)


def extract_size(product: Dict[str, Any]) -> str:
    value = first_value(
        product,
        [
            "itemMeasureReferenceText",
            "item_measure_reference_text",
            "measureReferenceText",
            "measure_reference_text",
        ],
    )
    if value:
        return value

    return first_value(
        product,
        ["measurements", "measurement", "dimensions", "dimension", "size"],
    )


def extract_pack_size(product: Dict[str, Any]) -> str:
    sales_price = product.get("salesPrice")
    if isinstance(sales_price, dict):
        price_unit = clean_text(sales_price.get("priceUnit"))
        unit_text = clean_text(sales_price.get("unitText"))
        if price_unit:
            return price_unit
        if unit_text:
            return unit_text
    return ""


def extract_badge(product: Dict[str, Any]) -> str:
    badges: List[str] = []

    for value in (
        product.get("tagText"),
        product.get("tag"),
    ):
        text = clean_text(value)
        if not text or text.upper() == "NONE":
            continue
        if text == clean_text(product.get("tag")):
            text = text.replace("_", " ").title()
        if text.lower() not in {x.lower() for x in badges}:
            badges.append(text)

    sales_price = product.get("salesPrice")
    if isinstance(sales_price, dict):
        tag_with_text = sales_price.get("tagWithText")
        if isinstance(tag_with_text, dict):
            text = clean_text(tag_with_text.get("text"))
            if text and text.lower() not in {x.lower() for x in badges}:
                badges.append(text)

    return ", ".join(badges)


# ============================================================
# DESCRIPTION
# ============================================================

COLOR_WORDS = {
    "green", "white", "pink", "rose", "red", "orange", "yellow",
    "blue", "black", "beige", "brown", "purple", "lilac", "grey",
    "gray", "mixed", "multicolour", "multicolor", "natural", "clear",
    "transparent", "off-white", "offwhite",
}


def clean_description_name(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""

    match = re.search(
        r"(?:in\s*/\s*outdoor|outdoor)\s*/?\s*(.*)$",
        text,
        flags=re.I,
    )
    if not match:
        return ""

    tail = match.group(1).strip()
    first_part = re.split(r"[,/]", tail, maxsplit=1)[0].strip()
    if not first_part:
        return ""

    removable = {"hanging", "potted", "pot", "artificial", "plant", "flower"}
    words = first_part.split()

    while words and words[0].lower() in removable:
        words.pop(0)

    # Remove colour suffixes but keep multi-word plant names.
    while len(words) > 1 and words[-1].lower() in COLOR_WORDS:
        words.pop()

    if (
        len(words) >= 3
        and words[-2].lower() in {"light", "dark"}
        and words[-1].lower() in COLOR_WORDS
    ):
        words = words[:-2]

    return " ".join(words).strip()


def extract_description(product: Dict[str, Any]) -> str:
    for candidate in (
        product.get("validDesignText"),
        product.get("gprDescriptionText"),
        product.get("description"),
        product.get("shortDescription"),
    ):
        if isinstance(candidate, str):
            value = clean_description_name(candidate)
            if value:
                return value
    return ""


# ============================================================
# NORMALIZE
# ============================================================

def normalize_product(product: Dict[str, Any]) -> Dict[str, str]:
    price, offer_price, discount = extract_price_fields(product)

    pid = product_id(product)
    return {
        "category": extract_category_path(product),
        "productID": pid,
        "variant_id": clean_text(product.get("_variant_id")),
        "parent_product_id": clean_text(product.get("_parent_product_id")),
        "is_variant": clean_text(product.get("_is_variant")) or "FALSE",
        "product_name": clean_text(product.get("name")),
        "price": price,
        "offer_price": offer_price,
        "discount": discount,
        "rating": clean_text(product.get("ratingValue")),
        "rating_count": clean_text(product.get("ratingCount")),
        "product_url": extract_product_url(product),
        "image_urls": extract_images(product),
        "badge": extract_badge(product),
        "colors": extract_colors(product),
        "size": extract_size(product),
        "pack_size": extract_pack_size(product),
        "product_type": clean_text(product.get("typeName")),
        "description": extract_description(product),
    }


# ============================================================
# PROGRESS / RAW DATA
# ============================================================

def load_progress() -> Dict[str, Any]:
    if not PROGRESS_FILE.exists():
        return {"completed": []}

    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"completed": []}
    except Exception:
        return {"completed": []}


def save_progress(completed: Set[str]) -> None:
    PROGRESS_FILE.write_text(
        json.dumps({"completed": sorted(completed)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_old_raw() -> Dict[str, Dict[str, Any]]:
    """
    Load the previous raw file, but also flatten any variants already present
    in it. This means a rerun can immediately recover variants such as
    00622674 from the existing raw JSON.
    """
    result: Dict[str, Dict[str, Any]] = {}

    if not RAW_FILE.exists():
        return result

    try:
        old_raw = json.loads(RAW_FILE.read_text(encoding="utf-8"))
    except Exception:
        return result

    if not isinstance(old_raw, list):
        return result

    for card in old_raw:
        if not isinstance(card, dict):
            continue
        for product in flatten_product_variants(card):
            pid = product_id(product)
            if pid:
                result[pid] = product

    return result


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 78)
    print("IKEA INDIA - FULL CATALOG / ALL ARTICLE VARIANTS")
    print("=" * 78)
    print("Variant mode: flatten gprDescription.variants")
    print()

    # --------------------------------------------------------
    # 1. Discover categories
    # --------------------------------------------------------
    categories = discover_category_ids()

    CATEGORY_FILE.write_text(
        json.dumps(categories, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # --------------------------------------------------------
    # 2. Load old raw data and progress
    # --------------------------------------------------------
    progress = load_progress()
    completed = set(progress.get("completed", []))

    raw_products = load_old_raw()

    print(f"Existing article-level products loaded: {len(raw_products)}")

    # IMPORTANT:
    # Because the previous scraper marked categories complete before it
    # flattened variants, we intentionally re-scan all categories once.
    # This creates the corrected article-level catalog.
    # The old progress file is NOT used to skip categories.
    remaining = categories

    print()
    print("2/4 Collecting category cards + ALL nested variants...")
    print(f"Workers: {MAX_WORKERS}")
    print(f"Categories to scan: {len(remaining)}")
    print()

    new_completed: Set[str] = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(fetch_category_products, category): category
            for category in remaining
        }

        finished = 0
        for future in as_completed(future_map):
            category = future_map[future]

            try:
                category_info, cards = future.result()
                variants = flatten_all_cards(cards)

                for product in variants:
                    pid = product_id(product)
                    if pid:
                        raw_products[pid] = product

                new_completed.add(category_info["category_id"])
                finished += 1

                print(
                    f"[{finished}/{len(remaining)}] DONE "
                    f"{category_info['category_id']} "
                    f"| cards={len(cards)} "
                    f"| article/variants={len(variants)} "
                    f"| unique articles={len(raw_products)}"
                )

                # Save after every completed category so a crash loses
                # at most one category's work.
                RAW_FILE.write_text(
                    json.dumps(list(raw_products.values()), ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                save_progress(new_completed)

            except Exception as exc:
                print(
                    f"CATEGORY ERROR {category.get('category_id')}: {exc}"
                )

    # --------------------------------------------------------
    # 3. Normalize
    # --------------------------------------------------------
    print()
    print("3/4 Building final CSV rows...")

    normalized_products: Dict[str, Dict[str, str]] = {}

    for pid, product in raw_products.items():
        try:
            normalized_products[pid] = normalize_product(product)
        except Exception as exc:
            print(f"NORMALIZE ERROR {pid}: {exc}")

    # --------------------------------------------------------
    # 4. Save
    # --------------------------------------------------------
    print("4/4 Saving CSV and raw JSON...")

    with CSV_FILE.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for pid in sorted(normalized_products):
            row = normalized_products[pid]
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})

    RAW_FILE.write_text(
        json.dumps(
            [raw_products[pid] for pid in sorted(raw_products)],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    save_progress(new_completed)

    offer_count = sum(
        bool(row.get("offer_price"))
        for row in normalized_products.values()
    )
    variant_count = sum(
        1 for row in normalized_products.values()
        if row.get("is_variant") == "TRUE"
    )

    print()
    print("=" * 78)
    print("COMPLETE")
    print("=" * 78)
    print(f"Category IDs scanned: {len(new_completed)} / {len(categories)}")
    print(f"Unique article/product IDs: {len(normalized_products)}")
    print(f"Nested variant article IDs: {variant_count}")
    print(f"Products with offer price: {offer_count}")
    print()
    print(f"CSV:       {CSV_FILE}")
    print(f"Raw JSON:  {RAW_FILE}")
    print(f"Categories:{CATEGORY_FILE}")
    print(f"Progress:  {PROGRESS_FILE}")
    print()
    print("=" * 78)


if __name__ == "__main__":
    main()
