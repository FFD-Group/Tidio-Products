"""Interrogation tool for saved_batches.json.

Usage:
  python inspect_batches.py                      # batch summary table
  python inspect_batches.py --batch 3            # detail view of batch 3 (1-indexed)
  python inspect_batches.py --batch 3 --index 12 # single product at position 12
  python inspect_batches.py --sku ABC-123        # find product by SKU
  python inspect_batches.py --check              # scan all batches for violations
"""

import argparse
import json
import os
import sys

BATCHES_FILE = "saved_batches.json"
FEATURE_LIMIT = 255
DISPLAY_TRUNCATE = 120

STATUS_COLOURS = {
    "sent": "\033[32m",     # green
    "failed": "\033[31m",   # red
    "pending": "\033[33m",  # yellow
}
RESET = "\033[0m"


def _colour(status: str) -> str:
    return STATUS_COLOURS.get(status, "") + status + RESET


def _trunc(value: str, limit: int = DISPLAY_TRUNCATE) -> str:
    if len(value) > limit:
        return value[:limit] + "…"
    return value


def load_manifest() -> dict:
    if not os.path.exists(BATCHES_FILE):
        sys.exit(f"Error: '{BATCHES_FILE}' not found. Run a sync first.")
    with open(BATCHES_FILE) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def cmd_summary(manifest: dict) -> None:
    meta = manifest["meta"]
    print(f"Manifest created : {meta['created_at']}")
    print(f"Total products   : {meta['total_products']}")
    print(f"Total batches    : {meta['total_batches']}")
    print()

    col = "{:<8} {:<10} {:<6} {}"
    print(col.format("Batch", "Status", "Size", "Sent at"))
    print("-" * 60)
    for b in manifest["batches"]:
        num = b["index"] + 1
        print(col.format(
            num,
            _colour(b["status"]),
            b["size"],
            b.get("sent_at") or "-",
        ))


def cmd_batch(manifest: dict, batch_num: int) -> None:
    batches = manifest["batches"]
    if batch_num < 1 or batch_num > len(batches):
        sys.exit(f"Error: batch {batch_num} does not exist (1–{len(batches)}).")

    b = batches[batch_num - 1]
    print(f"Batch {batch_num} — status: {_colour(b['status'])}  size: {b['size']}")
    print()

    col = "{:<6} {:<10} {:<30} {:<14} {:<8} {}"
    print(col.format("Index", "ID", "SKU", "Price", "Features", "Flags"))
    print("-" * 100)

    for i, p in enumerate(b["products"]):
        flags = []
        if p.get("price") is None:
            flags.append("NULL PRICE")
        oversized = [
            k for k, v in (p.get("features") or {}).items()
            if isinstance(v, str) and len(v) > FEATURE_LIMIT
        ]
        if oversized:
            flags.append(f"FEATURE>{FEATURE_LIMIT}: {', '.join(oversized)}")

        print(col.format(
            i,
            p.get("id", ""),
            _trunc(p.get("sku", ""), 30),
            str(p.get("price")),
            len(p.get("features") or {}),
            "  ".join(flags) if flags else "",
        ))


def cmd_product(manifest: dict, batch_num: int, index: int) -> None:
    batches = manifest["batches"]
    if batch_num < 1 or batch_num > len(batches):
        sys.exit(f"Error: batch {batch_num} does not exist (1–{len(batches)}).")

    b = batches[batch_num - 1]
    products = b["products"]
    if index < 0 or index >= len(products):
        sys.exit(f"Error: index {index} out of range (0–{len(products) - 1}) for batch {batch_num}.")

    p = products[index]
    print(f"Batch {batch_num}, index {index}")
    print("-" * 60)

    scalar_fields = ["id", "sku", "title", "status", "price", "default_currency",
                     "vendor", "product_type", "url", "image_url", "updated_at"]
    for field in scalar_fields:
        if field in p:
            print(f"  {field:<20} {p[field]}")

    description = p.get("description", "")
    if description:
        print(f"  {'description':<20} {_trunc(description)}")

    features = p.get("features") or {}
    if features:
        print()
        print(f"  Features ({len(features)}):")
        fcol = "    {:<40} {:<6} {}"
        print(fcol.format("Key", "Len", "Value"))
        print("    " + "-" * 90)
        for k, v in sorted(features.items()):
            v_str = str(v) if not isinstance(v, str) else v
            flag = "  *** OVER LIMIT" if len(v_str) > FEATURE_LIMIT else ""
            print(fcol.format(k, len(v_str), _trunc(v_str) + flag))


def cmd_sku(manifest: dict, sku: str) -> None:
    found = False
    for b in manifest["batches"]:
        for i, p in enumerate(b["products"]):
            if p.get("sku") == sku:
                found = True
                print(f"Found in batch {b['index'] + 1}, index {i}")
                cmd_product(manifest, b["index"] + 1, i)
                print()
    if not found:
        print(f"SKU '{sku}' not found in any batch.")


def cmd_check(manifest: dict) -> None:
    null_prices = []
    feature_violations = []

    for b in manifest["batches"]:
        bnum = b["index"] + 1
        for i, p in enumerate(b["products"]):
            if p.get("price") is None:
                null_prices.append((bnum, i, p.get("sku", ""), p.get("title", "")))

            for k, v in (p.get("features") or {}).items():
                v_str = str(v) if not isinstance(v, str) else v
                if len(v_str) > FEATURE_LIMIT:
                    feature_violations.append((bnum, i, p.get("sku", ""), k, len(v_str), v_str))

    if null_prices:
        print(f"NULL PRICES ({len(null_prices)} products):")
        col = "  {:<8} {:<7} {:<30} {}"
        print(col.format("Batch", "Index", "SKU", "Title"))
        print("  " + "-" * 80)
        for bnum, idx, sku, title in null_prices:
            print(col.format(bnum, idx, _trunc(sku, 30), _trunc(title, 40)))
    else:
        print("No null prices found.")

    print()

    if feature_violations:
        print(f"FEATURE VIOLATIONS >{FEATURE_LIMIT} chars ({len(feature_violations)} occurrences):")
        col = "  {:<8} {:<7} {:<30} {:<40} {:<6} {}"
        print(col.format("Batch", "Index", "SKU", "Feature key", "Len", "Value (truncated)"))
        print("  " + "-" * 120)
        for bnum, idx, sku, key, length, value in feature_violations:
            print(col.format(bnum, idx, _trunc(sku, 30), key, length, _trunc(value)))
    else:
        print(f"No feature values exceeding {FEATURE_LIMIT} characters found.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interrogate saved_batches.json for product data and validation issues."
    )
    parser.add_argument("--batch", type=int, metavar="N",
                        help="Inspect batch N in detail (1-indexed).")
    parser.add_argument("--index", type=int, metavar="I",
                        help="Show the single product at position I within --batch N (0-indexed).")
    parser.add_argument("--sku", metavar="SKU",
                        help="Find a product by SKU across all batches.")
    parser.add_argument("--check", action="store_true",
                        help="Scan all batches and report price/feature violations.")
    args = parser.parse_args()

    manifest = load_manifest()

    if args.sku:
        cmd_sku(manifest, args.sku)
    elif args.check:
        cmd_check(manifest)
    elif args.batch and args.index is not None:
        cmd_product(manifest, args.batch, args.index)
    elif args.batch:
        cmd_batch(manifest, args.batch)
    else:
        cmd_summary(manifest)


if __name__ == "__main__":
    main()
