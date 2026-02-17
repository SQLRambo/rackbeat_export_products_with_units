#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

API_BASE = "https://app.rackbeat.com/api/products"
FIELDS = "unit,name,quantity"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch product units from Rackbeat API for product numbers in a CSV file "
            "and write them to a semicolon-separated CSV."
        )
    )
    parser.add_argument(
        "--token",
        required=False,
        help="Bearer token for Rackbeat API. If omitted, RACKBEAT_BEARER_TOKEN is used.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV file containing product numbers.",
    )
    parser.add_argument(
        "--output",
        required=False,
        default=None,
        help=(
            "Path to output CSV file (semicolon-separated). "
            "If omitted, writes product_units.csv in the same folder as --input."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds. Default: 30",
    )
    return parser.parse_args()


def get_token(arg_token: str | None) -> str:
    token = arg_token or os.getenv("RACKBEAT_BEARER_TOKEN")
    if not token:
        raise ValueError("Bearer token is required via --token or RACKBEAT_BEARER_TOKEN.")
    return token.strip()


def detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t,")
        return dialect.delimiter
    except csv.Error:
        return ";"


def get_product_number_field(fieldnames: Sequence[str] | None) -> str | None:
    if not fieldnames:
        return None

    normalized: Dict[str, str] = {name.strip().lower(): name for name in fieldnames}
    candidates = [
        "product_number",
        "productnumber",
        "product no",
        "product_no",
        "product nr",
        "productnr",
        "itemnumber",
        "sku",
        "varenummer",
    ]
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]

    return fieldnames[0]


def read_product_numbers(input_path: Path) -> List[str]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = detect_delimiter(sample)

        reader = csv.DictReader(f, delimiter=delimiter)

        if reader.fieldnames:
            field = get_product_number_field(reader.fieldnames)
            if not field:
                raise ValueError("Unable to determine product number column in input CSV.")

            values = []
            for row in reader:
                value = (row.get(field) or "").strip()
                if value:
                    values.append(value)

            if values:
                return values

    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        values = []
        for line in f:
            value = line.strip().strip(";,")
            if value and not value.lower().startswith("product"):
                values.append(value)
        return values


def build_units_url(product_number: str) -> str:
    query = urlencode({"fields": FIELDS})
    return f"{API_BASE}/{product_number}/units?{query}"


def parse_units_payload(payload: object) -> List[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("item_units", "data", "units", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []


def fetch_units_for_product(product_number: str, token: str, timeout: float) -> List[dict]:
    url = build_units_url(product_number)
    request = Request(
        url=url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            payload = json.loads(body)
            return parse_units_payload(payload)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} for product '{product_number}' at {url}. Response: {detail}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for product '{product_number}': {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON for product '{product_number}': {exc}") from exc


def to_output_rows(product_number: str, units: Iterable[dict]) -> List[dict]:
    rows = []
    for unit_item in units:
        row = {
            "product_number": product_number,
            "unit": unit_item.get("unit", ""),
            "unitname": unit_item.get("name", ""),
            "quantity": unit_item.get("quantity", ""),
        }
        rows.append(row)
    return rows


def write_output_csv(output_path: Path, rows: Sequence[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["product_number", "unit", "unitname", "quantity"],
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()

    try:
        token = get_token(args.token)
        input_path = Path(args.input)
        if args.output:
            candidate_output = Path(args.output)
            output_path = (
                candidate_output
                if candidate_output.is_absolute()
                else input_path.parent / candidate_output
            )
        else:
            output_path = input_path.parent / "product_units.csv"

        product_numbers = read_product_numbers(input_path)
        if not product_numbers:
            print("No product numbers found in input CSV.", file=sys.stderr)
            return 1

        all_rows: List[dict] = []
        errors: List[str] = []

        for product_number in product_numbers:
            try:
                units = fetch_units_for_product(product_number, token, args.timeout)
                all_rows.extend(to_output_rows(product_number, units))
            except RuntimeError as exc:
                errors.append(str(exc))

        write_output_csv(output_path, all_rows)

        print(f"Processed products: {len(product_numbers)}")
        print(f"Output rows written: {len(all_rows)}")
        print(f"Output file: {output_path}")

        if errors:
            print(f"Errors: {len(errors)}", file=sys.stderr)
            for error in errors:
                print(f"- {error}", file=sys.stderr)
            return 2

        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
