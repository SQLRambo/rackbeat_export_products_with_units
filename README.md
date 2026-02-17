# Rackbeat product units fetcher

This project contains a Python script that reads product numbers from a CSV file, fetches units from Rackbeat API, and writes the result to a semicolon-separated CSV file.

## Files

- `fetch_product_units.py` - main script
- `products_sample.csv` - sample input with product numbers

## Requirements

- Python 3.10+
- A valid Rackbeat bearer token

## Usage

### Option 1: Pass token as argument

```powershell
python .\fetch_product_units.py --token "YOUR_BEARER_TOKEN" --input .\products_sample.csv --output .\product_units.csv
```

### Option 2: Use environment variable

```powershell
$env:RACKBEAT_BEARER_TOKEN="YOUR_BEARER_TOKEN"
python .\fetch_product_units.py --input .\products_sample.csv --output .\product_units.csv
```

## Input format

The input CSV must contain product numbers. The script supports common delimiters and headers.

Simple example:

```csv
product_number
10001
10002
```

## Output format

The output is semicolon-separated (`;`) with columns:

- `product_number`
- `unit`
- `unitname`
- `quantity`

Each unit for a product is written as a separate row.

## API endpoint used

For each product number, the script calls:

`https://app.rackbeat.com/api/products/{product_number}/units?fields=unit,name,quantity`
