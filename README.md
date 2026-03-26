# Elasticsearch CSV Export Tool

Bypass Kibana's CSV export limitations — empty CSVs, row count caps, missing `feature_discover.generate_report` privileges — by querying Elasticsearch directly via the **PIT + search_after** API and streaming results to CSV.

---

## Problem This Solves

Kibana Discover's CSV export can:
- Download an **empty CSV** even when documents are visible in the UI
- Hit **row count limits** (default 10,000 or `discover:sampleSize`)
- Require specific Kibana privileges (`feature_discover.generate_report`, `feature_dashboard.download_csv_report`) that may not be assigned

This script talks directly to the Elasticsearch API, so you only need **index-level read access** — no Kibana reporting privileges required.

---

## Features

| Feature | Description |
|---|---|
| **PIT + search_after** | Consistent, efficient pagination — no scroll context timeouts |
| **Sliced parallel export** | Split query across N threads for 3–5x throughput on large datasets |
| **Progress bar + ETA** | Real-time progress via `tqdm` with estimated time remaining |
| **Resumability** | Saves cursor to `.state.json` — resume after crashes with `--resume` |
| **Smart timezone detection** | Auto-detects UTC vs. local time from the query (see below) |
| **Adaptive batch sizing** | Starts at 5,000 docs/page; scales up to 10,000 if fast, down to 500 if slow |
| **_source filtering** | When specific fields are listed, filters at the shard level for performance |
| **Crash-safe writes** | Flushes CSV after each batch so partial results are always usable |

---

## Files

| File | Purpose |
|---|---|
| `es_csv_export.py` | Main script |
| `query.json` | Elasticsearch bool query (default, no need to pass as argument) |
| `fields.txt` | Fields to include in CSV (default, no need to pass as argument) |
| `es_clusters_config.json` | Cluster connection config (same format as `es_role_auto_update`) |

---

## Quick Start

```bash
# 1. Install dependencies
pip install requests pytz tqdm

# 2. Configure your cluster(s) in es_clusters_config.json

# 3. Put your query in query.json (see "How to Get the Query" below)

# 4. List the fields you want in fields.txt (or leave empty for all fields)

# 5. Run
python es_csv_export.py --cluster prod --index filebeat-*
```

That's it. The script reads `query.json`, `fields.txt`, and `es_clusters_config.json` automatically — no need to pass them as arguments.

---

## How to Get the Query from Kibana

1. Open **Discover** in Kibana
2. Build your query and filters as usual
3. Click **Inspect** (top right bar)
4. Click the **Request** tab
5. Copy the `"query"` → `"bool"` block into `query.json`

The script also accepts the **full** Kibana inspect payload (with `sort`, `_source`, etc.) — it automatically extracts just the `bool` block.

### query.json format

```json
{
  "bool": {
    "filter": [
      {
        "range": {
          "@timestamp": {
            "format": "strict_date_optional_time",
            "gte": "2025-12-24T05:00:00.000Z",
            "lte": "2026-03-24T15:22:59.972Z"
          }
        }
      },
      {
        "match_phrase": {
          "kubernetes.namespace": "hs-sql-console"
        }
      }
    ],
    "must_not": [
      {
        "match_phrase": {
          "user.name": "yatanasov"
        }
      }
    ]
  }
}
```

---

## Timezone Handling

The script intelligently handles `@timestamp` conversion based on what's in your query:

| Query timestamp format | Behavior | Example |
|---|---|---|
| Ends with `Z` or `+00:00` | **No conversion** — already absolute UTC | `2025-12-24T05:00:00.000Z` |
| Has `time_zone` in range filter | **Converts** using that timezone | `"time_zone": "US/Eastern"` |
| `--timezone` flag passed | **Overrides** auto-detection | `--timezone US/Pacific` |

For your current query with timestamps ending in `Z`, the `@timestamp` values will be exported exactly as Elasticsearch returns them (UTC) — no unnecessary conversion.

### Example: Query with timezone

If your query uses relative times with a timezone:

```json
{
  "range": {
    "@timestamp": {
      "gte": "now-90d",
      "lte": "now",
      "time_zone": "US/Eastern"
    }
  }
}
```

The script will auto-detect `US/Eastern` and convert `@timestamp` values accordingly.

---

## fields.txt Format

```text
# One field per line. Comments start with #.
# If this file is empty or missing, ALL fields are returned.
@timestamp
message
user.name
```

---

## Usage Examples

```bash
# Basic — uses all defaults (query.json, fields.txt, es_clusters_config.json)
python es_csv_export.py --cluster prod --index filebeat-*

# Custom output filename
python es_csv_export.py --cluster prod --index filebeat-* --output audit_report.csv

# Export ALL fields (empty fields.txt or no fields.txt file)
python es_csv_export.py --cluster prod --index filebeat-*

# Parallel export with 4 slices (for 100k+ documents)
python es_csv_export.py --cluster prod --index filebeat-* --slices 4

# Parallel with 8 slices for millions of docs
python es_csv_export.py --cluster prod --index filebeat-* --slices 8

# Limit export to 50,000 documents
python es_csv_export.py --cluster prod --index filebeat-* --max-docs 50000

# Resume after interruption (Ctrl+C or crash)
python es_csv_export.py --cluster prod --index filebeat-* \
    --output results.csv --resume

# Direct URL + API key (no config file)
python es_csv_export.py --url https://es:9200 --api-key <key> \
    --index filebeat-*

# Override timezone explicitly
python es_csv_export.py --cluster prod --index filebeat-* \
    --timezone US/Eastern

# Custom query and fields files
python es_csv_export.py --cluster prod --index filebeat-* \
    --query my_query.json --fields my_fields.txt

# List available clusters
python es_csv_export.py --list-clusters
```

---

## CLI Options

| Flag | Default | Description |
|---|---|---|
| `--cluster` | — | Cluster name from `es_clusters_config.json` |
| `--url` | — | Direct Elasticsearch URL (bypasses config file) |
| `--api-key` | — | Direct API key (use with `--url`) |
| `--config` | `es_clusters_config.json` | Path to cluster config file |
| `--index` | **(required)** | Index pattern, e.g. `filebeat-*` |
| `--query` | `query.json` | Path to query file |
| `--fields` | `fields.txt` | Path to fields file |
| `--output` | auto-generated | Output CSV filename |
| `--timezone` | auto-detected | Override timezone for `@timestamp` |
| `--batch-size` | `5000` | Initial docs per page (adaptive: 500–10,000) |
| `--max-docs` | `0` (unlimited) | Cap total exported documents |
| `--slices` | `0` (single-threaded) | Parallel slices (recommended: 2–8) |
| `--resume` | `false` | Resume interrupted export (single-threaded only) |
| `--verify-ssl` | `false` | Verify TLS certificates |
| `--list-clusters` | — | List clusters from config and exit |

---

## Performance Guide

| Dataset size | Recommended settings |
|---|---|
| < 10,000 docs | Defaults (single-threaded, batch 5000) |
| 10k – 100k docs | Defaults (adaptive batching handles it) |
| 100k – 1M docs | `--slices 4` |
| 1M – 10M docs | `--slices 8 --batch-size 10000` |
| > 10M docs | `--slices 8 --batch-size 10000 --max-docs` to limit |

### How sliced parallel export works

Each slice opens its own PIT (Point-in-Time) and paginates through a disjoint subset of the data using Elasticsearch's `slice` API. Results from all slices are merged and sorted by `@timestamp` before writing to CSV. This gives near-linear throughput scaling with the number of slices.

### Adaptive batch sizing

The script monitors response times and automatically adjusts the batch size:
- Response < 2 seconds → increase batch by 1,000 (up to 10,000)
- Response > 10 seconds → decrease batch by 1,000 (down to 500)

This prevents timeouts on clusters under load while maximizing throughput when the cluster is fast.

---

## Resumability

If the export is interrupted (Ctrl+C, network failure, crash), the script saves its pagination cursor to a state file (`<output>.state.json`).

```bash
# Original export (interrupted)
python es_csv_export.py --cluster prod --index filebeat-* --output big_export.csv
# ^C at 150,000 docs

# Resume from where it stopped
python es_csv_export.py --cluster prod --index filebeat-* \
    --output big_export.csv --resume
# Continues from doc 150,001
```

The state file is automatically cleaned up when the export completes successfully. Resume is only supported in single-threaded mode (not with `--slices`).

---

## es_clusters_config.json

Same format used by `es_role_auto_update.py` and `es_apikey_inventory.py`:

```json
{
  "clusters": {
    "prod": {
      "url": "https://prod-elasticsearch.example.com:9200",
      "api_key": "YOUR_PROD_API_KEY_HERE",
      "verify_ssl": false,
      "description": "Production Elasticsearch cluster"
    },
    "ccs": {
      "url": "https://ccs-elasticsearch.example.com:9200",
      "api_key": "YOUR_CCS_API_KEY_HERE",
      "verify_ssl": false,
      "description": "Cross-Cluster Search cluster"
    }
  }
}
```

Supports both API key auth (`api_key`) and basic auth (`username` + `password`).

---

## Dependencies

```bash
pip install requests pytz tqdm
```

| Package | Required | Purpose |
|---|---|---|
| `requests` | Yes | HTTP client for Elasticsearch API |
| `pytz` | Yes | Timezone conversion |
| `tqdm` | No (recommended) | Progress bar with ETA |

The script works without `tqdm` but will log a warning and skip the progress bar.

---

## Troubleshooting

| Issue | Solution |
|---|---|
| `Failed to open PIT` | Check index pattern, permissions, or if the index exists |
| Empty CSV | Verify the query returns results in Kibana Discover first |
| Slow export | Use `--slices 4` or `--slices 8` for parallel export |
| Memory issues with `--slices` | Sliced mode holds all rows in memory before writing; use single-threaded for very large exports |
| SSL errors | Use `--verify-ssl` if certs are valid, or ensure `verify_ssl: false` in config |
| `tqdm not installed` warning | `pip install tqdm` for progress bar support |
