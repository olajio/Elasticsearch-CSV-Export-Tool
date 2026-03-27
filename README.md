# Elasticsearch CSV Export Tool

Bypass Kibana's CSV export limitations — empty CSVs, row count caps, missing `feature_discover.generate_report` privileges — by querying Elasticsearch directly and streaming results to CSV.

Supports both **local indices** (`filebeat-*`) and **CCS remote indices** (`prod:filebeat-*`).

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
| **Local + Remote index support** | Automatically detects CCS patterns (`cluster:index`) and selects the right API |
| **PIT + search_after** | Consistent pagination for local indices — no scroll timeouts |
| **Scroll API** | Automatic fallback for CCS remote indices (PIT does not support CCS) |
| **Sliced parallel export** | Split query across N threads for 3–5x throughput (local only) |
| **Progress bar + ETA** | Real-time progress via `tqdm` with estimated time remaining |
| **Resumability** | Saves cursor to `.state.json` for crash recovery (PIT mode only) |
| **Smart timezone detection** | Auto-detects UTC vs. local time from the query |
| **Adaptive batch sizing** | Scales between 500–10,000 docs/page based on response times (PIT mode) |
| **_source filtering** | Filters at the shard level when specific fields are listed |
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

# 3. Put your query in query.json
#    (Copy the "bool" block from Kibana Inspect → Request)

# 4. List the fields you want in fields.txt
#    (Leave empty to export ALL fields)

# 5. Run
python es_csv_export.py --cluster prod --index filebeat-*
```

That's it. The script reads `query.json`, `fields.txt`, and `es_clusters_config.json` automatically.

---

## Index Patterns: Local vs. Remote

The script automatically detects whether you're querying a local or remote (CCS) index and selects the appropriate pagination API.

| Pattern | Type | API Used | Cluster Flag |
|---|---|---|---|
| `filebeat-*` | Local | PIT + search_after | `--cluster prod` |
| `sdp_amdb` | Local | PIT + search_after | `--cluster prod` |
| `prod:filebeat-*` | Remote (CCS) | Scroll | `--cluster ccs` |
| `qa:filebeat-*` | Remote (CCS) | Scroll | `--cluster ccs` |
| `dev:accounts` | Remote (CCS) | Scroll | `--cluster ccs` |
| `prod:filebeat-*,qa:filebeat-*` | Remote (CCS) | Scroll | `--cluster ccs` |

### Why two different APIs?

Elasticsearch's **Point-in-Time (PIT)** API provides the best pagination experience (consistent snapshots, no timeout issues), but it **does not support cross-cluster index patterns**. Calling `POST prod:filebeat-*/_pit` will fail.

The **Scroll API** works with CCS remote indices but has some trade-offs: scroll contexts can time out on very long exports, and interrupted scroll exports cannot be resumed (you must restart from the beginning).

The script handles all of this automatically — you just provide the index pattern and it selects the right strategy.

### Feature availability by strategy

| Feature | PIT (local) | Scroll (remote/CCS) |
|---|---|---|
| Pagination | search_after | scroll context |
| Adaptive batch sizing | Yes | No (fixed batch) |
| Sliced parallel export | Yes (`--slices`) | Not supported |
| Resumability (`--resume`) | Yes | Not supported |
| Progress bar | Yes | Yes |
| Max docs limit | Yes | Yes |

---

## How to Get the Query from Kibana

1. Open **Discover** in Kibana
2. Build your query and filters as usual
3. Click **Inspect** (top right bar)
4. Click the **Request** tab
5. Copy the `"query"` → `"bool"` block into `query.json`

The script also accepts the **full** Kibana inspect payload — it automatically extracts the `bool` block.

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

### Local indices (prod, qa, dev clusters)

```bash
# Basic — uses all defaults
python es_csv_export.py --cluster prod --index filebeat-*

# Specific local index
python es_csv_export.py --cluster prod --index sdp_amdb

# Parallel export with 4 slices (for 100k+ docs)
python es_csv_export.py --cluster prod --index filebeat-* --slices 4

# Resume interrupted export
python es_csv_export.py --cluster prod --index filebeat-* \
    --output results.csv --resume
```

### Remote indices (CCS cluster)

```bash
# Single remote index
python es_csv_export.py --cluster ccs --index prod:filebeat-*

# Multiple remote indices
python es_csv_export.py --cluster ccs \
    --index "prod:filebeat-*,qa:filebeat-*"

# Remote index with specific index name
python es_csv_export.py --cluster ccs --index prod:accounts

# Remote index with doc limit
python es_csv_export.py --cluster ccs --index dev:filebeat-* \
    --max-docs 50000
```

### Other options

```bash
# Custom output filename
python es_csv_export.py --cluster prod --index filebeat-* --output audit.csv

# Direct URL + API key (no config file)
python es_csv_export.py --url https://es:9200 --api-key <key> \
    --index filebeat-*

# Override timezone
python es_csv_export.py --cluster prod --index filebeat-* \
    --timezone US/Eastern

# Custom query and fields files
python es_csv_export.py --cluster ccs --index prod:filebeat-* \
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
| `--index` | **(required)** | Index pattern: `filebeat-*`, `prod:filebeat-*`, etc. |
| `--query` | `query.json` | Path to query file |
| `--fields` | `fields.txt` | Path to fields file |
| `--output` | auto-generated | Output CSV filename |
| `--timezone` | auto-detected | Override timezone for `@timestamp` |
| `--batch-size` | `5000` | Docs per page (adaptive in PIT mode: 500–10,000) |
| `--max-docs` | `0` (unlimited) | Cap total exported documents |
| `--slices` | `0` (single-threaded) | Parallel slices (local indices only, recommended: 2–8) |
| `--resume` | `false` | Resume interrupted export (PIT mode only) |
| `--verify-ssl` | `false` | Verify TLS certificates |
| `--list-clusters` | — | List clusters from config and exit |

---

## Performance Guide

| Dataset size | Index type | Recommended settings |
|---|---|---|
| < 10,000 docs | Local | Defaults |
| 10k – 100k docs | Local | Defaults (adaptive batching handles it) |
| 100k – 1M docs | Local | `--slices 4` |
| 1M – 10M docs | Local | `--slices 8 --batch-size 10000` |
| Any size | Remote (CCS) | Single-threaded only (scroll), increase `--batch-size` for throughput |

### Tips for large CCS exports

Since sliced parallel export is not available for remote indices, you can maximize throughput by:
- Increasing `--batch-size` to 10000 (scroll supports this)
- Narrowing the time range in your query to reduce total docs
- Running separate exports for each remote cluster and merging CSVs

---

## Resumability

If a PIT-mode export is interrupted (Ctrl+C, network failure, crash), the script saves its pagination cursor to `<output>.state.json`.

```bash
# Original export (interrupted)
python es_csv_export.py --cluster prod --index filebeat-* --output big.csv
# ^C at 150,000 docs

# Resume from where it stopped
python es_csv_export.py --cluster prod --index filebeat-* \
    --output big.csv --resume
# Continues from doc 150,001
```

The state file is automatically cleaned up on successful completion.

**Note:** Resumability is only available for local indices (PIT mode). Scroll-based exports (remote/CCS indices) cannot be resumed — if interrupted, the export must restart from the beginning.

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

The script works without `tqdm` but will skip the progress bar.

---

## Troubleshooting

| Issue | Solution |
|---|---|
| `Failed to open PIT` on remote index | Expected — the script should auto-detect and use scroll. If not, check the index pattern format |
| Empty CSV | Verify the query returns results in Kibana Discover first |
| Slow CCS export | Increase `--batch-size 10000`. Slices are not supported for CCS |
| `--slices` ignored for CCS | Expected — sliced PIT does not support remote indices |
| `--resume` ignored for CCS | Expected — scroll contexts cannot be resumed |
| Scroll timeout | For very large CCS exports, the 5-minute scroll timeout may expire between pages. Consider narrowing the time range |
| SSL errors | Use `--verify-ssl` if certs are valid, or ensure `verify_ssl: false` in config |
