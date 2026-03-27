#!/usr/bin/env python3
"""
es_csv_export.py - Elasticsearch CSV Export Tool
=================================================
Bypasses Kibana's CSV export limitations (empty CSVs, permission issues,
row count caps) by querying Elasticsearch directly and streaming results
into a CSV file.

Supports:
    - Local indices   : filebeat-*, sdp_amdb, logs-*
    - Remote indices   : prod:filebeat-*, qa:filebeat-*, dev:accounts
    - Mixed patterns   : filebeat-*,prod:filebeat-*

Pagination strategy:
    - Local indices  → PIT + search_after  (preferred — consistent, no timeout)
    - Remote indices → Scroll API          (PIT does not support CCS)

Features:
    - Smart local vs. remote detection and API selection
    - Sliced parallel export for massive local datasets
    - Progress bar with ETA (tqdm)
    - Resumability — saves cursor state for crash recovery
    - Smart timezone detection from query timestamps
    - _source filtering at the shard level for performance
    - Adaptive batch sizing based on response times

Usage:
    python es_csv_export.py --cluster prod --index filebeat-*
    python es_csv_export.py --cluster ccs  --index prod:filebeat-*
    python es_csv_export.py --cluster ccs  --index "prod:filebeat-*,qa:filebeat-*"
    python es_csv_export.py --cluster prod --index filebeat-* --slices 4
    python es_csv_export.py --cluster prod --index filebeat-* --resume

Files:
    query.json               - The Elasticsearch bool query (just the "bool" block).
    fields.txt               - One field name per line. If empty/missing, all fields returned.
    es_clusters_config.json  - Cluster connection config (same format as es_role_auto_update).

Author : Olamide Olajide
Created: 2026-03-25
Updated: 2026-03-26 — Added CCS remote index support (scroll API fallback)
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

import pytz
import requests

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Suppress insecure-request warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants / Defaults
# ---------------------------------------------------------------------------
DEFAULT_CONFIG_FILE = "es_clusters_config.json"
DEFAULT_QUERY_FILE = "query.json"
DEFAULT_FIELDS_FILE = "fields.txt"
DEFAULT_BATCH_SIZE = 5000
MAX_BATCH_SIZE = 10000
MIN_BATCH_SIZE = 500
BATCH_SCALE_UP_THRESHOLD = 2.0  # seconds — scale up if response faster
BATCH_SCALE_DOWN_THRESHOLD = 10.0  # seconds — scale down if response slower
STATE_FILE_SUFFIX = ".state.json"
PIT_KEEP_ALIVE = "5m"
SCROLL_KEEP_ALIVE = "5m"


# ---------------------------------------------------------------------------
# Index type detection
# ---------------------------------------------------------------------------


def is_remote_index(index: str) -> bool:
    """
    Detect whether the index pattern contains a CCS remote cluster prefix.

    Remote patterns contain a colon that is NOT part of a date-math expression.
    Examples:
        "prod:filebeat-*"              → True
        "qa:filebeat-*,dev:filebeat-*" → True
        "prod:accounts"                → True
        "filebeat-*"                   → False
        "sdp_amdb"                     → False
        "<filebeat-{now/d}>"           → False  (date math, not CCS)
    """
    # Split on commas to handle multi-index patterns
    for part in index.split(","):
        part = part.strip()
        # Skip date math expressions enclosed in < >
        if part.startswith("<") and ">" in part:
            continue
        if ":" in part:
            return True
    return False


def get_pagination_strategy(index: str) -> str:
    """
    Determine pagination strategy based on index pattern.

    Returns:
        'pit'    — PIT + search_after (local indices only)
        'scroll' — Scroll API (required for CCS remote indices)
    """
    if is_remote_index(index):
        return "scroll"
    return "pit"


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def load_clusters_config(config_path: str) -> dict:
    """
    Load cluster connection details from es_clusters_config.json.

    Expected format (same as es_role_auto_update / es_apikey_inventory):
    {
        "clusters": {
            "prod": {
                "url": "https://es-prod.example.com:9200",
                "api_key": "base64-encoded-api-key",
                "verify_ssl": false,
                "description": "Production cluster"
            }
        }
    }
    """
    path = Path(config_path)
    if not path.is_file():
        logger.error(
            f"Cluster config '{config_path}' not found. "
            f"Create it or pass --url and --api-key directly."
        )
        sys.exit(1)
    with open(path, "r") as fh:
        return json.load(fh)


def resolve_cluster(args, clusters_cfg: dict) -> tuple:
    """Return (url, headers, verify_ssl) for the target cluster."""
    # Direct URL + API key take precedence
    if args.url and args.api_key:
        url = args.url.rstrip("/")
        headers = {
            "Authorization": f"ApiKey {args.api_key}",
            "Content-Type": "application/json",
        }
        return url, headers, args.verify_ssl

    # Fall back to es_clusters_config.json
    clusters = clusters_cfg.get("clusters", {})
    if args.cluster not in clusters:
        logger.error(
            f"Cluster '{args.cluster}' not found in config. "
            f"Available: {list(clusters.keys())}"
        )
        sys.exit(1)

    cfg = clusters[args.cluster]
    url = cfg["url"].rstrip("/")
    verify_ssl = cfg.get("verify_ssl", False)
    api_key = cfg.get("api_key", "")
    username = cfg.get("username", "")
    password = cfg.get("password", "")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif username and password:
        import base64

        creds = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"
    else:
        logger.error(f"No credentials found for cluster '{args.cluster}'.")
        sys.exit(1)

    return url, headers, verify_ssl


# ---------------------------------------------------------------------------
# Query & fields loaders
# ---------------------------------------------------------------------------


def load_query(query_path: str) -> dict:
    """
    Load the bool query from a JSON file.

    Accepts:
      - A JSON object with a top-level "bool" key (preferred).
      - A full Kibana inspect payload (extracts query.bool automatically).
    """
    path = Path(query_path)
    if not path.is_file():
        logger.error(f"Query file '{query_path}' not found.")
        sys.exit(1)

    with open(path, "r") as fh:
        raw = json.load(fh)

    # Full Kibana inspect payload
    if "query" in raw and "bool" in raw.get("query", {}):
        logger.info("Detected full Kibana inspect payload; extracting 'query.bool'.")
        return {"bool": raw["query"]["bool"]}

    if "bool" in raw:
        return {"bool": raw["bool"]}

    logger.warning(
        "No 'bool' key found in query file. Using entire content as the query."
    )
    return raw


def load_fields(fields_path: str) -> list:
    """
    Load the list of fields from a text file (one per line).

    Blank lines and lines starting with '#' are ignored.
    Returns an empty list if the file is missing or empty →
    means 'return all fields'.
    """
    path = Path(fields_path)
    if not path.is_file():
        logger.info(f"Fields file '{fields_path}' not found — will return all fields.")
        return []

    fields = []
    with open(path, "r") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                fields.append(stripped)

    if not fields:
        logger.info("Fields file is empty — will return all fields.")
    else:
        logger.info(f"Fields to export: {fields}")

    return fields


# ---------------------------------------------------------------------------
# Timezone detection from the query
# ---------------------------------------------------------------------------


def detect_timezone_from_query(query: dict):
    """
    Inspect the bool query for a @timestamp range filter.

    Returns:
      - None if timestamps are absolute UTC (end with 'Z' or '+00:00')
      - The 'time_zone' string if present in the range filter
      - None otherwise (no conversion)
    """
    filters = query.get("bool", {}).get("filter", [])
    for f in filters:
        rng = f.get("range", {}).get("@timestamp", {})
        if not rng:
            continue

        # Explicit time_zone in the range filter
        if "time_zone" in rng:
            return rng["time_zone"]

        # Check if gte/lte values are absolute UTC
        for bound in ("gte", "lte", "gt", "lt"):
            val = rng.get(bound, "")
            if isinstance(val, str) and (
                val.endswith("Z") or val.endswith("+00:00")
            ):
                return None

    return None


# ---------------------------------------------------------------------------
# Timestamp conversion
# ---------------------------------------------------------------------------


def make_timestamp_converter(tz_name):
    """
    Return a function that converts @timestamp values.
    If tz_name is None, the converter is a no-op (returns as-is).
    """
    if tz_name is None:
        return lambda v: v if v is not None else ""

    target_tz = pytz.timezone(tz_name)

    def _convert(value):
        if value is None:
            return ""
        try:
            if isinstance(value, (int, float)):
                dt = datetime.utcfromtimestamp(value / 1000.0).replace(
                    tzinfo=pytz.utc
                )
            else:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=pytz.utc)
            return dt.astimezone(target_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            return value

    return _convert


# ---------------------------------------------------------------------------
# Flatten nested dicts for CSV
# ---------------------------------------------------------------------------


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Flatten a nested dict into dot-notation keys."""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        elif isinstance(v, list):
            items[new_key] = " | ".join(str(i) for i in v)
        else:
            items[new_key] = v
    return items


def get_nested_value(source: dict, dotted_key: str):
    """Retrieve a value from a nested dict using dot-notation."""
    keys = dotted_key.split(".")
    current = source
    for k in keys:
        if isinstance(current, dict) and k in current:
            current = current[k]
        else:
            return None
    return current


# ---------------------------------------------------------------------------
# State management for resumability
# ---------------------------------------------------------------------------


def state_file_path(output_path: str) -> str:
    return output_path + STATE_FILE_SUFFIX


def save_state(output_path: str, state: dict):
    """Persist the current pagination cursor so export can be resumed."""
    spath = state_file_path(output_path)
    with open(spath, "w") as fh:
        json.dump(state, fh, indent=2)


def load_state(output_path: str):
    """Load a previously saved state file, or return None."""
    spath = state_file_path(output_path)
    if not Path(spath).is_file():
        return None
    with open(spath, "r") as fh:
        return json.load(fh)


def clear_state(output_path: str):
    spath = state_file_path(output_path)
    if Path(spath).is_file():
        os.remove(spath)
        logger.info("State file removed (export completed successfully).")


# ---------------------------------------------------------------------------
# PIT helpers (local indices only)
# ---------------------------------------------------------------------------


def open_pit(es_url, index, headers, verify_ssl) -> str:
    resp = requests.post(
        f"{es_url}/{index}/_pit?keep_alive={PIT_KEEP_ALIVE}",
        headers=headers,
        verify=verify_ssl,
    )
    if resp.status_code != 200:
        logger.error(f"Failed to open PIT: {resp.status_code} — {resp.text}")
        sys.exit(1)
    return resp.json()["id"]


def close_pit(es_url, pit_id, headers, verify_ssl):
    try:
        requests.delete(
            f"{es_url}/_pit",
            headers=headers,
            json={"id": pit_id},
            verify=verify_ssl,
        )
    except Exception as e:
        logger.warning(f"Failed to close PIT: {e}")


# ---------------------------------------------------------------------------
# Scroll helpers (remote / CCS indices)
# ---------------------------------------------------------------------------


def clear_scroll(es_url, scroll_id, headers, verify_ssl):
    """Explicitly free a scroll context."""
    try:
        requests.delete(
            f"{es_url}/_search/scroll",
            headers=headers,
            json={"scroll_id": scroll_id},
            verify=verify_ssl,
        )
    except Exception as e:
        logger.warning(f"Failed to clear scroll: {e}")


# ---------------------------------------------------------------------------
# Get total hit count (for progress bar)
# ---------------------------------------------------------------------------


def get_total_hits(es_url, index, query, headers, verify_ssl) -> int:
    """Run a _count request to get total matching docs."""
    resp = requests.post(
        f"{es_url}/{index}/_count",
        headers=headers,
        json={"query": query},
        verify=verify_ssl,
    )
    if resp.status_code == 200:
        return resp.json().get("count", 0)
    logger.warning(
        f"_count failed ({resp.status_code}); progress bar may be inaccurate."
    )
    return 0


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _process_hits(hits: list, fields: list, convert_ts) -> list:
    """Convert a list of ES hits into flat row dicts."""
    rows = []
    for hit in hits:
        source = hit.get("_source", {})

        if fields:
            row = {}
            for f in fields:
                val = get_nested_value(source, f)
                if f == "@timestamp" and val is not None:
                    val = convert_ts(val)
                if isinstance(val, (dict, list)):
                    val = json.dumps(val)
                row[f] = val if val is not None else ""
            rows.append(row)
        else:
            flat = flatten_dict(source)
            if "@timestamp" in flat:
                flat["@timestamp"] = convert_ts(flat["@timestamp"])
            rows.append(flat)

    return rows


def _write_csv(rows: list, fields: list, output_path: str) -> int:
    """Write a list of row dicts to a CSV file."""
    if not rows:
        logger.warning("No rows to write.")
        return 0

    if fields:
        fieldnames = fields
    else:
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())
        fieldnames = sorted(all_keys)
        if "@timestamp" in fieldnames:
            fieldnames.remove("@timestamp")
            fieldnames.insert(0, "@timestamp")

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


# ---------------------------------------------------------------------------
# Single-threaded export: PIT + search_after (LOCAL indices)
# ---------------------------------------------------------------------------


def export_pit(
    es_url: str,
    headers: dict,
    index: str,
    query: dict,
    fields: list,
    output_path: str,
    convert_ts,
    batch_size: int,
    max_docs: int,
    verify_ssl: bool,
    resume: bool,
):
    """
    Single-threaded export using PIT + search_after.
    Only works with local indices.
    """
    total_hits = get_total_hits(es_url, index, query, headers, verify_ssl)
    effective_total = min(total_hits, max_docs) if max_docs > 0 else total_hits
    logger.info(f"Total matching documents: {total_hits:,}")
    if max_docs > 0:
        logger.info(f"Will export up to {max_docs:,} documents.")

    # ---- Resume handling ----
    search_after = None
    total_written = 0
    file_mode = "w"

    if resume:
        state = load_state(output_path)
        if state:
            if state.get("strategy") == "scroll":
                logger.warning(
                    "Previous export used scroll API — cannot resume scroll. "
                    "Starting fresh."
                )
            else:
                search_after = state.get("search_after")
                total_written = state.get("total_written", 0)
                file_mode = "a"
                logger.info(
                    f"Resuming from previous state — "
                    f"{total_written:,} docs already written."
                )
        else:
            logger.info("No state file found — starting fresh.")

    # ---- Open PIT ----
    pit_id = open_pit(es_url, index, headers, verify_ssl)
    logger.info("PIT opened.")

    # ---- Build search body ----
    body = {
        "size": batch_size,
        "query": query,
        "sort": [
            {"@timestamp": {"order": "desc", "unmapped_type": "boolean"}},
            {"_doc": {"order": "desc", "unmapped_type": "boolean"}},
        ],
        "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
        "track_total_hits": False,
    }

    if fields:
        body["_source"] = fields
    else:
        body["_source"] = True

    if search_after:
        body["search_after"] = search_after

    # ---- Progress bar ----
    pbar = None
    if TQDM_AVAILABLE and effective_total > 0:
        pbar = tqdm(
            total=effective_total,
            initial=total_written,
            unit="docs",
            desc="Exporting",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} "
            "[{elapsed}<{remaining}, {rate_fmt}]",
        )

    # ---- Pagination ----
    current_batch = batch_size
    all_keys_seen = set()
    writer = None
    csv_file = None

    try:
        csv_file = open(output_path, file_mode, newline="", encoding="utf-8")

        if fields and file_mode == "w":
            writer = csv.DictWriter(
                csv_file, fieldnames=fields, extrasaction="ignore"
            )
            writer.writeheader()
        elif fields and file_mode == "a":
            writer = csv.DictWriter(
                csv_file, fieldnames=fields, extrasaction="ignore"
            )

        page = 0
        while True:
            page += 1
            body["size"] = current_batch

            t0 = time.time()
            resp = requests.post(
                f"{es_url}/_search",
                headers=headers,
                json=body,
                verify=verify_ssl,
            )
            elapsed = time.time() - t0

            if resp.status_code != 200:
                logger.error(
                    f"Search failed: {resp.status_code} — {resp.text}"
                )
                break

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])

            if not hits:
                logger.info("No more hits — pagination complete.")
                break

            # ---- Adaptive batch sizing ----
            old_batch = current_batch
            if (
                elapsed < BATCH_SCALE_UP_THRESHOLD
                and current_batch < MAX_BATCH_SIZE
            ):
                current_batch = min(current_batch + 1000, MAX_BATCH_SIZE)
            elif (
                elapsed > BATCH_SCALE_DOWN_THRESHOLD
                and current_batch > MIN_BATCH_SIZE
            ):
                current_batch = max(current_batch - 1000, MIN_BATCH_SIZE)
            if current_batch != old_batch:
                logger.debug(
                    f"Adaptive batch: {old_batch} → {current_batch} "
                    f"(page took {elapsed:.1f}s)"
                )

            # ---- Process rows ----
            rows = _process_hits(hits, fields, convert_ts)

            if not fields:
                for row in rows:
                    all_keys_seen.update(row.keys())
                if writer is None:
                    sorted_keys = sorted(all_keys_seen)
                    writer = csv.DictWriter(
                        csv_file,
                        fieldnames=sorted_keys,
                        extrasaction="ignore",
                    )
                    if file_mode == "w":
                        writer.writeheader()

            writer.writerows(rows)
            csv_file.flush()
            total_written += len(rows)

            if pbar:
                pbar.update(len(rows))

            # ---- Save state ----
            last_sort = hits[-1]["sort"]
            save_state(
                output_path,
                {
                    "strategy": "pit",
                    "search_after": last_sort,
                    "total_written": total_written,
                    "page": page,
                    "timestamp": datetime.now().isoformat(),
                },
            )

            if max_docs > 0 and total_written >= max_docs:
                logger.info(
                    f"Reached max_docs limit ({max_docs:,}). Stopping."
                )
                break

            body["search_after"] = last_sort
            body["pit"]["id"] = data.get("pit_id", pit_id)

    except KeyboardInterrupt:
        logger.warning(
            "\nExport interrupted. "
            "Run with --resume to continue where you left off."
        )
    finally:
        if pbar:
            pbar.close()
        if csv_file:
            csv_file.close()
        close_pit(
            es_url, body["pit"].get("id", pit_id), headers, verify_ssl
        )

    if not resume or (max_docs == 0 and total_written >= total_hits) or (
        max_docs > 0 and total_written >= max_docs
    ):
        clear_state(output_path)

    logger.info(
        f"Export complete → {output_path}  ({total_written:,} documents)"
    )
    return total_written


# ---------------------------------------------------------------------------
# Single-threaded export: Scroll API (REMOTE / CCS indices)
# ---------------------------------------------------------------------------


def export_scroll(
    es_url: str,
    headers: dict,
    index: str,
    query: dict,
    fields: list,
    output_path: str,
    convert_ts,
    batch_size: int,
    max_docs: int,
    verify_ssl: bool,
):
    """
    Single-threaded export using the Scroll API.
    Required for CCS remote indices (prod:filebeat-*, etc.)
    because PIT does not support cross-cluster index patterns.

    Note: Scroll does not support resumability — if interrupted,
    the export must restart from the beginning.
    """
    total_hits = get_total_hits(es_url, index, query, headers, verify_ssl)
    effective_total = min(total_hits, max_docs) if max_docs > 0 else total_hits
    logger.info(f"Total matching documents: {total_hits:,}")
    if max_docs > 0:
        logger.info(f"Will export up to {max_docs:,} documents.")

    # ---- Initial search with scroll ----
    body = {
        "size": batch_size,
        "query": query,
        "sort": [
            {"@timestamp": {"order": "desc", "unmapped_type": "boolean"}},
            {"_doc": {"order": "desc", "unmapped_type": "boolean"}},
        ],
        "track_total_hits": False,
    }

    if fields:
        body["_source"] = fields
    else:
        body["_source"] = True

    logger.info("Opening scroll context ...")
    resp = requests.post(
        f"{es_url}/{index}/_search?scroll={SCROLL_KEEP_ALIVE}",
        headers=headers,
        json=body,
        verify=verify_ssl,
    )

    if resp.status_code != 200:
        logger.error(
            f"Initial scroll search failed: {resp.status_code} — {resp.text}"
        )
        sys.exit(1)

    data = resp.json()
    scroll_id = data.get("_scroll_id")
    hits = data.get("hits", {}).get("hits", [])

    if not hits:
        logger.info("No matching documents found.")
        clear_scroll(es_url, scroll_id, headers, verify_ssl)
        return 0

    logger.info("Scroll context opened.")

    # ---- Progress bar ----
    pbar = None
    if TQDM_AVAILABLE and effective_total > 0:
        pbar = tqdm(
            total=effective_total,
            unit="docs",
            desc="Exporting",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} "
            "[{elapsed}<{remaining}, {rate_fmt}]",
        )

    # ---- Pagination ----
    total_written = 0
    all_keys_seen = set()
    writer = None
    csv_file = None

    try:
        csv_file = open(output_path, "w", newline="", encoding="utf-8")

        if fields:
            writer = csv.DictWriter(
                csv_file, fieldnames=fields, extrasaction="ignore"
            )
            writer.writeheader()

        page = 0
        while hits:
            page += 1

            rows = _process_hits(hits, fields, convert_ts)

            if not fields:
                for row in rows:
                    all_keys_seen.update(row.keys())
                if writer is None:
                    sorted_keys = sorted(all_keys_seen)
                    writer = csv.DictWriter(
                        csv_file,
                        fieldnames=sorted_keys,
                        extrasaction="ignore",
                    )
                    writer.writeheader()

            writer.writerows(rows)
            csv_file.flush()
            total_written += len(rows)

            if pbar:
                pbar.update(len(rows))

            # Save state (limited — scroll_id cannot be resumed after crash,
            # but we track progress for logging)
            save_state(
                output_path,
                {
                    "strategy": "scroll",
                    "total_written": total_written,
                    "page": page,
                    "timestamp": datetime.now().isoformat(),
                    "note": "Scroll exports cannot be resumed — restart required",
                },
            )

            if max_docs > 0 and total_written >= max_docs:
                logger.info(
                    f"Reached max_docs limit ({max_docs:,}). Stopping."
                )
                break

            # ---- Next scroll page ----
            resp = requests.post(
                f"{es_url}/_search/scroll",
                headers=headers,
                json={
                    "scroll": SCROLL_KEEP_ALIVE,
                    "scroll_id": scroll_id,
                },
                verify=verify_ssl,
            )

            if resp.status_code != 200:
                logger.error(
                    f"Scroll failed: {resp.status_code} — {resp.text}"
                )
                break

            data = resp.json()
            scroll_id = data.get("_scroll_id", scroll_id)
            hits = data.get("hits", {}).get("hits", [])

        if not hits:
            logger.info("No more hits — pagination complete.")

    except KeyboardInterrupt:
        logger.warning(
            "\nExport interrupted. Scroll exports cannot be resumed — "
            "you will need to restart the export."
        )
    finally:
        if pbar:
            pbar.close()
        if csv_file:
            csv_file.close()
        clear_scroll(es_url, scroll_id, headers, verify_ssl)
        logger.info("Scroll context cleared.")

    clear_state(output_path)
    logger.info(
        f"Export complete → {output_path}  ({total_written:,} documents)"
    )
    return total_written


# ---------------------------------------------------------------------------
# Sliced parallel export (PIT — local indices only)
# ---------------------------------------------------------------------------


def export_slice(
    slice_id: int,
    num_slices: int,
    es_url: str,
    headers: dict,
    index: str,
    query: dict,
    fields: list,
    convert_ts,
    batch_size: int,
    max_docs_per_slice: int,
    verify_ssl: bool,
    pbar,
    pbar_lock: Lock,
) -> list:
    """Export a single slice of the data. Returns a list of row dicts."""
    pit_id = open_pit(es_url, index, headers, verify_ssl)

    body = {
        "size": batch_size,
        "query": query,
        "sort": [
            {"@timestamp": {"order": "desc", "unmapped_type": "boolean"}},
            {"_doc": {"order": "desc", "unmapped_type": "boolean"}},
        ],
        "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
        "slice": {"id": slice_id, "max": num_slices},
        "track_total_hits": False,
    }

    if fields:
        body["_source"] = fields
    else:
        body["_source"] = True

    all_rows = []
    total_fetched = 0

    try:
        while True:
            resp = requests.post(
                f"{es_url}/_search",
                headers=headers,
                json=body,
                verify=verify_ssl,
            )

            if resp.status_code != 200:
                logger.error(
                    f"[slice {slice_id}] Search failed: "
                    f"{resp.status_code} — {resp.text}"
                )
                break

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])

            if not hits:
                break

            rows = _process_hits(hits, fields, convert_ts)
            all_rows.extend(rows)
            total_fetched += len(rows)

            if pbar:
                with pbar_lock:
                    pbar.update(len(rows))

            if (
                max_docs_per_slice > 0
                and total_fetched >= max_docs_per_slice
            ):
                break

            body["search_after"] = hits[-1]["sort"]
            body["pit"]["id"] = data.get("pit_id", pit_id)

    finally:
        close_pit(
            es_url, body["pit"].get("id", pit_id), headers, verify_ssl
        )

    return all_rows


def export_parallel(
    es_url: str,
    headers: dict,
    index: str,
    query: dict,
    fields: list,
    output_path: str,
    convert_ts,
    batch_size: int,
    max_docs: int,
    verify_ssl: bool,
    num_slices: int,
):
    """
    Parallel export using sliced PIT (local indices only).
    Results are merged, sorted, and written to a single CSV.
    """
    total_hits = get_total_hits(es_url, index, query, headers, verify_ssl)
    effective_total = (
        min(total_hits, max_docs) if max_docs > 0 else total_hits
    )
    logger.info(f"Total matching documents: {total_hits:,}")
    logger.info(f"Exporting with {num_slices} parallel slices ...")

    max_per_slice = 0
    if max_docs > 0:
        max_per_slice = (max_docs // num_slices) + 1

    pbar = None
    pbar_lock = Lock()
    if TQDM_AVAILABLE and effective_total > 0:
        pbar = tqdm(
            total=effective_total,
            unit="docs",
            desc="Exporting",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} "
            "[{elapsed}<{remaining}, {rate_fmt}]",
        )

    all_rows = []
    rows_lock = Lock()

    def _run_slice(sid):
        rows = export_slice(
            slice_id=sid,
            num_slices=num_slices,
            es_url=es_url,
            headers=headers,
            index=index,
            query=query,
            fields=fields,
            convert_ts=convert_ts,
            batch_size=batch_size,
            max_docs_per_slice=max_per_slice,
            verify_ssl=verify_ssl,
            pbar=pbar,
            pbar_lock=pbar_lock,
        )
        with rows_lock:
            all_rows.extend(rows)
        return len(rows)

    with ThreadPoolExecutor(max_workers=num_slices) as executor:
        futures = {
            executor.submit(_run_slice, i): i for i in range(num_slices)
        }
        for future in as_completed(futures):
            sid = futures[future]
            try:
                count = future.result()
                logger.info(f"  Slice {sid} returned {count:,} docs.")
            except Exception as e:
                logger.error(f"  Slice {sid} failed: {e}")

    if pbar:
        pbar.close()

    # Sort by @timestamp descending
    ts_key = "@timestamp"
    if any(ts_key in row for row in all_rows[:1]):
        all_rows.sort(key=lambda r: r.get(ts_key, ""), reverse=True)

    if max_docs > 0 and len(all_rows) > max_docs:
        all_rows = all_rows[:max_docs]

    total_written = _write_csv(all_rows, fields, output_path)
    logger.info(
        f"Export complete → {output_path}  ({total_written:,} documents)"
    )
    return total_written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser(
        description="Export Elasticsearch query results to CSV, "
        "bypassing Kibana CSV limitations. Supports both local and "
        "CCS remote index patterns.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Files (defaults — no need to pass as arguments):
  {DEFAULT_QUERY_FILE}                Bool query
  {DEFAULT_FIELDS_FILE}               Fields to export (empty = all)
  {DEFAULT_CONFIG_FILE}  Cluster connection config

Index patterns:
  Local  : filebeat-*, sdp_amdb, logs-*
  Remote : prod:filebeat-*, qa:filebeat-*, dev:accounts
  Mixed  : filebeat-*,prod:filebeat-*

  Remote patterns automatically use the Scroll API (PIT does not
  support CCS). Local patterns use PIT + search_after.

Examples:
  # Local index on prod cluster
  python es_csv_export.py --cluster prod --index filebeat-*

  # Remote index on CCS cluster
  python es_csv_export.py --cluster ccs --index prod:filebeat-*

  # Multiple remote indices on CCS
  python es_csv_export.py --cluster ccs \\
      --index "prod:filebeat-*,qa:filebeat-*"

  # Parallel export (local indices only)
  python es_csv_export.py --cluster prod --index filebeat-* --slices 4

  # Resume interrupted export (PIT mode only)
  python es_csv_export.py --cluster prod --index filebeat-* \\
      --output results.csv --resume

  # Direct URL (no config file)
  python es_csv_export.py --url https://es:9200 --api-key <key> \\
      --index filebeat-*
        """,
    )

    conn = p.add_argument_group("Connection")
    conn.add_argument(
        "--cluster", help="Cluster name from es_clusters_config.json"
    )
    conn.add_argument(
        "--url", help="Elasticsearch URL (direct, bypasses config)"
    )
    conn.add_argument("--api-key", help="Elasticsearch API key (direct)")
    conn.add_argument(
        "--config",
        default=DEFAULT_CONFIG_FILE,
        help=f"Path to cluster config file (default: {DEFAULT_CONFIG_FILE})",
    )

    p.add_argument(
        "--index",
        required=True,
        help="Index pattern. Local: 'filebeat-*'. "
        "Remote: 'prod:filebeat-*'. "
        "Multiple: 'prod:filebeat-*,qa:filebeat-*'",
    )
    p.add_argument(
        "--query",
        default=DEFAULT_QUERY_FILE,
        help=f"Path to query JSON file (default: {DEFAULT_QUERY_FILE})",
    )
    p.add_argument(
        "--fields",
        default=DEFAULT_FIELDS_FILE,
        help=f"Path to fields file (default: {DEFAULT_FIELDS_FILE})",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: auto-generated)",
    )
    p.add_argument(
        "--timezone",
        default=None,
        help="Override timezone for @timestamp. "
        "Auto-detected from query by default.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Initial docs per page (default: {DEFAULT_BATCH_SIZE}). "
        f"Adaptive: {MIN_BATCH_SIZE}–{MAX_BATCH_SIZE} (PIT mode only).",
    )
    p.add_argument(
        "--max-docs",
        type=int,
        default=0,
        help="Max documents to export; 0 = unlimited (default: 0)",
    )
    p.add_argument(
        "--slices",
        type=int,
        default=0,
        help="Parallel slices (local indices only). "
        "Recommended: 2–8 for datasets > 100k docs.",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume interrupted export (PIT mode only — not supported "
        "for remote/CCS indices).",
    )
    p.add_argument(
        "--verify-ssl",
        action="store_true",
        default=False,
        help="Verify SSL certificates (default: False)",
    )
    p.add_argument(
        "--list-clusters",
        action="store_true",
        help="List available clusters from config and exit.",
    )

    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    args = parse_args()

    # ---- List clusters ----
    if args.list_clusters:
        cfg = load_clusters_config(args.config)
        clusters = cfg.get("clusters", {})
        print(f"\nAvailable clusters in {args.config}:\n")
        for name, info in clusters.items():
            desc = info.get("description", "")
            url = info.get("url", "")
            print(f"  {name:20s}  {url:45s}  {desc}")
        print()
        sys.exit(0)

    # ---- Validate connection args ----
    if not args.cluster and not (args.url and args.api_key):
        logger.error(
            "Provide either --cluster (with es_clusters_config.json) "
            "or both --url and --api-key."
        )
        sys.exit(1)

    # ---- Load cluster config ----
    clusters_cfg = {}
    if args.cluster:
        clusters_cfg = load_clusters_config(args.config)

    es_url, es_headers, verify_ssl = resolve_cluster(args, clusters_cfg)

    # ---- Load query ----
    query = load_query(args.query)
    logger.info(f"Query loaded from: {args.query}")

    # ---- Load fields ----
    fields = load_fields(args.fields)

    # ---- Determine pagination strategy ----
    strategy = get_pagination_strategy(args.index)
    if strategy == "scroll":
        logger.info(
            f"Remote index detected ('{args.index}') → using Scroll API "
            f"(PIT does not support CCS remote indices)."
        )
    else:
        logger.info(
            f"Local index detected ('{args.index}') → using PIT + search_after."
        )

    # ---- Validate options vs strategy ----
    if strategy == "scroll" and args.slices and args.slices > 1:
        logger.warning(
            "--slices is not supported with remote/CCS indices "
            "(sliced PIT requires local indices). "
            "Falling back to single-threaded scroll."
        )
        args.slices = 0

    if strategy == "scroll" and args.resume:
        logger.warning(
            "--resume is not supported with remote/CCS indices "
            "(scroll contexts cannot be resumed). Starting fresh."
        )
        args.resume = False

    # ---- Timezone handling ----
    if args.timezone:
        tz = args.timezone
        logger.info(f"Timezone override: {tz}")
    else:
        tz = detect_timezone_from_query(query)
        if tz:
            logger.info(
                f"Timezone detected from query range filter: {tz}"
            )
        else:
            logger.info(
                "Timestamps are absolute UTC — "
                "@timestamp exported as-is (no conversion)."
            )

    convert_ts = make_timestamp_converter(tz)

    # ---- Default output filename ----
    if args.output is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Clean up index name for filename
        safe_index = (
            args.index.replace("*", "all")
            .replace("-", "_")
            .replace(":", "_")
            .replace(",", "_")
        )
        args.output = f"{safe_index}_export_{ts}.csv"

    # ---- Summary ----
    logger.info("=" * 60)
    logger.info(f"Cluster       : {es_url}")
    logger.info(f"Index         : {args.index}")
    logger.info(f"Strategy      : {strategy.upper()}"
                f"{'  (CCS remote)' if strategy == 'scroll' else '  (local)'}")
    logger.info(
        f"Timezone      : {tz if tz else 'none (UTC passthrough)'}"
    )
    logger.info(
        f"Batch size    : {args.batch_size}"
        + (
            f" (adaptive {MIN_BATCH_SIZE}–{MAX_BATCH_SIZE})"
            if strategy == "pit"
            else ""
        )
    )
    logger.info(
        f"Max docs      : "
        f"{args.max_docs if args.max_docs else 'unlimited'}"
    )
    logger.info(
        f"Slices        : "
        f"{args.slices if args.slices else 'single-threaded'}"
    )
    logger.info(f"Resume        : {args.resume}")
    logger.info(f"Output        : {args.output}")
    logger.info("=" * 60)

    if not TQDM_AVAILABLE:
        logger.warning(
            "tqdm not installed — no progress bar. "
            "Install: pip install tqdm"
        )

    # ---- Run export ----
    t_start = time.time()

    if strategy == "scroll":
        # Remote / CCS indices → Scroll API
        export_scroll(
            es_url=es_url,
            headers=es_headers,
            index=args.index,
            query=query,
            fields=fields,
            output_path=args.output,
            convert_ts=convert_ts,
            batch_size=args.batch_size,
            max_docs=args.max_docs,
            verify_ssl=verify_ssl,
        )
    elif args.slices and args.slices > 1:
        # Local indices, parallel
        if args.resume:
            logger.warning(
                "--resume not supported with --slices. Ignoring."
            )
        export_parallel(
            es_url=es_url,
            headers=es_headers,
            index=args.index,
            query=query,
            fields=fields,
            output_path=args.output,
            convert_ts=convert_ts,
            batch_size=args.batch_size,
            max_docs=args.max_docs,
            verify_ssl=verify_ssl,
            num_slices=args.slices,
        )
    else:
        # Local indices, single-threaded
        export_pit(
            es_url=es_url,
            headers=es_headers,
            index=args.index,
            query=query,
            fields=fields,
            output_path=args.output,
            convert_ts=convert_ts,
            batch_size=args.batch_size,
            max_docs=args.max_docs,
            verify_ssl=verify_ssl,
            resume=args.resume,
        )

    elapsed = time.time() - t_start
    mins, secs = divmod(int(elapsed), 60)
    logger.info(f"Total time: {mins}m {secs}s ({elapsed:.1f}s)")


if __name__ == "__main__":
    main()
