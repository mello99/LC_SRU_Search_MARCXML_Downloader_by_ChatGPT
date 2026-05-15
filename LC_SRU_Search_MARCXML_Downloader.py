# Daily LC SRU Search MARCXML Downloader
# Queries the Library of Congress SRU endpoint and saves new MARCXML records.

from __future__ import annotations

import csv
import logging
import os
import re
import time
import random
import unicodedata
from datetime import datetime
from pathlib import Path

import requests

try:
    from defusedxml import ElementTree as ET  # safer XML parsing
except ImportError:
    import xml.etree.ElementTree as ET  # fallback if defusedxml not installed

# ============================
# CONFIGURATION
# All values can be overridden with environment variables for portability.
# Defaults are resolved relative to the script's own directory so the script
# works correctly regardless of which directory you run it from.
# ============================

# Directory containing this script — used to anchor all default paths.
SCRIPT_DIR = Path(__file__).resolve().parent

# Path to a plain-text file with one SRU CQL query per line.
QUERY_FILE = Path(os.environ.get("SRU_QUERY_FILE", SCRIPT_DIR / "queries.txt"))

# Root directory for output MARCXML files (organised into daily sub-folders).
OUTPUT_DIR = Path(os.environ.get("SRU_OUTPUT_DIR", SCRIPT_DIR / "output"))

# Directory for persistent logs and the seen-IDs deduplication file.
LOG_DIR = Path(os.environ.get("SRU_LOG_DIR", SCRIPT_DIR / "logs"))

# LC SRU base URL (Z39.50/SRU gateway).
# Note: LC's SRU endpoint does not support HTTPS — http:// is intentional.
BASE_URL = os.environ.get("SRU_BASE_URL", "http://lx2.loc.gov:210/lcdb")

# Polite crawl delay range (seconds) – please be kind to LC's servers.
MIN_DELAY = int(os.environ.get("SRU_MIN_DELAY", 8))
MAX_DELAY = int(os.environ.get("SRU_MAX_DELAY", 12))

MAX_RETRIES = int(os.environ.get("SRU_MAX_RETRIES", 3))
BACKOFF_BASE = int(os.environ.get("SRU_BACKOFF_BASE", 6))

USER_AGENT = "LCSRUHarvester/1.0 (+https://example.org)"

# ============================
# SETUP
# ============================

def setup() -> tuple[Path, Path, Path, str]:
    """Create required directories, configure logging, and return runtime paths."""
    today = datetime.now().strftime("%Y-%m-%d")
    query_log_csv = LOG_DIR / "sru_query_log.csv"
    seen_ids_csv  = LOG_DIR / "seen_marc_ids.csv"
    log_file      = LOG_DIR / "sru_run_log.txt"
    day_folder    = OUTPUT_DIR / today

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day_folder.mkdir(exist_ok=True)

    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    return day_folder, query_log_csv, seen_ids_csv, today


# ============================
# CSV HELPERS
# ============================

def ensure_csv(path: Path, header: list[str]) -> None:
    """Write the header row if the CSV does not yet exist or is empty."""
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", encoding="utf-8", newline="") as fh:
            csv.writer(fh).writerow(header)


def load_seen_ids(path: Path) -> set[str]:
    """Return the set of MARC record IDs already harvested in previous runs."""
    seen: set[str] = set()
    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            next(reader, None)  # skip header
            for row in reader:
                if row:
                    seen.add(row[0])
    return seen


def append_seen_ids(path: Path, new_ids: list[str], today: str) -> None:
    """Append newly harvested record IDs with today's date."""
    if not new_ids:
        return
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        for rid in new_ids:
            writer.writerow([rid, today])


# ============================
# QUERY HELPERS
# ============================

def load_queries(path: Path) -> list[str]:
    """Read raw CQL queries from a plain-text file, one per line."""
    with path.open("r", encoding="utf-8") as fh:
        return [line.strip().strip('"') for line in fh if line.strip()]


def query_to_filename(query: str) -> str:
    """
    Convert a CQL query string into a safe, human-readable filename stem.

    Steps:
      1. Strip characters illegal on Windows/macOS/Linux filesystems.
      2. Collapse non-alphanumeric runs to underscores.
      3. Normalise Unicode to ASCII.
    """
    name = re.sub(r'[\\/:*?"<>|]', " ", query)
    name = re.sub(r"[^A-Za-z0-9]+", " ", name)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"\s+", "_", name).strip("_")
    return name


# ============================
# HTTP / SRU FETCHING
# ============================

def fetch_sru(query: str) -> str | None:
    """
    Fetch MARCXML from the LC SRU endpoint for a single CQL query.

    The query is passed as a raw string; requests handles all URL encoding
    safely via the `params` dict, preventing any injection via crafted queries.
    Retries with exponential back-off on transient failures.

    Args:
        query: A raw (un-encoded) CQL query string.

    Returns:
        The raw XML response text, or None if all retries are exhausted.
    """
    params = {
        "version": "1.1",
        "operation": "searchRetrieve",
        "query": query,
        "startRecord": "1",
        "maximumRecords": "25",
        "recordSchema": "marcxml",
    }
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text

        except requests.RequestException as exc:
            logging.warning("Attempt %d/%d failed for query %r: %s", attempt, MAX_RETRIES, query, exc)
            if attempt == MAX_RETRIES:
                logging.error("All retries exhausted for query %r.", query)
                return None
            time.sleep(BACKOFF_BASE * (2 ** (attempt - 1)))

    return None  # unreachable, but satisfies type checkers


# ============================
# MARCXML PARSING / WRITING
# ============================

def parse_records(xml_text: str) -> list[tuple[str | None, ET.Element]]:
    """
    Parse MARCXML and return a list of (record_id, element) tuples.
    record_id is the value of the 001 control field, or None if absent.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logging.error("XML parse error: %s", exc)
        return []

    results: list[tuple[str | None, ET.Element]] = []
    ns = "{http://www.loc.gov/MARC21/slim}"
    for record in root.findall(f".//{ns}record"):
        record_id = next(
            (cf.text.strip()
             for cf in record.findall(f"{ns}controlfield")
             if cf.get("tag") == "001" and cf.text),
            None,
        )
        results.append((record_id, record))
    return results


def build_marcxml_collection(records: list[ET.Element]) -> str:
    """Wrap a list of MARC record elements in a <collection> and serialise to UTF-8 XML."""
    collection = ET.Element("{http://www.loc.gov/MARC21/slim}collection")
    collection.extend(records)
    return ET.tostring(collection, encoding="utf-8", xml_declaration=True).decode("utf-8")


# ============================
# MAIN
# ============================

def main() -> None:
    day_folder, query_log_csv, seen_ids_csv, today = setup()

    ensure_csv(query_log_csv, ["date", "query", "xml_file_path", "total_records",
                              "new_records", "skipped_records", "error_message"])
    ensure_csv(seen_ids_csv, ["record_id", "first_seen_date"])

    seen_ids = load_seen_ids(seen_ids_csv)
    logging.info("Loaded %d previously seen record IDs.", len(seen_ids))

    try:
        queries = load_queries(QUERY_FILE)
    except FileNotFoundError:
        logging.error("Query file not found: %s", QUERY_FILE)
        print(f"ERROR: Query file not found: {QUERY_FILE}")
        return

    with query_log_csv.open("a", encoding="utf-8", newline="") as log_fh:
        log_writer = csv.writer(log_fh)

        for query in queries:
            print(f"[SEARCH] {query}")
            logging.info("Starting query: %s", query)

            xml_file_path = ""
            total = new_count = skipped = 0
            error_msg = ""

            xml_text = fetch_sru(query)

            if xml_text is None:
                error_msg = "Failed to retrieve SRU response after retries."
                print(f"  → ERROR: {error_msg}")
                logging.error(error_msg)

            else:
                record_tuples = parse_records(xml_text)
                total = len(record_tuples)

                new_elements: list[ET.Element] = []
                new_ids: list[str] = []

                for rec_id, elem in record_tuples:
                    if rec_id is not None and rec_id in seen_ids:
                        skipped += 1
                        continue
                    new_elements.append(elem)
                    new_count += 1
                    if rec_id is not None:
                        new_ids.append(rec_id)
                        seen_ids.add(rec_id)

                if new_elements:
                    out_file = day_folder / f"{query_to_filename(query)}_NEW.marcxml.xml"
                    out_file.write_text(build_marcxml_collection(new_elements), encoding="utf-8")
                    xml_file_path = str(out_file)
                    print(f"  → Wrote {new_count} new records to {out_file}")
                    logging.info("Wrote %d new records for query %r to %s", new_count, query, out_file)
                    append_seen_ids(seen_ids_csv, new_ids, today)
                else:
                    print("  → No new records found for this query.")
                    logging.info("No new records for query %r.", query)

            log_writer.writerow([today, query, xml_file_path,
                                  total, new_count, skipped, error_msg])

            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            print(f"  → Waiting {delay:.1f}s before next query …")
            time.sleep(delay)

    logging.info("=== Completed daily SRU harvesting run ===")
    print("\n*** DONE ***")


if __name__ == "__main__":
    main()
