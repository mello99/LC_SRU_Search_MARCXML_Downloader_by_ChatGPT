#!/usr/bin/env python3
"""
loc_sru_query.py
----------------
Reads a CSV file containing either a "title" or "ISBN" column, queries each
value against the Library of Congress SRU server, downloads the MARCXML for
matched records, and writes a timestamped log file.

Usage:
    python loc_sru_query.py <input.csv> [--output-dir <dir>]

Arguments:
    input.csv       Path to the input CSV file.
    --output-dir    Directory for MARCXML files and the log (default: ./loc_output)
"""

import argparse
import csv
import logging
import os
import re
import sys
import time
import random
import urllib.parse
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SRU_BASE_URL = "http://lx2.loc.gov:210/lcdb"
SRU_VERSION = "1.1"
MAX_RECORDS = 10          # Maximum records to retrieve / display per query
MAX_RETRIES = 3           # Maximum retry attempts for a failed query
RETRY_WAIT_MIN = 6        # Minimum seconds between retries
RETRY_WAIT_MAX = 10       # Maximum seconds between retries (adds jitter)
POLITE_WAIT_MIN = 10      # Minimum polite crawl delay between queries (seconds)
POLITE_WAIT_MAX = 15      # Maximum polite crawl delay between queries (seconds)

# Accepted column header variants (lowercased for comparison)
TITLE_HEADERS = {"title"}
ISBN_HEADERS = {"isbn", "isbn-13", "isbn-10", "isbn10", "isbn13"}

MARCXML_NS = "http://www.loc.gov/MARC21/slim"

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(output_dir: Path) -> logging.Logger:
    """Configure a logger that writes to both stdout and a timestamped log file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = output_dir / f"loc_sru_query_{timestamp}.log"

    logger = logging.getLogger("loc_sru")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info("Log file: %s", log_path)
    return logger


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def detect_query_column(fieldnames: list[str]) -> tuple[str, str]:
    """
    Inspect CSV headers and return (canonical_type, actual_header).
    canonical_type is either 'title' or 'isbn'.
    Raises ValueError if neither is found.
    """
    for name in fieldnames:
        lower = name.strip().lower()
        if lower in TITLE_HEADERS:
            return ("title", name)
        if lower in ISBN_HEADERS:
            return ("isbn", name)
    raise ValueError(
        f"CSV must contain a 'title' or ISBN column. Found: {fieldnames}"
    )


def read_queries(csv_path: Path) -> tuple[str, list[str]]:
    """
    Parse the CSV and return (query_type, list_of_query_strings).
    Empty / whitespace-only values are skipped.
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("CSV file appears to be empty.")
        query_type, col = detect_query_column(list(reader.fieldnames))
        queries = [
            row[col].strip()
            for row in reader
            if row[col].strip()
        ]
    return query_type, queries


# ---------------------------------------------------------------------------
# SRU helpers
# ---------------------------------------------------------------------------

def build_sru_url(query_type: str, value: str) -> str:
    """Construct an SRU searchRetrieve URL for the given query type and value."""
    if query_type == "title":
        # LC MARC convention requires a space on both sides of a colon that
        # separates a main title from its subtitle (e.g. "Title : Subtitle").
        # Normalise any colon in the input to that form before querying, so the
        # string matches the catalogued form exactly.
        # Also escape any internal double-quotes to keep the CQL string valid.
        normalised = re.sub(r'\s*:\s*', ' : ', value)
        escaped = normalised.replace('"', '\\"')
        cql = f'dc.title = "{escaped}"'
    else:
        # Standard ISBN index
        clean_isbn = re.sub(r"[-\s]", "", value)   # strip hyphens / spaces
        cql = f'bath.isbn = "{clean_isbn}"'

    params = {
        "operation": "searchRetrieve",
        "version": SRU_VERSION,
        "query": cql,
        "maximumRecords": str(MAX_RECORDS),
        "recordSchema": "marcxml",
    }
    return f"{SRU_BASE_URL}?{urllib.parse.urlencode(params)}"


def fetch_url(url: str, logger: logging.Logger) -> bytes | None:
    """
    Perform an HTTP GET and return the raw response body.
    Returns None on network / HTTP error (caller handles retries).
    """
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "loc_sru_query/1.0 (library research tool)"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        logger.warning("HTTP %s for URL: %s", exc.code, url)
    except urllib.error.URLError as exc:
        logger.warning("URL error (%s) for: %s", exc.reason, url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unexpected error fetching URL: %s", exc)
    return None


def parse_sru_response(raw_xml: bytes) -> tuple[int, list[ET.Element]]:
    """
    Parse an SRU response and return (number_of_records, list_of_record_elements).
    The record elements are the <record> children inside each <recordData>.
    """
    root = ET.fromstring(raw_xml)
    ns = {"sru": "http://www.loc.gov/zing/srw/"}

    # numberOfRecords is a required element in SRU 1.1
    num_el = root.find("sru:numberOfRecords", ns)
    total = int(num_el.text.strip()) if num_el is not None and num_el.text else 0

    records = []
    for rd in root.findall(".//sru:recordData", ns):
        # Each recordData contains one <record> in MARCXML namespace
        rec = rd.find(f"{{{MARCXML_NS}}}record")
        if rec is not None:
            records.append(rec)

    return total, records


def get_marc_title(record: ET.Element) -> str:
    """Extract the title from MARC field 245 subfield a (best-effort)."""
    for field in record.findall(f"{{{MARCXML_NS}}}datafield[@tag='245']"):
        for sub in field.findall(f"{{{MARCXML_NS}}}subfield[@code='a']"):
            return sub.text.strip() if sub.text else ""
    return "(title not found)"


def get_marc_control_number(record: ET.Element) -> str:
    """Return the MARC 001 control number, or a placeholder."""
    cf = record.find(f"{{{MARCXML_NS}}}controlfield[@tag='001']")
    return cf.text.strip() if (cf is not None and cf.text) else "unknown"


# ---------------------------------------------------------------------------
# Query + download logic
# ---------------------------------------------------------------------------

def query_with_retries(
    query_type: str,
    value: str,
    logger: logging.Logger,
) -> tuple[int, list[ET.Element]] | None:
    """
    Query the SRU server with up to MAX_RETRIES retries.
    Returns (total_found, records) on success, or None if all attempts fail.
    """
    url = build_sru_url(query_type, value)
    logger.debug("SRU URL: %s", url)

    for attempt in range(1, MAX_RETRIES + 1):
        raw = fetch_url(url, logger)
        if raw is not None:
            try:
                return parse_sru_response(raw)
            except ET.ParseError as exc:
                logger.warning("XML parse error on attempt %d: %s", attempt, exc)

        if attempt < MAX_RETRIES:
            wait = random.uniform(RETRY_WAIT_MIN, RETRY_WAIT_MAX)
            logger.info(
                "  Attempt %d/%d failed – retrying in %.1f s …",
                attempt, MAX_RETRIES, wait,
            )
            time.sleep(wait)

    logger.error("All %d attempts failed for: %s", MAX_RETRIES, value)
    return None


def save_marcxml(
    record: ET.Element,
    output_dir: Path,
    stem: str,
    index: int,
    logger: logging.Logger,
) -> Path:
    """
    Wrap a single MARC record element in a MARCXML collection and save it.
    Returns the path of the saved file.
    """
    # Build a minimal MARCXML wrapper
    collection = ET.Element(
        "collection",
        attrib={"xmlns": MARCXML_NS},
    )
    collection.append(record)
    tree = ET.ElementTree(collection)
    ET.indent(tree, space="  ")   # Python ≥ 3.9

    safe_stem = re.sub(r'[^\w\-]+', '_', stem)[:80]   # filesystem-safe name
    filename = output_dir / f"{safe_stem}_{index}.xml"

    with open(filename, "wb") as fh:
        tree.write(fh, xml_declaration=True, encoding="utf-8")

    logger.debug("Saved MARCXML: %s", filename)
    return filename


def process_query(
    query_type: str,
    value: str,
    output_dir: Path,
    logger: logging.Logger,
) -> dict:
    """
    Run one query end-to-end and return a result summary dict.
    """
    result = {
        "value": value,
        "query_type": query_type,
        "status": None,         # 'not_found' | 'found' | 'multiple' | 'error'
        "total": 0,
        "files": [],
        "flag_review": False,
    }

    logger.info("Querying %s: %s", query_type.upper(), value)
    response = query_with_retries(query_type, value, logger)

    if response is None:
        result["status"] = "error"
        logger.error("  → FAILED (no response after retries)")
        return result

    total, records = response

    if total == 0:
        result["status"] = "not_found"
        logger.info("  → NOT FOUND")
        return result

    result["total"] = total

    if total > 1:
        result["status"] = "multiple"
        result["flag_review"] = True
        logger.warning(
            "  → MULTIPLE RESULTS (%d total, downloading up to %d) – FLAGGED FOR REVIEW",
            total, len(records),
        )
        for i, rec in enumerate(records, start=1):
            logger.info("    [%d] %s  (control# %s)",
                        i, get_marc_title(rec), get_marc_control_number(rec))
    else:
        result["status"] = "found"
        logger.info("  → FOUND: %s", get_marc_title(records[0]))

    # Download MARCXML for all retrieved records
    for i, rec in enumerate(records, start=1):
        path = save_marcxml(rec, output_dir, value, i, logger)
        result["files"].append(str(path))

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query the Library of Congress SRU server and download MARCXML.",
    )
    parser.add_argument("csv_file", help="Input CSV file with Title or ISBN column.")
    parser.add_argument(
        "--output-dir", default="loc_output",
        help="Directory for MARCXML files and the log (default: ./loc_output).",
    )
    return parser.parse_args()


def print_summary(results: list[dict], logger: logging.Logger) -> None:
    """Log a concise summary table after all queries are complete."""
    found     = [r for r in results if r["status"] in ("found", "multiple")]
    not_found = [r for r in results if r["status"] == "not_found"]
    errors    = [r for r in results if r["status"] == "error"]
    flagged   = [r for r in results if r["flag_review"]]

    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("  Total queries  : %d", len(results))
    logger.info("  Found          : %d", len(found))
    logger.info("  Not found      : %d", len(not_found))
    logger.info("  Errors         : %d", len(errors))
    logger.info("  Flagged review : %d", len(flagged))

    if flagged:
        logger.info("")
        logger.info("Items flagged for review (multiple matches):")
        for r in flagged:
            logger.info("  • %s  (%d results)", r["value"], r["total"])

    if not_found:
        logger.info("")
        logger.info("Items not found:")
        for r in not_found:
            logger.info("  • %s", r["value"])

    if errors:
        logger.info("")
        logger.info("Items that errored:")
        for r in errors:
            logger.info("  • %s", r["value"])

    logger.info("=" * 60)


def main() -> None:
    args = parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)
    logger.info("Starting LOC SRU query script")
    logger.info("Input file  : %s", args.csv_file)
    logger.info("Output dir  : %s", output_dir.resolve())

    # --- Read CSV ---
    try:
        query_type, queries = read_queries(Path(args.csv_file))
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Could not read CSV: %s", exc)
        sys.exit(1)

    logger.info("Query type  : %s", query_type.upper())
    logger.info("Items found : %d", len(queries))

    if not queries:
        logger.warning("No queryable values found in CSV. Exiting.")
        sys.exit(0)

    # --- Process each query ---
    results = []
    for idx, value in enumerate(queries, start=1):
        logger.info("-" * 60)
        logger.info("[%d/%d]", idx, len(queries))
        result = process_query(query_type, value, output_dir, logger)
        results.append(result)

        # Polite crawl delay – skip after the last item
        if idx < len(queries):
            wait = random.uniform(POLITE_WAIT_MIN, POLITE_WAIT_MAX)
            logger.debug("Waiting %.1f s before next query …", wait)
            time.sleep(wait)

    # --- Summary ---
    print_summary(results, logger)
    logger.info("Done.")


if __name__ == "__main__":
    main()
