# Created with ChatGPT 5.1
# Daily LC SRU Search MARCXML Downloader

import os
import csv
import re
import unicodedata
import time
import random
import logging
import requests
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# ============================
# CONFIGURATION
# ============================

QUERY_FILE = r"C:\path\to\query_file.csv"

OUTPUT_DIR = Path(r"C:\path\to\output_directory")
LOG_DIR = Path(r"C:\path\to\LC_SRU_Logs")

QUERY_LOG_CSV = LOG_DIR / "sru_query_log.csv"
SEEN_IDS_CSV = LOG_DIR / "seen_marc_ids.csv"

BASE_URL = "http://lx2.loc.gov:210/lcdb"
USER_AGENT = "Mozilla/5.0 (compatible; LCSRUHarvester/1.0; +https://example.org)"

# Be nice to LC's servers!
MIN_DELAY = 8
MAX_DELAY = 12

MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 5

# ============================
# SETUP
# ============================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "sru_run_log.txt"
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

TODAY_STR = datetime.now().strftime("%Y-%m-%d")
DAY_FOLDER = OUTPUT_DIR / TODAY_STR
DAY_FOLDER.mkdir(exist_ok=True)

# ============================
# UTILITIES
# ============================

def init_csv_with_header(path: Path, header: list[str]) -> None:
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(header)


def load_seen_ids(path: Path) -> set[str]:
    seen = set()
    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    seen.add(row[0])
    return seen


def append_seen_ids(path: Path, new_ids: list[str]) -> None:
    if new_ids:
        with path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for rid in new_ids:
                writer.writerow([rid, TODAY_STR])

def pretty_filename_from_query(query: str) -> str:
    """
    Convert a URL-encoded SRU query into a clean, safe, human-friendly filename.
    - Replaces %20 and punctuation with underscores
    - Removes all Windows-illegal characters
    - Normalizes Unicode
    - Collapses multiple underscores
    """

    # Replace encoded spaces with real spaces first
    name = query.replace("%20", " ")

    # Decode %XX sequences
    try:
        name = urllib.parse.unquote(name)
    except Exception:
        pass

    # Remove all Windows-illegal filename characters
    name = re.sub(r'[\\/:*?"<>|]', " ", name)

    # Replace ANY group of non-alphanumeric chars with a space
    name = re.sub(r"[^A-Za-z0-9]+", " ", name)

    # Normalize Unicode → ASCII
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")

    # Collapse whitespace → single underscore
    name = re.sub(r"\s+", "_", name)

    # Trim leading/trailing underscores
    name = name.strip("_")

    return name


# ============================
# NEW: QUERY NORMALIZATION
# ============================

def normalize_query(query: str) -> str:
    """
    URL-encode queries from queries.txt so LC SRU will accept them.
    Spaces + punctuation → %20.
    Preserve SRU operators (= * : ,) so CQL still works.
    """
    query = query.strip().strip('"')
    return urllib.parse.quote(query, safe="=*:,")  # keep CQL operators intact


# ============================
# HTTP / SRU FETCHING
# ============================

def fetch_sru_with_retries(encoded_query: str) -> str | None:
    """
    Fetch MARCXML from LC SRU. Receives a *pre-encoded* query string.
    """
    # UPDATED: no encoding here → already encoded
    url = (
        f"{BASE_URL}?version=1.1&operation=searchRetrieve"
        f"&query=\"{encoded_query}\""
        f"&startRecord=1&maximumRecords=25"
        f"&recordSchema=marcxml"
    )

    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.text

        except Exception as e:
            logging.warning(
                f"Attempt {attempt}/{MAX_RETRIES} failed for encoded query '{encoded_query}': {e}"
            )
            if attempt == MAX_RETRIES:
                logging.error(
                    f"All retries failed for query '{encoded_query}'. Giving up."
                )
                return None

            time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))

    return None


# ============================
# MARCXML PARSING
# ============================

NS_MARC = "{http://www.loc.gov/MARC21/slim}"

def extract_records_and_ids(xml_text: str):
    records = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logging.error(f"XML parse error: {e}")
        return records

    for record_elem in root.findall(f".//{NS_MARC}record"):
        record_id = None
        for cf in record_elem.findall(f"{NS_MARC}controlfield"):
            if cf.get("tag") == "001" and cf.text:
                record_id = cf.text.strip()
                break
        records.append((record_id, record_elem))

    return records


def build_marcxml_collection(records):
    collection = ET.Element(f"{NS_MARC}collection")
    for rec in records:
        collection.append(rec)
    return ET.tostring(collection, encoding="utf-8", xml_declaration=True).decode("utf-8")


# ============================
# MAIN
# ============================

def main() -> None:
    init_csv_with_header(
        QUERY_LOG_CSV,
        ["date", "query", "xml_file_path", "total_records", "new_records", "skipped_records", "error_message"],
    )
    init_csv_with_header(
        SEEN_IDS_CSV,
        ["record_id", "first_seen_date"],
    )

    seen_ids = load_seen_ids(SEEN_IDS_CSV)
    logging.info(f"Loaded {len(seen_ids)} previously seen record IDs.")

    # ============================
    # UPDATED: LOAD + ENCODE QUERIES
    # ============================
    try:
        with open(QUERY_FILE, "r", encoding="utf-8") as f:
            raw_queries = [line.strip().strip('"') for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"Query file not found: {QUERY_FILE}")
        print(f"ERROR: Query file not found: {QUERY_FILE}")
        return

    # Normalize (URL-encode) all queries
    queries = [normalize_query(q) for q in raw_queries]

    # ============================

    with QUERY_LOG_CSV.open("a", encoding="utf-8", newline="") as log_f:
        log_writer = csv.writer(log_f)

        for q in queries:
            print(f"[SEARCH] {q}")
            logging.info(f"Starting query: {q}")

            error_message = ""
            xml_file_path_str = ""
            total_records = 0
            new_records_count = 0
            skipped_records = 0

            # Pass encoded query directly
            xml = fetch_sru_with_retries(q)

            if xml is None:
                error_message = "Failed to retrieve SRU response after retries."
                print(f"  → ERROR: {error_message}")
                logging.error(error_message)

            else:
                record_tuples = extract_records_and_ids(xml)
                total_records = len(record_tuples)

                new_record_elements = []
                new_ids = []

                for rec_id, rec_elem in record_tuples:
                    if rec_id is None:
                        new_record_elements.append(rec_elem)
                        new_records_count += 1
                    elif rec_id in seen_ids:
                        skipped_records += 1
                    else:
                        new_record_elements.append(rec_elem)
                        new_ids.append(rec_id)
                        new_records_count += 1
                        seen_ids.add(rec_id)

                if new_record_elements:
                    safe_name = pretty_filename_from_query(q)
                    out_file = DAY_FOLDER / f"{safe_name}_NEW.marcxml.xml"

                    xml_out = build_marcxml_collection(new_record_elements)

                    with out_file.open("w", encoding="utf-8") as out_f:
                        out_f.write(xml_out)

                    xml_file_path_str = str(out_file)
                    print(f"  → Wrote {new_records_count} new records to {out_file}")
                    logging.info(
                        f"Wrote {new_records_count} new records for query '{q}' to {out_file}"
                    )

                    append_seen_ids(SEEN_IDS_CSV, new_ids)

                else:
                    print("  → No new records found for this query.")
                    logging.info(f"No new records for query '{q}'.")

            log_writer.writerow(
                [TODAY_STR, q, xml_file_path_str, total_records,
                 new_records_count, skipped_records, error_message]
            )

            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            print(f"  → Waiting {delay:.1f} seconds before next query...")
            time.sleep(delay)

    logging.info("=== Completed daily SRU harvesting run ===")
    print("\n*** DONE ***")


if __name__ == "__main__":
    main()
