import requests
import xml.etree.ElementTree as ET
import json
import time
import logging
from datetime import datetime

# Configuration
XML_URL = "https://www.bcp-bonn.de/stellplatz/bcpext.xml"
OUTPUT_FILE = "parking_data.json"
LOG_FILE = "parking_fetcher.log"
MAX_RUNS = 60               # Max number of fetches
FETCH_INTERVAL = 60         # Seconds between fetches
MAX_RETRIES = 3             # Retry count on failure
RETRY_BACKOFF = 5           # Seconds between retries

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def fetch_xml_with_retries(url, max_retries=MAX_RETRIES):
    """Fetch the XML data from the URL with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logging.info(f"Successfully fetched XML data on attempt {attempt}")
            return response.content
        except requests.RequestException as e:
            logging.warning(f"Fetch attempt {attempt} failed: {e}")
            if attempt < max_retries:
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                logging.error("Max retries reached. Giving up on this iteration.")
                return None

def parse_xml_to_json(xml_data):
    """Convert XML parking data to JSON serializable Python objects."""
    root = ET.fromstring(xml_data)
    result = []
    for parkhaus in root.findall('parkhaus'):
        data = {child.tag: child.text for child in parkhaus}
        result.append(data)
    logging.info(f"Parsed {len(result)} parking entries from XML.")
    return {
        "timestamp": datetime.now().isoformat(),
        "data": result
    }

def write_json_entry(entry, filepath):
    """Append a new entry to the JSON file."""
    try:
        # Read existing content
        try:
            with open(filepath, "r") as f:
                existing_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        existing_data.append(entry)

        with open(filepath, "w") as f:
            json.dump(existing_data, f, indent=2)
        logging.info(f"Appended data to {filepath}")
    except Exception as e:
        logging.error(f"Failed to write data to JSON: {e}")

def main():
    logging.info("Starting parking data fetcher.")
    run_counter = 0

    while run_counter < MAX_RUNS:
        logging.info(f"Run {run_counter + 1}/{MAX_RUNS}")

        xml_data = fetch_xml_with_retries(XML_URL)

        if xml_data:
            try:
                json_entry = parse_xml_to_json(xml_data)
                write_json_entry(json_entry, OUTPUT_FILE)
            except Exception as e:
                logging.error(f"Error while parsing or writing data: {e}")
        else:
            logging.warning("Skipping this run due to fetch failure.")

        run_counter += 1
        time.sleep(FETCH_INTERVAL)

    logging.info("Reached maximum run count. Exiting.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Unexpected error occurred: {e}", exc_info=True)
