import argparse
import concurrent.futures
import functools
import json
import logging
import re
import sys
import tempfile
import threading
import time
import traceback
import urllib
import uuid
import warnings
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Optional

import boto3
import colorlog
import lxml.html as LH
import pandas as pd
import requests
import urllib3
from tqdm import tqdm
from botocore import UNSIGNED
from botocore.client import Config
from bs4 import BeautifulSoup
from PIL import Image

from captcha_solver import get_text
from process_metadata import (
    SupremeCourtS3Processor,
)

# Configure root logger with colors
root_logger = logging.getLogger()
root_logger.setLevel("INFO")

# Remove any existing handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Add colored handler
console_handler = colorlog.StreamHandler()
console_handler.setFormatter(
    colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
)
root_logger.addHandler(console_handler)

# Get logger for this module
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message=".*pin_memory.*not supported on MPS.*")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


root_url = "https://scr.sci.gov.in"
output_dir = Path("./sc_data")
START_DATE = "1950-01-01"

# Updated payload for Supreme Court based on curl request
DEFAULT_SEARCH_PAYLOAD = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=10&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&act=&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=3&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ncn=&bool_opt=&sort_flg=&ajax_req=true&app_token="

# Updated PDF payload for Supreme Court
pdf_link_payload = "val=0&lang_flg=undefined&path=2025_5_275_330&citation_year=2025&fcourt_type=3&nc_display=2025INSC555&ajax_req=true"

PAGE_SIZE = 1000
MATH_CAPTCHA = False
NO_CAPTCHA_BATCH_SIZE = 25
lock = threading.Lock()

CHANGE_LOG_PREVIEW_LIMIT = 20

captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir = Path("./captcha-tmp")
temp_files_dir = Path("./temp-files")
captcha_failures_dir.mkdir(parents=True, exist_ok=True)
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)
temp_files_dir.mkdir(parents=True, exist_ok=True)

S3_BUCKET = "indian-supreme-court-judgments-test"
S3_PREFIX = ""
LOCAL_DIR = Path("./local_sc_judgments_data")
PACKAGES_DIR = Path("./packages")
IST = timezone(timedelta(hours=5, minutes=30))


def get_json_file(file_path) -> dict:
    try:
        with open(file_path, "r") as f:
            content = f.read().strip()
            if not content:
                return {}  # Return empty dict for empty file
            return json.loads(content)
    except FileNotFoundError:
        return {}  # Return empty dict if file doesn't exist
    except json.JSONDecodeError:
        logging.warning(f"Invalid JSON in {file_path}, returning empty dict")
        return {}


def get_tracking_data():
    try:
        tracking_data = get_json_file("./sc_track.json")
    except FileNotFoundError:
        tracking_data = {}
    return tracking_data


def save_tracking_data(tracking_data):
    with open("./sc_track.json", "w") as f:
        json.dump(tracking_data, f)


def save_tracking_date(tracking_data):
    # acquire a lock
    lock.acquire()
    save_tracking_data(tracking_data)
    # release the lock
    lock.release()


def get_new_date_range(
    last_date: str, day_step: int = 30
) -> tuple[str | None, str | None]:
    last_date_dt = datetime.strptime(last_date, "%Y-%m-%d")
    new_from_date_dt = last_date_dt + timedelta(days=1)
    new_to_date_dt = new_from_date_dt + timedelta(days=day_step - 1)
    if new_from_date_dt.date() > datetime.now().date():
        return None, None

    if new_to_date_dt.date() > datetime.now().date():
        new_to_date_dt = datetime.now().date()
    new_from_date = new_from_date_dt.strftime("%Y-%m-%d")
    new_to_date = new_to_date_dt.strftime("%Y-%m-%d")
    return new_from_date, new_to_date


def get_date_ranges_to_process(start_date=None, end_date=None, day_step=30):
    """
    Generate date ranges to process for Supreme Court.
    If start_date is provided but no end_date, use current date as end_date.
    If neither is provided, use tracking data to determine the next date range.
    """
    # If start_date is provided but end_date is not, use current date as end_date
    if start_date and not end_date:
        end_date = datetime.now().date().strftime("%Y-%m-%d")

    if start_date and end_date:
        # Convert string dates to datetime objects
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Cap end_date at current date (don't process future dates)
        current_date_dt = datetime.now().date()
        current_date_str = current_date_dt.strftime("%Y-%m-%d")
        current_date_parsed = datetime.strptime(current_date_str, "%Y-%m-%d")

        if end_date_dt.date() > current_date_dt:
            logger.info(
                f"End date {end_date} is in the future, capping at current date {current_date_str}"
            )
            end_date_dt = current_date_parsed

        # Generate date ranges with specified step
        current_date = start_date_dt
        while current_date <= end_date_dt:
            # Calculate range end, ensuring we don't exceed end_date_dt
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            # Move to next day after range_end
            current_date = range_end + timedelta(days=1)
    else:
        # Use tracking data to get next date range
        tracking_data = get_tracking_data()
        last_date = tracking_data.get("last_date", START_DATE)

        # Process from last_date to current date in chunks
        current_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        # Use date() to avoid time component issues
        end_date_dt = datetime.now().date()
        end_date_parsed = datetime.strptime(
            end_date_dt.strftime("%Y-%m-%d"), "%Y-%m-%d"
        )

        while current_date <= end_date_parsed:
            # Calculate range end, ensuring we don't exceed end_date_parsed
            range_end = min(
                current_date + timedelta(days=day_step - 1), end_date_parsed
            )
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            # Move to next day after range_end
            current_date = range_end + timedelta(days=1)

            # If we're at the last chunk and it's smaller than day_step, log it
            if range_end == end_date_parsed:
                days_in_chunk = (range_end - current_date + timedelta(days=1)).days
                if days_in_chunk < day_step:
                    logger.info(f"Processing final chunk of {days_in_chunk} days")


def extract_year_from_path(path):
    """Extract year from judgment path like '2025_5_275_330' or 'S_1991_3_524_533'"""
    # Extract year from path pattern like 2025_5_275_330
    parts = path.split("_")
    if len(parts) >= 1 and parts[0].isdigit() and len(parts[0]) == 4:
        return int(parts[0])

    # Handle patterns like S_1991_3_524_533 (older format)
    if (
        len(parts) >= 2
        and parts[0] == "S"
        and parts[1].isdigit()
        and len(parts[1]) == 4
    ):
        return int(parts[1])

    # Log the problematic path for debugging
    logger.warning(f"Could not extract year from path: {path} (parts: {parts})")
    raise ValueError(f"Could not extract year from path: {path}")


class SCDateTask:
    """A task representing a date range to process for Supreme Court"""

    def __init__(self, from_date, to_date):
        self.id = str(uuid.uuid4())
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self):
        return f"SCDateTask(id={self.id}, from_date={self.from_date}, to_date={self.to_date})"


def generate_tasks(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    day_step: int = 1,
) -> Generator[SCDateTask, None, None]:
    """Generate tasks for processing Supreme Court date ranges as a generator"""
    for from_date, to_date in get_date_ranges_to_process(
        start_date, end_date, day_step
    ):
        yield SCDateTask(from_date, to_date)


def process_task(task, archive_manager=None):
    """Process a single date task"""
    try:
        downloader = Downloader(task, archive_manager)
        downloader.download()
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        traceback.print_exc()


def run(
    start_date=None,
    end_date=None,
    day_step=1,
    max_workers=5,
    package_on_startup=True,
    archive_manager=None,
):
    """
    Run the downloader with optional parameters using Python's multiprocessing
    with a generator that yields tasks on demand.
    """
    # Package existing individual files into zip archives on startup
    if package_on_startup:
        logger.info("Packaging existing individual files into zip archives...")
        try:
            # Import here to avoid circular imports
            from package_zip_files import ZipPackager

            packager = ZipPackager()
            packager.package_all()
            logger.info("Startup packaging completed")

            # Always cleanup individual files after packaging
            logger.info("Cleaning up individual files after packaging...")
            packager.cleanup_individual_files()
            logger.info("Startup cleanup completed")

        except Exception as e:
            logger.warning(f"Startup packaging failed: {e}")
            logger.warning("Continuing with downloads anyway...")

    # Create a task generator
    tasks = generate_tasks(start_date, end_date, day_step)

    # Use ProcessPoolExecutor with map to process tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # map automatically consumes the iterator and processes tasks in parallel
        # it returns results in the same order as the input iterator
        for i, result in enumerate(
            executor.map(lambda task: process_task(task, archive_manager), tasks)
        ):
            # process_task doesn't return anything, so we're just tracking progress
            logger.info(f"Completed task {i + 1}")

    logger.info("All tasks completed")

    # Optional: Package newly downloaded files after completion
    if package_on_startup:  # Reuse the same flag for post-processing
        logger.info("Packaging newly downloaded files...")
        try:
            packager = ZipPackager()
            packager.package_all()
            logger.info("Post-download packaging completed")

            # Always cleanup newly downloaded individual files
            logger.info("Cleaning up newly downloaded individual files...")
            packager.cleanup_individual_files()
            logger.info("Post-download cleanup completed")
        except Exception as e:
            logger.warning(f"Post-download packaging failed: {e}")
            traceback.print_exc()


class S3ArchiveManager:
    def __init__(self, s3_bucket, s3_prefix, local_dir: Path, immediate_upload=False):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.local_dir = Path(local_dir)
        self.s3 = boto3.client("s3")
        self.archives = {}
        self.indexes = {}
        self.modified_archives = set()  # Track which archives have new content
        self.lock = threading.RLock()  # Reentrant lock for nested calls
        self.immediate_upload = (
            immediate_upload  # Upload immediately instead of on __exit__
        )
        self.uploaded_archives = (
            set()
        )  # Track already uploaded archives to prevent duplicates
        self.new_files_added = defaultdict(lambda: defaultdict(list))
        self.year_upload_metadata = defaultdict(dict)

    def __enter__(self):
        self.local_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Only upload on exit if not in immediate_upload mode
        if not self.immediate_upload:
            self.upload_archives()
        else:
            # Just close archives without uploading
            for archive in self.archives.values():
                archive.close()
            self.cleanup_empty_year_directories()

    def get_archive(self, year, archive_type):
        # New naming convention for archives
        archive_name = f"{archive_type}.zip"
        index_name = f"{archive_type}.index.json"

        if (year, archive_type) in self.archives:
            return self.archives[(year, archive_type)]

        # Create year directory structure if it doesn't exist
        year_dir = self.local_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        local_path = year_dir / archive_name

        # Determine the correct S3 prefix based on archive type
        if archive_type == "metadata":
            s3_dir = f"metadata/zip/year={year}/"
        else:
            s3_dir = f"data/zip/year={year}/"

        s3_key = f"{s3_dir}{archive_name}"
        index_s3_key = f"{s3_dir}{index_name}"

        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=s3_key)
            logger.info(f"Downloading existing archive: {s3_key}")
            self.s3.download_file(self.s3_bucket, s3_key, str(local_path))

            # Download index
            index_local_path = year_dir / index_name
            self.s3.download_file(self.s3_bucket, index_s3_key, str(index_local_path))
            with open(index_local_path, "r") as f:
                self.indexes[(year, archive_type)] = json.load(f)

        except self.s3.exceptions.ClientError as e:
            if "404" in str(e):
                logger.info(f"Archive not found on S3, creating new one: {s3_key}")
                self.indexes[(year, archive_type)] = {
                    "files": [],
                    "file_count": 0,
                    "created_at": datetime.now().isoformat(),
                }
            else:
                raise

        archive = zipfile.ZipFile(local_path, "a", zipfile.ZIP_DEFLATED)
        self.archives[(year, archive_type)] = archive
        return archive

    def add_to_archive(self, year, archive_type, filename, content):
        with self.lock:
            archive = self.get_archive(year, archive_type)
            archive.writestr(filename, content)

            self.indexes[(year, archive_type)]["files"].append(filename)
            self.modified_archives.add((year, archive_type))  # Mark as modified

            # Track newly added files for summary purposes
            if filename not in self.new_files_added[year][archive_type]:
                self.new_files_added[year][archive_type].append(filename)

    def file_exists(self, year, archive_type, filename):
        with self.lock:
            if (year, archive_type) not in self.indexes:
                self.get_archive(year, archive_type)  # This will load the index

            return filename in self.indexes[(year, archive_type)]["files"]

    def upload_year_archives(self, year):
        """Upload all archives for a specific year immediately"""
        with self.lock:
            uploaded_count = 0
            for archive_type in ["metadata", "english", "regional"]:
                if (year, archive_type) in self.archives:
                    # Skip if already uploaded
                    if (year, archive_type) in self.uploaded_archives:
                        continue

                    # Only upload if modified
                    if (year, archive_type) not in self.modified_archives:
                        continue

                    archive = self.archives[(year, archive_type)]
                    archive.close()

                    year_dir = self.local_dir / str(year)
                    archive_name = f"{archive_type}.zip"
                    local_path = year_dir / archive_name

                    # Determine S3 path
                    if archive_type == "metadata":
                        s3_dir = f"metadata/zip/year={year}/"
                    else:
                        s3_dir = f"data/zip/year={year}/"

                    s3_key = f"{s3_dir}{archive_name}"

                    # Update index
                    index_name = f"{archive_type}.index.json"
                    index_local_path = year_dir / index_name
                    index_data = self.indexes[(year, archive_type)]
                    index_data["file_count"] = len(index_data["files"])
                    index_data["updated_at"] = datetime.now(IST).isoformat()

                    if local_path.exists():
                        zip_size_bytes = local_path.stat().st_size
                        index_data["zip_size"] = zip_size_bytes
                        index_data["zip_size_human"] = self.format_file_size(
                            zip_size_bytes
                        )
                    else:
                        zip_size_bytes = None

                    with open(index_local_path, "w") as f:
                        json.dump(index_data, f, indent=2)

                    # Upload to S3
                    logger.info(f"Uploading {archive_name} for year {year}...")
                    self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)

                    index_s3_key = f"{s3_dir}{index_name}"
                    self.s3.upload_file(
                        str(index_local_path), self.s3_bucket, index_s3_key
                    )

                    # Mark as uploaded
                    self.uploaded_archives.add((year, archive_type))
                    uploaded_count += 1

                    # Persist metadata about this upload for later summaries
                    self.year_upload_metadata[year][archive_type] = {
                        "zip_size_bytes": zip_size_bytes,
                        "zip_size_human": index_data.get("zip_size_human"),
                        "files_added": list(
                            self.new_files_added.get(year, {}).get(archive_type, [])
                        ),
                    }

                    added_files = self.new_files_added.get(year, {}).get(
                        archive_type, []
                    )
                    if added_files:
                        logger.info(
                            f"  • {archive_name}: added {len(added_files)} file(s)"
                        )
                    else:
                        logger.info(f"  • {archive_name}: no new files to upload")

                    # Clean up local files after upload
                    local_path.unlink()
                    index_local_path.unlink()

                    # Reopen archive for continued use
                    self.archives[(year, archive_type)] = zipfile.ZipFile(
                        local_path, "a", zipfile.ZIP_DEFLATED
                    )

            if uploaded_count > 0:
                logger.info(f"✅ Uploaded {uploaded_count} archives for year {year}")

            return uploaded_count

    def get_yearly_changes(self, year):
        """Return a summary of new files added for a particular year."""
        with self.lock:
            return {
                archive_type: list(files)
                for archive_type, files in self.new_files_added.get(year, {}).items()
                if files
            }

    def get_all_changes(self):
        """Return a nested dict of {year: {archive_type: [files...]}} for the current session."""
        with self.lock:
            summary = {}
            for year, archive_map in self.new_files_added.items():
                filtered = {
                    archive_type: list(files)
                    for archive_type, files in archive_map.items()
                    if files
                }
                if filtered:
                    summary[str(year)] = filtered
            return summary

    def get_upload_metadata(self):
        with self.lock:
            return json.loads(json.dumps(self.year_upload_metadata, default=str))

    def upload_archives(self):
        uploaded_count = 0
        for (year, archive_type), archive in self.archives.items():
            archive.close()

            # Year directory structure
            year_dir = self.local_dir / str(year)
            archive_name = f"{archive_type}.zip"
            local_path = year_dir / archive_name

            # Only upload if this archive was modified
            if (year, archive_type) not in self.modified_archives:
                logger.info(
                    f"Skipping upload for unchanged archive: {archive_name} (year {year})"
                )
                # Clean up local files for unchanged archives
                if local_path.exists():
                    local_path.unlink()
                index_local_path = year_dir / f"{archive_type}.index.json"
                if index_local_path.exists():
                    index_local_path.unlink()
                continue

            # Determine the correct S3 prefix based on archive type
            if archive_type == "metadata":
                s3_dir = f"metadata/zip/year={year}/"
            else:
                s3_dir = f"data/zip/year={year}/"

            s3_key = f"{s3_dir}{archive_name}"

            # Update and write index
            index_name = f"{archive_type}.index.json"
            index_local_path = year_dir / index_name
            index_data = self.indexes[(year, archive_type)]
            index_data["file_count"] = len(index_data["files"])
            index_data["updated_at"] = datetime.now(IST).isoformat()

            # Get and add the ZIP file size to the index
            if local_path.exists():
                zip_size_bytes = local_path.stat().st_size
                # Store size in bytes
                index_data["zip_size"] = zip_size_bytes
                # Also store human-readable size for convenience
                index_data["zip_size_human"] = self.format_file_size(zip_size_bytes)
                logger.info(
                    f"Archive {archive_name} size: {index_data['zip_size_human']}"
                )

            with open(index_local_path, "w") as f:
                json.dump(index_data, f, indent=2)

            logger.info(f"Uploading modified archive: {s3_key}")
            self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)

            index_s3_key = f"{s3_dir}{index_name}"
            logger.info(f"Uploading index: {index_s3_key}")
            self.s3.upload_file(str(index_local_path), self.s3_bucket, index_s3_key)
            uploaded_count += 1

        # Clean up empty year directories
        self.cleanup_empty_year_directories()

        if uploaded_count > 0:
            logger.info(f"Successfully uploaded {uploaded_count} modified archives")
        else:
            logger.info("No archives needed uploading - all data was already present")

    def cleanup_empty_year_directories(self):
        """Remove year directories that have no files after processing"""
        for year_dir in self.local_dir.glob("*"):
            if year_dir.is_dir() and year_dir.name.isdigit():
                # Check if directory is empty or only contains empty subdirectories
                has_files = any(f.is_file() for f in year_dir.rglob("*"))
                if not has_files:
                    logger.info(f"Cleaning up empty year directory: {year_dir}")
                    import shutil

                    shutil.rmtree(year_dir)

    def format_file_size(self, size_bytes):
        """Convert bytes to a human-readable format"""
        # Define units and their respective sizes in bytes
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        unit_index = 0

        # Find the appropriate unit
        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        # Format with 2 decimal places if not bytes
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        else:
            return f"{size:.2f} {units[unit_index]}"


class Downloader:
    def __init__(self, task: SCDateTask, archive_manager: S3ArchiveManager):
        self.task = task
        self.root_url = "https://scr.sci.gov.in"
        self.search_url = f"{self.root_url}/scrsearch/?p=pdf_search/home/"
        self.captcha_url = (
            f"{self.root_url}/scrsearch/vendor/securimage/securimage_show.php"
        )
        self.captcha_token_url = f"{self.root_url}/scrsearch/?p=pdf_search/checkCaptcha"
        self.pdf_link_url = f"{self.root_url}/scrsearch/?p=pdf_search/openpdfcaptcha"
        self.pdf_link_url_wo_captcha = (
            f"{self.root_url}/scrsearch/?p=pdf_search/openpdf"
        )

        self.tracking_data = get_tracking_data()
        self.session_cookie_name = "SCR_SESSID"
        self.alt_session_cookie_name = "PHPSESSID"
        self.ecourts_token_cookie_name = "JSESSION"
        self.session_id = None
        self.ecourts_token = None
        self.tar_checker = YearlyFileChecker()
        self.archive_manager = archive_manager

    def _results_exist_in_search_response(self, res_dict):
        results_exist = (
            "reportrow" in res_dict
            and "aaData" in res_dict["reportrow"]
            and len(res_dict["reportrow"]["aaData"]) > 0
        )
        if results_exist:
            no_of_results = len(res_dict["reportrow"]["aaData"])
            logger.info(f"Found {no_of_results} results for task: {self.task}")
        return results_exist

    def _prepare_next_iteration(self, search_payload):
        search_payload["sEcho"] += 1
        search_payload["iDisplayStart"] += PAGE_SIZE
        logger.info(
            f"Next iteration: {search_payload['iDisplayStart']}, task: {self.task.id}"
        )
        return search_payload

    def process_result_row(self, row, row_pos):
        html = row[1]
        soup = BeautifulSoup(html, "html.parser")

        # First check for direct PDF button (single language) with role="link"
        button = soup.find("button", {"role": "link"})
        assert (
            button and "onclick" in button.attrs
        ), f"No PDF button found, task: {self.task}"
        pdf_info = self.extract_pdf_fragment_from_button(button["onclick"])
        assert pdf_info, f"No PDF info found, task: {self.task}"

        # Check for multi-language selector
        select_element = soup.find("select", {"name": "language"})
        if select_element:
            language_codes = [
                option.get("value") for option in select_element.find_all("option")
            ]
        else:
            language_codes = [""]

        pdf_info["language_codes"] = language_codes

        # Use the year from the task date (the date we're scraping)
        # This ensures we download the correct year's archive from S3
        year = datetime.strptime(self.task.from_date, "%Y-%m-%d").year

        # Check if metadata already exists
        metadata_filename = f"{pdf_info['path']}.json"
        if not self.archive_manager.file_exists(year, "metadata", metadata_filename):
            # Create metadata
            order_metadata = {
                "raw_html": html,
                "path": pdf_info["path"],
                "citation_year": pdf_info.get("citation_year", ""),
                "nc_display": pdf_info.get("nc_display", ""),
                "scraped_at": datetime.now().isoformat(),
            }

            self.archive_manager.add_to_archive(
                year,
                "metadata",
                metadata_filename,
                json.dumps(order_metadata, indent=2),
            )

        # Download PDFs for each language
        pdfs_downloaded = 0
        for lang_code in language_codes:
            pdf_filename = self.get_pdf_filename(pdf_info["path"], lang_code)
            # Classify as English if lang_code is empty string or "EN" (case-insensitive)
            archive_type = (
                "english"
                if (lang_code == "" or lang_code.upper() == "EN")
                else "regional"
            )

            # Check if PDF already exists
            if self.archive_manager.file_exists(year, archive_type, pdf_filename):
                continue  # Skip download

            try:
                pdf_content = self.download_pdf(pdf_info, lang_code)
                if pdf_content:
                    self.archive_manager.add_to_archive(
                        year, archive_type, pdf_filename, pdf_content
                    )
                    pdfs_downloaded += 1
            except Exception as e:
                logger.error(
                    f"Error downloading {pdf_filename}: {e}, task: {self.task}"
                )
                traceback.print_exc()

        return pdfs_downloaded

    def extract_pdf_fragment_from_button(self, onclick_attr):
        """Extract PDF fragment from button onclick attribute"""
        # Pattern: onclick=open_pdf('3','2009','2009_9_572_578','2009INSC834','N')
        pattern = r"javascript:open_pdf\('(.*?)','(.*?)','(.*?)','(.*?)'\)"
        match = re.search(pattern, onclick_attr)
        if match:
            val = match.group(1)
            path = match.group(3).split("#")[0]
            citation_year = match.group(2)
            nc_display = match.group(4)
            return {
                "val": val,
                "path": path,
                "citation_year": citation_year,
                "nc_display": nc_display,
            }
        return None

    def solve_math_expression(self, expression):
        # credits to: https://github.com/NoelShallum
        expression = expression.strip().replace(" ", "").replace(".", "")

        # Check if it's a math expression
        separators = ["+", "-", "*", "/", "÷", "x", "×", "X"]
        if not any(sep in expression for sep in separators):
            raise ValueError(f"Not a mathematical expression: {expression}")

        if "+" in expression:
            nums = expression.split("+")
            return str(int(nums[0]) + int(nums[1]))
        elif "-" in expression:
            nums = expression.split("-")
            return str(int(nums[0]) - int(nums[1]))
        elif (
            "*" in expression
            or "X" in expression
            or "x" in expression
            or "×" in expression
        ):
            expression = (
                expression.replace("x", "*").replace("×", "*").replace("X", "*")
            )
            nums = expression.split("*")
            return str(int(nums[0]) * int(nums[1]))
        elif "/" in expression or "÷" in expression:
            expression = expression.replace("÷", "/")
            nums = expression.split("/")
            return str(int(nums[0]) // int(nums[1]))
        else:
            raise ValueError(f"Unsupported mathematical expression: {expression}")

    def solve_captcha(self, retries=0, captcha_url=None):
        logger.debug(f"Solving captcha, retries: {retries}, task: {self.task.id}")
        if retries > 10:
            raise ValueError("Failed to solve captcha")
        if captcha_url is None:
            captcha_url = self.captcha_url

        # download captcha image and save
        captcha_response = requests.get(
            captcha_url, headers={"Cookie": self.get_cookie()}, verify=False, timeout=30
        )

        # Generate a unique filename using UUID
        unique_id = uuid.uuid4().hex[:8]
        captcha_filename = Path(f"{captcha_tmp_dir}/captcha_sc_{unique_id}.png")
        with open(captcha_filename, "wb") as f:
            f.write(captcha_response.content)

        pil_img = Image.open(captcha_filename)

        captcha_text = get_text(pil_img)

        if MATH_CAPTCHA:
            try:
                answer = self.solve_math_expression(captcha_text)
                captcha_filename.unlink()
                return answer
            except ValueError:
                logger.debug(f"Not a math expression: {captcha_text}")
                # If not a math expression, try again
                captcha_filename.unlink()  # Clean up the file
                return self.solve_captcha(retries + 1, captcha_url)
            except Exception as e:
                logger.error(
                    f"Error solving math expression, task: {self.task.id}, retries: {retries}, captcha text: {captcha_text}, Error: {e}"
                )
                # move the captcha image to a new folder for debugging
                new_filename = f"{uuid.uuid4().hex[:8]}_{captcha_filename.name}"
                captcha_filename.rename(Path(f"{captcha_failures_dir}/{new_filename}"))
                return self.solve_captcha(retries + 1, captcha_url)
        else:
            captcha_text = captcha_text.strip()
            if len(captcha_text) != 6:
                if retries > 10:
                    raise Exception("Captcha not solved")
                return self.solve_captcha(retries + 1)
            return captcha_text

    def solve_pdf_download_captcha(self, response, pdf_link_payload, retries=0):
        html_str = response["filename"]
        html = LH.fromstring(html_str)
        img_src = html.xpath("//img[@id='captcha_image_pdf']/@src")[0]
        img_src = self.root_url + img_src

        # download captcha image and save
        captcha_text = self.solve_captcha(captcha_url=img_src)
        pdf_link_payload["captcha1"] = captcha_text

        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url_wo_captcha, pdf_link_payload
        )
        res_json = pdf_link_response.json()

        if "message" in res_json and res_json["message"] == "Invalid Captcha":
            logger.warning(
                f"Captcha not solved, task: {self.task.id}, retries: {retries}, Error: {pdf_link_response.json()}"
            )
            if retries == 5:
                return res_json
            logger.info(f"Retrying pdf captcha solve, task: {self.task.id}")
            return self.solve_pdf_download_captcha(
                response, pdf_link_payload, retries + 1
            )
        return pdf_link_response

    def refresh_token(self):
        logger.debug(f"Current session id {self.session_id}")
        answer = self.solve_captcha()
        captcha_check_payload = {
            "captcha": answer,
            "search_opt": "PHRASE",
            "ajax_req": "true",
        }

        res = requests.request(
            "POST",
            self.captcha_token_url,
            headers=self.get_headers(),
            data=captcha_check_payload,
            verify=False,
            timeout=30,
        )
        self.update_session_id(res)
        logger.debug("Refreshed token")

    def request_api(self, method, url, payload, **kwargs):
        headers = self.get_headers()
        max_retries = 3
        base_delay = 2  # Base delay in seconds

        for attempt in range(max_retries):
            try:
                logger.debug(
                    f"api_request {self.session_id} {url} (attempt {attempt + 1}/{max_retries})"
                )
                response = requests.request(
                    method,
                    url,
                    headers=headers,
                    data=payload,
                    **kwargs,
                    timeout=60,
                    verify=False,
                )

                # if response is json
                try:
                    response_dict = response.json()
                except Exception:
                    response_dict = {}

                self.update_session_id(response)

                if url == self.captcha_token_url:
                    return response

                if (
                    "filename" in response_dict
                    and "securimage_show" in response_dict["filename"]
                ):
                    return self.solve_pdf_download_captcha(response_dict, payload)

                elif response_dict.get("session_expire") == "Y":
                    self.refresh_token()
                    return self.request_api(method, url, payload, **kwargs)

                elif "errormsg" in response_dict:
                    logger.error(f"Error {response_dict['errormsg']}")
                    self.refresh_token()
                    return self.request_api(method, url, payload, **kwargs)
                elif response.text.strip() == "":
                    self.refresh_token()
                    logger.error(f"Empty response, task: {self.task.id}")
                    return self.request_api(method, url, payload, **kwargs)

                elif "curl_error() expects exactly 1 argument" in response.text:
                    logger.warning(
                        f"Server-side PHP error detected, retrying: {self.task.id}"
                    )
                    time.sleep(2)  # Brief delay before retry
                    return self.request_api(method, url, payload, **kwargs)

                return response

            except requests.exceptions.SSLError as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    delay = base_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"SSL Error occurred (attempt {attempt + 1}/{max_retries}): {str(e)}"
                    )
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"SSL Error after {max_retries} attempts: {str(e)}")
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    delay = base_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}"
                    )
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(
                        f"Request failed after {max_retries} attempts: {str(e)}"
                    )
                    raise

    def get_pdf_output_path(self, pdf_fragment, lang_code=""):
        if lang_code:
            return output_dir / (pdf_fragment.split("#")[0] + f"_{lang_code}.pdf")
        else:
            return output_dir / (pdf_fragment.split("#")[0] + "_EN.pdf")

    def default_search_payload(self):
        search_payload = urllib.parse.parse_qs(DEFAULT_SEARCH_PAYLOAD)
        search_payload = {k: v[0] for k, v in search_payload.items()}
        search_payload["sEcho"] = 1
        search_payload["iDisplayStart"] = 0
        search_payload["iDisplayLength"] = PAGE_SIZE
        return search_payload

    def default_pdf_link_payload(self):
        pdf_link_payload_o = urllib.parse.parse_qs(pdf_link_payload)
        pdf_link_payload_o = {k: v[0] for k, v in pdf_link_payload_o.items()}
        return pdf_link_payload_o

    def init_user_session(self):
        res = requests.request(
            "GET",
            f"{self.root_url}/scrsearch/",
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            },
            timeout=30,
        )
        self.session_id = res.cookies.get(
            self.session_cookie_name, res.cookies.get(self.alt_session_cookie_name)
        )
        self.ecourts_token = res.cookies.get(self.ecourts_token_cookie_name)
        if self.ecourts_token is None:
            raise ValueError(
                "Failed to get session token, not expected to happen. This could happen if the IP might have been detected as spam"
            )

    def get_cookie(self):
        return f"{self.ecourts_token_cookie_name}={self.ecourts_token}; {self.session_cookie_name}={self.session_id}"

    def update_session_id(self, response):
        new_session_cookie = response.cookies.get(
            self.session_cookie_name, response.cookies.get(self.alt_session_cookie_name)
        )
        if new_session_cookie:
            self.session_id = new_session_cookie

    def get_headers(self):
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": self.get_cookie(),
            "DNT": "1",
            "Origin": self.root_url,
            "Pragma": "no-cache",
            "Referer": self.root_url + "/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        return headers

    def download(self):
        """Process a specific date range for Supreme Court"""

        search_payload = self.default_search_payload()
        search_payload["from_date"] = self.task.from_date
        search_payload["to_date"] = self.task.to_date
        self.init_user_session()
        results_available = True
        pdfs_downloaded = 0

        logger.info(f"Downloading data for: task: {self.task}")

        try:
            while results_available:
                try:
                    response = self.request_api("POST", self.search_url, search_payload)
                    res_dict = response.json()
                    if self._results_exist_in_search_response(res_dict):
                        for idx, row in enumerate(res_dict["reportrow"]["aaData"]):
                            try:
                                pdfs_downloaded += self.process_result_row(
                                    row, row_pos=idx
                                )
                                if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                    # after 25 downloads, need to solve captcha for every pdf link request
                                    logger.info(
                                        f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session, task: {self.task}"
                                    )
                                    break  # Break inner loop to process next page
                            except Exception as e:
                                logger.error(
                                    f"Error processing row {row}: {e}, task: {self.task}"
                                )
                                traceback.print_exc()

                        # Only prepare next iteration if we haven't already done so
                        if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                            pdfs_downloaded = 0
                            self.init_user_session()
                            continue
                        search_payload = self._prepare_next_iteration(search_payload)
                    else:
                        # No more results for this date range
                        results_available = False
                        # Update tracking data
                        self.tracking_data["last_date"] = self.task.to_date
                        save_tracking_date(self.tracking_data)

                except Exception as e:
                    logger.error(f"Error processing task: {self.task}, {e}")
                    traceback.print_exc()
                    results_available = False
        except Exception as e:
            logger.error(f"Error in download method: {e}")
            traceback.print_exc()

    def download_pdf(self, pdf_info, lang_code):
        """Download PDF and return its content"""
        val = pdf_info.get("val", "0")
        citation_year = pdf_info.get("citation_year", "")
        nc_display = pdf_info.get("nc_display", "")

        pdf_link_payload = self.default_pdf_link_payload()
        pdf_link_payload["val"] = val
        pdf_link_payload["path"] = pdf_info["path"]
        pdf_link_payload["citation_year"] = citation_year
        pdf_link_payload["nc_display"] = nc_display
        pdf_link_payload["fcourt_type"] = "3"
        pdf_link_payload["ajax_req"] = "true"
        pdf_link_payload["lang_flg"] = lang_code or ""

        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url, pdf_link_payload
        )

        if "outputfile" not in pdf_link_response.json():
            logger.error(f"Error downloading pdf: {pdf_link_response.json()}")
            return None

        pdf_download_link = pdf_link_response.json()["outputfile"]
        pdf_response = requests.request(
            "GET",
            self.root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
            timeout=30,
            allow_redirects=True,
        )

        # Validate response
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0 or no_of_bytes == 315:  # Empty or 404 response
            return None

        return pdf_response.content

    def get_pdf_filename(self, path, lang_code):
        """Get standardized PDF filename"""
        # Normalize English language code to uppercase
        if lang_code and lang_code.upper() == "EN":
            return f"{path}_EN.pdf"
        elif lang_code:
            return f"{path}_{lang_code}.pdf"
        else:
            return f"{path}_EN.pdf"


class YearlyFileChecker:
    def __init__(self, output_dir="./sc_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories for organized storage
        (self.output_dir / "english").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "regional").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "metadata").mkdir(parents=True, exist_ok=True)

        # Package directory for zip files and indexes
        self.packages_dir = Path("./packages")
        self.packages_dir.mkdir(parents=True, exist_ok=True)

    def _get_index_path(self, year, archive_type):
        """Get path to index file for a specific year/type"""
        return self.packages_dir / f"sc-judgments-{year}-{archive_type}.index.json"

    def _load_index_files(self, year, archive_type):
        """Load files from index file if it exists"""
        index_path = self._get_index_path(year, archive_type)

        if index_path.exists():
            try:
                with open(index_path, "r") as f:
                    index_data = json.load(f)
                    return set(index_data.get("files", []))
            except Exception as e:
                logger.warning(f"Failed to load index {index_path}: {e}")

        return set()

    def pdf_exists(self, year, filename, lang_code):
        """Check if PDF exists - verify actual file on disk first, then check if it's in zip"""
        # Get the actual file path
        pdf_path = self.get_pdf_path(year, filename, lang_code)

        # Check if file actually exists on disk as individual file
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            return True

        # Check if file is already packaged in zip (from index)
        archive_type = "english" if lang_code == "" else "regional"
        packaged_files = self._load_index_files(year, archive_type)
        return filename in packaged_files

    def metadata_exists(self, year, filename):
        """Check if JSON metadata exists - verify actual file on disk first, then check if it's in zip"""
        # Get the actual file path
        metadata_path = self.get_metadata_path(year, filename)

        # Check if file actually exists on disk as individual file
        if metadata_path.exists() and metadata_path.stat().st_size > 0:
            return True

        # Check if file is already packaged in zip (from index)
        packaged_files = self._load_index_files(year, "metadata")
        return filename in packaged_files

    def get_pdf_path(self, year, filename, lang_code):
        """Get the final path where a PDF should be stored"""
        archive_type = "english" if lang_code == "" else "regional"
        year_dir = self.output_dir / archive_type / str(year)
        return year_dir / filename

    def get_metadata_path(self, year, filename):
        """Get the final path where metadata should be stored"""
        year_dir = self.output_dir / "metadata" / str(year)
        return year_dir / filename


def timer(func):
    """Decorator to time function execution"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.perf_counter()
            execution_time = end_time - start_time

            # Get function name and class name if it's a method
            func_name = func.__name__
            if args and hasattr(args[0], "__class__"):
                class_name = args[0].__class__.__name__
                func_name = f"{class_name}.{func_name}"

            logger.info(f"⏱️  {func_name} took {execution_time:.3f} seconds")

    return wrapper


def timer_with_args(include_args=False, include_result=False):
    """Enhanced timer decorator with optional argument and result logging"""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()

            # Get function name and class name if it's a method
            func_name = func.__name__
            if args and hasattr(args[0], "__class__"):
                class_name = args[0].__class__.__name__
                func_name = f"{class_name}.{func_name}"

            # Log function start with args if requested
            if include_args:
                args_str = ", ".join([str(arg)[:50] for arg in args[1:]])  # Skip self
                kwargs_str = ", ".join(
                    [f"{k}={str(v)[:50]}" for k, v in kwargs.items()]
                )
                all_args = ", ".join(filter(None, [args_str, kwargs_str]))
                logger.info(f"🚀 Starting {func_name}({all_args})")

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.perf_counter()
                execution_time = end_time - start_time

                log_msg = f"⏱️  {func_name} took {execution_time:.3f} seconds"
                if include_result and "result" in locals():
                    log_msg += f" | Result: {str(result)[:100]}"

                logger.info(log_msg)

        return wrapper

    return decorator


def sync_latest_metadata_zip(force_refresh=True):
    """
    Download the current year's metadata zip file from S3, or latest available.
    If force_refresh is True, always download a fresh copy.
    """
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # First try to get current year's metadata
    current_year = datetime.now().year
    current_year_key = f"metadata/zip/year={current_year}/metadata.zip"

    # Check if current year metadata exists
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=current_year_key)
        latest_zip_key = current_year_key
        logger.info(f"Found current year ({current_year}) metadata")
    except Exception:
        # Fall back to finding the latest available year
        logger.info("Current year metadata not found, finding latest available...")
        zips = []

        # Search for metadata zip files in the new structure
        paginator = s3.get_paginator("list_objects_v2")
        prefix = "metadata/zip/"

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith("/metadata.zip"):
                    # Extract year from path like metadata/zip/year=2023/metadata.zip
                    year_match = re.search(r"year=(\d{4})/", key)
                    if year_match:
                        zips.append((key, int(year_match.group(1))))

        if not zips:
            raise Exception("No metadata zip files found")

        # Sort by year descending and take the most recent
        zips.sort(key=lambda x: x[1], reverse=True)
        latest_zip_key = zips[0][0]

    # Create year directory for the zip file
    year_match = re.search(r"year=(\d{4})/", latest_zip_key)
    if year_match:
        year = year_match.group(1)
        year_dir = LOCAL_DIR / year
        year_dir.mkdir(parents=True, exist_ok=True)
        local_path = year_dir / "metadata.zip"
    else:
        local_path = LOCAL_DIR / Path(latest_zip_key).name

    # Force a fresh download if requested
    if force_refresh and local_path.exists():
        logger.info("Removing cached metadata zip to force refresh...")
        local_path.unlink()

    if not local_path.exists():
        logger.info(f"Downloading {latest_zip_key} ...")
        s3.download_file(S3_BUCKET, latest_zip_key, local_path)
    else:
        logger.info(f"Using cached metadata zip: {local_path}")

    return local_path


def extract_decision_date_from_json(json_obj):
    raw_html = json_obj.get("raw_html", "")
    # Try to find DD-MM-YYYY after 'Decision Date'
    m = re.search(
        r"Decision Date\s*:\s*<font[^>]*>\s*(\d{2}-\d{2}-\d{4})\s*</font>", raw_html
    )
    if not m:
        # Fallback: try to find any date pattern
        m = re.search(r"(\d{2}-\d{2}-\d{4})", raw_html)
        # print(m.group(1))
    if m:
        try:
            # print(datetime.strptime(m.group(1), "%d-%m-%Y"))
            return datetime.strptime(m.group(1), "%d-%m-%Y")
        except Exception:
            pass
    return None


def find_latest_decision_date_in_zip(zip_path):
    latest_date = None
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if not name.endswith(".json"):
                continue
            with z.open(name) as f:
                try:
                    data = json.load(f)
                    decision_date = extract_decision_date_from_json(data)
                    if decision_date and (
                        latest_date is None or decision_date > latest_date
                    ):
                        latest_date = decision_date
                except Exception:
                    continue
    if latest_date:
        logger.info(f"Latest decision date in metadata zip: {latest_date.date()}")
    else:
        logger.warning(
            "No decision date found in metadata zip, falling back to ZIP entry date."
        )
        # fallback (not recommended)
        with zipfile.ZipFile(zip_path, "r") as z:
            latest_date = max(datetime(*zi.date_time[:3]) for zi in z.infolist())
    return latest_date


def run_downloader(start_date, end_date):
    logger.info(f"Fetching new data from {start_date} to {end_date} ...")
    run(
        start_date=(start_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )


def get_latest_date_from_metadata(force_check_files=False):
    """
    Get the latest decision date from metadata, preferring index.json if available.
    Falls back to parsing individual files if needed or if force_check_files=True.
    """
    # First try to download the index.json file from S3
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    current_year = datetime.now().year

    # Create a separate directory for index files outside of LOCAL_DIR
    index_cache_dir = Path("./index_cache")
    index_cache_dir.mkdir(parents=True, exist_ok=True)

    # Updated path for metadata index - store outside LOCAL_DIR
    index_path = index_cache_dir / f"{current_year}_metadata.index.json"
    index_key = f"metadata/zip/year={current_year}/metadata.index.json"

    if not force_check_files:
        try:
            # Try to get current year index
            s3.download_file(S3_BUCKET, index_key, str(index_path))
            with open(index_path, "r") as f:
                index_data = json.load(f)

            # Check if updated_at is available
            if "updated_at" in index_data:
                updated_at = datetime.fromisoformat(index_data["updated_at"])
                logger.info(f"Found updated_at in index.json: {updated_at}")
                return updated_at

        except Exception as e:
            logger.info(f"Could not use index.json for date detection: {e}")

    # Fall back to the original method - parsing individual files
    logger.info("Falling back to parsing individual files for decision dates...")
    latest_zip = sync_latest_metadata_zip()
    return find_latest_decision_date_in_zip(latest_zip)


def generate_parquet_from_metadata(s3_bucket, years_to_process=None):
    """
    Process metadata files in S3 and generate parquet files

    Args:
        s3_bucket: S3 bucket name where metadata files are stored
        years_to_process: Optional list of years to process (if None, process all)
    """
    logger.info("Starting metadata to parquet conversion...")
    processor = SupremeCourtS3Processor(
        s3_bucket=s3_bucket,
        s3_prefix="",
        batch_size=10000,
        years_to_process=years_to_process,  # Pass years to process
    )

    processed_years, total_records = processor.process_bucket_metadata()

    if total_records > 0:
        logger.info(
            f"Successfully processed {total_records} records across {len(processed_years)} years"
        )
    else:
        logger.warning("No metadata records were processed to parquet format")

    return total_records > 0


def get_fill_progress_file():
    """Get path to the fill progress tracking file"""
    return Path("./sc_fill_progress.json")


def save_fill_progress(start_date, end_date, current_date, completed_ranges):
    """Save progress for gap filling process"""
    progress_data = {
        "start_date": start_date,
        "end_date": end_date,
        "current_date": current_date,
        "completed_ranges": completed_ranges,
        "last_updated": datetime.now().isoformat(),
    }

    with open(get_fill_progress_file(), "w") as f:
        json.dump(progress_data, f, indent=2)

    logger.info(f"Progress saved: currently at {current_date}")


def load_fill_progress():
    """Load existing progress for gap filling process"""
    progress_file = get_fill_progress_file()
    if not progress_file.exists():
        return None

    try:
        with open(progress_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load progress file: {e}")
        return None


def clear_fill_progress():
    """Clear progress file when process completes"""
    progress_file = get_fill_progress_file()
    if progress_file.exists():
        progress_file.unlink()
        logger.info("Progress file cleared - gap filling completed")


def sync_s3_fill_gaps(
    s3_bucket,
    start_date=None,
    end_date=None,
    day_step=30,
    max_workers=5,
    timeout_hours=None,
):
    """
    Fill gaps in S3 data by processing ONE 5-year chunk per run.
    Uses immediate upload after each year to prevent data loss.
    Run this repeatedly - it will automatically pick up the next 5-year chunk each time.

    Args:
        s3_bucket: S3 bucket name
        start_date: Start date for gap analysis (defaults to START_DATE = 1950-01-01)
        end_date: End date for gap analysis (defaults to current date)
        day_step: Number of days per download chunk
        max_workers: Number of parallel workers
        timeout_hours: Maximum hours to run before graceful exit (default None = no timeout)
    """
    start_time = time.time()
    timeout_seconds = timeout_hours * 3600 if timeout_hours else None

    logger.info("🚀 Starting 5-year chunk S3 gap-filling process...")
    if timeout_hours:
        logger.info(
            f"⏰ Timeout set to {timeout_hours} hours ({timeout_seconds/60:.0f} minutes)"
        )
    else:
        logger.info("⏰ No timeout - will run until completion")

    # Check for existing progress
    existing_progress = load_fill_progress()

    # Determine the overall start and end dates
    overall_start = start_date or START_DATE
    overall_end = end_date or datetime.now().strftime("%Y-%m-%d")

    if existing_progress:
        logger.info("📋 Found existing progress from previous run:")
        logger.info(
            f"  Overall range: {existing_progress['start_date']} to {existing_progress['end_date']}"
        )
        logger.info(
            f"  Last completed chunk: {existing_progress.get('last_chunk_end', 'None')}"
        )
        logger.info(
            f"  Completed chunks: {len(existing_progress.get('completed_chunks', []))}"
        )

        # Use existing overall range
        overall_start = existing_progress["start_date"]
        overall_end = existing_progress["end_date"]
        completed_chunks = [
            tuple(c) for c in existing_progress.get("completed_chunks", [])
        ]
    else:
        completed_chunks = []

    logger.info(f"📅 Overall processing range: {overall_start} to {overall_end}")

    # Generate 5-year chunks
    start_dt = datetime.strptime(overall_start, "%Y-%m-%d")
    end_dt = datetime.strptime(overall_end, "%Y-%m-%d")

    all_five_year_chunks = []
    current_chunk_start = start_dt

    while current_chunk_start <= end_dt:
        # Calculate 5-year chunk end
        chunk_end = min(
            datetime(current_chunk_start.year + 5, 1, 1) - timedelta(days=1), end_dt
        )

        chunk_tuple = (
            current_chunk_start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
        )
        all_five_year_chunks.append(chunk_tuple)

        # Move to next chunk
        current_chunk_start = datetime(current_chunk_start.year + 5, 1, 1)

    # Filter out completed chunks
    remaining_chunks = [c for c in all_five_year_chunks if c not in completed_chunks]

    logger.info(f"📊 Total 5-year chunks: {len(all_five_year_chunks)}")
    logger.info(f"✅ Already completed: {len(completed_chunks)}")
    logger.info(f"⏳ Remaining chunks: {len(remaining_chunks)}")

    if not remaining_chunks:
        logger.info("🎉 All chunks completed! Clearing progress file.")
        clear_fill_progress()
        return

    # Process ALL remaining chunks in this run
    for chunk_index, (chunk_start, chunk_end) in enumerate(remaining_chunks):
        logger.info("")
        logger.info(f"{'='*70}")
        logger.info(
            f"📦 Processing chunk {len(completed_chunks) + chunk_index + 1}/{len(all_five_year_chunks)}: {chunk_start} to {chunk_end}"
        )
        logger.info(f"{'='*70}")
        logger.info("")

        # Check timeout before starting a new chunk
        if timeout_seconds and time.time() - start_time >= timeout_seconds:
            logger.warning(
                f"⏰ Timeout reached before starting chunk {chunk_start} to {chunk_end}"
            )
            logger.info("💾 Progress saved. Run again to continue from this chunk.")
            return

        # Check if we're resuming an incomplete chunk
        completed_years_in_chunk = set()
        if existing_progress and existing_progress.get("current_chunk") == [
            chunk_start,
            chunk_end,
        ]:
            completed_years_in_chunk = set(
                existing_progress.get("completed_years_in_current_chunk", [])
            )
            if completed_years_in_chunk:
                logger.info(
                    f"📋 Resuming chunk - already uploaded years: {sorted(completed_years_in_chunk)}"
                )

        # Track years processed in this chunk
        years_in_chunk = (
            completed_years_in_chunk.copy()
        )  # Start with already completed years
        current_year = None
        chunk_changes = {}
        upload_metadata = {}

        # Use immediate upload mode
        with S3ArchiveManager(
            s3_bucket, S3_PREFIX, LOCAL_DIR, immediate_upload=True
        ) as archive_manager:
            try:
                # Generate date ranges for this chunk only
                chunk_date_ranges = list(
                    get_date_ranges_to_process(chunk_start, chunk_end, day_step)
                )
                total_ranges = len(chunk_date_ranges)
                logger.info(f"📝 Processing {total_ranges} date ranges in this chunk")

                date_range_bar = tqdm(
                    chunk_date_ranges,
                    desc=f"\x1b[36m📆 Ranges {chunk_start}→{chunk_end}\x1b[0m",  # Cyan text
                    unit="range",
                    leave=True,
                    colour="cyan",  # Cyan bar
                    bar_format="\x1b[36m{l_bar}\x1b[0m{bar}\x1b[36m{r_bar}\x1b[0m",  # Cyan text, keep bar colored
                    ncols=100,
                    position=0,
                    file=sys.stderr,
                )
                print()

                try:
                    for i, (from_date, to_date) in enumerate(date_range_bar, 1):
                        # Skip dates from already-completed years
                        date_year = datetime.strptime(from_date, "%Y-%m-%d").year
                        if date_year in completed_years_in_chunk:
                            continue  # Skip this date range entirely

                        # Check timeout
                        elapsed_time = time.time() - start_time
                        if elapsed_time >= timeout_seconds:
                            logger.warning(
                                f"⏰ Timeout reached after {elapsed_time/3600:.2f} hours"
                            )
                            # Upload any remaining year data before exiting
                            if current_year is not None:
                                logger.info(
                                    f"📤 Uploading final year {current_year} before timeout..."
                                )
                                archive_manager.upload_year_archives(current_year)
                                years_in_chunk.add(current_year)

                            # Save progress with completed years so far
                            if years_in_chunk:
                                logger.info(
                                    f"💾 Saving progress: uploaded years {sorted(years_in_chunk)}"
                                )
                                progress_data = {
                                    "start_date": overall_start,
                                    "end_date": overall_end,
                                    "current_chunk": (chunk_start, chunk_end),
                                    "completed_years_in_current_chunk": sorted(
                                        list(years_in_chunk)
                                    ),
                                    "completed_chunks": completed_chunks,
                                    "last_updated": datetime.now().isoformat(),
                                }
                                with open(get_fill_progress_file(), "w") as f:
                                    json.dump(progress_data, f, indent=2)

                            logger.warning(
                                "⚠️  Did not complete the full chunk - will resume from next year on next run"
                            )
                            return  # Exit without marking chunk as complete
                        logger.info("")
                        logger.debug(
                            f"  Range {i}/{total_ranges}: {from_date} to {to_date}"
                        )

                        # Use the existing run() function which already has parallel processing built-in
                        run(
                            start_date=from_date,
                            end_date=to_date,
                            day_step=1,
                            max_workers=max_workers,
                            package_on_startup=False,
                            archive_manager=archive_manager,
                        )

                        # Track year for uploads after processing the date range
                        date_year = datetime.strptime(from_date, "%Y-%m-%d").year
                        if current_year is not None and date_year != current_year:
                            # Year changed - upload previous year's data (if not already uploaded)
                            if current_year not in completed_years_in_chunk:
                                logger.info(
                                    f"📤 Year changed from {current_year} to {date_year}, uploading {current_year} data..."
                                )
                                archive_manager.upload_year_archives(current_year)
                                years_in_chunk.add(current_year)

                                # Save progress immediately after uploading a year
                                logger.info(
                                    f"💾 Saving progress: completed year {current_year}"
                                )
                                progress_data = {
                                    "start_date": overall_start,
                                    "end_date": overall_end,
                                    "current_chunk": (chunk_start, chunk_end),
                                    "completed_years_in_current_chunk": sorted(
                                        list(years_in_chunk)
                                    ),
                                    "completed_chunks": completed_chunks,
                                    "last_updated": datetime.now().isoformat(),
                                }
                                with open(get_fill_progress_file(), "w") as f:
                                    json.dump(progress_data, f, indent=2)
                            else:
                                logger.info(
                                    f"⏭️  Skipping upload for {current_year} (already uploaded)"
                                )

                        current_year = date_year
                finally:
                    date_range_bar.close()

                # Upload final year's data (if not already uploaded)
                if (
                    current_year is not None
                    and current_year not in completed_years_in_chunk
                ):
                    logger.info(f"📤 Uploading final year {current_year} data...")
                    archive_manager.upload_year_archives(current_year)
                    years_in_chunk.add(current_year)

                logger.info("")
                logger.info(f"✅ Completed chunk: {chunk_start} to {chunk_end}")

                # Mark this chunk as completed
                completed_chunks.append((chunk_start, chunk_end))

                # Save progress
                progress_data = {
                    "start_date": overall_start,
                    "end_date": overall_end,
                    "last_chunk_end": chunk_end,
                    "completed_chunks": completed_chunks,
                    "last_updated": datetime.now().isoformat(),
                }

                with open(get_fill_progress_file(), "w") as f:
                    json.dump(progress_data, f, indent=2)

                logger.info("💾 Progress saved")

            except KeyboardInterrupt:
                logger.warning("")
                logger.warning("⚠️  Process interrupted by user (Ctrl+C)")
                logger.warning(
                    "⚠️  Chunk was NOT fully completed - will retry from chunk start next run"
                )
                return
            except Exception as e:
                logger.error(f"❌ Error processing chunk: {e}")
                logger.error(
                    "⚠️  Chunk was NOT completed - will retry from chunk start next run"
                )
                traceback.print_exc()
                return

            chunk_changes = archive_manager.get_all_changes()
            upload_metadata = archive_manager.get_upload_metadata()

        # Summarize chunk changes outside the context manager once uploads are done
        if chunk_changes:
            logger.info("")
            logger.info("🆕 Change summary for this chunk:")
            for year in sorted(chunk_changes.keys(), key=int):
                logger.info(f"  📁 Year {year}:")
                for archive_type, files in chunk_changes[year].items():
                    logger.info(f"    • {archive_type}: {len(files)} file(s)")
                    preview = files[:CHANGE_LOG_PREVIEW_LIMIT]
                    for filename in preview:
                        logger.info(f"       - {filename}")
                    if len(files) > CHANGE_LOG_PREVIEW_LIMIT:
                        logger.info(
                            f"       … plus {len(files) - CHANGE_LOG_PREVIEW_LIMIT} more (see chunk_changes_summary.json)"
                        )
        else:
            logger.info("ℹ️  No new files were added in this chunk.")

        summary_payload = {
            "chunk": {"start": chunk_start, "end": chunk_end},
            "generated_at": datetime.now().isoformat(),
            "years": {str(year): meta for year, meta in upload_metadata.items()},
            "files": chunk_changes,
        }
        summary_path = Path("./chunk_changes_summary.json")
        with open(summary_path, "w") as summary_file:
            json.dump(summary_payload, summary_file, indent=2)
        logger.info(f"📝 Detailed change summary written to {summary_path.resolve()}")

        # Process metadata to parquet for the years in this chunk
        if years_in_chunk:
            logger.info("")
            logger.info(
                f"📊 Processing metadata to parquet for years: {sorted(years_in_chunk)}"
            )
            # Give S3 a moment to propagate the newly uploaded files
            import time as time_module

            time_module.sleep(5)
            try:
                # Convert years to strings to match --sync-s3 behavior (year_dir.name returns strings)
                generate_parquet_from_metadata(
                    s3_bucket, [str(y) for y in years_in_chunk]
                )
            except Exception as e:
                logger.warning(f"⚠️  Parquet generation failed: {e}")

            # Check timeout after completing a chunk
            if timeout_seconds and time.time() - start_time >= timeout_seconds:
                logger.warning(
                    f"⏰ Timeout reached after completing chunk {chunk_start} to {chunk_end}"
                )
                logger.info("💾 Progress saved. Run again to continue from next chunk.")
                remaining = len(all_five_year_chunks) - len(completed_chunks)
                logger.info("")
                logger.info("📌 " + "=" * 66)
                logger.info(f"📌 Chunk completed! {remaining} chunks remaining")
                logger.info("📌 Run the same command again to continue processing")
                logger.info("📌 " + "=" * 66)
                return

            # Continue to next chunk in the loop

    # All chunks completed (loop finished naturally)
    logger.info("")
    logger.info("🎉 " + "=" * 66)
    logger.info("🎉 ALL CHUNKS COMPLETED! Gap-filling process finished successfully!")
    logger.info("🎉 " + "=" * 66)
    clear_fill_progress()


def find_missing_date_ranges(s3_bucket, start_date=None, end_date=None):
    """
    DEPRECATED: This function is no longer used.
    The new approach downloads everything and lets deduplication handle existing files.
    """
    logger.warning(
        "find_missing_date_ranges is deprecated. Use sync_s3_fill_gaps directly."
    )
    return []


def generate_parquet_from_local_metadata(local_dir, s3_bucket):
    """
    Process metadata files from local directory only and append to S3 parquet files

    Args:
        local_dir: Local directory with newly downloaded files
        s3_bucket: S3 bucket name for output
    """
    logger.info("Processing newly downloaded metadata to parquet...")

    # Track all processed records
    total_records = 0
    processed_years = set()

    # Create S3 client for uploading
    s3 = boto3.client("s3")

    # For each year directory in local_dir
    for year_dir in Path(local_dir).glob("*"):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue

        year = year_dir.name
        processed_years.add(year)
        logger.info(f"Processing local metadata for year: {year}")

        # Check if this year has metadata files
        metadata_zip = year_dir / "metadata.zip"
        if not metadata_zip.exists():
            logger.info(f"No metadata.zip found for year {year}, skipping")
            continue

        # Verify it's a valid zip file before processing
        try:
            # Test open the zip file to validate it
            with zipfile.ZipFile(metadata_zip, "r") as test_zip:
                file_count = len(test_zip.namelist())
                logger.info(f"Found valid metadata.zip with {file_count} files")

                # Process the metadata files
                records = []
                for filename in test_zip.namelist():
                    if filename.endswith(".json"):
                        with test_zip.open(filename) as f:
                            try:
                                metadata = json.load(f)
                                processed = (
                                    SupremeCourtS3Processor.process_metadata_static(
                                        metadata, year
                                    )
                                )
                                if processed:
                                    records.append(processed)
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON in {filename}, skipping")
        except zipfile.BadZipFile:
            logger.error(f"Invalid zip file for year {year}: {metadata_zip}")

            # Try downloading from S3 instead
            s3_key = f"metadata/zip/year={year}/metadata.zip"
            temp_zip = year_dir / "metadata_temp.zip"

            try:
                logger.info(f"Attempting to download metadata from S3: {s3_key}")
                s3.download_file(s3_bucket, s3_key, str(temp_zip))

                with zipfile.ZipFile(temp_zip, "r") as z:
                    records = []
                    for filename in z.namelist():
                        if filename.endswith(".json"):
                            with z.open(filename) as f:
                                metadata = json.load(f)
                                processed = (
                                    SupremeCourtS3Processor.process_metadata_static(
                                        metadata, year
                                    )
                                )
                                if processed:
                                    records.append(processed)
            except Exception as s3_err:
                logger.error(f"Failed to recover metadata from S3: {s3_err}")
                continue

        except Exception as e:
            logger.error(f"Error processing metadata for year {year}: {e}")
            continue

        # If we found records, write them to parquet
        if records:
            total_records += len(records)
            logger.info(f"Found {len(records)} records for year {year}")

            # Convert to DataFrame with proper schema
            df = pd.DataFrame(records)

            # Write to temp parquet file
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=True
            ) as tmp_file:
                df.to_parquet(tmp_file.name, compression="snappy")

                # Target S3 path for this year
                s3_key = f"metadata/parquet/year={year}/metadata.parquet"

                # Check if existing parquet file exists, and merge if so
                try:
                    with tempfile.NamedTemporaryFile(
                        suffix=".parquet", delete=True
                    ) as existing_file:
                        s3.download_file(s3_bucket, s3_key, existing_file.name)
                        existing_df = pd.read_parquet(existing_file.name)
                        df = pd.concat([existing_df, df], ignore_index=True)
                        df.to_parquet(tmp_file.name, compression="snappy")
                except Exception as e:
                    logger.info(f"Creating new parquet file for year {year}: {e}")

                # Upload to S3
                s3.upload_file(tmp_file.name, s3_bucket, s3_key)
                logger.info(f"Uploaded metadata for year {year} to S3")

    logger.info(
        f"Processed {total_records} records across {len(processed_years)} years"
    )
    return total_records > 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--day_step", type=int, default=1, help="Number of days per chunk"
    )
    parser.add_argument("--max_workers", type=int, default=5, help="Number of workers")
    parser.add_argument(
        "--no-package",
        action="store_true",
        help="Skip packaging individual files into zip archives on startup/completion",
    )
    parser.add_argument(
        "--sync-s3",
        action="store_true",
        default=False,
        help="Sync data from S3 before running the downloader",
    )
    parser.add_argument(
        "--sync-s3-fill",
        action="store_true",
        default=False,
        help="Process ONE 5-year chunk of data per run. Automatically resumes from where it left off. Run repeatedly to complete all chunks from 1950 to present.",
    )
    parser.add_argument(
        "--timeout-hours",
        type=float,
        default=5.5,
        help="Maximum hours to run before graceful exit (default: 5.5 hours)",
    )
    args = parser.parse_args()

    if args.sync_s3_fill:
        # Gap-filling mode: identify and fill missing date ranges
        sync_s3_fill_gaps(
            s3_bucket=S3_BUCKET,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            timeout_hours=args.timeout_hours,
        )
    elif args.sync_s3:
        with S3ArchiveManager(S3_BUCKET, S3_PREFIX, LOCAL_DIR) as archive_manager:
            latest_date = get_latest_date_from_metadata()
            logger.info(
                f"Latest date in metadata: {latest_date.date() if latest_date else 'Unknown'}"
            )
            today = datetime.now().date()
            if latest_date.date() < today:
                run(
                    start_date=(latest_date - timedelta(days=1)).strftime("%Y-%m-%d"),
                    end_date=today.strftime("%Y-%m-%d"),
                    archive_manager=archive_manager,
                )
                logger.info(
                    "Download and packaging complete. Ready to upload new packages."
                )

        # AFTER the with block completes (archives are now uploaded to S3)
        # Check if any new files were actually downloaded in this run
        if latest_date.date() < today:
            # Only process if we actually ran the downloader and found new data
            logger.info("Checking for newly downloaded data...")
            downloaded_years = set()
            for year_dir in LOCAL_DIR.glob("*"):
                if year_dir.is_dir() and year_dir.name.isdigit():
                    downloaded_years.add(year_dir.name)

            if downloaded_years:
                logger.info(f"Found new data for years: {sorted(downloaded_years)}")
                # Process metadata AFTER archives are uploaded to S3
                logger.info(
                    f"Processing metadata files for years: {sorted(downloaded_years)}..."
                )

                # Now use the standard S3 processor since files are already uploaded
                generate_parquet_from_metadata(S3_BUCKET, downloaded_years)
            else:
                logger.info("No new files downloaded. Skipping parquet conversion.")
        else:
            logger.info("Data is already up to date. Skipping parquet conversion.")
    else:
        run(
            args.start_date,
            args.end_date,
            args.day_step,
            args.max_workers,
            package_on_startup=not args.no_package,
        )

"""
Supreme Court judgment scraper for scr.sci.gov.in
Based on the ecourts scraper but simplified for single court structure
"""
