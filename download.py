import argparse
import concurrent.futures
import functools
import json
import logging
import os
import re
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import urllib
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Optional

import boto3
import colorlog
import lxml.html as LH
import pandas as pd
import requests
import urllib3
from botocore import UNSIGNED
from botocore.client import Config
from bs4 import BeautifulSoup
from PIL import Image

# Add src to path for captcha_solver import
sys.path.insert(0, str(Path(__file__).parent / "src"))

from archive_manager import S3ArchiveManager
from process_metadata import SupremeCourtS3Processor
from src.captcha_solver.main import get_text

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

S3_BUCKET = "indian-supreme-court-judgments"
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
            # If we're at the last chunk and it's smaller than day_step, log it
            if range_end == end_date_parsed:
                days_in_chunk = (range_end - current_date).days + 1
                if days_in_chunk < day_step:
                    logger.info(f"Processing final chunk of {days_in_chunk} days")

            # Move to next day after range_end
            current_date = range_end + timedelta(days=1)


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


def get_archive_type(lang_code):
    """Determine archive type based on language code.

    Returns 'english' for empty string or 'EN' (case-insensitive),
    otherwise returns 'regional'.
    """
    return "english" if (lang_code == "" or lang_code.upper() == "EN") else "regional"


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
    # Package existing individual files into tar archives on startup
    if package_on_startup:
        logger.info("Packaging existing individual files into tar archives...")
        try:
            # Import here to avoid circular imports
            from package_tar_files import TarPackager

            packager = TarPackager()
            packager.package_all()
            logger.info("Startup packaging completed")

            # Always cleanup individual files after packaging
            logger.info("Cleaning up individual files after packaging...")
            packager.cleanup_individual_files()
            logger.info("Startup cleanup completed")

        except Exception as e:
            logger.warning(f"Startup packaging failed: {e}")
            logger.warning("Continuing with downloads anyway...")

    # Create archive_manager if not provided (for local scraping mode)
    if archive_manager is None:
        archive_manager = S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            local_only=True,  # Default to local-only mode (no S3 access required)
        )

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
            packager = TarPackager()
            packager.package_all()
            logger.info("Post-download packaging completed")

            # Always cleanup newly downloaded individual files
            logger.info("Cleaning up newly downloaded individual files...")
            packager.cleanup_individual_files()
            logger.info("Post-download cleanup completed")
        except Exception as e:
            logger.warning(f"Post-download packaging failed: {e}")
            traceback.print_exc()


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
        assert button and "onclick" in button.attrs, (
            f"No PDF button found, task: {self.task}"
        )
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
            archive_type = get_archive_type(lang_code)

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
                except Exception as e:
                    logger.debug(f"Response is not JSON: {e}")
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

        # Package directory for tar files and indexes
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
        """Check if PDF exists - verify actual file on disk first, then check if it's in tar"""
        # Get the actual file path
        pdf_path = self.get_pdf_path(year, filename, lang_code)

        # Check if file actually exists on disk as individual file
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            return True

        # Check if file is already packaged in tar (from index)
        archive_type = "english" if lang_code == "" else "regional"
        packaged_files = self._load_index_files(year, archive_type)
        return filename in packaged_files

    def metadata_exists(self, year, filename):
        """Check if JSON metadata exists - verify actual file on disk first, then check if it's in tar"""
        # Get the actual file path
        metadata_path = self.get_metadata_path(year, filename)

        # Check if file actually exists on disk as individual file
        if metadata_path.exists() and metadata_path.stat().st_size > 0:
            return True

        # Check if file is already packaged in tar (from index)
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


def sync_latest_metadata_tar(force_refresh=True):
    """
    Download the current year's metadata tar file from S3, or latest available.
    If force_refresh is True, always download a fresh copy.
    """
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # First try to get current year's metadata
    current_year = datetime.now().year
    current_year_key = f"metadata/tar/year={current_year}/metadata.tar"

    # Check if current year metadata exists
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=current_year_key)
        latest_tar_key = current_year_key
        logger.info(f"Found current year ({current_year}) metadata")
    except Exception as e:
        # Fall back to finding the latest available year
        logger.debug(f"Current year metadata not available: {e}")
        logger.info("Current year metadata not found, finding latest available...")
        tars = []

        # Search for metadata tar files
        paginator = s3.get_paginator("list_objects_v2")
        prefix = "metadata/tar/"

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith("/metadata.tar"):
                    # Extract year from path like metadata/tar/year=2023/metadata.tar
                    year_match = re.search(r"year=(\d{4})/", key)
                    if year_match:
                        tars.append((key, int(year_match.group(1))))

        if not tars:
            raise Exception("No metadata tar files found in S3")

        # Sort by year descending and take the most recent
        tars.sort(key=lambda x: x[1], reverse=True)
        latest_tar_key = tars[0][0]

    # Create year directory for the tar file
    year_match = re.search(r"year=(\d{4})/", latest_tar_key)
    if year_match:
        year = year_match.group(1)
        year_dir = LOCAL_DIR / year
        year_dir.mkdir(parents=True, exist_ok=True)
        local_path = year_dir / "metadata.tar"
    else:
        local_path = LOCAL_DIR / Path(latest_tar_key).name

    # Force a fresh download if requested
    if force_refresh and local_path.exists():
        logger.info("Removing cached metadata tar to force refresh...")
        local_path.unlink()

    if not local_path.exists():
        logger.info(f"Downloading {latest_tar_key} ...")
        s3.download_file(S3_BUCKET, latest_tar_key, str(local_path))
    else:
        logger.info(f"Using cached metadata tar: {local_path}")

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
        except Exception as e:
            logger.debug(f"Could not parse date '{m.group(1)}': {e}")
    return None


def find_latest_decision_date_in_tar(tar_path):
    latest_date = None
    with tarfile.open(tar_path, "r") as tf:
        for member in tf.getmembers():
            if not member.name.endswith(".json"):
                continue
            f = tf.extractfile(member)
            if f:
                try:
                    data = json.load(f)
                    decision_date = extract_decision_date_from_json(data)
                    if decision_date and (
                        latest_date is None or decision_date > latest_date
                    ):
                        latest_date = decision_date
                except Exception as e:
                    logger.debug(f"Could not parse JSON from {member.name}: {e}")
                    continue
    if latest_date:
        logger.info(f"Latest decision date in metadata tar: {latest_date.date()}")
    else:
        logger.warning(
            "No decision date found in metadata tar, falling back to TAR entry date."
        )
        # fallback (not recommended)
        with tarfile.open(tar_path, "r") as tf:
            latest_date = max(datetime.fromtimestamp(m.mtime) for m in tf.getmembers())
    return latest_date


def run_downloader(start_date, end_date, archive_manager=None):
    """Helper function to run the downloader for a date range"""
    logger.info(f"Fetching new data from {start_date} to {end_date} ...")
    run(
        start_date=(start_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        archive_manager=archive_manager,
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
    index_key = f"metadata/tar/year={current_year}/metadata.index.json"

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
    latest_tar = sync_latest_metadata_tar()
    return find_latest_decision_date_in_tar(latest_tar)


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
        metadata_tar = year_dir / "metadata.tar"
        if not metadata_tar.exists():
            logger.info(f"No metadata.tar found for year {year}, skipping")
            continue

        # Initialize records list
        records = []

        # Verify it's a valid tar file before processing
        try:
            # Test open the tar file to validate it
            with tarfile.open(metadata_tar, "r") as test_tar:
                members = test_tar.getmembers()
                file_count = len(members)
                logger.info(f"Found valid metadata.tar with {file_count} files")

                # Process the metadata files
                records = []
                for member in members:
                    if member.name.endswith(".json"):
                        f = test_tar.extractfile(member)
                        if f:
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
                                logger.warning(
                                    f"Invalid JSON in {member.name}, skipping"
                                )
        except tarfile.TarError:
            logger.error(f"Invalid tar file for year {year}: {metadata_tar}")

            # Try downloading from S3 instead
            s3_key = f"metadata/tar/year={year}/metadata.tar"
            temp_tar = year_dir / "metadata_temp.tar"

            try:
                logger.info(f"Attempting to download metadata from S3: {s3_key}")
                s3.download_file(s3_bucket, s3_key, str(temp_tar))

                with tarfile.open(temp_tar, "r") as tf:
                    records = []
                    for member in tf.getmembers():
                        if member.name.endswith(".json"):
                            f = tf.extractfile(member)
                            if f:
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
            finally:
                # Clean up temporary tar file
                try:
                    if temp_tar.exists():
                        temp_tar.unlink()
                except Exception as cleanup_err:
                    logger.debug(f"Failed to cleanup temp tar file: {cleanup_err}")

        except Exception as e:
            logger.error(f"Error processing metadata for year {year}: {e}")
            continue

        # If we found records, write them to parquet
        if records:
            total_records += len(records)
            logger.info(f"Found {len(records)} records for year {year}")

            # Convert to DataFrame with proper schema
            df = pd.DataFrame(records)

            # Target S3 path for this year
            s3_key = f"metadata/parquet/year={year}/metadata.parquet"

            # Check if existing parquet file exists, and merge if so
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".parquet", delete=False
                ) as existing_file:
                    existing_path = existing_file.name

                try:
                    s3.download_file(s3_bucket, s3_key, existing_path)
                    existing_df = pd.read_parquet(existing_path)

                    # Merge and remove duplicates based on 'path' field
                    df = pd.concat([existing_df, df], ignore_index=True)

                    # Remove duplicates, keeping the last occurrence (newest data)
                    if "path" in df.columns:
                        df = df.drop_duplicates(subset=["path"], keep="last")
                        logger.info(
                            f"Removed duplicates, {len(df)} unique records remain"
                        )

                    logger.info(f"Merged with existing data for year {year}")
                except Exception as e:
                    logger.info(f"Creating new parquet file for year {year}: {e}")
                finally:
                    # Clean up downloaded file
                    try:
                        if os.path.exists(existing_path):
                            os.unlink(existing_path)
                    except Exception as cleanup_err:
                        logger.debug(f"Failed to cleanup temp file: {cleanup_err}")

            except Exception as e:
                logger.error(f"Error handling temp file for year {year}: {e}")

            # Write to temp parquet file and upload
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False
            ) as tmp_file:
                tmp_path = tmp_file.name

            try:
                df.to_parquet(tmp_path, compression="snappy", index=False)

                # Upload to S3
                s3.upload_file(tmp_path, s3_bucket, s3_key)
                logger.info(f"Uploaded {len(df)} records for year {year} to S3")
            finally:
                # Clean up temp file
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception as cleanup_err:
                    logger.debug(f"Failed to cleanup temp file: {cleanup_err}")

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
        help="Skip packaging individual files into tar archives on startup/completion",
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
        from sync_s3_fill import sync_s3_fill_gaps

        sync_s3_fill_gaps(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            timeout_hours=args.timeout_hours,
        )
    elif args.sync_s3:
        # Sync mode: download new data from S3 and process
        from sync_s3 import run_sync_s3

        run_sync_s3(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
        )
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