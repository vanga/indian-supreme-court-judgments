from typing import Optional, Generator
from PIL import Image
from captcha_solver import get_text
import argparse
from datetime import datetime, timedelta
import traceback
import re
import json
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import lxml.html as LH
import urllib
import easyocr
import logging
import threading
import concurrent.futures
import urllib3
import uuid
import time
import warnings
import functools
import colorlog

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

reader = easyocr.Reader(["en"])

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

captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir = Path("./captcha-tmp")
temp_files_dir = Path("./temp-files")
captcha_failures_dir.mkdir(parents=True, exist_ok=True)
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)
temp_files_dir.mkdir(parents=True, exist_ok=True)


def get_json_file(file_path) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


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


def process_task(task):
    """Process a single date task"""
    try:
        downloader = Downloader(task)
        downloader.download()
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        traceback.print_exc()


def run(
    start_date=None, end_date=None, day_step=1, max_workers=5, package_on_startup=True
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
        for i, result in enumerate(executor.map(process_task, tasks)):
            # process_task doesn't return anything, so we're just tracking progress
            logger.info(f"Completed task {i+1}")

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


class Downloader:
    def __init__(self, task: SCDateTask):
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

        year = extract_year_from_path(pdf_info["path"])

        # Check if metadata already exists
        metadata_filename = f"{pdf_info['path']}.json"
        if not self.tar_checker.metadata_exists(year, metadata_filename):
            # Create metadata
            order_metadata = {
                "raw_html": html,
                "path": pdf_info["path"],
                "citation_year": pdf_info.get("citation_year", ""),
                "nc_display": pdf_info.get("nc_display", ""),
                "scraped_at": datetime.now().isoformat(),
            }

            # Get final path and save directly
            final_metadata_path = self.tar_checker.get_metadata_path(
                year, metadata_filename
            )
            final_metadata_path.parent.mkdir(parents=True, exist_ok=True)

            with open(final_metadata_path, "w") as f:
                json.dump(order_metadata, f, indent=2)

        # Download PDFs for each language
        pdfs_downloaded = 0
        for lang_code in language_codes:
            pdf_filename = self.get_pdf_filename(pdf_info["path"], lang_code)

            # Check if PDF already exists
            if self.tar_checker.pdf_exists(year, pdf_filename, lang_code):
                continue  # Skip download

            # Get final path and download directly
            final_pdf_path = self.tar_checker.get_pdf_path(
                year, pdf_filename, lang_code
            )
            final_pdf_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                success = self.download_pdf_to_file(final_pdf_path, pdf_info, lang_code)
                if success:
                    pdfs_downloaded += 1
            except Exception as e:
                logger.error(
                    f"Error downloading {pdf_filename}: {e}, task: {self.task}"
                )
                traceback.print_exc()
                # Clean up partial file on error
                if final_pdf_path.exists():
                    final_pdf_path.unlink()

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
            except ValueError as e:
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

    def download_pdf_to_file(self, final_pdf_path, pdf_info, lang_code):
        """Download PDF directly to specified final file path"""
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
            return False

        pdf_download_link = pdf_link_response.json()["outputfile"]
        pdf_response = requests.request(
            "GET",
            self.root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
            timeout=30,
        )

        # Validate response
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0 or no_of_bytes == 315:  # Empty or 404 response
            return False

        # Save directly to final path
        with open(final_pdf_path, "wb") as f:
            f.write(pdf_response.content)

        return True

    def get_pdf_filename(self, path, lang_code):
        """Get standardized PDF filename"""
        if lang_code:
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
    args = parser.parse_args()

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
