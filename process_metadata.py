import concurrent.futures
import json
import logging
import os
import re
import tempfile
import zipfile
from typing import Dict, Optional

import boto3
import lxml.html as LH
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# Get logger
logger = logging.getLogger(__name__)


class SupremeCourtS3Processor:
    def __init__(self, s3_bucket, s3_prefix="", batch_size=5000, years_to_process=None):
        """
        Initialize the Supreme Court Metadata Processor with S3 for both input and output

        Args:
            s3_bucket: S3 bucket name containing judgment data and for output
            s3_prefix: Prefix (folder) within the bucket to look for data
            batch_size: Number of records to process before writing a batch
            years_to_process: Optional set/list of years to process (if None, process all)
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.without_rh = 0
        self.record_count = 0
        self.batch_size = batch_size
        self.years_to_process = set(years_to_process) if years_to_process else None

        # Initialize S3 client
        self.s3 = boto3.client("s3")  # Use default credentials

        # Define fields to extract (Supreme Court specific)
        self.all_fields = [
            "title",
            "petitioner",
            "respondent",
            "description",
            "judge",
            "author_judge",
            "citation",
            "case_id",
            "cnr",
            "decision_date",
            "disposal_nature",
            "court",
            "available_languages",
            "raw_html",
            "path",
            "nc_display",
            "scraped_at",
            "year",  # Year field for partitioning
        ]

    def list_s3_objects(self, prefix=""):
        """List objects in the S3 bucket with the given prefix."""
        paginator = self.s3.get_paginator("list_objects_v2")
        full_prefix = os.path.join(self.s3_prefix, prefix) if self.s3_prefix else prefix

        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=full_prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    yield obj["Key"]

    def read_s3_json(self, s3_key):
        """Read JSON data directly from S3."""
        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except Exception as e:
            logger.error(f"Error reading {s3_key}: {e}")
            return None

    def process_s3_zip(self, s3_key, year):
        """Process a ZIP file from S3 with minimal local storage."""
        record_buffer = []
        record_count = 0

        # We need to download the ZIP temporarily because zipfile needs random access
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=True) as tmp_zip:
            try:
                # Download ZIP to temp file
                self.s3.download_file(self.s3_bucket, s3_key, tmp_zip.name)

                # Process ZIP contents in memory
                with zipfile.ZipFile(tmp_zip.name, "r") as zip_ref:
                    for file_info in zip_ref.infolist():
                        if file_info.filename.endswith(".json"):
                            # Extract JSON data directly to memory
                            with zip_ref.open(file_info) as json_file:
                                content = json_file.read().decode("utf-8")
                                metadata = json.loads(content)

                                # Try to determine year from metadata if possible
                                metadata_year = (
                                    self._extract_year_from_metadata(metadata) or year
                                )
                                processed = self.process_metadata(
                                    metadata, metadata_year
                                )

                                if processed:
                                    record_buffer.append(processed)

                                    # Write batch if buffer is full
                                    if len(record_buffer) >= self.batch_size:
                                        self.write_records_to_s3(
                                            record_buffer, metadata_year
                                        )
                                        record_count += len(record_buffer)
                                        record_buffer = []

            except Exception as e:
                logger.error(f"Error processing ZIP file {s3_key}: {e}")

        # Write any remaining records
        if record_buffer:
            self.write_records_to_s3(record_buffer, year)
            record_count += len(record_buffer)

        return record_count

    def process_s3_json(self, s3_key, year):
        """Process a single JSON file directly from S3."""
        try:
            metadata = self.read_s3_json(s3_key)
            if not metadata:
                return 0

            # Try to determine year from metadata if possible
            metadata_year = self._extract_year_from_metadata(metadata) or year
            processed = self.process_metadata(metadata, metadata_year)

            if processed:
                self.write_records_to_s3([processed], metadata_year)
                return 1

        except Exception as e:
            logger.error(f"Error processing JSON file {s3_key}: {e}")

        return 0

    def _extract_year_from_filename(self, filename):
        """Extract the year from a filename like 'sc-judgments-2025-metadata.zip'."""
        match = re.search(r"(\d{4})", filename)
        if match:
            return match.group(1)
        return "unknown"

    def get_all_s3_sources(self):
        """Get all source files (ZIP archives and JSON files) from S3."""
        # Check for ZIP archives in the new path structure first
        zip_files = []

        # Look for metadata zip files in metadata/zip/year=YYYY/ directory structure
        for s3_key in self.list_s3_objects("metadata/zip/"):
            if s3_key.endswith("metadata.zip"):
                # Extract year from path like metadata/zip/year=2023/metadata.zip
                year_match = re.search(r"year=(\d{4})/", s3_key)
                if year_match:
                    year = year_match.group(1)
                    # Skip if we're only processing specific years and this isn't one of them
                    if self.years_to_process and year not in self.years_to_process:
                        continue
                    zip_files.append((s3_key, year))

        # Also look for files with the old naming pattern as a fallback
        if not zip_files:
            for s3_key in self.list_s3_objects():
                if s3_key.endswith("-metadata.zip") and "sc-judgments" in s3_key:
                    filename = os.path.basename(s3_key)
                    year = self._extract_year_from_filename(filename)
                    # Skip if we're only processing specific years and this isn't one of them
                    if self.years_to_process and year not in self.years_to_process:
                        continue
                    zip_files.append((s3_key, year))

        if zip_files:
            return zip_files

        # If no ZIP files found at all, look for individual JSON files
        json_files = []
        for s3_key in self.list_s3_objects():
            if s3_key.endswith(".json"):
                year = "unknown"
                # Try to extract year from path structure first
                year_match = re.search(r"/year=(\d{4})/|/(\d{4})/", s3_key)
                if year_match:
                    # Group 1 or 2 will contain the year
                    year = (
                        year_match.group(1)
                        if year_match.group(1)
                        else year_match.group(2)
                    )

                # Skip if we're only processing specific years and this isn't one of them
                if self.years_to_process and year not in self.years_to_process:
                    continue

                json_files.append((s3_key, year))

        return json_files

    def _extract_year_from_metadata(self, metadata):
        """Try to extract year from metadata."""
        # First check if there's citation_year in the metadata
        if "citation_year" in metadata:
            return metadata["citation_year"]

        # Next try to extract from nc_display
        if "nc_display" in metadata:
            match = re.search(r"(\d{4})INSC", metadata["nc_display"])
            if match:
                return match.group(1)

        # Try from raw_html
        if "raw_html" in metadata:
            match = re.search(r"(\d{4})INSC", metadata["raw_html"])
            if match:
                return match.group(1)

        return None

    def write_records_to_s3(self, records, year):
        """Write records directly to S3 as parquet."""
        if not records:
            return

        # Ensure all records have all fields
        for record in records:
            for field in self.all_fields:
                if field not in record:
                    record[field] = None

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Ensure all expected columns
        for field in self.all_fields:
            if field not in df.columns:
                df[field] = None

        # Order columns
        df = df[self.all_fields]

        # Convert string columns to string type
        string_cols = [
            field
            for field in self.all_fields
            if field not in ["pdf_exists", "pdf_linearized"]
        ]

        for col in string_cols:
            if col in df.columns:
                try:
                    df[col] = df[col].astype("string")
                except Exception as e:
                    logger.debug(f"Could not convert {col} to string: {e}")

        # Upload to S3 directly in the requested format:
        # s3_bucket/metadata/parquet/year=YYYY/metadata.parquet
        s3_key = f"metadata/parquet/year={year}/metadata.parquet"

        # Check if a file already exists and merge if so
        try:
            # If the file exists, we need to merge with it
            self.s3.head_object(Bucket=self.s3_bucket, Key=s3_key)

            # Download existing file
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False
            ) as existing_file:
                existing_path = existing_file.name

            try:
                self.s3.download_file(self.s3_bucket, s3_key, existing_path)
                existing_df = pd.read_parquet(existing_path)

                # Merge files
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # Remove duplicates based on 'path' field, keeping the last occurrence (newest)
                if "path" in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(
                        subset=["path"], keep="last"
                    )
                    logger.info(
                        f"Removed duplicates for year {year}, {len(combined_df)} unique records remain"
                    )

                df = combined_df
            finally:
                # Clean up temp file
                try:
                    if os.path.exists(existing_path):
                        os.unlink(existing_path)
                except Exception:
                    pass
        except Exception as e:
            # File doesn't exist or error downloading, no need to merge
            logger.debug(f"No existing parquet to merge for year {year}: {e}")

        # Write to temp parquet file and upload
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            df.to_parquet(tmp_path, compression="snappy", index=False)

            # Upload file to S3
            self.s3.upload_file(tmp_path, self.s3_bucket, s3_key)
            logger.info(f"Uploaded {len(df)} records for year {year} to S3")
        finally:
            # Clean up temp file
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass

        # Return the actual number of records written (after deduplication)
        return len(df)

    def process_metadata(self, metadata: dict, year=None) -> Optional[dict]:
        """
        Process raw HTML metadata and extract structured information.

        This handles Supreme Court specific format.
        """
        if "raw_html" not in metadata:
            self.without_rh += 1
            return None

        html_s = metadata["raw_html"]

        # Parse HTML
        html_element = LH.fromstring(html_s)
        soup = BeautifulSoup(html_s, "html.parser")

        # Initialize case details
        case_details = {
            "raw_html": html_s,
            "path": metadata.get("path", ""),
            "nc_display": metadata.get("nc_display", ""),
            "scraped_at": metadata.get("scraped_at", ""),
            "year": year if year else "unknown",
        }

        # Extract all metadata fields
        case_details["available_languages"] = self._extract_languages(soup)
        case_details.update(self._extract_case_title(soup, html_element))

        description_elem = html_element.xpath("./text()")
        case_details["description"] = (
            description_elem[0].strip() if description_elem else ""
        )

        case_details.update(self._extract_judges(soup, html_element))
        case_details.update(self._extract_citation(soup))
        case_details.update(self._extract_case_details(soup, html_element))

        return case_details

    def _extract_languages(self, soup: BeautifulSoup) -> str:
        """Extract available languages from the language selector as CSV string."""
        language_codes = []
        lang_select = soup.select_one('select[id^="language"]')

        if not lang_select:
            return ""

        for option in lang_select.find_all("option"):
            value = option.get("value", "")
            text = option.text.strip()

            # Handle the default language (English) with empty value
            if value == "" and text == "English":
                language_codes.append("ENG")
            elif value:
                language_codes.append(value)

        return ",".join(language_codes)

    def _extract_case_title(self, soup: BeautifulSoup, html_element) -> Dict[str, str]:
        """Extract case title, petitioner and respondent."""
        result = {"title": "", "petitioner": "", "respondent": ""}

        # Try BeautifulSoup approach first
        title_btn = soup.select_one('button[id^="link_"]')
        if title_btn:
            title_elem = title_btn.find("strong")
            if title_elem:
                full_title = title_elem.get_text().strip()
                result["title"] = full_title

                # Try to extract petitioner and respondent
                if "versus" in full_title.lower():
                    parts = re.split(r"\s+versus\s+", full_title, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        result["petitioner"] = parts[0].strip()
                        result["respondent"] = parts[1].strip()
                return result

        # Fallback to lxml approach if BeautifulSoup didn't find it
        try:
            title = html_element.xpath("./button//text()")[0].strip()
            result["title"] = title

            # Try to extract petitioner and respondent
            if "versus" in title.lower():
                parts = re.split(r"\s+versus\s+", title, flags=re.IGNORECASE)
                if len(parts) >= 2:
                    result["petitioner"] = parts[0].strip()
                    result["respondent"] = parts[1].strip()
        except (IndexError, KeyError):
            pass

        return result

    def _extract_judges(self, soup: BeautifulSoup, html_element) -> Dict[str, str]:
        """Extract judges information with author judge identification."""
        result = {"judge": "", "author_judge": None}

        # Try BeautifulSoup approach
        judges_elem = soup.find("strong", string=re.compile(r"Coram\s*:"))
        if judges_elem:
            judges_text = judges_elem.get_text().strip()
            if ":" in judges_text:
                judges_text = judges_text.split(":", 1)[1].strip()

            # Clean up judges text and identify author (marked with *)
            judges_list = [j.strip() for j in re.split(r",\s*", judges_text)]
            clean_judges = []

            for judge in judges_list:
                clean_judge = re.sub(r"\*$", "", judge)  # Remove asterisk
                clean_judges.append(clean_judge)

                # Check if this judge is the author (has asterisk)
                if "*" in judge:
                    result["author_judge"] = clean_judge

            result["judge"] = ", ".join(clean_judges)
            return result

        # Fallback to lxml approach
        judge_txt = html_element.xpath("./strong/text()")
        if judge_txt:
            if ":" in judge_txt[0]:
                judges_text = judge_txt[0].split(":", 1)[1].strip()
                result["judge"] = judges_text

                # Try to identify author judge
                if "*" in judges_text:
                    # Split by comma and look for the one with asterisk
                    judges_list = [j.strip() for j in re.split(r",\s*", judges_text)]
                    for judge in judges_list:
                        if "*" in judge:
                            result["author_judge"] = judge.replace("*", "").strip()
                            break
            else:
                result["judge"] = judge_txt[0].strip()

        return result

    def _extract_citation(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract citation information."""
        result = {"citation": "", "case_id": "", "cnr": ""}

        # Extract standard citation
        citation_elem = soup.select_one(".escrText")
        if citation_elem:
            result["citation"] = citation_elem.get_text().strip()

        # Extract case identifier
        nc_display_elem = soup.select_one(".ncDisplay")
        if nc_display_elem:
            result["case_id"] = nc_display_elem.get_text().strip()

        # Extract CNR (Case Number Record)
        cnr_input = soup.select_one('input[id="cnr"]')
        if cnr_input and cnr_input.has_attr("value"):
            result["cnr"] = cnr_input["value"]

        return result

    def _extract_case_details(
        self, soup: BeautifulSoup, html_element
    ) -> Dict[str, str]:
        """Extract case details like date, case number, disposal nature."""
        result = {
            "decision_date": None,
            "disposal_nature": "",
            "court": "Supreme Court of India",  # Default for Supreme Court data
        }

        # First try with BeautifulSoup for Supreme Court format
        details_elem = soup.find("strong", class_="caseDetailsTD")
        if details_elem:
            # Extract decision date
            date_span = details_elem.find("span", string=re.compile(r"Decision Date"))
            if date_span and date_span.find_next("font"):
                result["decision_date"] = date_span.find_next("font").get_text().strip()

            # Extract disposal nature
            disposal_span = details_elem.find(
                "span", string=re.compile(r"Disposal Nature")
            )
            if disposal_span and disposal_span.find_next("font"):
                result["disposal_nature"] = (
                    disposal_span.find_next("font").get_text().strip()
                )

            return result

        # Fallback to lxml XPath approach for other formats
        try:
            case_details_elements = html_element.xpath(
                '//strong[@class="caseDetailsTD"]'
            )[0]

            try:
                result["decision_date"] = case_details_elements.xpath(
                    './/span[contains(text(), "Decision Date")]/following-sibling::font/text()'
                )[0].strip()
            except (IndexError, KeyError):
                pass

            try:
                result["disposal_nature"] = case_details_elements.xpath(
                    './/span[contains(text(), "Disposal Nature")]/following-sibling::font/text()'
                )[0].strip()
            except (IndexError, KeyError):
                pass

        except (IndexError, KeyError):
            pass

        return result

    def process_all(self, max_workers=None):
        """Process all available sources from S3 using parallel processing."""
        sources = list(self.get_all_s3_sources())

        if not sources:
            logger.info(f"No source files found in S3 bucket {self.s3_bucket}!")
            return

        logger.info(f"Found {len(sources)} source files to process from S3")

        # Use appropriate number of workers based on CPU count
        if max_workers is None:
            max_workers = min(32, os.cpu_count() + 4)

        total_records = 0
        processed_years = set()

        # Group sources by year for more efficient processing
        sources_by_year = {}
        for s3_key, year in sources:
            if year not in sources_by_year:
                sources_by_year[year] = []
            sources_by_year[year].append(s3_key)

        # Process each year's data in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for year, year_sources in sources_by_year.items():
                for s3_key in year_sources:
                    # Skip processing if this year is not in the years_to_process set
                    if (
                        self.years_to_process is not None
                        and year not in self.years_to_process
                    ):
                        logger.info(
                            f"Skipping {s3_key} for year {year} (not in years_to_process)"
                        )
                        continue

                    # Choose processing method based on file type
                    if s3_key.endswith(".zip"):
                        future = executor.submit(self.process_s3_zip, s3_key, year)
                    else:
                        future = executor.submit(self.process_s3_json, s3_key, year)
                    futures[future] = (year, s3_key)

            # Monitor progress
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Processing years",
            ):
                year, s3_key = futures[future]
                try:
                    record_count = future.result()
                    total_records += record_count
                    processed_years.add(year)
                    logger.info(
                        f"Completed {os.path.basename(s3_key)} for year {year}: {record_count} records"
                    )
                except Exception as e:
                    logger.error(f"Error processing {s3_key} for year {year}: {e}")

        logger.info(
            f"Processed {len(processed_years)} years with {total_records} total records"
        )
        logger.info(
            f"Output files saved to S3 at {self.s3_bucket}/metadata/parquet/year=YYYY/metadata.parquet"
        )

    # Add new method for use when imported
    def process_bucket_metadata(self, max_workers=None):
        """
        Process all metadata in the bucket and generate parquet files.
        Can be called directly from other modules.

        Args:
            max_workers: Number of worker threads to use for processing

        Returns:
            tuple: (processed_years, total_records)
        """
        sources = list(self.get_all_s3_sources())

        if not sources:
            logger.info(f"No source files found in S3 bucket {self.s3_bucket}!")
            return set(), 0

        logger.info(f"Found {len(sources)} source files to process from S3")

        # Use appropriate number of workers based on CPU count
        if max_workers is None:
            max_workers = min(32, os.cpu_count() + 4)

        total_records = 0
        processed_years = set()

        # Group sources by year for more efficient processing
        sources_by_year = {}
        for s3_key, year in sources:
            if year not in sources_by_year:
                sources_by_year[year] = []
            sources_by_year[year].append(s3_key)

        # Process each year's data in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for year, year_sources in sources_by_year.items():
                for s3_key in year_sources:
                    # Choose processing method based on file type
                    if s3_key.endswith(".zip"):
                        future = executor.submit(self.process_s3_zip, s3_key, year)
                    else:
                        future = executor.submit(self.process_s3_json, s3_key, year)
                    futures[future] = (year, s3_key)

            # Monitor progress
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Processing metadata to parquet",
            ):
                year, s3_key = futures[future]
                try:
                    record_count = future.result()
                    total_records += record_count
                    processed_years.add(year)
                    logger.info(
                        f"Completed {os.path.basename(s3_key)} for year {year}: {record_count} records"
                    )
                except Exception as e:
                    logger.error(f"Error processing {s3_key} for year {year}: {e}")

        logger.info(
            f"Processed {len(processed_years)} years with {total_records} total records"
        )
        logger.info(
            f"Output files saved to S3 at {self.s3_bucket}/metadata/parquet/year=YYYY/metadata.parquet"
        )

        return processed_years, total_records

    @staticmethod
    def process_metadata_static(metadata: dict, year=None) -> Optional[dict]:
        """
        Static version of process_metadata that can be called without an instance
        """
        processor = SupremeCourtS3Processor("dummy-bucket")  # Temporary instance
        return processor.process_metadata(metadata, year)


# Main execution for when running as a script
def main():
    # S3 bucket information
    s3_bucket = "indian-supreme-court-judgments"  # Replace with your S3 bucket name
    s3_prefix = ""  # Optional prefix (folder) in the bucket

    processor = SupremeCourtS3Processor(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        batch_size=10000,  # Increased batch size for efficiency
    )

    # Process all available files using parallel processing
    processor.process_all(max_workers=os.cpu_count())


if __name__ == "__main__":
    main()
