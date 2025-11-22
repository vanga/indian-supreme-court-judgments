"""
S3 Sync Module for Supreme Court Judgments
Handles syncing with S3 and incremental downloads
"""

import json
import logging
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.client import Config

logger = logging.getLogger(__name__)


def sync_latest_metadata_zip(s3_bucket, local_dir, force_refresh=True):
    """
    Download the current year's metadata zip file from S3, or latest available.
    If force_refresh is True, always download a fresh copy.
    """
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # First try to get current year's metadata
    current_year = datetime.now().year
    current_year_key = f"metadata/zip/year={current_year}/metadata.zip"

    # Check if current year metadata exists
    try:
        s3.head_object(Bucket=s3_bucket, Key=current_year_key)
        latest_zip_key = current_year_key
        logger.info(f"Found current year ({current_year}) metadata")
    except Exception:
        # Fall back to finding the latest available year
        logger.info("Current year metadata not found, finding latest available...")
        zips = []

        # Search for metadata zip files in the new structure
        paginator = s3.get_paginator("list_objects_v2")
        prefix = "metadata/zip/"

        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                # Extract year from path like metadata/zip/year=2025/metadata.zip
                year_match = re.search(r"year=(\d{4})/metadata\.zip", key)
                if year_match:
                    year = int(year_match.group(1))
                    zips.append((key, year))

        if not zips:
            raise Exception("No metadata zip files found")

        # Sort by year descending and take the most recent
        zips.sort(key=lambda x: x[1], reverse=True)
        latest_zip_key = zips[0][0]

    # Create year directory for the zip file
    year_match = re.search(r"year=(\d{4})/", latest_zip_key)
    if year_match:
        year = year_match.group(1)
        year_dir = local_dir / year
        year_dir.mkdir(parents=True, exist_ok=True)
        local_path = year_dir / "metadata.zip"
    else:
        local_path = local_dir / Path(latest_zip_key).name

    # Force a fresh download if requested
    if force_refresh and local_path.exists():
        logger.info("Removing cached metadata zip to force refresh...")
        local_path.unlink()

    if not local_path.exists():
        logger.info(f"Downloading {latest_zip_key} ...")
        s3.download_file(s3_bucket, latest_zip_key, local_path)
    else:
        logger.info(f"Using cached metadata zip: {local_path}")

    return local_path


def extract_decision_date_from_json(json_obj):
    """Extract decision date from metadata JSON"""
    raw_html = json_obj.get("raw_html", "")
    # Try to find DD-MM-YYYY after 'Decision Date'
    m = re.search(
        r"Decision Date\s*:\s*<font[^>]*>\s*(\d{2}-\d{2}-\d{4})\s*</font>", raw_html
    )
    if not m:
        # Fallback: try to find any date pattern
        m = re.search(r"(\d{2}-\d{2}-\d{4})", raw_html)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d-%m-%Y")
        except Exception:
            pass
    return None


def find_latest_decision_date_in_zip(zip_path):
    """Find the latest decision date in a metadata zip file"""
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


def get_latest_date_from_metadata(s3_bucket, force_check_files=False):
    """
    Get the latest decision date from metadata, preferring index.json if available.
    Falls back to parsing individual files if needed or if force_check_files=True.
    """
    # First try to download the index.json file from S3
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    current_year = datetime.now().year

    # Create a separate directory for index files
    index_cache_dir = Path("./index_cache")
    index_cache_dir.mkdir(parents=True, exist_ok=True)

    # Updated path for metadata index
    index_path = index_cache_dir / f"{current_year}_metadata.index.json"
    index_key = f"metadata/zip/year={current_year}/metadata.index.json"

    if not force_check_files:
        try:
            # Try to get current year index
            s3.download_file(s3_bucket, index_key, str(index_path))
            with open(index_path, "r") as f:
                index_data = json.load(f)

            # Check if updated_at is available
            if "updated_at" in index_data:
                # Parse the ISO format timestamp
                updated_at_str = index_data["updated_at"]
                # Handle both with and without timezone
                try:
                    latest_date = datetime.fromisoformat(
                        updated_at_str.replace("Z", "+00:00")
                    )
                except Exception:
                    latest_date = datetime.fromisoformat(updated_at_str)

                logger.info(f"Latest date from index.json: {latest_date.date()}")
                return latest_date

        except Exception as e:
            logger.info(f"Could not use index.json for date detection: {e}")

    # Fall back to the original method - parsing individual files
    logger.info("Falling back to parsing individual files for decision dates...")
    local_dir = Path("./local_sc_judgments_data")
    latest_zip = sync_latest_metadata_zip(s3_bucket, local_dir)
    return find_latest_decision_date_in_zip(latest_zip)


def run_downloader(start_date, end_date):
    """Helper function to run the downloader for a date range"""
    # Import here to avoid circular dependency
    from download import run

    logger.info(f"Fetching new data from {start_date} to {end_date} ...")
    run(
        start_date=(start_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    )


def run_sync_s3(
    s3_bucket, s3_prefix, local_dir, start_date, end_date, day_step, max_workers
):
    """
    Run the sync-s3 operation: check latest date in S3 and download new data.
    """
    from archive_manager import S3ArchiveManager
    from process_metadata import SupremeCourtS3Processor

    logger.info("Checking latest date from S3 metadata...")
    latest_date = get_latest_date_from_metadata(s3_bucket)
    today = datetime.now().date()

    logger.info(f"Latest decision date in S3: {latest_date.date()}")
    logger.info(f"Today's date: {today}")

    # Track changes for summary
    changes_made = False
    all_changes = {}
    upload_metadata = {}

    # The archive_manager context ensures proper cleanup even if we don't write to it
    with S3ArchiveManager(s3_bucket, s3_prefix, local_dir) as archive_manager:
        # Check if we're up to date
        if latest_date.date() >= today:
            logger.info("✅ Data is up-to-date. No new downloads needed.")
        else:
            logger.info(
                f"📥 New data available from {latest_date.date() + timedelta(days=1)} to {today}"
            )
            run_downloader(latest_date.date(), today)
            changes_made = True

        # Get changes before exiting context
        if changes_made:
            all_changes = archive_manager.get_all_changes()
            upload_metadata = archive_manager.get_upload_metadata()

    # AFTER the with block completes (archives are now uploaded to S3)
    # Save changes summary if any changes were made
    if changes_made and all_changes:
        summary_payload = {
            "sync_type": "sync-s3",
            "date_range": {
                "from": str(latest_date.date() + timedelta(days=1)),
                "to": str(today),
            },
            "generated_at": datetime.now().isoformat(),
            "years": {str(year): meta for year, meta in upload_metadata.items()},
            "files": all_changes,
        }

        # Append to cumulative all_changes.json
        all_changes_path = Path("./all_sync_changes.json")
        all_sync_changes = []
        if all_changes_path.exists():
            try:
                with open(all_changes_path, "r") as f:
                    all_sync_changes = json.load(f)
            except Exception:
                all_sync_changes = []

        all_sync_changes.append(summary_payload)

        with open(all_changes_path, "w") as f:
            json.dump(all_sync_changes, f, indent=2)
        logger.info(f"📝 Changes appended to {all_changes_path.resolve()}")

    # Check if any new files were actually downloaded in this run
    if latest_date.date() < today:
        logger.info("🔄 Processing newly downloaded metadata to parquet format...")

        # Generate parquet only for the newly downloaded data
        try:
            # Process only the years that were just downloaded
            start_year = latest_date.year
            end_year = today.year
            # Convert years to strings as expected by SupremeCourtS3Processor
            years_to_process = [str(year) for year in range(start_year, end_year + 1)]

            logger.info(f"Processing parquet for years: {years_to_process}")

            processor = SupremeCourtS3Processor(
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                batch_size=10000,
                years_to_process=years_to_process,
            )

            processed_years, total_records = processor.process_bucket_metadata()

            if total_records > 0:
                logger.info(
                    f"✅ Successfully processed {total_records} records to parquet"
                )
            else:
                logger.warning("⚠️ No new records were processed to parquet")

        except Exception as e:
            logger.error(f"❌ Error processing metadata to parquet: {e}")
            import traceback

            traceback.print_exc()
    else:
        logger.info("No new data to process to parquet format")

    # Clean up LOCAL_DIR after processing
    local_dir_path = Path(local_dir)
    if local_dir_path.exists():
        import shutil

        shutil.rmtree(local_dir_path)
        logger.info(f"Cleaned up local directory: {local_dir_path}")
