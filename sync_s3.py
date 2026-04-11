"""
S3 Sync Module for Supreme Court Judgments
Handles syncing with S3 and incremental downloads
"""

import json
import logging
import re
import tarfile
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.client import Config

logger = logging.getLogger(__name__)


def sync_latest_metadata_tar(s3_bucket, local_dir, force_refresh=True):
    """
    Download the current year's metadata tar file from S3, or latest available.
    If force_refresh is True, always download a fresh copy.
    """
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # First try to get current year's metadata
    current_year = datetime.now().year
    current_year_key = f"metadata/tar/year={current_year}/metadata.tar"

    # Check if current year metadata exists
    try:
        s3.head_object(Bucket=s3_bucket, Key=current_year_key)
        latest_tar_key = current_year_key
        logger.info(f"Found current year ({current_year}) metadata")
    except Exception:
        # Fall back to finding the latest available year
        logger.info("Current year metadata not found, finding latest available...")
        tars = []

        # Search for metadata tar files in the new structure
        paginator = s3.get_paginator("list_objects_v2")
        prefix = "metadata/tar/"

        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                # Extract year from path like metadata/tar/year=2025/metadata.tar
                year_match = re.search(r"year=(\d{4})/metadata\.tar", key)
                if year_match:
                    year = int(year_match.group(1))
                    tars.append((key, year))

        if not tars:
            raise Exception("No metadata tar files found in S3")

        # Sort by year descending and take the most recent
        tars.sort(key=lambda x: x[1], reverse=True)
        latest_tar_key = tars[0][0]

    # Create year directory for the tar file
    year_match = re.search(r"year=(\d{4})/", latest_tar_key)
    if year_match:
        year = year_match.group(1)
        year_dir = local_dir / year
        year_dir.mkdir(parents=True, exist_ok=True)
        local_path = year_dir / "metadata.tar"
    else:
        local_path = local_dir / Path(latest_tar_key).name

    # Force a fresh download if requested
    if force_refresh and local_path.exists():
        logger.info("Removing cached metadata tar to force refresh...")
        local_path.unlink()

    if not local_path.exists():
        logger.info(f"Downloading {latest_tar_key} ...")
        s3.download_file(s3_bucket, latest_tar_key, str(local_path))
    else:
        logger.info(f"Using cached metadata tar: {local_path}")

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


def find_latest_decision_date_in_tar(tar_path):
    """Find the latest decision date in a metadata tar file"""
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
                except Exception:
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


def _max_decision_date_from_parquet(s3_bucket, year):
    """Return the max decision_date from metadata/parquet/year={year}/metadata.parquet,
    or None if the file is missing, empty, or has no parseable dates.

    decision_date is stored as DD-MM-YYYY strings (see process_metadata.py).
    """
    import pyarrow.parquet as pq

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    key = f"metadata/parquet/year={year}/metadata.parquet"
    local_path = Path("./index_cache") / f"{year}_metadata.parquet"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        s3.download_file(s3_bucket, key, str(local_path))
    except Exception as e:
        logger.info(f"No parquet for year {year}: {e}")
        return None

    try:
        table = pq.read_table(local_path, columns=["decision_date"])
    except Exception as e:
        logger.warning(f"Could not read parquet for year {year}: {e}")
        return None

    latest = None
    for row in table.to_pylist():
        raw = row.get("decision_date")
        if not raw:
            continue
        try:
            parsed = datetime.strptime(raw, "%d-%m-%Y")
        except ValueError:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def get_latest_date_from_metadata(s3_bucket, force_check_files=False):
    """Return the latest decision date across S3 metadata for the current year,
    falling back to the previous year if the current year has no data, and
    to parsing the metadata tar as a last resort.
    """
    current_year = datetime.now().year

    if not force_check_files:
        for year in (current_year, current_year - 1):
            latest = _max_decision_date_from_parquet(s3_bucket, year)
            if latest is not None:
                logger.info(
                    f"Latest decision date in parquet (year={year}): {latest.date()}"
                )
                return latest

    # Last resort: parse the metadata tar directly. Used on force_check_files
    # or when parquet isn't available yet.
    logger.info("Falling back to parsing individual files for decision dates...")
    local_dir = Path("./local_sc_judgments_data")
    latest_tar = sync_latest_metadata_tar(s3_bucket, local_dir)
    return find_latest_decision_date_in_tar(latest_tar)


# ecourts keeps editing the last couple of weeks: new judgments dated in that
# window appear on the site for several days after the decision date. A small
# trailing lookback catches this recent churn cheaply. Older back-fills
# (judgments published months after their decision date) are not this script's
# job -- run `download.py --sync-s3-fill` periodically for the 1950-onwards
# historical sweep. Re-scanning is idempotent because the archive manager
# skips files that already exist via file_exists().
SYNC_LOOKBACK_DAYS = 14


def run_downloader(start_date, end_date, archive_manager=None):
    """Helper function to run the downloader for a date range"""
    # Import here to avoid circular dependency
    from download import run

    logger.info(f"Fetching new data from {start_date} to {end_date} ...")
    run(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        archive_manager=archive_manager,
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

    # Always re-scan the trailing SYNC_LOOKBACK_DAYS window (ecourts keeps
    # editing recent dates for ~2 weeks). If we've fallen further behind than
    # that, fall back to the real cursor so we don't skip a gap.
    buffer_start = today - timedelta(days=SYNC_LOOKBACK_DAYS)
    scan_start = min(latest_date.date(), buffer_start)

    # The archive_manager with immediate_upload enabled uploads each part as it's created
    # This prevents data loss if the script crashes mid-download
    with S3ArchiveManager(
        s3_bucket, s3_prefix, local_dir, immediate_upload=True
    ) as archive_manager:
        logger.info(
            f"📥 Scanning {scan_start} to {today} "
            f"(buffer_start={buffer_start}, latest_decision={latest_date.date()})"
        )
        run_downloader(scan_start, today, archive_manager)
        changes_made = True

        # Get changes before exiting context
        if changes_made:
            all_changes = archive_manager.get_all_changes()

    # AFTER the with block completes (archives are now uploaded to S3)
    # Log summary of changes
    if changes_made and all_changes:
        logger.info("\n📊 Sync Summary:")
        logger.info(f"  Date range: {scan_start} to {today}")
        for year, archives in all_changes.items():
            logger.info(f"  Year {year}:")
            for archive_type, files in archives.items():
                logger.info(f"    {archive_type}: {len(files)} files")

    # Check if any new files were actually downloaded in this run
    if changes_made and all_changes:
        logger.info("🔄 Processing newly downloaded metadata to parquet format...")

        # Generate parquet only for the newly downloaded data
        try:
            # Process only the years that were just downloaded
            start_year = scan_start.year
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
