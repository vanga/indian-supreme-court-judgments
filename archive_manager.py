"""
S3 Archive Manager for Supreme Court Judgments
Handles ZIP archive creation, indexing, and S3 uploads
"""

import json
import logging
import threading
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)

# Indian Standard Time timezone
IST = timezone(timedelta(hours=5, minutes=30))


class S3ArchiveManager:
    """
    Manages ZIP archives for Supreme Court judgments with S3 sync.
    Supports both immediate upload mode and batch upload mode.
    """

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
        """Get or create a ZIP archive for a specific year and type.

        Note: This method acquires the lock to ensure thread-safety when
        multiple threads request the same archive simultaneously.
        """
        with self.lock:
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
                self.s3.download_file(
                    self.s3_bucket, index_s3_key, str(index_local_path)
                )
                with open(index_local_path, "r") as f:
                    self.indexes[(year, archive_type)] = json.load(f)

            except self.s3.exceptions.ClientError as e:
                if "404" in str(e):
                    logger.info(f"Archive not found on S3, creating new one: {s3_key}")
                    self.indexes[(year, archive_type)] = {
                        "year": year,
                        "archive_type": archive_type,
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
        """Add a file to an archive"""
        with self.lock:
            archive = self.get_archive(year, archive_type)

            # Check if file already exists in archive to prevent duplicates
            if filename in self.indexes[(year, archive_type)]["files"]:
                logger.debug(
                    f"File {filename} already exists in {year}/{archive_type}, skipping"
                )
                return

            archive.writestr(filename, content)

            self.indexes[(year, archive_type)]["files"].append(filename)
            self.modified_archives.add((year, archive_type))  # Mark as modified

            # Track newly added files for summary purposes
            if filename not in self.new_files_added[year][archive_type]:
                self.new_files_added[year][archive_type].append(filename)

    def file_exists(self, year, archive_type, filename):
        """Check if a file exists in an archive"""
        with self.lock:
            if (year, archive_type) not in self.indexes:
                self.get_archive(year, archive_type)

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
                        logger.warning(
                            f"Archive file {local_path} does not exist after closing. Skipping upload for year {year}."
                        )
                        continue

                    with open(index_local_path, "w") as f:
                        json.dump(index_data, f, indent=2)

                    # Upload to S3
                    logger.info(
                        f"\x1b[36mUploading {archive_name} for year {year}...\x1b[0m"
                    )
                    self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)

                    index_s3_key = f"{s3_dir}{index_name}"
                    logger.info(
                        f"\x1b[36mUploading {index_name} for year {year}...\x1b[0m"
                    )
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

                    # Reopen archive for potential future writes
                    archive = zipfile.ZipFile(local_path, "a", zipfile.ZIP_DEFLATED)
                    self.archives[(year, archive_type)] = archive

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
        """Get metadata about uploaded archives"""
        with self.lock:
            return json.loads(json.dumps(self.year_upload_metadata, default=str))

    def upload_archives(self):
        """Upload all modified archives to S3 (batch mode)"""
        uploaded_count = 0
        for (year, archive_type), archive in self.archives.items():
            archive.close()

            # Year directory structure
            year_dir = self.local_dir / str(year)
            archive_name = f"{archive_type}.zip"
            local_path = year_dir / archive_name

            # Only upload if this archive was modified
            if (year, archive_type) not in self.modified_archives:
                logger.debug(
                    f"Skipping unmodified archive: year={year}, type={archive_type}"
                )
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
                file_size = local_path.stat().st_size
                index_data["zip_size"] = file_size
                index_data["zip_size_human"] = self.format_file_size(file_size)

            with open(index_local_path, "w") as f:
                json.dump(index_data, f, indent=2)

            logger.info(f"\x1b[36mUploading modified archive: {s3_key}\x1b[0m")
            self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)

            index_s3_key = f"{s3_dir}{index_name}"
            logger.info(f"\x1b[36mUploading index: {index_s3_key}\x1b[0m")
            self.s3.upload_file(str(index_local_path), self.s3_bucket, index_s3_key)
            uploaded_count += 1

        # Clean up empty year directories
        self.cleanup_empty_year_directories()

        if uploaded_count > 0:
            logger.info(
                f"\x1b[36mSuccessfully uploaded {uploaded_count} modified archives\x1b[0m"
            )
        else:
            logger.info(
                "\x1b[36mNo archives needed uploading - all data was already present\x1b[0m"
            )

    def cleanup_empty_year_directories(self):
        """Remove year directories that have no files after processing"""
        for year_dir in self.local_dir.glob("*"):
            if year_dir.is_dir() and year_dir.name.isdigit():
                if not any(year_dir.iterdir()):
                    year_dir.rmdir()
                    logger.debug(f"Removed empty directory: {year_dir}")

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
