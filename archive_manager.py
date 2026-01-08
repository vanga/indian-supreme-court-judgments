"""
S3 Archive Manager for Supreme Court Judgments
Handles ZIP/TAR archive creation, indexing, and S3 uploads with size-based partitioning.

Similar to the indian-high-court-judgments project, this module supports:
- Multiple archive parts when size exceeds MAX_ARCHIVE_SIZE
- IndexFileV2 format with parts array
- Both ZIP and TAR archive formats
"""

import json
import logging
import threading
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import boto3

logger = logging.getLogger(__name__)

# Indian Standard Time timezone
IST = timezone(timedelta(hours=5, minutes=30))

# Maximum size for each archive part (1GB for easier management)
MAX_ARCHIVE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB in bytes


def format_size(size_bytes: int) -> str:
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0 B"

    size_units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    unit_index = 0

    while size >= 1024.0 and unit_index < len(size_units) - 1:
        size /= 1024.0
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {size_units[unit_index]}"
    else:
        return f"{size:.2f} {size_units[unit_index]}"


def utc_now_iso() -> str:
    """Return current UTC time in ISO format"""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_part_name(now_iso: str) -> str:
    """Generate a unique part name using timestamp"""
    # Use compact timestamp: YYYYMMDDThhmmssZ
    ts = datetime.fromisoformat(now_iso.replace("Z", "+00:00")).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    return f"part-{ts}"


@dataclass
class IndexPart:
    """
    Represents a single archive part (tar/zip file).
    Each part corresponds to one archive created during a run.
    """

    name: str  # Archive filename, e.g., 'part-20250101T120000Z.zip'
    files: List[str] = field(default_factory=list)  # Files contained in this part
    file_count: int = 0
    size: int = 0  # Part size in bytes
    size_human: str = "0 B"
    created_at: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "files": self.files,
            "file_count": self.file_count,
            "size": self.size,
            "size_human": self.size_human,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "IndexPart":
        return cls(
            name=data.get("name", ""),
            files=data.get("files", []),
            file_count=data.get("file_count", 0),
            size=data.get("size", 0),
            size_human=data.get("size_human", "0 B"),
            created_at=data.get("created_at", ""),
        )


@dataclass
class IndexFileV2:
    """
    Index file format V2 with support for multiple parts.

    This format tracks:
    - Aggregated file count and size across all parts
    - Individual parts with their own file lists and sizes
    """

    year: int = 0
    archive_type: str = ""
    file_count: int = 0  # Total files across all parts
    total_size: int = 0  # Total size across all parts in bytes
    total_size_human: str = "0 B"
    created_at: str = ""
    updated_at: str = ""
    parts: List[IndexPart] = field(default_factory=list)

    # Legacy field for backward compatibility
    files: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        result = {
            "year": self.year,
            "archive_type": self.archive_type,
            "file_count": self.file_count,
            "total_size": self.total_size,
            "total_size_human": self.total_size_human,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "parts": [p.to_dict() for p in self.parts],
        }
        # Include legacy 'files' field for backward compatibility
        if self.files:
            result["files"] = self.files
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "IndexFileV2":
        parts = [IndexPart.from_dict(p) for p in data.get("parts", [])]
        return cls(
            year=data.get("year", 0),
            archive_type=data.get("archive_type", ""),
            file_count=data.get("file_count", 0),
            total_size=data.get(
                "total_size", data.get("zip_size", 0)
            ),  # Support legacy field
            total_size_human=data.get(
                "total_size_human", data.get("zip_size_human", "0 B")
            ),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            parts=parts,
            files=data.get("files", []),
        )

    def get_all_files(self) -> List[str]:
        """Get all files across all parts"""
        all_files = set(self.files)  # Include legacy files
        for part in self.parts:
            all_files.update(part.files)
        return list(all_files)

    def add_part(self, part: IndexPart):
        """Add a new part and update aggregated stats"""
        self.parts.append(part)
        self.file_count += part.file_count
        self.total_size += part.size
        self.total_size_human = format_size(self.total_size)
        self.updated_at = utc_now_iso()


class S3ArchiveManager:
    """
    Manages ZIP/TAR archives for Supreme Court judgments with S3 sync.
    Supports both immediate upload mode and batch upload mode.

    Key features:
    - Size-based partitioning: creates new archive parts when MAX_ARCHIVE_SIZE is exceeded
    - IndexFileV2 format with parts array for tracking multiple archives
    - Support for both ZIP and TAR formats
    - Thread-safe operations
    """

    def __init__(
        self,
        s3_bucket,
        s3_prefix,
        local_dir: Path,
        immediate_upload=False,
        max_archive_size: int = MAX_ARCHIVE_SIZE,
    ):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.local_dir = Path(local_dir)
        self.s3 = boto3.client("s3")
        self.max_archive_size = max_archive_size

        # Archive tracking
        self.archives: Dict[
            tuple, zipfile.ZipFile
        ] = {}  # (year, archive_type) -> ZipFile
        self.archive_paths: Dict[tuple, Path] = {}  # (year, archive_type) -> local path
        self.indexes: Dict[
            tuple, IndexFileV2
        ] = {}  # (year, archive_type) -> IndexFileV2
        self.current_part_files: Dict[tuple, List[str]] = defaultdict(
            list
        )  # Files in current part
        self.current_part_size: Dict[tuple, int] = defaultdict(
            int
        )  # Size of current part

        self.modified_archives = set()  # Track which archives have new content
        self.lock = threading.RLock()  # Reentrant lock for nested calls
        self.immediate_upload = immediate_upload
        self.uploaded_archives = set()  # Track already uploaded archives
        self.new_files_added = defaultdict(lambda: defaultdict(list))
        self.year_upload_metadata = defaultdict(dict)

        # Track parts that need to be uploaded
        self.pending_parts: Dict[tuple, List[dict]] = defaultdict(list)

        # Track total number of parts created for each (year, archive_type) in this session
        self.parts_created_count: Dict[tuple, int] = defaultdict(int)

    def __enter__(self):
        self.local_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Upload any remaining parts
        if self.immediate_upload:
            # Finalize and upload any currently open archives
            for key in list(self.archives.keys()):
                year, archive_type = key
                logger.info(
                    f"Finalizing remaining archive: {archive_type} for year {year}"
                )
                self._finalize_current_part(year, archive_type)
        else:
            # Batch upload mode
            self.upload_archives()

        self.cleanup_empty_year_directories()

    def _get_s3_dir(self, year: int, archive_type: str) -> str:
        """Get S3 directory path for an archive type"""
        if archive_type == "metadata":
            return f"metadata/zip/year={year}/"
        else:
            # Separate english and regional into their own subfolders
            return f"data/zip/year={year}/{archive_type}/"

    def _get_archive_extension(self, archive_type: str) -> str:
        """Get file extension for archive type"""
        # For now, all archives use ZIP format
        # This can be extended to support TAR format
        return ".zip"

    def _load_index_from_s3(self, year: int, archive_type: str) -> IndexFileV2:
        """Load index file from S3, returning empty index if not found"""
        s3_dir = self._get_s3_dir(year, archive_type)
        index_key = f"{s3_dir}{archive_type}.index.json"

        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=index_key)
            data = json.loads(response["Body"].read().decode("utf-8"))

            # Convert to IndexFileV2 format
            index = IndexFileV2.from_dict(data)
            index.year = year
            index.archive_type = archive_type
            return index

        except self.s3.exceptions.ClientError as e:
            if "NoSuchKey" in str(e) or "404" in str(e):
                # Create new empty index
                now = utc_now_iso()
                return IndexFileV2(
                    year=year,
                    archive_type=archive_type,
                    file_count=0,
                    total_size=0,
                    total_size_human="0 B",
                    created_at=now,
                    updated_at=now,
                    parts=[],
                    files=[],
                )
            raise
        except Exception as e:
            logger.error(f"Error loading index from S3: {e}")
            now = utc_now_iso()
            return IndexFileV2(
                year=year,
                archive_type=archive_type,
                file_count=0,
                total_size=0,
                total_size_human="0 B",
                created_at=now,
                updated_at=now,
                parts=[],
                files=[],
            )

    def _download_main_archive_if_exists(
        self, year: int, archive_type: str
    ) -> Optional[Path]:
        """Download the main archive (e.g., english.zip) if it exists in S3"""
        s3_dir = self._get_s3_dir(year, archive_type)
        ext = self._get_archive_extension(archive_type)
        archive_name = f"{archive_type}{ext}"
        s3_key = f"{s3_dir}{archive_name}"

        year_dir = self.local_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        local_path = year_dir / archive_name

        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=s3_key)
            logger.info(f"Downloading existing archive: {s3_key}")
            self.s3.download_file(self.s3_bucket, s3_key, str(local_path))
            return local_path
        except self.s3.exceptions.ClientError as e:
            if "404" in str(e) or "NoSuchKey" in str(e):
                return None
            raise

    def get_archive(self, year, archive_type):
        """Get or create a ZIP archive for a specific year and type.

        This method manages archive parts. If the current archive exceeds MAX_ARCHIVE_SIZE,
        it will be finalized and a new part will be created.
        """
        with self.lock:
            key = (year, archive_type)

            if key in self.archives:
                # Check if we need to rotate to a new part
                current_path = self.archive_paths.get(key)
                if current_path and current_path.exists():
                    current_size = current_path.stat().st_size
                    if current_size >= self.max_archive_size:
                        logger.info(
                            f"Archive {current_path} reached size limit ({format_size(current_size)}), creating new part"
                        )
                        self._finalize_current_part(year, archive_type)
                        return self._create_new_part(year, archive_type)

                return self.archives[key]

            # Load index from S3 if not already loaded
            if key not in self.indexes:
                self.indexes[key] = self._load_index_from_s3(year, archive_type)

            # Check if main archive exists and download it
            local_path = self._download_main_archive_if_exists(year, archive_type)

            if local_path and local_path.exists():
                # Open existing archive for appending
                archive = zipfile.ZipFile(local_path, "a", zipfile.ZIP_DEFLATED)
                self.archives[key] = archive
                self.archive_paths[key] = local_path

                # Set current part size
                self.current_part_size[key] = local_path.stat().st_size
            else:
                # Create new archive
                return self._create_new_part(year, archive_type, is_first=True)

            return self.archives[key]

    def _create_new_part(
        self, year: int, archive_type: str, is_first: bool = False
    ) -> zipfile.ZipFile:
        """Create a new archive part"""
        key = (year, archive_type)
        year_dir = self.local_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        ext = self._get_archive_extension(archive_type)

        # First part: {archive_type}{ext}, subsequent parts: part-{ist-timestamp}{ext}
        # Use the session parts count, not the index length (which isn't updated until upload)
        index = self.indexes.get(key, IndexFileV2())
        total_parts = len(index.parts) + self.parts_created_count.get(key, 0)

        if total_parts == 0:
            # First part uses normal name
            archive_name = f"{archive_type}{ext}"
        else:
            # Subsequent parts use part-{ist-timestamp} format
            now_iso = utc_now_iso()
            ts = datetime.fromisoformat(now_iso.replace("Z", "+00:00")).strftime(
                "%Y%m%dT%H%M%S"
            )
            archive_name = f"part-{ts}{ext}"

        local_path = year_dir / archive_name

        archive = zipfile.ZipFile(local_path, "w", zipfile.ZIP_DEFLATED)
        self.archives[key] = archive
        self.archive_paths[key] = local_path
        self.current_part_files[key] = []
        self.current_part_size[key] = 0

        # Increment the parts created count for this session
        self.parts_created_count[key] += 1

        logger.info(f"Created new archive part: {local_path}")
        return archive

    def _finalize_current_part(self, year: int, archive_type: str):
        """Finalize the current archive part and upload immediately if in immediate_upload mode"""
        key = (year, archive_type)

        if key not in self.archives:
            return

        archive = self.archives[key]
        archive.close()

        local_path = self.archive_paths.get(key)
        if not local_path or not local_path.exists():
            return

        # Record this part info
        part_size = local_path.stat().st_size
        part_info = {
            "name": local_path.name,
            "files": list(self.current_part_files[key]),
            "file_count": len(self.current_part_files[key]),
            "size": part_size,
            "size_human": format_size(part_size),
            "local_path": str(local_path),
        }

        # If immediate upload is enabled, upload this part now
        if self.immediate_upload:
            logger.info(f"Immediately uploading finalized part: {local_path.name}")
            self._upload_single_part(year, archive_type, part_info)
        else:
            # Store for batch upload later
            self.pending_parts[key].append(part_info)

        # Clear current tracking
        del self.archives[key]
        del self.archive_paths[key]
        self.current_part_files[key] = []
        self.current_part_size[key] = 0

    def _upload_single_part(self, year: int, archive_type: str, part_info: dict):
        """Upload a single part to S3 and update the index"""
        key = (year, archive_type)
        s3_dir = self._get_s3_dir(year, archive_type)
        local_path = Path(part_info["local_path"])

        if not local_path.exists():
            logger.error(f"Part file not found: {local_path}")
            return

        part_name = part_info["name"]
        s3_key = f"{s3_dir}{part_name}"

        # Upload archive part
        logger.info(
            f"\x1b[36mUploading {part_name} ({part_info['size_human']}) for year {year}...\x1b[0m"
        )
        self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)
        logger.info(f"\x1b[32m✓ Uploaded {part_name}\x1b[0m")

        # Get or create index
        if key not in self.indexes:
            self.indexes[key] = self._load_index_from_s3(year, archive_type)

        index = self.indexes[key]

        # Create IndexPart and add to index
        new_part = IndexPart(
            name=part_name,
            files=part_info["files"],
            file_count=part_info["file_count"],
            size=part_info["size"],
            size_human=part_info["size_human"],
            created_at=utc_now_iso(),
        )
        index.add_part(new_part)

        # Upload updated index
        self._upload_index(year, archive_type, index)
        logger.info(f"\x1b[32m✓ Updated index for {archive_type}\x1b[0m")

        # Track upload metadata
        self.year_upload_metadata[year][archive_type] = {
            "total_size_bytes": index.total_size,
            "total_size_human": index.total_size_human,
            "parts_count": len(index.parts),
            "files_added": list(
                self.new_files_added.get(year, {}).get(archive_type, [])
            ),
        }

        # Delete local file after successful upload to save space
        try:
            local_path.unlink()
            logger.info(f"Cleaned up local part: {local_path.name}")
        except Exception as e:
            logger.warning(f"Could not delete local part {local_path}: {e}")

    def add_to_archive(self, year, archive_type, filename, content):
        """Add a file to an archive, handling size-based partitioning"""
        with self.lock:
            key = (year, archive_type)

            # Ensure index is loaded
            if key not in self.indexes:
                self.indexes[key] = self._load_index_from_s3(year, archive_type)

            index = self.indexes[key]

            # Check if file already exists in any part
            all_existing_files = index.get_all_files()
            if (
                filename in all_existing_files
                or filename in self.current_part_files[key]
            ):
                logger.debug(
                    f"File {filename} already exists in {year}/{archive_type}, skipping"
                )
                return

            archive = self.get_archive(year, archive_type)

            # Write the file
            archive.writestr(filename, content)

            # Track this file
            self.current_part_files[key].append(filename)
            content_size = (
                len(content)
                if isinstance(content, bytes)
                else len(content.encode("utf-8"))
            )
            self.current_part_size[key] += content_size

            self.modified_archives.add(key)

            # Track newly added files for summary
            if filename not in self.new_files_added[year][archive_type]:
                self.new_files_added[year][archive_type].append(filename)

    def file_exists(self, year, archive_type, filename):
        """Check if a file exists in any archive part"""
        with self.lock:
            key = (year, archive_type)

            if key not in self.indexes:
                self.indexes[key] = self._load_index_from_s3(year, archive_type)

            index = self.indexes[key]
            all_files = index.get_all_files()

            # Also check current part files
            all_files.extend(self.current_part_files.get(key, []))

            return filename in all_files

    def upload_year_archives(self, year):
        """Upload all archives for a specific year immediately, supporting multi-part uploads"""
        with self.lock:
            uploaded_count = 0
            for archive_type in ["metadata", "english", "regional"]:
                key = (year, archive_type)

                # Skip if already uploaded
                if key in self.uploaded_archives:
                    continue

                # Only upload if modified
                if key not in self.modified_archives:
                    continue

                # Finalize current part if exists
                if key in self.archives:
                    self._finalize_current_part(year, archive_type)

                # Upload all pending parts
                uploaded_count += self._upload_parts_for_key(year, archive_type)

                # Mark as uploaded
                self.uploaded_archives.add(key)

            return uploaded_count

    def _upload_parts_for_key(self, year: int, archive_type: str) -> int:
        """Upload all parts for a given year/archive_type and update index"""
        key = (year, archive_type)
        s3_dir = self._get_s3_dir(year, archive_type)

        # Get or create index
        if key not in self.indexes:
            self.indexes[key] = self._load_index_from_s3(year, archive_type)

        index = self.indexes[key]
        parts_to_upload = self.pending_parts.get(key, [])

        if not parts_to_upload:
            return 0

        uploaded_count = 0
        for part_info in parts_to_upload:
            local_path = Path(part_info["local_path"])
            if not local_path.exists():
                logger.warning(f"Part file not found: {local_path}")
                continue

            part_name = part_info["name"]
            s3_key = f"{s3_dir}{part_name}"

            # Upload archive part
            logger.info(f"\x1b[36mUploading {part_name} for year {year}...\x1b[0m")
            self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)

            # Create IndexPart and add to index
            new_part = IndexPart(
                name=part_name,
                files=part_info["files"],
                file_count=part_info["file_count"],
                size=part_info["size"],
                size_human=part_info["size_human"],
                created_at=utc_now_iso(),
            )
            index.add_part(new_part)

            uploaded_count += 1

            # Track upload metadata
            self.year_upload_metadata[year][archive_type] = {
                "total_size_bytes": index.total_size,
                "total_size_human": index.total_size_human,
                "parts_count": len(index.parts),
                "files_added": list(
                    self.new_files_added.get(year, {}).get(archive_type, [])
                ),
            }

        # Upload updated index
        self._upload_index(year, archive_type, index)

        # Clear pending parts
        self.pending_parts[key] = []

        return uploaded_count

    def _upload_index(self, year: int, archive_type: str, index: IndexFileV2):
        """Upload the index file to S3"""
        s3_dir = self._get_s3_dir(year, archive_type)
        index_name = f"{archive_type}.index.json"
        index_s3_key = f"{s3_dir}{index_name}"

        # Write index locally first
        year_dir = self.local_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        index_local_path = year_dir / index_name

        with open(index_local_path, "w") as f:
            json.dump(index.to_dict(), f, indent=2)

        # Upload to S3
        logger.info(f"\x1b[36mUploading {index_name} for year {year}...\x1b[0m")
        self.s3.upload_file(str(index_local_path), self.s3_bucket, index_s3_key)

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
        """Upload all modified archives to S3 (batch mode) with multi-part support"""
        uploaded_count = 0

        # First, finalize all current parts
        for key in list(self.archives.keys()):
            year, archive_type = key
            if key in self.modified_archives:
                self._finalize_current_part(year, archive_type)

        # Upload all pending parts for modified archives
        for key in self.modified_archives:
            year, archive_type = key
            uploaded_count += self._upload_parts_for_key(year, archive_type)

        # Clean up empty year directories
        self.cleanup_empty_year_directories()

        if uploaded_count > 0:
            logger.info(
                f"\x1b[36mSuccessfully uploaded {uploaded_count} archive parts\x1b[0m"
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
