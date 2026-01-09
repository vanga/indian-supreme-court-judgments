#!/usr/bin/env python3
"""
Migration Script: Convert Archives to Multi-Part Format with New Structure

This script:
1. Scans S3 bucket for existing archives
2. Identifies archives larger than 1GB
3. Downloads and extracts them
4. Splits files into 1GB parts with timestamped names
5. Uploads to new structure: data/zip/year=YYYY/english/ or regional/
6. Creates/updates index files with parts array

Usage:
    python migrate_to_multipart.py --bucket indian-supreme-court-judgments
    python migrate_to_multipart.py --bucket indian-supreme-court-judgments --year 2023
    python migrate_to_multipart.py --bucket indian-supreme-court-judgments --dry-run
"""

import argparse
import json
import logging
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Indian Standard Time timezone
IST = timezone(timedelta(hours=5, minutes=30))

# Maximum archive size before creating a new part (1GB for easier management)
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
    """Return current IST time in ISO format"""
    return datetime.now(IST).isoformat()


def generate_part_name(now_iso: str) -> str:
    """Generate a unique part name using timestamp"""
    ts = datetime.fromisoformat(now_iso).strftime("%Y%m%dT%H%M%S")
    return f"part-{ts}"


@dataclass
class IndexPart:
    """Represents a single archive part"""

    name: str
    files: List[str] = field(default_factory=list)
    file_count: int = 0
    size: int = 0
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


@dataclass
class IndexFileV2:
    """Index file format V2 with support for multiple parts"""

    year: int = 0
    archive_type: str = ""
    file_count: int = 0
    total_size: int = 0
    total_size_human: str = "0 B"
    created_at: str = ""
    updated_at: str = ""
    parts: List[IndexPart] = field(default_factory=list)
    files: List[str] = field(default_factory=list)  # Legacy compatibility

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
        if self.files:
            result["files"] = self.files
        return result


class ArchiveMigrator:
    """Migrate existing archives to multi-part format"""

    def __init__(self, bucket_name: str, dry_run: bool = False):
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        self.s3 = boto3.client("s3")

        logger.info(f"Initialized migrator for bucket: {bucket_name}")
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")

    def list_years(self) -> List[int]:
        """List all years in the bucket"""
        years = set()

        # Check data/zip directory
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix="data/zip/", Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        # Extract year from 'data/zip/year=YYYY/'
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years: {e}")

        return sorted(list(years))

    def get_archive_info(self, year: int) -> Dict[str, dict]:
        """Get info about existing archives for a year"""
        archives = {}

        for archive_type in ["english", "regional", "metadata"]:
            archive_name = f"{archive_type}.zip"

            # Determine old and new structure paths
            if archive_type == "metadata":
                # Metadata only has one location
                old_s3_key = f"metadata/zip/year={year}/{archive_name}"
                new_s3_key = old_s3_key  # Same location
                new_s3_dir = f"metadata/zip/year={year}/"
            else:
                # English/Regional: check OLD flat structure first, then NEW subfolder structure
                old_s3_key = f"data/zip/year={year}/{archive_name}"
                new_s3_key = f"data/zip/year={year}/{archive_type}/{archive_name}"
                new_s3_dir = f"data/zip/year={year}/{archive_type}/"

            # Try OLD structure first (for migration)
            s3_key = None
            s3_dir = None
            size = None

            try:
                response = self.s3.head_object(Bucket=self.bucket_name, Key=old_s3_key)
                size = response["ContentLength"]
                s3_key = old_s3_key
                # For old structure, set destination dir as new structure
                s3_dir = (
                    new_s3_dir
                    if archive_type != "metadata"
                    else f"metadata/zip/year={year}/"
                )
                logger.info(
                    f"Found {archive_type}.zip in OLD structure: {format_size(size)}"
                )
            except self.s3.exceptions.ClientError:
                # Old structure not found, try NEW structure
                if archive_type != "metadata":
                    try:
                        response = self.s3.head_object(
                            Bucket=self.bucket_name, Key=new_s3_key
                        )
                        size = response["ContentLength"]
                        s3_key = new_s3_key
                        s3_dir = new_s3_dir
                        logger.info(
                            f"Found {archive_type}.zip in NEW structure: {format_size(size)}"
                        )
                    except self.s3.exceptions.ClientError as e:
                        if "404" not in str(e):
                            logger.error(f"Error checking {new_s3_key}: {e}")
                        continue

            if s3_key and size is not None:
                # Check if index already exists in new location
                index_key = f"{s3_dir}{archive_type}.index.json"
                index_exists = False
                existing_index = None

                try:
                    index_response = self.s3.get_object(
                        Bucket=self.bucket_name, Key=index_key
                    )
                    existing_index = json.loads(
                        index_response["Body"].read().decode("utf-8")
                    )
                    index_exists = True
                except self.s3.exceptions.ClientError:
                    pass

                archives[archive_type] = {
                    "s3_key": s3_key,
                    "s3_dir": s3_dir,
                    "archive_name": archive_name,
                    "size": size,
                    "size_human": format_size(size),
                    "needs_split": size > MAX_ARCHIVE_SIZE,
                    "index_exists": index_exists,
                    "existing_index": existing_index,
                    "is_old_structure": s3_key == old_s3_key
                    and archive_type != "metadata",
                }

                logger.info(
                    f"  Size: {format_size(size)}, Index exists: {index_exists}"
                )

        return archives

    def download_archive(self, s3_key: str, local_path: Path) -> bool:
        """Download archive from S3"""
        try:
            logger.info(f"Downloading {s3_key}...")
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Get file size for progress bar
            response = self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            file_size = response["ContentLength"]

            with tqdm(
                total=file_size, unit="B", unit_scale=True, desc="Downloading"
            ) as pbar:

                def callback(bytes_transferred):
                    pbar.update(bytes_transferred)

                self.s3.download_file(
                    self.bucket_name, s3_key, str(local_path), Callback=callback
                )

            logger.info(f"Downloaded to {local_path}")
            return True

        except Exception as e:
            logger.error(f"Error downloading {s3_key}: {e}")
            return False

    def extract_archive(self, archive_path: Path, extract_dir: Path) -> List[str]:
        """Extract archive and return list of files"""
        try:
            logger.info(f"Extracting {archive_path.name}...")
            extract_dir.mkdir(parents=True, exist_ok=True)

            files = []
            with zipfile.ZipFile(archive_path, "r") as zf:
                members = zf.namelist()

                with tqdm(total=len(members), desc="Extracting files") as pbar:
                    for member in members:
                        zf.extract(member, extract_dir)
                        files.append(member)
                        pbar.update(1)

            logger.info(f"Extracted {len(files)} files")
            return files

        except Exception as e:
            logger.error(f"Error extracting {archive_path}: {e}")
            return []

    def split_files_into_parts(
        self, extract_dir: Path, files: List[str], archive_type: str
    ) -> List[Tuple[List[str], int]]:
        """
        Split files into parts that don't exceed MAX_ARCHIVE_SIZE
        Returns list of (file_list, estimated_size) tuples
        """
        parts = []
        current_part_files = []
        current_part_size = 0

        logger.info(f"Splitting {len(files)} files into parts...")

        for filename in files:
            file_path = extract_dir / filename
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                continue

            file_size = file_path.stat().st_size

            # If adding this file would exceed the limit, start a new part
            if (
                current_part_size > 0
                and (current_part_size + file_size) > MAX_ARCHIVE_SIZE
            ):
                parts.append((current_part_files.copy(), current_part_size))
                logger.info(
                    f"Part {len(parts)}: {len(current_part_files)} files, {format_size(current_part_size)}"
                )
                current_part_files = []
                current_part_size = 0

            current_part_files.append(filename)
            current_part_size += file_size

        # Add the last part
        if current_part_files:
            parts.append((current_part_files.copy(), current_part_size))
            logger.info(
                f"Part {len(parts)}: {len(current_part_files)} files, {format_size(current_part_size)}"
            )

        logger.info(f"Split into {len(parts)} parts")
        return parts

    def create_archive_part(
        self, extract_dir: Path, files: List[str], output_path: Path
    ) -> int:
        """Create a zip archive from files and return its size"""
        try:
            logger.info(f"Creating archive part: {output_path.name}")

            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
                with tqdm(total=len(files), desc="Adding files") as pbar:
                    for filename in files:
                        file_path = extract_dir / filename
                        if file_path.exists():
                            zf.write(file_path, arcname=filename)
                        pbar.update(1)

            size = output_path.stat().st_size
            logger.info(f"Created {output_path.name}: {format_size(size)}")
            return size

        except Exception as e:
            logger.error(f"Error creating archive: {e}")
            return 0

    def upload_archive_part(self, local_path: Path, s3_key: str) -> bool:
        """Upload archive part to S3"""
        try:
            file_size = local_path.stat().st_size
            logger.info(
                f"Uploading {local_path.name} ({format_size(file_size)}) to {s3_key}"
            )

            if self.dry_run:
                logger.info(f"DRY RUN: Would upload to {s3_key}")
                return True

            with tqdm(
                total=file_size, unit="B", unit_scale=True, desc="Uploading"
            ) as pbar:

                def callback(bytes_transferred):
                    pbar.update(bytes_transferred)

                self.s3.upload_file(
                    str(local_path), self.bucket_name, s3_key, Callback=callback
                )

            logger.info(f"Uploaded to {s3_key}")
            return True

        except Exception as e:
            logger.error(f"Error uploading to {s3_key}: {e}")
            return False

    def delete_old_archive(self, s3_key: str) -> bool:
        """Delete old archive from S3"""
        try:
            if self.dry_run:
                logger.info(f"DRY RUN: Would delete {s3_key}")
                return True

            logger.info(f"Deleting old archive: {s3_key}")
            self.s3.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted {s3_key}")
            return True

        except Exception as e:
            logger.error(f"Error deleting {s3_key}: {e}")
            return False

    def delete_old_index(self, year: int, archive_type: str) -> bool:
        """Delete old index file from flat structure (year=YYYY/*.index.json)"""
        if archive_type == "metadata":
            return True  # Metadata uses same location, no old index to delete

        old_index_key = f"data/zip/year={year}/{archive_type}.index.json"

        try:
            # Check if old index exists
            self.s3.head_object(Bucket=self.bucket_name, Key=old_index_key)

            if self.dry_run:
                logger.info(f"DRY RUN: Would delete old index {old_index_key}")
                return True

            logger.info(f"Deleting old index: {old_index_key}")
            self.s3.delete_object(Bucket=self.bucket_name, Key=old_index_key)
            logger.info(f"Deleted old index: {old_index_key}")
            return True

        except self.s3.exceptions.ClientError as e:
            if "404" in str(e):
                logger.debug(
                    f"Old index not found: {old_index_key} (already deleted or never existed)"
                )
            else:
                logger.error(f"Error deleting old index {old_index_key}: {e}")
            return False

    def is_already_v2_format(self, index_data: dict) -> bool:
        """Check if index is already in V2 format (has parts array)"""
        return (
            "parts" in index_data
            and isinstance(index_data["parts"], list)
            and len(index_data["parts"]) > 0
        )

    def create_and_upload_index(
        self,
        year: int,
        archive_type: str,
        s3_dir: str,
        parts_info: List[Tuple[str, List[str], int]],
    ) -> bool:
        """
        Create and upload IndexFileV2
        parts_info: List of (part_name, files, size) tuples
        """
        try:
            now = utc_now_iso()

            # Create index with parts
            index = IndexFileV2(
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

            # Add each part
            for part_name, files, size in parts_info:
                part = IndexPart(
                    name=part_name,
                    files=files,
                    file_count=len(files),
                    size=size,
                    size_human=format_size(size),
                    created_at=now,
                )
                index.parts.append(part)
                index.file_count += len(files)
                index.total_size += size

            index.total_size_human = format_size(index.total_size)
            index.updated_at = now

            # Upload index
            index_key = f"{s3_dir}{archive_type}.index.json"
            index_content = json.dumps(index.to_dict(), indent=2)

            if self.dry_run:
                logger.info(f"DRY RUN: Would upload index to {index_key}")
                logger.info(f"Index content:\n{index_content}")
                return True

            logger.info(f"Uploading index: {index_key}")
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=index_key,
                Body=index_content.encode("utf-8"),
                ContentType="application/json",
            )

            logger.info(f"Uploaded index: {archive_type}.index.json")
            logger.info(f"  Total files: {index.file_count}")
            logger.info(f"  Total size: {index.total_size_human}")
            logger.info(f"  Parts: {len(index.parts)}")

            return True

        except Exception as e:
            logger.error(f"Error creating/uploading index: {e}")
            return False

    def migrate_archive(self, year: int, archive_type: str, archive_info: dict) -> bool:
        """Migrate a single archive to multi-part format"""

        logger.info(f"\n{'=' * 80}")
        logger.info(f"Migrating {archive_type} for year {year}")
        logger.info(f"Current size: {archive_info['size_human']}")
        logger.info(f"{'=' * 80}\n")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Download original archive
            original_archive = temp_path / archive_info["archive_name"]
            if not self.download_archive(archive_info["s3_key"], original_archive):
                return False

            # Extract files
            extract_dir = temp_path / "extracted"
            files = self.extract_archive(original_archive, extract_dir)
            if not files:
                return False

            # Split into parts
            parts = self.split_files_into_parts(extract_dir, files, archive_type)

            if len(parts) == 1:
                logger.info("Archive fits in single part, keeping normal name")
                # Also delete old index file if migrating from old structure
                if archive_info.get("is_old_structure", False):
                    self.delete_old_index(year, archive_type)
                # Create the single part with normal naming
                part_files, estimated_size = parts[0]

                # First part uses normal name: {archive_type}.zip
                part_name = f"{archive_type}.zip"

                # Create archive part
                part_path = temp_path / part_name
                actual_size = self.create_archive_part(
                    extract_dir, part_files, part_path
                )

                if actual_size == 0:
                    logger.error(f"Failed to create part {part_name}")
                    return False

                # Upload part
                part_s3_key = f"{archive_info['s3_dir']}{part_name}"
                if not self.upload_archive_part(part_path, part_s3_key):
                    return False

                # Delete old archive if migrating from old structure
                if archive_info.get("is_old_structure", False):
                    if not self.delete_old_archive(archive_info["s3_key"]):
                        logger.warning(
                            "Failed to delete old archive, but continuing..."
                        )

                # Create and upload index
                parts_info = [(part_name, part_files, actual_size)]
                return self.create_and_upload_index(
                    year, archive_type, archive_info["s3_dir"], parts_info
                )

            # Create and upload parts
            parts_info = []

            # Also delete old index file if migrating from old structure
            if archive_info.get("is_old_structure", False):
                self.delete_old_index(year, archive_type)

            for idx, (part_files, estimated_size) in enumerate(parts):
                # First part uses normal name, subsequent parts use part-{ist-timestamp}.zip
                if idx == 0:
                    part_name = f"{archive_type}.zip"
                else:
                    now_iso = utc_now_iso()
                    ts = datetime.fromisoformat(now_iso).strftime("%Y%m%dT%H%M%S")
                    part_name = f"part-{ts}.zip"

                # Create archive part
                part_path = temp_path / part_name
                actual_size = self.create_archive_part(
                    extract_dir, part_files, part_path
                )

                if actual_size == 0:
                    logger.error(f"Failed to create part {part_name}")
                    return False

                # Upload part
                part_s3_key = f"{archive_info['s3_dir']}{part_name}"
                if not self.upload_archive_part(part_path, part_s3_key):
                    return False

                parts_info.append((part_name, part_files, actual_size))

            # Always delete old archive after successful multi-part upload
            # (either from old structure or if it was a large single file being split)
            if not self.delete_old_archive(archive_info["s3_key"]):
                logger.warning("Failed to delete old archive, but continuing...")

            # Create and upload index
            if not self.create_and_upload_index(
                year, archive_type, archive_info["s3_dir"], parts_info
            ):
                return False

            logger.info(f"✓ Successfully migrated {archive_type} for year {year}")
            return True

    def migrate_year(self, year: int) -> bool:
        """Migrate all archives for a specific year"""
        logger.info(f"\n{'#' * 80}")
        logger.info(f"# Processing Year {year}")
        logger.info(f"{'#' * 80}\n")

        # Get info about existing archives
        archives = self.get_archive_info(year)

        if not archives:
            logger.info(f"No archives found for year {year}")
            return True

        # Track migration statistics
        stats = {"migrated": 0, "already_v2": 0, "created_index": 0, "errors": 0}
        success_count = 0
        for archive_type, archive_info in archives.items():
            # Check if archive is in old structure and needs migration
            is_old_structure = archive_info.get("is_old_structure", False)

            # Check if already in V2 format
            if archive_info["index_exists"] and archive_info["existing_index"]:
                if (
                    self.is_already_v2_format(archive_info["existing_index"])
                    and not is_old_structure
                ):
                    logger.info(
                        f"✓ Archive {archive_type} already in V2 format with {len(archive_info['existing_index']['parts'])} part(s), skipping"
                    )
                    stats["already_v2"] += 1
                    success_count += 1
                    continue

            # ALWAYS migrate/cleanup if OLD structure file exists, regardless of index
            if is_old_structure and archive_info["needs_split"]:
                # Large archive in OLD structure that needs to be split into parts
                logger.info(
                    f"Archive {archive_type} in OLD structure needs splitting (>{format_size(MAX_ARCHIVE_SIZE)})"
                )
                if self.migrate_archive(year, archive_type, archive_info):
                    stats["migrated"] += 1
                    success_count += 1
                else:
                    stats["errors"] += 1
            elif is_old_structure:
                # Archive is in old structure and needs to be migrated to new structure
                logger.info(
                    f"Archive {archive_type} is under size limit but in OLD structure, migrating to NEW structure"
                )
                # Download archive to list files and upload to new location
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = Path(temp_dir)
                    archive_path = temp_path / archive_info["archive_name"]

                    if self.download_archive(archive_info["s3_key"], archive_path):
                        try:
                            # List files for index
                            with zipfile.ZipFile(archive_path, "r") as zf:
                                files = zf.namelist()

                            # Upload to new location
                            logger.info(
                                f"Uploading {archive_type}.zip to NEW structure"
                            )
                            new_s3_key = f"{archive_info['s3_dir']}{archive_info['archive_name']}"
                            if not self.upload_archive_part(archive_path, new_s3_key):
                                logger.error(
                                    f"Failed to upload {archive_type}.zip to new location"
                                )
                                continue

                            # Also delete old index file
                            self.delete_old_index(year, archive_type)

                            # Delete old archive after successful upload
                            if not self.delete_old_archive(archive_info["s3_key"]):
                                logger.warning(
                                    "Failed to delete old archive, but continuing..."
                                )

                            # Create/update index with the archive as a single part
                            part_name = f"{archive_type}.zip"
                            parts_info = [(part_name, files, archive_info["size"])]

                            if self.create_and_upload_index(
                                year,
                                archive_type,
                                archive_info["s3_dir"],
                                parts_info,
                            ):
                                stats["migrated"] += 1
                                success_count += 1
                            else:
                                stats["errors"] += 1
                        except Exception as e:
                            logger.error(f"Error migrating {archive_type}: {e}")
                            stats["errors"] += 1
            elif archive_info["index_exists"]:
                # Archive has index, but need to verify it's V2 format
                if self.is_already_v2_format(archive_info["existing_index"]):
                    logger.info(
                        f"✓ Archive {archive_type} is under size limit and already in V2 format, skipping"
                    )
                    stats["already_v2"] += 1
                    success_count += 1
                else:
                    # Index exists but is OLD format - need to convert to V2
                    logger.info(
                        f"Archive {archive_type} has OLD format index, converting to V2"
                    )
                    with tempfile.TemporaryDirectory() as temp_dir:
                        temp_path = Path(temp_dir)
                        archive_path = temp_path / archive_info["archive_name"]

                        if self.download_archive(archive_info["s3_key"], archive_path):
                            try:
                                with zipfile.ZipFile(archive_path, "r") as zf:
                                    files = zf.namelist()

                                # Create V2 index with single part
                                part_name = f"{archive_type}.zip"
                                parts_info = [(part_name, files, archive_info["size"])]

                                if self.create_and_upload_index(
                                    year,
                                    archive_type,
                                    archive_info["s3_dir"],
                                    parts_info,
                                ):
                                    stats["created_index"] += 1
                                    success_count += 1
                                else:
                                    stats["errors"] += 1
                            except Exception as e:
                                logger.error(
                                    f"Error converting index for {archive_type}: {e}"
                                )
                                stats["errors"] += 1
            else:
                # Archive in new structure but missing index - create index only
                logger.info(
                    f"Archive {archive_type} is in NEW structure but missing index, creating index"
                )
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = Path(temp_dir)
                    archive_path = temp_path / archive_info["archive_name"]

                    if self.download_archive(archive_info["s3_key"], archive_path):
                        try:
                            with zipfile.ZipFile(archive_path, "r") as zf:
                                files = zf.namelist()

                            # Create index with the existing archive as a single part
                            part_name = f"{archive_type}.zip"
                            parts_info = [(part_name, files, archive_info["size"])]

                            if self.create_and_upload_index(
                                year,
                                archive_type,
                                archive_info["s3_dir"],
                                parts_info,
                            ):
                                stats["created_index"] += 1
                                success_count += 1
                            else:
                                stats["errors"] += 1
                        except Exception as e:
                            logger.error(
                                f"Error creating index for {archive_type}: {e}"
                            )
                            stats["errors"] += 1

        # Print statistics for this year
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Year {year} Migration Summary:")
        logger.info(f"  Total archives: {len(archives)}")
        logger.info(f"  Migrated/split: {stats['migrated']}")
        logger.info(f"  Already V2 format: {stats['already_v2']}")
        logger.info(f"  Index created: {stats['created_index']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"  Success rate: {success_count}/{len(archives)}")
        logger.info(f"{'=' * 60}")
        return success_count == len(archives)

    def migrate_all(self, specific_year: Optional[int] = None) -> bool:
        """Migrate all years or a specific year"""
        if specific_year:
            years = [specific_year]
        else:
            years = self.list_years()

        if not years:
            logger.error("No years found in bucket")
            return False

        logger.info(f"Found {len(years)} year(s) to process: {years}")

        success_count = 0
        for year in years:
            if self.migrate_year(year):
                success_count += 1

        logger.info(f"\n{'=' * 80}")
        logger.info("MIGRATION COMPLETE")
        logger.info(f"{'=' * 80}")
        logger.info(f"Total years processed: {len(years)}")
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Failed: {len(years) - success_count}")
        if self.dry_run:
            logger.info("\n[DRY RUN MODE] No changes were made to S3")
        logger.info(f"{'=' * 80}")

        return success_count == len(years)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate existing archives to multi-part format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate all years in bucket
  python migrate_to_multipart.py --bucket my-test-bucket
  
  # Migrate specific year
  python migrate_to_multipart.py --bucket my-test-bucket --year 2023
  
  # Dry run (no changes)
  python migrate_to_multipart.py --bucket my-test-bucket --dry-run
  
  # Migrate with verbose logging
  python migrate_to_multipart.py --bucket my-test-bucket --verbose
        """,
    )

    parser.add_argument("--bucket", required=True, help="S3 bucket name")

    parser.add_argument(
        "--year", type=int, help="Specific year to migrate (default: all years)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create migrator and run
    migrator = ArchiveMigrator(args.bucket, dry_run=args.dry_run)

    try:
        success = migrator.migrate_all(specific_year=args.year)
        exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nMigration interrupted by user")
        exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
