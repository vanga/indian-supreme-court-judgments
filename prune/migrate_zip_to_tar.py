#!/usr/bin/env python3
"""
Migration Script: Convert ZIP archives to TAR format in S3

This script migrates the bucket from ZIP to TAR format:
1. Each ZIP file becomes a TAR file (1:1 conversion, preserving multi-part structure)
2. regional.zip -> regional.tar
3. part-20260109T093548.zip -> part-20260109T093548.tar
4. Index file is updated with all parts

Multi-part structure is PRESERVED:
- regional.zip (968 MB) -> regional.tar
- part-20260109T093804.zip (950 MB) -> part-20260109T093804.tar
- part-20260109T093849.zip (326 MB) -> part-20260109T093849.tar

Usage:
    AWS_PROFILE=dattam-supreme python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test
    AWS_PROFILE=dattam-supreme python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --year 2023
    AWS_PROFILE=dattam-supreme python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --dry-run
"""

import argparse
import json
import logging
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import boto3
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Indian Standard Time timezone
IST = timezone(timedelta(hours=5, minutes=30))


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


def ist_now_iso() -> str:
    """Return current IST time in ISO format"""
    return datetime.now(IST).isoformat()


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

    def to_dict(self) -> dict:
        return {
            "year": self.year,
            "archive_type": self.archive_type,
            "file_count": self.file_count,
            "total_size": self.total_size,
            "total_size_human": self.total_size_human,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "parts": [p.to_dict() for p in self.parts],
        }


class ZipToTarMigrator:
    def __init__(self, bucket_name: str, source_prefix: str = "", dry_run: bool = False):
        self.bucket_name = bucket_name
        self.source_prefix = source_prefix  # e.g., "data-old/" for migrating from data-old
        self.dry_run = dry_run
        self.s3 = boto3.client("s3")

    def list_years(self) -> List[int]:
        """List all years in the bucket from zip directories"""
        years = set()

        paginator = self.s3.get_paginator("list_objects_v2")

        # Check data/zip directory
        data_prefix = f"{self.source_prefix}data/zip/"
        try:
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=data_prefix, Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years from {data_prefix}: {e}")

        # Check metadata/zip directory
        metadata_prefix = f"{self.source_prefix}metadata/zip/"
        try:
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=metadata_prefix, Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years from {metadata_prefix}: {e}")

        return sorted(list(years))

    def get_zip_archives(self, year: int) -> Dict[str, dict]:
        """
        Get info about existing ZIP archives for a year.

        This handles multi-part archives where:
        - Main file: {archive_type}.zip (e.g., regional.zip)
        - Additional parts: part-{datetime}.zip (e.g., part-20260109T093548.zip)
        """
        archives = {}
        paginator = self.s3.get_paginator("list_objects_v2")

        for archive_type in ["english", "regional", "metadata"]:
            if archive_type == "metadata":
                zip_dir = f"{self.source_prefix}metadata/zip/year={year}/"
            else:
                zip_dir = f"{self.source_prefix}data/zip/year={year}/{archive_type}/"

            zip_files = []
            total_size = 0

            try:
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=zip_dir):
                    if "Contents" not in page:
                        continue

                    for obj in page["Contents"]:
                        key = obj["Key"]
                        size = obj["Size"]

                        # Skip index files
                        if key.endswith(".index.json"):
                            continue

                        # Match main zip or part-*.zip files
                        filename = key.split("/")[-1]
                        if filename == f"{archive_type}.zip" or filename.startswith("part-"):
                            if filename.endswith(".zip"):
                                zip_files.append({
                                    "key": key,
                                    "filename": filename,
                                    "size": size,
                                    "is_main": filename == f"{archive_type}.zip"
                                })
                                total_size += size

            except Exception as e:
                logger.error(f"Error listing {zip_dir}: {e}")
                continue

            if zip_files:
                # Sort: main file first, then parts by datetime
                zip_files.sort(key=lambda x: (not x["is_main"], x["filename"]))

                archives[archive_type] = {
                    "zip_dir": zip_dir,
                    "zip_files": zip_files,
                    "total_size": total_size,
                    "archive_type": archive_type,
                }

                logger.info(f"  Found {archive_type}: {len(zip_files)} file(s), {format_size(total_size)}")
                for zf in zip_files:
                    logger.info(f"    - {zf['filename']}: {format_size(zf['size'])}")

        return archives

    def convert_single_zip_to_tar(
        self, zip_s3_key: str, temp_dir: Path
    ) -> Tuple[bool, Path, List[str]]:
        """
        Convert a single ZIP file to TAR format.
        Downloads ZIP, extracts to disk, creates TAR, returns TAR path and file list.
        Memory efficient - uses disk for extraction.
        """
        zip_filename = zip_s3_key.split("/")[-1]
        tar_filename = zip_filename.replace(".zip", ".tar")

        zip_path = temp_dir / zip_filename
        extract_dir = temp_dir / "extract"
        extract_dir.mkdir(exist_ok=True)
        tar_path = temp_dir / tar_filename

        # Download ZIP
        try:
            logger.info(f"Downloading {zip_filename}...")
            self.s3.download_file(self.bucket_name, zip_s3_key, str(zip_path))
        except Exception as e:
            logger.error(f"Error downloading {zip_s3_key}: {e}")
            return False, None, []

        # Extract ZIP to disk
        file_list = []
        try:
            logger.info(f"Extracting {zip_filename} to disk...")
            with zipfile.ZipFile(zip_path, "r") as zf:
                members = zf.namelist()
                for name in tqdm(members, desc=f"Extracting"):
                    zf.extract(name, extract_dir)
                    file_list.append(name)

            logger.info(f"Extracted {len(file_list)} files")

            # Remove ZIP to save disk space
            zip_path.unlink()

        except Exception as e:
            logger.error(f"Error extracting {zip_filename}: {e}")
            return False, None, []

        # Create TAR from extracted files
        try:
            logger.info(f"Creating {tar_filename}...")
            with tarfile.open(tar_path, "w") as tf:
                for name in tqdm(file_list, desc="Creating TAR"):
                    file_path = extract_dir / name
                    if file_path.exists():
                        tf.add(file_path, arcname=name)

            logger.info(f"Created {tar_filename}: {format_size(tar_path.stat().st_size)}")

        except Exception as e:
            logger.error(f"Error creating TAR: {e}")
            return False, None, []

        # Clean up extracted files to save disk space
        import shutil
        shutil.rmtree(extract_dir)

        return True, tar_path, file_list

    def upload_tar(self, local_path: Path, s3_key: str) -> bool:
        """Upload a TAR file to S3"""
        if self.dry_run:
            logger.info(f"DRY RUN: Would upload {local_path.name} ({format_size(local_path.stat().st_size)}) to {s3_key}")
            return True

        try:
            logger.info(f"Uploading {local_path.name} to {s3_key}...")
            self.s3.upload_file(str(local_path), self.bucket_name, s3_key)
            return True
        except Exception as e:
            logger.error(f"Error uploading {s3_key}: {e}")
            return False

    def create_and_upload_index(
        self,
        year: int,
        archive_type: str,
        tar_s3_dir: str,
        parts_info: List[Tuple[str, List[str], int]],
    ) -> bool:
        """Create and upload index file for the archive"""
        now = ist_now_iso()

        # Calculate totals
        total_files = []
        total_size = 0
        parts = []

        for part_name, files, size in parts_info:
            total_files.extend(files)
            total_size += size

            part = IndexPart(
                name=part_name,
                files=files,
                file_count=len(files),
                size=size,
                size_human=format_size(size),
                created_at=now,
            )
            parts.append(part)

        # Create index
        index = IndexFileV2(
            year=year,
            archive_type=archive_type,
            file_count=len(total_files),
            total_size=total_size,
            total_size_human=format_size(total_size),
            created_at=now,
            updated_at=now,
            parts=parts,
        )

        # Upload index
        index_key = f"{tar_s3_dir}{archive_type}.index.json"

        if self.dry_run:
            logger.info(f"DRY RUN: Would upload index to {index_key}")
            logger.info(f"  - file_count: {index.file_count}")
            logger.info(f"  - total_size: {index.total_size_human}")
            logger.info(f"  - parts: {len(parts)}")
            for p in parts:
                logger.info(f"    - {p.name}: {p.file_count} files, {p.size_human}")
            return True

        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=index_key,
                Body=json.dumps(index.to_dict(), indent=2),
                ContentType="application/json",
            )
            logger.info(f"Uploaded index: {index_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading index: {e}")
            return False

    def migrate_year(self, year: int) -> Dict[str, bool]:
        """Migrate all archives for a year from ZIP to TAR"""
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Migrating year {year}")
        logger.info(f"{'=' * 60}")

        results = {}

        # Get ZIP archives (including multi-part)
        archives = self.get_zip_archives(year)

        if not archives:
            logger.warning(f"No ZIP archives found for year {year}")
            return results

        for archive_type, archive_info in archives.items():
            logger.info(f"\n--- Migrating {archive_type} ({len(archive_info['zip_files'])} parts) ---")

            # Determine target S3 directory
            if archive_type == "metadata":
                tar_s3_dir = f"metadata/tar/year={year}/"
            else:
                tar_s3_dir = f"data/tar/year={year}/{archive_type}/"

            # Process each ZIP file separately (1:1 conversion)
            parts_info = []
            all_success = True

            for zip_info in archive_info["zip_files"]:
                logger.info(f"\n  Processing {zip_info['filename']}...")

                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_path = Path(temp_dir)

                    # Convert single ZIP to TAR
                    success, tar_path, file_list = self.convert_single_zip_to_tar(
                        zip_info["key"], temp_path
                    )

                    if not success:
                        all_success = False
                        break

                    # Determine TAR filename (same as ZIP but with .tar)
                    tar_filename = zip_info["filename"].replace(".zip", ".tar")
                    tar_s3_key = f"{tar_s3_dir}{tar_filename}"

                    # Upload TAR
                    if not self.upload_tar(tar_path, tar_s3_key):
                        all_success = False
                        break

                    # Record part info for index
                    tar_size = tar_path.stat().st_size
                    parts_info.append((tar_filename, file_list, tar_size))

                    logger.info(f"  Converted {zip_info['filename']} -> {tar_filename}")
                    logger.info(f"    Files: {len(file_list)}, Size: {format_size(tar_size)}")

            if not all_success:
                results[archive_type] = False
                continue

            # Create and upload index with all parts
            if not self.create_and_upload_index(year, archive_type, tar_s3_dir, parts_info):
                results[archive_type] = False
                continue

            results[archive_type] = True
            total_files = sum(len(p[1]) for p in parts_info)
            total_size = sum(p[2] for p in parts_info)
            logger.info(f"\nSuccessfully migrated {archive_type} for year {year}")
            logger.info(f"  - Parts: {len(parts_info)}")
            logger.info(f"  - Total files: {total_files}")
            logger.info(f"  - Total size: {format_size(total_size)}")

        return results

    def migrate_all(self, specific_year: int = None):
        """Migrate all years or a specific year"""
        if specific_year:
            years = [specific_year]
        else:
            years = self.list_years()

        if not years:
            logger.warning("No years found to migrate")
            return

        logger.info(f"Found {len(years)} years to migrate: {years[0]} - {years[-1]}")

        # Summary tracking
        success_count = 0
        fail_count = 0
        all_results = {}

        for year in years:
            results = self.migrate_year(year)
            all_results[year] = results

            for archive_type, success in results.items():
                if success:
                    success_count += 1
                else:
                    fail_count += 1

        # Print summary
        logger.info(f"\n{'=' * 60}")
        logger.info("MIGRATION SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Total archives processed: {success_count + fail_count}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {fail_count}")

        if self.dry_run:
            logger.info("\n[DRY RUN MODE] No changes were made to S3")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate ZIP archives to TAR format in S3"
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--year", type=int, help="Specific year to migrate (optional)")
    parser.add_argument(
        "--source-prefix",
        default="",
        help="Source prefix for ZIP files (e.g., 'data-old/' to read from data-old/data/zip/)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    args = parser.parse_args()

    logger.info(f"Starting ZIP to TAR migration for bucket: {args.bucket}")
    if args.source_prefix:
        logger.info(f"Source prefix: {args.source_prefix}")
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    migrator = ZipToTarMigrator(args.bucket, source_prefix=args.source_prefix, dry_run=args.dry_run)
    migrator.migrate_all(specific_year=args.year)

    logger.info("\nMigration complete!")


if __name__ == "__main__":
    main()
