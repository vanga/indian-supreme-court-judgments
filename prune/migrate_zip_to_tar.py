#!/usr/bin/env python3
"""
Migration Script: Convert ZIP archives to TAR format in S3

This script migrates the test bucket from ZIP to TAR format:
1. Downloads ZIP files from data/zip/ and metadata/zip/
2. Converts them to uncompressed TAR format
3. Uploads to data/tar/ and metadata/tar/ with new structure
4. Creates/updates index files

Usage:
    AWS_PROFILE=dattam-supreme python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test
    AWS_PROFILE=dattam-supreme python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --year 2023
    AWS_PROFILE=dattam-supreme python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --dry-run
"""

import argparse
import io
import json
import logging
import os
import tarfile
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

# Target part size (1GB)
TARGET_PART_SIZE = 1 * 1024 * 1024 * 1024


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
    def __init__(self, bucket_name: str, dry_run: bool = False):
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        self.s3 = boto3.client("s3")

    def list_years(self) -> List[int]:
        """List all years in the bucket from zip directories"""
        years = set()

        paginator = self.s3.get_paginator("list_objects_v2")

        # Check data/zip directory
        try:
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix="data/zip/", Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years from data/zip: {e}")

        # Check metadata/zip directory
        try:
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix="metadata/zip/", Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years from metadata/zip: {e}")

        return sorted(list(years))

    def get_zip_archives(self, year: int) -> Dict[str, dict]:
        """Get info about existing ZIP archives for a year"""
        archives = {}

        for archive_type in ["english", "regional", "metadata"]:
            if archive_type == "metadata":
                # Check both old flat structure and new subfolder structure
                old_zip_key = f"metadata/zip/year={year}/metadata.zip"
                new_zip_key = f"metadata/zip/year={year}/metadata/metadata.zip"
            else:
                old_zip_key = f"data/zip/year={year}/{archive_type}.zip"
                new_zip_key = f"data/zip/year={year}/{archive_type}/{archive_type}.zip"

            # Try old structure first
            zip_key = None
            size = None

            try:
                response = self.s3.head_object(Bucket=self.bucket_name, Key=old_zip_key)
                size = response["ContentLength"]
                zip_key = old_zip_key
            except self.s3.exceptions.ClientError:
                # Try new structure
                try:
                    response = self.s3.head_object(
                        Bucket=self.bucket_name, Key=new_zip_key
                    )
                    size = response["ContentLength"]
                    zip_key = new_zip_key
                except self.s3.exceptions.ClientError:
                    pass

            if zip_key and size:
                archives[archive_type] = {
                    "zip_key": zip_key,
                    "size": size,
                    "archive_type": archive_type,
                }
                logger.info(
                    f"  Found {archive_type}.zip: {format_size(size)} at {zip_key}"
                )

        return archives

    def download_zip(self, s3_key: str, local_path: Path) -> bool:
        """Download a ZIP file from S3"""
        try:
            logger.info(f"Downloading {s3_key}...")
            self.s3.download_file(self.bucket_name, s3_key, str(local_path))
            return True
        except Exception as e:
            logger.error(f"Error downloading {s3_key}: {e}")
            return False

    def convert_zip_to_tar(
        self, zip_path: Path, tar_path: Path
    ) -> Tuple[bool, List[str]]:
        """Convert a ZIP file to TAR format"""
        files = []
        try:
            logger.info(f"Converting {zip_path.name} to TAR format...")

            with zipfile.ZipFile(zip_path, "r") as zf:
                with tarfile.open(tar_path, "w") as tf:
                    members = zf.namelist()
                    with tqdm(total=len(members), desc="Converting") as pbar:
                        for name in members:
                            # Read file content from ZIP
                            content = zf.read(name)
                            files.append(name)

                            # Create TAR info
                            info = tarfile.TarInfo(name=name)
                            info.size = len(content)

                            # Add to TAR
                            tf.addfile(info, io.BytesIO(content))
                            pbar.update(1)

            logger.info(f"Converted {len(files)} files to {tar_path.name}")
            return True, files

        except Exception as e:
            logger.error(f"Error converting ZIP to TAR: {e}")
            return False, []

    def upload_tar(self, local_path: Path, s3_key: str) -> bool:
        """Upload a TAR file to S3"""
        if self.dry_run:
            logger.info(f"DRY RUN: Would upload {local_path.name} to {s3_key}")
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

        # Get ZIP archives
        archives = self.get_zip_archives(year)

        if not archives:
            logger.warning(f"No ZIP archives found for year {year}")
            return results

        for archive_type, archive_info in archives.items():
            logger.info(f"\n--- Migrating {archive_type} ---")

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                zip_path = temp_path / f"{archive_type}.zip"
                tar_path = temp_path / f"{archive_type}.tar"

                # Download ZIP
                if not self.download_zip(archive_info["zip_key"], zip_path):
                    results[archive_type] = False
                    continue

                # Convert to TAR
                success, files = self.convert_zip_to_tar(zip_path, tar_path)
                if not success:
                    results[archive_type] = False
                    continue

                # Determine target S3 path
                if archive_type == "metadata":
                    tar_s3_dir = f"metadata/tar/year={year}/"
                else:
                    tar_s3_dir = f"data/tar/year={year}/{archive_type}/"

                tar_s3_key = f"{tar_s3_dir}{archive_type}.tar"

                # Upload TAR
                if not self.upload_tar(tar_path, tar_s3_key):
                    results[archive_type] = False
                    continue

                # Create and upload index
                tar_size = tar_path.stat().st_size
                parts_info = [(f"{archive_type}.tar", files, tar_size)]

                if not self.create_and_upload_index(
                    year, archive_type, tar_s3_dir, parts_info
                ):
                    results[archive_type] = False
                    continue

                results[archive_type] = True
                logger.info(f"Successfully migrated {archive_type} for year {year}")

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
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    args = parser.parse_args()

    logger.info(f"Starting ZIP to TAR migration for bucket: {args.bucket}")
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    migrator = ZipToTarMigrator(args.bucket, dry_run=args.dry_run)
    migrator.migrate_all(specific_year=args.year)

    logger.info("\nMigration complete!")


if __name__ == "__main__":
    main()
