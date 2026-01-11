#!/usr/bin/env python3
"""
Convert Metadata Index Files to IndexFileV2 Format

This script converts old metadata index.json files to the new IndexFileV2 format
with parts array. For metadata files, since they're always single files, we create
a single part entry.

Usage:
    python convert_metadata_indexes.py --bucket indian-supreme-court-judgments
    python convert_metadata_indexes.py --bucket indian-supreme-court-judgments --year 2023
    python convert_metadata_indexes.py --bucket indian-supreme-court-judgments --dry-run
"""

import argparse
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List

import boto3

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


class MetadataIndexConverter:
    def __init__(self, bucket_name: str, dry_run: bool = False):
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        self.s3 = boto3.client("s3")

    def list_metadata_years(self) -> List[int]:
        """List all years that have metadata archives"""
        prefix = "metadata/zip/year="
        years = set()

        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                # Extract year from path like metadata/zip/year=2023/...
                if key.startswith(prefix) and "/" in key[len(prefix) :]:
                    year_part = key[len(prefix) :].split("/")[0]
                    try:
                        years.add(int(year_part))
                    except ValueError:
                        continue

        return sorted(years)

    def get_metadata_index(self, year: int) -> dict:
        """Download metadata index.json for a given year"""
        index_key = f"metadata/zip/year={year}/metadata.index.json"

        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=index_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except self.s3.exceptions.NoSuchKey:
            logger.warning(f"No metadata index found for year {year}")
            return None
        except Exception as e:
            logger.error(f"Error loading metadata index for year {year}: {e}")
            return None

    def is_already_v2_format(self, index_data: dict) -> bool:
        """Check if index is already in V2 format (has parts array)"""
        return "parts" in index_data and isinstance(index_data["parts"], list)

    def convert_to_v2(self, year: int, old_index: dict) -> IndexFileV2:
        """Convert old index format to V2 with parts array"""
        now = ist_now_iso()

        # Get metadata.zip size from S3
        metadata_zip_key = f"metadata/zip/year={year}/metadata.zip"
        try:
            response = self.s3.head_object(
                Bucket=self.bucket_name, Key=metadata_zip_key
            )
            zip_size = response["ContentLength"]
        except Exception as e:
            logger.warning(
                f"Could not get metadata.zip size for year {year}, using index size: {e}"
            )
            zip_size = old_index.get("zip_size", old_index.get("total_size", 0))

        # Create single part with all files
        files = old_index.get("files", [])
        part = IndexPart(
            name="metadata.zip",
            files=files,
            file_count=len(files),
            size=zip_size,
            size_human=format_size(zip_size),
            created_at=old_index.get("created_at", now),
        )

        # Create V2 index
        index_v2 = IndexFileV2(
            year=year,
            archive_type="metadata",
            file_count=len(files),
            total_size=zip_size,
            total_size_human=format_size(zip_size),
            created_at=old_index.get("created_at", now),
            updated_at=now,
            parts=[part],
        )

        return index_v2

    def upload_index(self, year: int, index: IndexFileV2):
        """Upload the converted index back to S3"""
        index_key = f"metadata/zip/year={year}/metadata.index.json"

        if self.dry_run:
            logger.info(f"[DRY RUN] Would upload converted index to: {index_key}")
            logger.info(
                f"[DRY RUN] Index content: {json.dumps(index.to_dict(), indent=2)}"
            )
            return

        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=index_key,
                Body=json.dumps(index.to_dict(), indent=2),
                ContentType="application/json",
            )
            logger.info(f"✓ Uploaded converted index for year {year}")
        except Exception as e:
            logger.error(f"✗ Failed to upload index for year {year}: {e}")
            raise

    def convert_year(self, year: int):
        """Convert metadata index for a single year"""
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Processing year {year}")
        logger.info(f"{'=' * 60}")

        # Get current index
        index_data = self.get_metadata_index(year)
        if index_data is None:
            logger.warning(f"Skipping year {year}: no index found")
            return

        # Check if already V2
        if self.is_already_v2_format(index_data):
            logger.info(f"✓ Year {year} metadata index is already in V2 format")
            return

        logger.info(f"Converting year {year} metadata index to V2 format...")

        # Convert to V2
        index_v2 = self.convert_to_v2(year, index_data)

        logger.info(f"  Files: {index_v2.file_count}")
        logger.info(f"  Size: {index_v2.total_size_human}")
        logger.info(f"  Parts: {len(index_v2.parts)}")

        # Upload
        self.upload_index(year, index_v2)

    def convert_all_years(self, specific_year: int = None):
        """Convert metadata indexes for all years or a specific year"""
        if specific_year:
            years = [specific_year]
        else:
            years = self.list_metadata_years()
            logger.info(f"Found metadata for {len(years)} years: {years}")

        if not years:
            logger.warning("No metadata years found")
            return

        converted = 0
        already_v2 = 0
        errors = 0

        for year in years:
            try:
                index_data = self.get_metadata_index(year)
                if index_data is None:
                    continue

                if self.is_already_v2_format(index_data):
                    already_v2 += 1
                    logger.info(f"Year {year}: Already V2 format ✓")
                else:
                    self.convert_year(year)
                    converted += 1
            except Exception as e:
                logger.error(f"Error processing year {year}: {e}")
                errors += 1
                continue

        # Summary
        logger.info(f"\n{'=' * 60}")
        logger.info("CONVERSION SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Total years processed: {len(years)}")
        logger.info(f"Converted to V2: {converted}")
        logger.info(f"Already V2 format: {already_v2}")
        logger.info(f"Errors: {errors}")

        if self.dry_run:
            logger.info("\n[DRY RUN MODE] No changes were made to S3")


def main():
    parser = argparse.ArgumentParser(
        description="Convert metadata index files to IndexFileV2 format"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name (e.g., indian-supreme-court-judgments)",
    )
    parser.add_argument("--year", type=int, help="Specific year to convert (optional)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    args = parser.parse_args()

    logger.info(f"Starting metadata index conversion for bucket: {args.bucket}")
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    converter = MetadataIndexConverter(args.bucket, dry_run=args.dry_run)
    converter.convert_all_years(specific_year=args.year)

    logger.info("\nConversion complete!")


if __name__ == "__main__":
    main()
