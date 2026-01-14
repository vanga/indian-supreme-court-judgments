#!/usr/bin/env python3
"""
Migration Verification Script

This script verifies that the migration to multi-part format was successful by checking:
1. All index files are in V2 format (have "parts" array)
2. No old structure files remain (data/tar/year=YYYY/*.tar)
3. No old index files remain (data/tar/year=YYYY/*.index.json)
4. All archives have corresponding V2 indexes

Usage:
    python verify_migration.py --bucket indian-supreme-court-judgments
    python verify_migration.py --bucket indian-supreme-court-judgments --year 2023
    python verify_migration.py --bucket indian-supreme-court-judgments --fix
"""

import argparse
import json
import logging
from collections import defaultdict
from typing import Dict, List, Set

import boto3
from tabulate import tabulate

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MigrationVerifier:
    def __init__(self, bucket_name: str, fix: bool = False):
        self.bucket_name = bucket_name
        self.fix = fix
        self.s3 = boto3.client("s3")

        # Track issues
        self.issues = defaultdict(list)
        self.old_structure_archives = []
        self.old_structure_indexes = []
        self.missing_indexes = []
        self.old_format_indexes = []
        self.v2_indexes = []

    def list_years(self) -> Set[int]:
        """List all years in the bucket"""
        years = set()

        # Check data/tar directory
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix="data/tar/", Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years from data/tar: {e}")

        # Check metadata/tar directory
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix="metadata/tar/", Delimiter="/"
            ):
                if "CommonPrefixes" in page:
                    for prefix in page["CommonPrefixes"]:
                        parts = prefix["Prefix"].split("/")
                        for part in parts:
                            if part.startswith("year="):
                                year = int(part.split("=")[1])
                                years.add(year)
        except Exception as e:
            logger.error(f"Error listing years from metadata/tar: {e}")

        return years

    def check_old_structure_files(self, year: int) -> List[str]:
        """Check for files in old flat structure"""
        old_files = []

        # Check for old structure archives: data/tar/year=YYYY/{english,regional}.tar
        for archive_type in ["english", "regional"]:
            old_key = f"data/tar/year={year}/{archive_type}.tar"
            try:
                self.s3.head_object(Bucket=self.bucket_name, Key=old_key)
                old_files.append(old_key)
                self.old_structure_archives.append(
                    {"year": year, "type": archive_type, "key": old_key}
                )
            except self.s3.exceptions.ClientError:
                pass

        return old_files

    def check_old_index_files(self, year: int) -> List[str]:
        """Check for old index files in flat structure"""
        old_indexes = []

        # Check for old index files: data/tar/year=YYYY/{english,regional}.index.json
        for archive_type in ["english", "regional"]:
            old_index_key = f"data/tar/year={year}/{archive_type}.index.json"
            try:
                self.s3.head_object(Bucket=self.bucket_name, Key=old_index_key)
                old_indexes.append(old_index_key)
                self.old_structure_indexes.append(
                    {"year": year, "type": archive_type, "key": old_index_key}
                )
            except self.s3.exceptions.ClientError:
                pass

        return old_indexes

    def check_index_format(self, year: int, archive_type: str, s3_dir: str) -> Dict:
        """Check if index is in V2 format"""
        index_key = f"{s3_dir}{archive_type}.index.json"

        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=index_key)
            index_data = json.loads(response["Body"].read().decode("utf-8"))

            # Check if V2 format
            has_parts = "parts" in index_data and isinstance(index_data["parts"], list)

            if has_parts and len(index_data["parts"]) > 0:
                self.v2_indexes.append(
                    {
                        "year": year,
                        "type": archive_type,
                        "key": index_key,
                        "parts": len(index_data["parts"]),
                        "files": index_data.get("file_count", 0),
                    }
                )
                return {
                    "exists": True,
                    "is_v2": True,
                    "parts_count": len(index_data["parts"]),
                    "file_count": index_data.get("file_count", 0),
                    "key": index_key,
                }
            else:
                self.old_format_indexes.append(
                    {
                        "year": year,
                        "type": archive_type,
                        "key": index_key,
                        "format": "OLD",
                    }
                )
                return {"exists": True, "is_v2": False, "key": index_key}

        except self.s3.exceptions.ClientError as e:
            if "404" in str(e) or "NoSuchKey" in str(e):
                self.missing_indexes.append(
                    {"year": year, "type": archive_type, "expected_key": index_key}
                )
                return {"exists": False, "is_v2": False, "key": index_key}
            else:
                logger.error(f"Error checking index {index_key}: {e}")
                return {"exists": False, "is_v2": False, "error": str(e)}

    def verify_year(self, year: int) -> Dict:
        """Verify migration for a specific year"""
        results = {
            "year": year,
            "old_structure_files": [],
            "old_index_files": [],
            "indexes": {},
        }

        # Check for old structure files
        old_files = self.check_old_structure_files(year)
        if old_files:
            results["old_structure_files"] = old_files
            self.issues[year].append(f"Found {len(old_files)} old structure archive(s)")

        # Check for old index files
        old_indexes = self.check_old_index_files(year)
        if old_indexes:
            results["old_index_files"] = old_indexes
            self.issues[year].append(f"Found {len(old_indexes)} old index file(s)")

        # Check new structure indexes
        for archive_type in ["english", "regional", "metadata"]:
            if archive_type == "metadata":
                s3_dir = f"metadata/tar/year={year}/"
            else:
                s3_dir = f"data/tar/year={year}/{archive_type}/"

            index_info = self.check_index_format(year, archive_type, s3_dir)
            results["indexes"][archive_type] = index_info

            if not index_info["exists"]:
                self.issues[year].append(f"Missing index for {archive_type}")
            elif not index_info["is_v2"]:
                self.issues[year].append(f"Index for {archive_type} is OLD format")

        return results

    def fix_issues(self):
        """Fix detected issues by deleting old files"""
        if not self.fix:
            logger.info("Fix mode not enabled. Use --fix to delete old files.")
            return

        logger.info("\n" + "=" * 80)
        logger.info("FIXING ISSUES")
        logger.info("=" * 80)

        # Delete old structure archives
        if self.old_structure_archives:
            logger.info(
                f"\nDeleting {len(self.old_structure_archives)} old structure archive(s)..."
            )
            for item in self.old_structure_archives:
                try:
                    logger.info(f"  Deleting {item['key']}")
                    self.s3.delete_object(Bucket=self.bucket_name, Key=item["key"])
                    logger.info(f"  ✓ Deleted {item['key']}")
                except Exception as e:
                    logger.error(f"  ✗ Failed to delete {item['key']}: {e}")

        # Delete old index files
        if self.old_structure_indexes:
            logger.info(
                f"\nDeleting {len(self.old_structure_indexes)} old index file(s)..."
            )
            for item in self.old_structure_indexes:
                try:
                    logger.info(f"  Deleting {item['key']}")
                    self.s3.delete_object(Bucket=self.bucket_name, Key=item["key"])
                    logger.info(f"  ✓ Deleted {item['key']}")
                except Exception as e:
                    logger.error(f"  ✗ Failed to delete {item['key']}: {e}")

        if not self.old_structure_archives and not self.old_structure_indexes:
            logger.info("\n✓ No old files to delete")

    def print_summary(self):
        """Print verification summary"""
        print("\n" + "=" * 80)
        print("MIGRATION VERIFICATION SUMMARY")
        print("=" * 80)

        # Summary statistics
        total_years = len(self.issues) if self.issues else len(self.v2_indexes) // 3
        clean_years = sum(1 for issues in self.issues.values() if not issues)
        problem_years = len([y for y, issues in self.issues.items() if issues])

        print(f"\nTotal Years Checked: {total_years}")
        print(f"Clean Years: {clean_years}")
        print(f"Years with Issues: {problem_years}")

        # V2 Indexes
        if self.v2_indexes:
            print(f"\n✓ V2 Format Indexes: {len(self.v2_indexes)}")
            table_data = []
            for item in self.v2_indexes:
                table_data.append(
                    [item["year"], item["type"], item["parts"], item["files"]]
                )
            print(
                tabulate(
                    table_data,
                    headers=["Year", "Type", "Parts", "Files"],
                    tablefmt="simple",
                )
            )

        # Old Structure Archives
        if self.old_structure_archives:
            print(
                f"\n✗ Old Structure Archives Found: {len(self.old_structure_archives)}"
            )
            table_data = []
            for item in self.old_structure_archives:
                table_data.append([item["year"], item["type"], item["key"]])
            print(
                tabulate(
                    table_data, headers=["Year", "Type", "S3 Key"], tablefmt="simple"
                )
            )
        else:
            print("\n✓ No old structure archives found")

        # Old Index Files
        if self.old_structure_indexes:
            print(f"\n✗ Old Index Files Found: {len(self.old_structure_indexes)}")
            table_data = []
            for item in self.old_structure_indexes:
                table_data.append([item["year"], item["type"], item["key"]])
            print(
                tabulate(
                    table_data, headers=["Year", "Type", "S3 Key"], tablefmt="simple"
                )
            )
        else:
            print("\n✓ No old index files found")

        # Old Format Indexes
        if self.old_format_indexes:
            print(f"\n✗ OLD Format Indexes: {len(self.old_format_indexes)}")
            table_data = []
            for item in self.old_format_indexes:
                table_data.append([item["year"], item["type"], item["key"]])
            print(
                tabulate(
                    table_data, headers=["Year", "Type", "S3 Key"], tablefmt="simple"
                )
            )
        else:
            print("\n✓ All indexes are in V2 format")

        # Missing Indexes
        if self.missing_indexes:
            print(f"\n✗ Missing Indexes: {len(self.missing_indexes)}")
            table_data = []
            for item in self.missing_indexes:
                table_data.append([item["year"], item["type"], item["expected_key"]])
            print(
                tabulate(
                    table_data,
                    headers=["Year", "Type", "Expected Key"],
                    tablefmt="simple",
                )
            )
        else:
            print("\n✓ All expected indexes exist")

        # Overall Status
        print("\n" + "=" * 80)
        if (
            not self.old_structure_archives
            and not self.old_structure_indexes
            and not self.old_format_indexes
            and not self.missing_indexes
        ):
            print("✓ MIGRATION SUCCESSFUL - All checks passed!")
        else:
            print("✗ MIGRATION INCOMPLETE - Issues found above")
            if not self.fix:
                print("\nRun with --fix to automatically clean up old files")
        print("=" * 80)

    def verify_all(self, specific_year: int = None):
        """Verify migration for all years or specific year"""
        if specific_year:
            years = [specific_year]
        else:
            years = sorted(self.list_years())

        if not years:
            logger.error("No years found in bucket")
            return

        logger.info(f"Verifying {len(years)} year(s): {years}")

        for year in years:
            logger.info(f"\nChecking year {year}...")
            self.verify_year(year)

        self.print_summary()

        # Fix issues if requested
        if self.fix:
            self.fix_issues()
            logger.info("\n✓ Fix completed. Run verification again to confirm.")


def main():
    parser = argparse.ArgumentParser(
        description="Verify migration to multi-part format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Verify all years
  python verify_migration.py --bucket indian-supreme-court-judgments
  
  # Verify specific year
  python verify_migration.py --bucket indian-supreme-court-judgments --year 2023
  
  # Verify and fix issues (delete old files)
  python verify_migration.py --bucket indian-supreme-court-judgments --fix
        """,
    )

    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--year", type=int, help="Specific year to verify (optional)")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Automatically fix issues by deleting old structure files",
    )

    args = parser.parse_args()

    logger.info(f"Starting migration verification for bucket: {args.bucket}")
    if args.fix:
        logger.warning("FIX MODE ENABLED - Old files will be deleted!")

    verifier = MigrationVerifier(args.bucket, fix=args.fix)
    verifier.verify_all(specific_year=args.year)


if __name__ == "__main__":
    main()
