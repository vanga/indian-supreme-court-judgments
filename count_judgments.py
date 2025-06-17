#!/usr/bin/env python3
"""
Script to count judgments in zip files and their index files
"""

import json
from pathlib import Path
import logging
import argparse
from collections import defaultdict
import zipfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def count_zip_files(zip_path):
    """Count files in a zip archive"""
    try:
        with zipfile.ZipFile(zip_path, "r") as zipf:
            # Get list of files, excluding directories
            files = [f for f in zipf.namelist() if not f.endswith("/")]
            return len(files)
    except Exception as e:
        logger.error(f"Error reading zip file {zip_path}: {e}")
        return 0


def count_index_files(index_path):
    """Count files listed in an index file"""
    try:
        with open(index_path, "r") as f:
            index_data = json.load(f)
            return len(index_data.get("files", []))
    except Exception as e:
        logger.error(f"Error reading index file {index_path}: {e}")
        return 0


def count_judgments():
    """Count judgments in all zip files and their index files"""
    packages_dir = Path("./packages")

    if not packages_dir.exists():
        logger.error("packages directory not found")
        return

    # Find all zip files and their index files
    zip_files = list(packages_dir.glob("*.zip"))
    index_files = list(packages_dir.glob("*.index.json"))

    logger.info(f"Found {len(zip_files)} zip files and {len(index_files)} index files")

    # Count by year and type
    zip_counts = defaultdict(lambda: defaultdict(int))
    index_counts = defaultdict(lambda: defaultdict(int))

    # Count files in zip archives
    for zip_path in zip_files:
        # Extract year and type from filename (e.g., sc-judgments-2023-english.zip)
        parts = zip_path.stem.split("-")
        if len(parts) >= 4:
            year = parts[2]
            archive_type = parts[3]
            count = count_zip_files(zip_path)
            zip_counts[year][archive_type] = count
            logger.info(f"Zip {zip_path.name}: {count} files")

    # Count files in index files
    for index_path in index_files:
        # Extract year and type from filename (e.g., sc-judgments-2023-english.index.json)
        parts = index_path.stem.split("-")
        if len(parts) >= 4:
            year = parts[2]
            archive_type = parts[3]
            count = count_index_files(index_path)
            index_counts[year][archive_type] = count
            logger.info(f"Index {index_path.name}: {count} files")

    # Print summary
    print("\nSummary by Year and Type:")
    print("-" * 80)
    print(
        f"{'Year':<10} {'Type':<10} {'Zip Count':<12} {'Index Count':<12} {'Match':<8}"
    )
    print("-" * 80)

    all_years = sorted(set(list(zip_counts.keys()) + list(index_counts.keys())))
    total_zip = 0
    total_index = 0

    for year in all_years:
        for archive_type in ["english", "regional", "metadata"]:
            zip_count = zip_counts[year][archive_type]
            index_count = index_counts[year][archive_type]
            match = "✓" if zip_count == index_count else "✗"

            print(
                f"{year:<10} {archive_type:<10} {zip_count:<12} {index_count:<12} {match:<8}"
            )

            total_zip += zip_count
            total_index += index_count

    print("-" * 80)
    print(
        f"{'TOTAL':<10} {'ALL':<10} {total_zip:<12} {total_index:<12} {'✓' if total_zip == total_index else '✗':<8}"
    )

    # Check for mismatches
    if total_zip != total_index:
        print("\nMismatches found:")
        for year in all_years:
            for archive_type in ["english", "regional", "metadata"]:
                zip_count = zip_counts[year][archive_type]
                index_count = index_counts[year][archive_type]
                if zip_count != index_count:
                    print(
                        f"  {year}-{archive_type}: Zip has {zip_count}, Index has {index_count}"
                    )


def main():
    parser = argparse.ArgumentParser(
        description="Count judgments in zip files and their index files"
    )
    args = parser.parse_args()

    count_judgments()


if __name__ == "__main__":
    main()
