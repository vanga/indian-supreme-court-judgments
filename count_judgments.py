#!/usr/bin/env python3
"""
Script to count judgments in tar files and their index files
"""

import json
from pathlib import Path
import logging
from collections import defaultdict
import tarfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def count_tar_files(tar_path):
    """Count files in a tar archive"""
    try:
        with tarfile.open(tar_path, "r") as tf:
            # Get list of files, excluding directories
            files = [m for m in tf.getmembers() if m.isfile()]
            return len(files)
    except Exception as e:
        logger.error(f"Error reading tar file {tar_path}: {e}")
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
    """Count judgments in all tar files and their index files"""
    packages_dir = Path("./packages")

    if not packages_dir.exists():
        logger.error("packages directory not found")
        return

    # Find all tar files and their index files
    tar_files = list(packages_dir.glob("*.tar"))
    index_files = list(packages_dir.glob("*.index.json"))

    logger.info(f"Found {len(tar_files)} tar files and {len(index_files)} index files")

    # Count by year and type
    tar_counts = defaultdict(lambda: defaultdict(int))
    index_counts = defaultdict(lambda: defaultdict(int))

    # Count files in tar archives
    for tar_path in tar_files:
        # Extract year and type from filename (e.g., sc-judgments-2023-english.tar)
        parts = tar_path.stem.split("-")
        if len(parts) >= 4:
            year = parts[2]
            archive_type = parts[3]
            count = count_tar_files(tar_path)
            tar_counts[year][archive_type] = count
            logger.info(f"Tar {tar_path.name}: {count} files")

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
        f"{'Year':<10} {'Type':<10} {'Tar Count':<12} {'Index Count':<12} {'Match':<8}"
    )
    print("-" * 80)

    all_years = sorted(set(list(tar_counts.keys()) + list(index_counts.keys())))
    total_tar = 0
    total_index = 0

    for year in all_years:
        for archive_type in ["english", "regional", "metadata"]:
            tar_count = tar_counts[year][archive_type]
            index_count = index_counts[year][archive_type]
            match = "✓" if tar_count == index_count else "✗"

            print(
                f"{year:<10} {archive_type:<10} {tar_count:<12} {index_count:<12} {match:<8}"
            )

            total_tar += tar_count
            total_index += index_count

    print("-" * 80)
    print(
        f"{'TOTAL':<10} {'ALL':<10} {total_tar:<12} {total_index:<12} {'✓' if total_tar == total_index else '✗':<8}"
    )

    # Check for mismatches
    if total_tar != total_index:
        print("\nMismatches found:")
        for year in all_years:
            for archive_type in ["english", "regional", "metadata"]:
                tar_count = tar_counts[year][archive_type]
                index_count = index_counts[year][archive_type]
                if tar_count != index_count:
                    print(
                        f"  {year}-{archive_type}: Tar has {tar_count}, Index has {index_count}"
                    )


def main():
    count_judgments()


if __name__ == "__main__":
    main()
