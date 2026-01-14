import csv
import json
import logging
import re
from collections import defaultdict

import boto3
from botocore import UNSIGNED
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "indian-supreme-court-judgments-test"


def bytes_to_gb(bytes_size):
    """Convert bytes to GB"""
    return round(bytes_size / (1024 * 1024 * 1024), 2)


def get_dataset_sizes():
    """Get dataset sizes from S3 index files"""
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # Store: year -> total_size_bytes
    year_sizes = defaultdict(int)

    logger.info("Reading index files from S3...")

    # List all index.json files - ONLY from data/ and metadata/, NOT data-old/
    paginator = s3.get_paginator("list_objects_v2")

    # Process data/tar/ directory only
    for prefix in ["data/tar/", "metadata/tar/"]:
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Look for index.json files
                if not key.endswith(".index.json"):
                    continue

                # Extract year from path like: data/tar/year=2023/english/english.index.json
                year_match = re.search(r"year=(\d{4})/", key)
                if not year_match:
                    continue

                year = year_match.group(1)

                try:
                    # Download and read the index file
                    response = s3.get_object(Bucket=BUCKET, Key=key)
                    index_data = json.loads(response["Body"].read().decode("utf-8"))

                    # Get total_size from index (V2 format) or fall back to zip_size (old format)
                    total_size = index_data.get(
                        "total_size", index_data.get("zip_size", 0)
                    )
                    if total_size > 0:
                        year_sizes[year] += total_size
                        logger.info(
                            f"Year {year}: Found {bytes_to_gb(total_size)} GB in {key}"
                        )

                except Exception as e:
                    logger.warning(f"Could not read {key}: {e}")

    return year_sizes


def create_csv_report(year_sizes):
    """Create CSV report with sizes by year"""

    # Sort years
    sorted_years = sorted(year_sizes.keys())
    total_size = sum(year_sizes.values())

    csv_file = "dataset_sizes.csv"

    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Year", "Size (GB)"])

        for year in sorted_years:
            size_gb = bytes_to_gb(year_sizes[year])
            writer.writerow([year, size_gb])

        # Add total
        writer.writerow(["TOTAL", bytes_to_gb(total_size)])

    logger.info(f"Created {csv_file}")
    return csv_file, bytes_to_gb(total_size)


def main():
    """Main function"""
    print("Calculating dataset sizes...")

    # Get sizes from S3
    year_sizes = get_dataset_sizes()

    if not year_sizes:
        print("No data found!")
        return

    # Create CSV report
    csv_file, total_gb = create_csv_report(year_sizes)

    # Summary
    print("\nSummary:")
    print(f"   Years covered: {min(year_sizes.keys())} - {max(year_sizes.keys())}")
    print(f"   Total years: {len(year_sizes)}")
    print(f"   Total size: {total_gb} GB")
    print(f"   CSV report: {csv_file}")
    print("\nDone!")


if __name__ == "__main__":
    main()
