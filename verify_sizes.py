#!/usr/bin/env python3
"""
Verification Script: Compare actual S3 sizes vs index reported sizes

This script:
1. Lists actual zip files in each directory and sums their sizes
2. Reads index files and gets reported sizes
3. Compares them to find discrepancies
"""

import json
import re

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from tabulate import tabulate

BUCKET = "indian-supreme-court-judgments-test"


def bytes_to_human(size_bytes):
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    unit_idx = 0
    while size >= 1024.0 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1
    return f"{size:.2f} {units[unit_idx]}"


def verify_directory(s3, prefix, year, archive_type):
    """
    Verify a single directory by comparing:
    - Actual zip file sizes in S3
    - Index file reported sizes
    """
    result = {
        "year": year,
        "type": archive_type,
        "actual_size": 0,
        "index_size": 0,
        "actual_files": [],
        "index_files": [],
        "status": "OK",
        "issues": [],
    }

    # List actual files in the directory
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            size = obj["Size"]
            filename = key.split("/")[-1]

            if key.endswith(".zip"):
                result["actual_files"].append({"name": filename, "size": size})
                result["actual_size"] += size
            elif key.endswith(".index.json"):
                # Read index file
                try:
                    response = s3.get_object(Bucket=BUCKET, Key=key)
                    index_data = json.loads(response["Body"].read().decode("utf-8"))

                    # Get total_size from index
                    result["index_size"] = index_data.get(
                        "total_size", index_data.get("zip_size", 0)
                    )

                    # Get files from parts (V2 format)
                    if "parts" in index_data:
                        for part in index_data["parts"]:
                            result["index_files"].append(
                                {"name": part["name"], "size": part.get("size", 0)}
                            )
                except Exception as e:
                    result["issues"].append(f"Error reading index: {e}")

    # Compare
    if abs(result["actual_size"] - result["index_size"]) > 1024:  # Allow 1KB variance
        result["status"] = "MISMATCH"
        result["issues"].append(
            f"Size mismatch: actual={bytes_to_human(result['actual_size'])}, index={bytes_to_human(result['index_size'])}"
        )

    # Check if all actual files are in index
    actual_names = {f["name"] for f in result["actual_files"]}
    index_names = {f["name"] for f in result["index_files"]}

    missing_in_index = actual_names - index_names
    extra_in_index = index_names - actual_names

    if missing_in_index:
        result["status"] = "MISMATCH"
        result["issues"].append(f"Files not in index: {missing_in_index}")

    if extra_in_index:
        result["status"] = "MISMATCH"
        result["issues"].append(f"Files in index but not in S3: {extra_in_index}")

    return result


def main():
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    print("=" * 80)
    print("S3 SIZE VERIFICATION")
    print("=" * 80)

    # First, get actual S3 recursive sizes for data/ and metadata/
    print("\n1. ACTUAL S3 SIZES (aws s3 ls --recursive equivalent):")
    print("-" * 60)

    totals = {"data/": 0, "metadata/": 0, "data-old/": 0}

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            size = obj["Size"]
            if key.startswith("data-old/"):
                totals["data-old/"] += size
            elif key.startswith("data/"):
                totals["data/"] += size
            elif key.startswith("metadata/"):
                totals["metadata/"] += size

    for prefix, size in totals.items():
        print(f"   {prefix}: {bytes_to_human(size)} ({size:,} bytes)")

    print(
        f"\n   TOTAL (data/ + metadata/): {bytes_to_human(totals['data/'] + totals['metadata/'])}"
    )
    print(f"   TOTAL (including data-old/): {bytes_to_human(sum(totals.values()))}")

    # Now verify each year/type directory
    print("\n2. DIRECTORY-BY-DIRECTORY VERIFICATION:")
    print("-" * 60)

    results = []
    total_actual = 0
    total_index = 0

    # Get all years from data/zip/
    years = set()
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/zip/", Delimiter="/"):
        if "CommonPrefixes" in page:
            for cp in page["CommonPrefixes"]:
                match = re.search(r"year=(\d{4})", cp["Prefix"])
                if match:
                    years.add(match.group(1))

    for year in sorted(years):
        for archive_type in ["english", "regional"]:
            prefix = f"data/zip/year={year}/{archive_type}/"
            result = verify_directory(s3, prefix, year, archive_type)
            results.append(result)
            total_actual += result["actual_size"]
            total_index += result["index_size"]

    # Print summary table
    table_data = []
    for r in results:
        if r["actual_size"] > 0 or r["index_size"] > 0:
            table_data.append(
                [
                    r["year"],
                    r["type"],
                    bytes_to_human(r["actual_size"]),
                    bytes_to_human(r["index_size"]),
                    r["status"],
                    "; ".join(r["issues"][:1]) if r["issues"] else "",
                ]
            )

    print(
        tabulate(
            table_data,
            headers=["Year", "Type", "Actual Size", "Index Size", "Status", "Issues"],
            tablefmt="grid",
        )
    )

    print("\n3. TOTALS:")
    print("-" * 60)
    print(f"   Total actual ZIP sizes: {bytes_to_human(total_actual)}")
    print(f"   Total from index files: {bytes_to_human(total_index)}")
    print(f"   Difference: {bytes_to_human(abs(total_actual - total_index))}")

    # Check for issues
    issues = [r for r in results if r["status"] != "OK"]
    if issues:
        print(f"\n4. ISSUES FOUND ({len(issues)}):")
        print("-" * 60)
        for r in issues:
            print(f"   {r['year']}/{r['type']}:")
            for issue in r["issues"]:
                print(f"      - {issue}")


if __name__ == "__main__":
    main()
