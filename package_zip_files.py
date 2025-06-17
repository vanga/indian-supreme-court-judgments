#!/usr/bin/env python3
"""
Script to package downloaded individual files into zip archives
Run this after downloading to create compressed archives for distribution
"""

import zipfile
import json
from pathlib import Path
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZipPackager:
    def __init__(self, data_dir="./sc_data", packages_dir="./packages"):
        self.data_dir = Path(data_dir)
        self.packages_dir = Path(packages_dir)
        self.packages_dir.mkdir(parents=True, exist_ok=True)

    def get_years_to_process(self, specific_year=None):
        """Get list of years that have data to package"""
        if specific_year:
            return [specific_year]

        years = set()
        for archive_type in ["english", "regional", "metadata"]:
            type_dir = self.data_dir / archive_type
            if type_dir.exists():
                for year_dir in type_dir.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        years.add(int(year_dir.name))

        return sorted(years)

    def package_year_archive(self, year, archive_type):
        """Package files for a specific year and archive type into zip"""
        source_dir = self.data_dir / archive_type / str(year)
        zip_path = self.packages_dir / f"sc-judgments-{year}-{archive_type}.zip"
        index_path = (
            self.packages_dir / f"sc-judgments-{year}-{archive_type}.index.json"
        )

        if not source_dir.exists():
            logger.debug(f"No data directory for {year}-{archive_type}")
            return False

        # Get list of files to add
        files_to_add = list(source_dir.glob("*"))
        if not files_to_add:
            logger.debug(f"No files to package in {source_dir}")
            return False

        logger.debug(f"Packaging {source_dir} -> {zip_path}")

        # Get existing files from index.json (much faster than reading zip)
        existing_files = set()
        if index_path.exists():
            try:
                with open(index_path, "r") as f:
                    index_data = json.load(f)
                    existing_files = set(index_data.get("files", []))
            except Exception as e:
                logger.warning(f"Could not read index {index_path}: {e}")

        # Determine which files to add (avoid duplicates)
        new_files_to_add = []
        for file_path in files_to_add:
            if file_path.is_file():
                if file_path.name not in existing_files:
                    new_files_to_add.append(file_path)
                else:
                    logger.debug(f"Skipping duplicate file: {file_path.name}")

        if not new_files_to_add:
            logger.debug(f"No new files to add to {zip_path}")
            return False

        # Add files to zip (append mode for existing, write mode for new)
        if zip_path.exists():
            logger.info(
                f"Appending {len(new_files_to_add)} new files to existing zip: {zip_path}"
            )
            mode = "a"
        else:
            logger.info(f"Creating new zip file: {zip_path}")
            mode = "w"

        try:
            with zipfile.ZipFile(zip_path, mode, zipfile.ZIP_DEFLATED) as zipf:
                for file_path in new_files_to_add:
                    if file_path.is_file():
                        # Add file with just the filename (not full path)
                        zipf.write(file_path, arcname=file_path.name)
        except Exception as e:
            logger.error(f"Error adding files to zip, {zip_path}: {e}")
            return False

        # Update index file with ALL files (existing + new)
        all_files = existing_files.union(
            {f.name for f in new_files_to_add if f.is_file()}
        )
        file_list = sorted(list(all_files))

        index_data = {
            "archive_type": archive_type,
            "year": year,
            "created_at": datetime.now().isoformat(),
            "zip_file": f"sc-judgments-{year}-{archive_type}.zip",
            "source_directory": f"sc_data/{archive_type}/{year}/",
            "file_count": len(file_list),
            "files": file_list,
        }

        with open(index_path, "w") as f:
            json.dump(index_data, f, indent=2)

        logger.info(f"Updated {zip_path} - now contains {len(file_list)} total files")
        return True

    def package_all(self, specific_year=None):
        """Package all available data into zip files"""
        years = self.get_years_to_process(specific_year)

        if not years:
            logger.info("No data found to package")
            return

        logger.info(f"Processing years: {years}")

        total_updated = 0
        for year in years:
            for archive_type in ["english", "regional", "metadata"]:
                if self.package_year_archive(year, archive_type):
                    total_updated += 1

        logger.info(f"Packaging complete. Updated {total_updated} archives.")

    def cleanup_individual_files(self, specific_year=None):
        """
        Clean up individual files after packaging (optional)
        Only removes files that are successfully packaged in zip files
        """
        years = self.get_years_to_process(specific_year)

        for year in years:
            for archive_type in ["english", "regional", "metadata"]:
                source_dir = self.data_dir / archive_type / str(year)
                zip_path = self.packages_dir / f"sc-judgments-{year}-{archive_type}.zip"

                if not source_dir.exists() or not zip_path.exists():
                    continue

                # Verify zip file integrity by listing contents
                try:
                    with zipfile.ZipFile(zip_path, "r") as zipf:
                        zip_files = set(zipf.namelist())

                    # Only remove files that are in the zip
                    files_to_remove = []
                    for file_path in source_dir.glob("*"):
                        if file_path.is_file() and file_path.name in zip_files:
                            files_to_remove.append(file_path)

                    if files_to_remove:
                        logger.debug(
                            f"Removing {len(files_to_remove)} files from {source_dir}"
                        )

                        for file_path in files_to_remove:
                            file_path.unlink()

                        # Remove empty directory if no files left
                        if not any(source_dir.iterdir()):
                            source_dir.rmdir()

                except Exception as e:
                    logger.error(f"Error verifying zip file {zip_path}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Package downloaded files into zip archives"
    )
    parser.add_argument("--year", type=int, help="Package specific year only")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up individual files after packaging",
    )

    args = parser.parse_args()

    packager = ZipPackager()

    # Package files
    packager.package_all(args.year)

    # Optionally clean up individual files
    if args.cleanup:
        packager.cleanup_individual_files(args.year)


if __name__ == "__main__":
    main()
