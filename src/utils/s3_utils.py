"""
S3 Utility Functions for Supreme Court Judgments
Handles TAR archive uploads, downloads, and index file management
"""

import json
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set

import boto3

logger = logging.getLogger(__name__)


def upload_large_file_to_s3(
    s3_client, bucket: str, key: str, file_path: str, chunk_size: int = 100 * 1024 * 1024
):
    """
    Upload large files using multipart upload

    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        key: S3 object key
        file_path: Local file path
        chunk_size: Size of each chunk (default 100MB)
    """
    file_size = os.path.getsize(file_path)

    if file_size < 5 * 1024 * 1024 * 1024:  # Less than 5GB
        s3_client.upload_file(file_path, bucket, key)
        return

    # Use multipart upload for files >= 5GB
    config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=5 * 1024 * 1024 * 1024,
        multipart_chunksize=chunk_size,
        max_concurrency=10,
    )
    s3_client.upload_file(file_path, bucket, key, Config=config)


def upload_single_file_to_s3(s3_client, bucket: str, key: str, file_path: str):
    """Upload a single file to S3"""
    s3_client.upload_file(file_path, bucket, key)


def load_index_v2(s3_client, bucket: str, index_key: str) -> dict:
    """Load or create an empty IndexFileV2 format index"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=index_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data
    except s3_client.exceptions.NoSuchKey:
        return {
            "year": 0,
            "archive_type": "",
            "file_count": 0,
            "total_size": 0,
            "total_size_human": "0 B",
            "created_at": "",
            "updated_at": "",
            "parts": [],
            "files": [],
        }
    except Exception as e:
        logger.error(f"Error loading index from S3: {e}")
        return {
            "year": 0,
            "archive_type": "",
            "file_count": 0,
            "total_size": 0,
            "total_size_human": "0 B",
            "created_at": "",
            "updated_at": "",
            "parts": [],
            "files": [],
        }


def update_index_file(
    s3_client,
    bucket: str,
    index_key: str,
    new_files: List[str],
    archive_name: str,
    archive_size: int,
):
    """Update the index file with new file information"""
    index_data = load_index_v2(s3_client, bucket, index_key)

    # Add files to list
    existing_files = set(index_data.get("files", []))
    existing_files.update(new_files)
    index_data["files"] = sorted(list(existing_files))
    index_data["file_count"] = len(index_data["files"])

    # Upload updated index
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(index_data, f, indent=2)
        temp_path = f.name

    try:
        s3_client.upload_file(temp_path, bucket, index_key)
    finally:
        os.unlink(temp_path)


def get_existing_files_from_s3_v2(s3_client, bucket: str, index_key: str) -> Set[str]:
    """Get set of existing files from V2 index"""
    try:
        index_data = load_index_v2(s3_client, bucket, index_key)

        existing_files = set(index_data.get("files", []))

        # Also collect files from parts
        for part in index_data.get("parts", []):
            existing_files.update(part.get("files", []))

        return existing_files
    except Exception as e:
        logger.debug(f"Could not load index {index_key}: {e}")
        return set()


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
