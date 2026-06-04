"""Backfill individual Supreme Court S3 objects from published tar archives.

This publishes loose objects that mirror the high-court bucket layout:

- metadata/tar/year=YYYY/metadata.tar -> metadata/json/year=YYYY/*.json
- data/tar/year=YYYY/english/*.tar -> data/pdf/year=YYYY/english/*.pdf
- data/tar/year=YYYY/regional/*.tar -> data/pdf/year=YYYY/regional/*.pdf

Dry-run is the default. Pass --execute to write to S3.
"""

from __future__ import annotations

import argparse
import logging
import tarfile
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import PurePosixPath

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError


DEFAULT_BUCKET = "indian-supreme-court-judgments"
ARCHIVE_TYPES = ("metadata", "english", "regional")
CONTENT_TYPES = {
    "metadata": "application/json",
    "english": "application/pdf",
    "regional": "application/pdf",
}

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchivePart:
    year: int
    archive_type: str
    key: str
    size: int


def parse_years(values: list[str] | None) -> list[int]:
    if not values:
        return []

    years: set[int] = set()
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                start_raw, end_raw = part.split("-", 1)
                start = int(start_raw)
                end = int(end_raw)
                if end < start:
                    raise argparse.ArgumentTypeError(
                        f"Invalid year range {part}: end is before start"
                    )
                years.update(range(start, end + 1))
            else:
                years.add(int(part))

    return sorted(years)


def tar_prefix(year: int, archive_type: str) -> str:
    if archive_type == "metadata":
        return f"metadata/tar/year={year}/"
    return f"data/tar/year={year}/{archive_type}/"


def individual_key(year: int, archive_type: str, member_name: str) -> str:
    filename = PurePosixPath(member_name).name
    if archive_type == "metadata":
        return f"metadata/json/year={year}/{filename}"
    return f"data/pdf/year={year}/{archive_type}/{filename}"


def iter_objects(s3, bucket: str, prefix: str) -> Iterable[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def list_archive_parts(
    s3, bucket: str, years: list[int], archive_types: list[str]
) -> list[ArchivePart]:
    parts: list[ArchivePart] = []
    for year in years:
        for archive_type in archive_types:
            prefix = tar_prefix(year, archive_type)
            for obj in iter_objects(s3, bucket, prefix):
                key = obj["Key"]
                if key.endswith(".tar"):
                    parts.append(
                        ArchivePart(
                            year=year,
                            archive_type=archive_type,
                            key=key,
                            size=obj.get("Size", 0),
                        )
                    )
    return parts


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def put_with_retries(
    s3,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
    max_retries: int,
) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
            )
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}:
                raise
            if attempt == max_retries:
                raise
            sleep_seconds = 2**attempt
            logger.warning(
                "Upload failed for %s on attempt %s/%s; retrying in %ss",
                key,
                attempt,
                max_retries,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
        except Exception:
            if attempt == max_retries:
                raise
            sleep_seconds = 2**attempt
            logger.warning(
                "Upload failed for %s on attempt %s/%s; retrying in %ss",
                key,
                attempt,
                max_retries,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)


def backfill_part(
    s3,
    bucket: str,
    part: ArchivePart,
    *,
    execute: bool,
    skip_existing: bool,
    max_retries: int,
    remaining_limit: int | None,
    max_failures: int,
) -> tuple[int, int, int]:
    logger.info("Reading s3://%s/%s", bucket, part.key)
    response = s3.get_object(Bucket=bucket, Key=part.key)

    uploaded = 0
    skipped = 0
    failed = 0

    with response["Body"] as body:
        with tarfile.open(fileobj=body, mode="r|") as tar:
            for member in tar:
                if (
                    remaining_limit is not None
                    and uploaded + skipped + failed >= remaining_limit
                ):
                    break
                if not member.isfile():
                    continue

                target_key = individual_key(part.year, part.archive_type, member.name)
                if skip_existing and object_exists(s3, bucket, target_key):
                    skipped += 1
                    continue

                if not execute:
                    logger.info("DRY RUN: would upload s3://%s/%s", bucket, target_key)
                    skipped += 1
                    continue

                extracted = tar.extractfile(member)
                if extracted is None:
                    failed += 1
                    logger.warning(
                        "Could not extract %s from %s", member.name, part.key
                    )
                    continue

                try:
                    put_with_retries(
                        s3,
                        bucket,
                        target_key,
                        extracted.read(),
                        CONTENT_TYPES[part.archive_type],
                        max_retries,
                    )
                    uploaded += 1
                except Exception as e:
                    failed += 1
                    logger.error(
                        "Failed to upload s3://%s/%s: %s", bucket, target_key, e
                    )
                    if failed >= max_failures:
                        logger.error(
                            "Stopping current tar part after %s upload failure(s)",
                            failed,
                        )
                        break

    return uploaded, skipped, failed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill individual S3 JSON/PDF objects from Supreme Court tar archives"
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--years",
        nargs="*",
        help="Years or ranges, e.g. 2024 2025 or 1950-2025. Default: all years with tar objects.",
    )
    parser.add_argument(
        "--archive-types",
        nargs="+",
        choices=ARCHIVE_TYPES,
        default=list(ARCHIVE_TYPES),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually upload loose objects. Without this, the script is read-only.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="HEAD each target key and skip uploads that already exist.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Stop after this many tar members have been considered.",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--max-failures",
        type=int,
        default=10,
        help="Stop after this many upload failures in a single tar part.",
    )
    parser.add_argument("--profile", help="AWS profile to use")
    parser.add_argument(
        "--unsigned",
        action="store_true",
        help="Use unsigned S3 requests for read-only dry-runs against the public bucket.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    session = (
        boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    )
    if args.execute and args.unsigned:
        parser.error("--unsigned cannot be used with --execute")
    client_config = Config(signature_version=UNSIGNED) if args.unsigned else None
    s3 = session.client("s3", config=client_config)

    years = parse_years(args.years)
    if not years:
        discovered: set[int] = set()
        for obj in iter_objects(s3, args.bucket, "metadata/tar/"):
            parts = PurePosixPath(obj["Key"]).parts
            for part in parts:
                if part.startswith("year="):
                    discovered.add(int(part.removeprefix("year=")))
                    break
        years = sorted(discovered)

    archive_parts = list_archive_parts(s3, args.bucket, years, args.archive_types)
    logger.info(
        "Found %s tar part(s) for years=%s archive_types=%s execute=%s",
        len(archive_parts),
        years,
        args.archive_types,
        args.execute,
    )

    total_uploaded = 0
    total_skipped = 0
    total_failed = 0
    considered = 0

    for part in archive_parts:
        remaining_limit = None if args.limit is None else args.limit - considered
        if remaining_limit is not None and remaining_limit <= 0:
            break

        uploaded, skipped, failed = backfill_part(
            s3,
            args.bucket,
            part,
            execute=args.execute,
            skip_existing=args.skip_existing,
            max_retries=args.max_retries,
            remaining_limit=remaining_limit,
            max_failures=args.max_failures,
        )
        total_uploaded += uploaded
        total_skipped += skipped
        total_failed += failed
        considered += uploaded + skipped + failed
        if failed >= args.max_failures:
            logger.error("Stopping backfill after repeated upload failures")
            break

    logger.info(
        "Done: uploaded=%s skipped_or_dry_run=%s failed=%s considered=%s",
        total_uploaded,
        total_skipped,
        total_failed,
        considered,
    )
    return 1 if total_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
