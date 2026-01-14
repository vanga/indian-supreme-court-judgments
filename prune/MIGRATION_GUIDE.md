# Migration Guide: ZIP to TAR Format

This guide explains how to migrate the test bucket from ZIP to TAR format and test all functionality.

## Prerequisites

```bash
# Navigate to project root
cd /path/to/indian-supreme-court-judgments

# Activate virtual environment
source .venv/bin/activate

# Set AWS profile for test bucket access
export AWS_PROFILE=dattam-supreme
```

## Step 1: Migrate Test Bucket (ZIP to TAR)

The migration script converts all ZIP archives to uncompressed TAR format and uploads them to the new structure.

**Note**: Run these commands from the project root directory, not from the `prune/` folder.

### Dry Run (Preview changes)
```bash
python prune/migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --dry-run
```

### Migrate a Single Year (for testing)
```bash
python prune/migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --year 2025
```

### Migrate All Years
```bash
python prune/migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test
```

**Note**: This will take ~1-2 hours for 76 years of data. Each year takes about 60-90 seconds.

### Migrate All Remaining Years (Batch)
```bash
for year in $(seq 1950 2025); do
    echo "Processing year $year..."
    python prune/migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --year $year
done
```

## Step 2: Verify Migration

After migration, verify that all TAR files and indexes are in place:

```bash
python prune/verify_migration.py --bucket indian-supreme-court-judgments-test
```

To verify a specific year:
```bash
python prune/verify_migration.py --bucket indian-supreme-court-judgments-test --year 2025
```

## Step 3: Test Local Download (Scraping)

Test scraping judgments from the website for a specific date range.

**Note**: The `download.py` script scrapes from the Supreme Court website - it does NOT download from S3. The S3 bucket is hardcoded in the script (`indian-supreme-court-judgments-test`).

```bash
# Scrape judgments for a recent date range (saves to local TAR archives)
python download.py --start_date 2025-01-01 --end_date 2025-01-07

# With more workers
python download.py --start_date 2025-01-01 --end_date 2025-01-07 --max_workers 10
```

Downloaded data is saved locally in `./local_sc_judgments_data/` directory as TAR archives.

## Step 4: Test Syncing (Updates from S3)

The sync functionality checks the latest date in S3 metadata and downloads new data from the website to fill the gap.

```bash
# Sync mode: check S3 for latest date, download new data from website
python download.py --sync-s3
```

This will:
1. Check S3 for the latest decision date in metadata
2. Download new judgments from the website (not from S3)
3. Upload the new data to S3

## Step 5: Test Gap Filling (Historical Data)

The gap-filling mode processes historical data in 5-year chunks:

```bash
# Fill gaps: processes 5-year chunks from 1950 to present
# Run repeatedly - it automatically resumes from where it left off
python download.py --sync-s3-fill

# With custom timeout (default is 5.5 hours)
python download.py --sync-s3-fill --timeout-hours 2

# Process a specific date range
python download.py --sync-s3-fill --start_date 2020-01-01 --end_date 2024-12-31
```

## S3 Structure After Migration

### Before (ZIP)
```
data/zip/year=YYYY/english.zip
data/zip/year=YYYY/regional.zip
metadata/zip/year=YYYY/metadata.zip
```

### After (TAR)
```
data/tar/year=YYYY/english/english.tar
data/tar/year=YYYY/english/english.index.json
data/tar/year=YYYY/regional/regional.tar
data/tar/year=YYYY/regional/regional.index.json
metadata/tar/year=YYYY/metadata.tar
metadata/tar/year=YYYY/metadata.index.json
```

## Index File Format (V2)

Each archive has an accompanying `.index.json` file with this structure:

```json
{
  "year": 2025,
  "archive_type": "english",
  "file_count": 578,
  "total_size": 85000000,
  "total_size_human": "81.01 MB",
  "created_at": "2025-01-14T19:25:47+05:30",
  "updated_at": "2025-01-14T19:25:47+05:30",
  "parts": [
    {
      "name": "english.tar",
      "files": ["file1.html", "file2.html", ...],
      "file_count": 578,
      "size": 85000000,
      "size_human": "81.01 MB",
      "created_at": "2025-01-14T19:25:47+05:30"
    }
  ]
}
```

## Troubleshooting

### Check Current S3 Structure
```bash
python -c "
import boto3
s3 = boto3.client('s3')
bucket = 'indian-supreme-court-judgments-test'

for prefix in ['data/tar/', 'metadata/tar/']:
    print(f'{prefix}:')
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        if 'CommonPrefixes' in page:
            for cp in page['CommonPrefixes']:
                print(f'  {cp[\"Prefix\"]}')
"
```

### List Files in a Specific Year
```bash
python -c "
import boto3
s3 = boto3.client('s3')
bucket = 'indian-supreme-court-judgments-test'
year = 2025

for prefix in [f'data/tar/year={year}/', f'metadata/tar/year={year}/']:
    print(f'{prefix}:')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        print(f'  {obj[\"Key\"]} ({obj[\"Size\"]} bytes)')
"
```

### Verify TAR File Contents
```bash
python -c "
import boto3
import tarfile
import tempfile

s3 = boto3.client('s3')
bucket = 'indian-supreme-court-judgments-test'
key = 'data/tar/year=2025/english/english.tar'

with tempfile.NamedTemporaryFile(suffix='.tar') as tmp:
    s3.download_file(bucket, key, tmp.name)
    with tarfile.open(tmp.name, 'r') as tf:
        files = tf.getnames()
        print(f'Files in archive: {len(files)}')
        print(f'First 5 files: {files[:5]}')
"
```

### Check Progress File (Gap Filling)
```bash
cat sc_fill_progress.json
```

## Clean Up Old ZIP Files (After Verification)

After verifying the migration is successful, you can optionally delete the old ZIP files:

```bash
# WARNING: Only run this after verifying migration is complete!
python -c "
import boto3
s3 = boto3.client('s3')
bucket = 'indian-supreme-court-judgments-test'

# List and delete old zip files
paginator = s3.get_paginator('list_objects_v2')
for prefix in ['data/zip/', 'metadata/zip/']:
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            print(f'Deleting: {obj[\"Key\"]}')
            # s3.delete_object(Bucket=bucket, Key=obj['Key'])  # Uncomment to actually delete
"
```

## Command Summary

| Command | Description |
|---------|-------------|
| `python download.py --start_date YYYY-MM-DD --end_date YYYY-MM-DD` | Scrape judgments from website |
| `python download.py --sync-s3` | Check S3 for latest date, download new data |
| `python download.py --sync-s3-fill` | Fill historical gaps (5-year chunks) |
| `python prune/migrate_zip_to_tar.py --bucket BUCKET` | Migrate ZIP to TAR in S3 |
| `python prune/verify_migration.py --bucket BUCKET` | Verify TAR migration |
