# Migration Guide: ZIP to TAR Format

This guide explains how to migrate the test bucket from ZIP to TAR format and test all functionality.

## Prerequisites

```bash
# Activate virtual environment
source .venv/bin/activate

# Set AWS profile for test bucket access
export AWS_PROFILE=dattam-supreme
```

## Step 1: Migrate Test Bucket (ZIP to TAR)

The migration script converts all ZIP archives to uncompressed TAR format and uploads them to the new structure.

### Dry Run (Preview changes)
```bash
python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --dry-run
```

### Migrate a Single Year (for testing)
```bash
python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --year 2025
```

### Migrate All Years
```bash
python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test
```

**Note**: This will take ~1-2 hours for 76 years of data. Each year takes about 60-90 seconds.

### Progress Status
The migration has completed years 1950-1954. To continue from where it left off, you can run individual years:
```bash
for year in $(seq 1955 2025); do
    python migrate_zip_to_tar.py --bucket indian-supreme-court-judgments-test --year $year
done
```

## Step 2: Verify Migration

After migration, verify that all TAR files and indexes are in place:

```bash
python verify_migration.py --bucket indian-supreme-court-judgments-test
```

To verify a specific year:
```bash
python verify_migration.py --bucket indian-supreme-court-judgments-test --year 2025
```

## Step 3: Test Local Download

Test downloading judgments for a specific date range:

```bash
# Test for a recent date range
python download.py --start_date 2025-01-01 --end_date 2025-01-07 --bucket indian-supreme-court-judgments-test

# With explicit S3 prefix
python download.py --start_date 2025-01-01 --end_date 2025-01-07 --bucket indian-supreme-court-judgments-test --s3_prefix data/tar
```

## Step 4: Test Syncing

Test the sync functionality that checks for new data and downloads it:

```bash
python sync_s3.py --bucket indian-supreme-court-judgments-test --s3_prefix data/tar --local_dir ./local_data
```

Or using the main download script with sync mode:
```bash
python download.py --sync --bucket indian-supreme-court-judgments-test
```

## Step 5: Test Filling Gaps

Test the gap-filling functionality:

```bash
python sync_s3_fill.py --bucket indian-supreme-court-judgments-test --year 2025
```

## S3 Structure After Migration

### Before (ZIP)
```
data/zip/year=YYYY/english/english.zip
data/zip/year=YYYY/regional/regional.zip
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
