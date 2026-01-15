# Indian Supreme Court Judgments Scraper

This project scrapes and archives Indian Supreme Court judgments from scr.sci.gov.in.

## Project Structure

```
.
├── download.py              # Main entry point for scraping
├── archive_manager.py       # S3 archive management (TAR format)
├── package_tar_files.py     # Local TAR packaging utilities
├── process_metadata.py      # Metadata processing and parquet generation
├── sync_s3.py               # S3 sync module for incremental updates
├── sync_s3_fill.py          # Gap-filling for historical data
├── calculate_dataset_sizes.py # Calculate dataset sizes from S3
├── count_judgments.py       # Count judgments in local TAR files
├── clean-metadata.py        # Metadata cleaning utilities
├── src/
│   ├── captcha_solver/      # CAPTCHA solving module
│   │   ├── main.py          # CAPTCHA solver entry point
│   │   ├── tokenizer_base.py # Tokenizer for CAPTCHA
│   │   └── captcha.onnx     # ONNX model for CAPTCHA
│   └── utils/
│       ├── s3_utils.py      # S3 utility functions
│       └── file_utils.py    # File handling utilities
└── prune/                   # Migration and verification scripts (not for regular use)
    ├── migrate_zip_to_tar.py
    ├── migrate_to_multipart.py
    ├── verify_migration.py
    ├── verify_sizes.py
    ├── convert_metadata_indexes.py
    └── MIGRATION_GUIDE.md
```

## S3 Structure

All archives use uncompressed TAR format (`.tar`).

```
indian-supreme-court-judgments/
├── data/
│   └── tar/
│       └── year=YYYY/
│           ├── english/
│           │   ├── english.tar
│           │   └── english.index.json
│           └── regional/
│               ├── regional.tar
│               └── regional.index.json
├── metadata/
│   ├── tar/
│   │   └── year=YYYY/
│   │       ├── metadata.tar
│   │       └── metadata.index.json
│   └── parquet/
│       └── year=YYYY/
│           └── metadata.parquet
```

## Configuration

Key constants in `download.py`:

- `S3_BUCKET`: Target S3 bucket (default: `indian-supreme-court-judgments`)
- `S3_PREFIX`: S3 prefix (default: empty)
- `LOCAL_DIR`: Local directory for temporary files (default: `./local_sc_judgments_data`)
- `PACKAGES_DIR`: Local directory for packaged TAR files (default: `./packages`)

## Usage

### Prerequisites

```bash
# Activate virtual environment
source .venv/bin/activate

# For test bucket access (requires AWS credentials)
export AWS_PROFILE=dattam-supreme
```

### Scrape Data for a Date Range

```bash
# Scrape judgments for a specific date range
python download.py --start_date 2025-01-01 --end_date 2025-01-07

# Scrape with multiple workers
python download.py --start_date 2025-01-01 --end_date 2025-01-07 --max_workers 10

# Skip packaging (for faster iteration during development)
python download.py --start_date 2025-01-01 --end_date 2025-01-01 --no-package
```

### Sync with S3

```bash
# Sync and download new data since last update
python download.py --sync-s3

# Fill historical gaps (processes one 5-year chunk per run)
python download.py --sync-s3-fill
```

### Command Line Arguments

| Argument          | Description                     | Default |
| ----------------- | ------------------------------- | ------- |
| `--start_date`    | Start date (YYYY-MM-DD)         | None    |
| `--end_date`      | End date (YYYY-MM-DD)           | None    |
| `--day_step`      | Days per chunk                  | 1       |
| `--max_workers`   | Parallel workers                | 5       |
| `--no-package`    | Skip TAR packaging              | False   |
| `--sync-s3`       | Sync mode (incremental updates) | False   |
| `--sync-s3-fill`  | Gap-filling mode                | False   |
| `--timeout-hours` | Max runtime hours               | 5.5     |

## Index File Format (V2)

Each archive has an accompanying `.index.json` file:

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
      "files": ["file1.pdf", "file2.pdf", ...],
      "file_count": 578,
      "size": 85000000,
      "size_human": "81.01 MB",
      "created_at": "2025-01-14T19:25:47+05:30"
    }
  ]
}
```

## Archive Types

- **english**: English language judgments (PDFs)
- **regional**: Regional language judgments (PDFs in various Indian languages)
- **metadata**: JSON metadata files with case details

## Data Flow

1. **Scraping**: `download.py` scrapes the Supreme Court website
2. **Local Storage**: Files saved to `./sc_data/{type}/{year}/`
3. **Packaging**: `package_tar_files.py` creates TAR archives
4. **S3 Upload**: `archive_manager.py` uploads to S3 with indexes
5. **Parquet**: `process_metadata.py` converts metadata to parquet

## Key Components

### archive_manager.py

Manages TAR archives in S3 with:

- Multi-part support for large archives (>1GB splits)
- Index file tracking (V2 format with parts array)
- Immediate upload mode for crash recovery
- File existence checking against S3

### package_tar_files.py

Local TAR packaging:

- Groups files by year and type
- Creates uncompressed TAR archives
- Generates local index files

### process_metadata.py

Metadata processing:

- Extracts structured data from raw HTML
- Parses decision dates, case numbers, judges
- Generates parquet files for analytics

## Notes

- All archives use uncompressed TAR format for faster read/write
- IST timezone (UTC+5:30) used for timestamps
- CAPTCHA solving uses ONNX model (src/captcha_solver/)
- No `__init__.py` files in src/ (direct imports used)
