(OUTDATED: Please refer to the [main README.md](../../README.md) for the most up-to-date information)

# Indian Supreme Court Judgments

### Summary

This dataset contains judgements from the Indian Supreme Court, downloaded from [ecourts website](https://scr.sci.gov.in). It contains judgments from 1950 to 2025, along with raw metadata (in json format) and structured metadata. Judgments from the website are further compressed to optimize for size (care has been taken to not have any loss of data either in content or in visual appearance). Judgments are available in both English and regional Indian languages in tar format for easier download.

## Data

- Comprehensive coverage from 1950 to 2025
- ~35K judgments
- ~52.24GB of data (see [dataset_sizes.csv](../../dataset_sizes.csv) for detailed breakdown)
- Both English and regional language versions
- Detailed metadata for each judgment
- Code used to download and process the data is [here](https://github.com/vanga/indian-supreme-court-judgments)

#### Update cadence

- Twice every month

### Structure of the data in the bucket

```
s3://indian-supreme-court-judgments/
├── data/
│   └── tar/
│       └── year=YYYY/
│           ├── english/
│           │   ├── english.tar
│           │   └── english.index.json
│           └── regional/
│               ├── regional.tar
│               └── regional.index.json
└── metadata/
    ├── tar/
    │   └── year=YYYY/
    │       ├── metadata.tar
    │       └── metadata.index.json
    └── parquet/
        └── year=YYYY/
            └── metadata.parquet
```

Where YYYY represents the year (1950-2025).

Each year has three main components:

- English judgments (TAR file and index JSON)
- Regional language judgments (TAR file and index JSON)
- Metadata (TAR file and index JSON)

### Example usage

- Example command to list all available years: `aws s3 ls s3://indian-supreme-court-judgments/data/tar --no-sign-request`
- Example command to download English judgments for 2023: `aws s3 cp s3://indian-supreme-court-judgments/data/tar/year=2023/english/english.tar . --no-sign-request`
- Example command to view metadata index for 2023: `aws s3 cp s3://indian-supreme-court-judgments/metadata/tar/year=2023/metadata.index.json . --no-sign-request`
- Since the S3 bucket is public, files can also be downloaded using links like `https://indian-supreme-court-judgments.s3.amazonaws.com/data/tar/year=2023/english/english.tar`

### Working with the data

- Index files (JSON) provide information about the contents of each TAR file without downloading the entire archive
- English and regional language files contain the full text of judgments
- Metadata files contain structured information about each judgment, including case numbers, judgment dates, bench information, petitioners, and respondents

See the [tutorial](../tutorials/README.md) for more detailed examples of:

- Downloading and extracting judgment data
- Querying metadata using AWS Athena [here](../tutorials/ATHENA.md)
