# Indian Supreme Court Judgments

### Summary

This repo contains the code to download judgments from [ecourts website](https://scr.sci.gov.in). Data for bulk download is available freely from [AWS](#https://registry.opendata.aws/indian-supreme-court-judgments/).
It contains judgments from 1950 to Present, along with raw metadata (in json format) and structured metadata(parquet format). Judgments are available in both English and regional Indian languages in tar format for easier download.

- The data is licensed under [Creative Commons Attribution 4.0 (CC-BY-4.0)](https://creativecommons.org/licenses/by/4.0/), which means you are free to use, share, and adapt the data as long as you provide appropriate attribution.
- AWS sponsors the storage and data transfer costs of the data.
- Join [discord server](https://discord.gg/mQhghxCRJU) if you want to collaborate on this repo or have any questions.
- **Be responsible, considerate, and think about the maintainers of the ecourts website. Avoid scraping with high concurrency.**
- Data downloaded using previous version of this repo from an API is availavble in [Kaggle](https://www.kaggle.com/datasets/vangap/indian-supreme-court-judgments/data). Code for that is available in the branch [old](https://github.com/vanga/indian-supreme-court-judgments/tree/old)

## Data

- From 1950 to Present
- ~35K judgments in english, some of which have regional language versions.
- ~52.24GB of data (see [dataset_sizes.csv](./dataset_sizes.csv) for detailed breakdown)
- Any metadata about judgment like Disposal nature, Decision date etc have also been part of the dataset.

### Structure of the data in the s3 bucket

```
s3://indian-supreme-court-judgments/
├── data/
│   └── tar/
│       └── year=YYYY/
│           ├── english/
│           │   ├── english.tar              # Main archive (or part-*.tar for large archives)
│           │   └── english.index.json       # Index with parts info
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

Each year has following data:

- English judgments (english.tar, or multiple part-\*.tar files for large archives)
- Regional language judgments (regional.tar)
- Metadata (metadata.tar and metadata.parquet)
- index.json files that contain info about the files in the tar files

#### Index File Structure (V2)

The index files use a V2 format that supports multiple archive parts. When an archive exceeds 1GB, new parts are created with timestamped names:

```json
{
  "year": 2025,
  "archive_type": "english",
  "file_count": 8131,
  "total_size": 5740309504,
  "total_size_human": "5.35 GB",
  "created_at": "2025-01-15T07:12:14.860503Z",
  "updated_at": "2025-12-27T10:30:00.000000Z",
  "parts": [
    {
      "name": "english.tar",
      "files": ["judgment1.pdf", "judgment2.pdf"],
      "file_count": 5000,
      "size": 4000000000,
      "size_human": "3.73 GB",
      "created_at": "2025-01-15T07:11:49.283600Z"
    },
    {
      "name": "part-20251227T103000.tar",
      "files": ["judgment5001.pdf", "judgment5002.pdf"],
      "file_count": 3131,
      "size": 1740309504,
      "size_human": "1.62 GB",
      "created_at": "2025-12-27T10:30:00.000000Z"
    }
  ]
}
```

Columns/fields in the metadata.parquet are

- title
- petitioner
- respondent
- description
- judge
- author_judge
- citation
- case_id
- cnr
- decision_date
- disposal_nature
- court
- available_languages
- raw_html
- path
- nc_display
- scraped_at
- year

### Working with the data in AWS

- Example command to list all available years: `aws s3 ls s3://indian-supreme-court-judgments/data/tar/ --no-sign-request`
- Example command to download English judgments for 2023: `aws s3 cp s3://indian-supreme-court-judgments/data/tar/year=2023/english/english.tar . --no-sign-request`
- Example command to view metadata index for 2023: `aws s3 cp s3://indian-supreme-court-judgments/metadata/tar/year=2023/metadata.index.json . --no-sign-request`
- Since the S3 bucket is public, files can also be downloaded using links like `https://indian-supreme-court-judgments.s3.amazonaws.com/data/tar/year=2023/english/english.tar`

See the AWS [tutorials](./opendata/tutorials/README.md) for more detailed examples of:

- Downloading and extracting judgment data
- Querying metadata using AWS Athena [here](./opendata/tutorials/ATHENA.md)

#### Local development

- install [uv](https://docs.astral.sh/uv/getting-started/installation/)
- install dependencies: `uv sync`
- `source .venv/bin/activate`
- `python3 download.py`
- VS Code extensions: `Python`, `Pylance`, `ruff`