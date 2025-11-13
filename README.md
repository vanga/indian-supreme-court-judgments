# Indian Supreme Court Judgments

### Summary
This repo contains the code to download judgments from [ecourts website](https://scr.sci.gov.in). Data for bulk download is available freely from [AWS](#https://registry.opendata.aws/indian-supreme-court-judgments/). 
It contains judgments from 1950 to Present, along with raw metadata (in json format) and structured metadata(parquet format). Judgments are available in both English and regional Indian languages in zip format for easier download.


* The data is licensed under [Creative Commons Attribution 4.0 (CC-BY-4.0)](https://creativecommons.org/licenses/by/4.0/), which means you are free to use, share, and adapt the data as long as you provide appropriate attribution.
* AWS sponsors the storage and data transfer costs of the data.
* Join [discord server](https://discord.gg/mQhghxCRJU) if you want to collaborate on this repo or have any questions.
* **Be responsible, considerate, and think about the maintainers of the ecourts website. Avoid scraping with high concurrency.**
* Data downloaded using previous version of this repo from an API is availavble in [Kaggle](https://www.kaggle.com/datasets/vangap/indian-supreme-court-judgments/data). Code for that is available in the branch [old](https://github.com/vanga/indian-supreme-court-judgments/tree/old)

## Data
* From 1950 to Present
* ~35K judgments in english, some of which have regional language versions.
* ~52.24GB of data (see [dataset_sizes.csv](./dataset_sizes.csv) for detailed breakdown)
* Any metadata about judgment like Disposal nature, Decision date etc have also been part of the dataset.

### Structure of the data in the s3 bucket
```
s3://indian-supreme-court-judgments/
├── data/
│   └── zip/
│       └── year=YYYY/
│           ├── english.zip
│           ├── english.index.json
│           ├── regional.zip
│           └── regional.index.json
└── metadata/
    ├── zip/
    │   └── year=YYYY/
    │       ├── metadata.zip
    │       └── metadata.index.json
    └── parquet/
        └── year=YYYY/
            └── metadata.parquet
```

Where YYYY represents the year (1950-2025).

Each year has following data:
* English judgments (english.zip)
* Regional language judgments (regional.zip)
* Metadata (metadata.zip and metadata.parquet)
* index.json files that contain info about the files in the zip files

Columns/fields in the metadata.parquet are
* title
* petitioner
* respondent
* description
* judge
* author_judge
* citation
* case_id
* cnr
* decision_date
* disposal_nature
* court
* available_languages
* raw_html
* path
* nc_display
* scraped_at
* year

### Working with the data in AWS
* Example command to list all available years: `aws s3 ls s3://indian-supreme-court-judgments/data/zip --no-sign-request`
* Example command to download English judgments for 2023: `aws s3 cp s3://indian-supreme-court-judgments/data/zip/year=2023/english.zip . --no-sign-request`
* Example command to view metadata index for 2023: `aws s3 cp s3://indian-supreme-court-judgments/data/zip/year=2023/metadata.index.json . --no-sign-request`
* Since the S3 bucket is public, files can also be downloaded using links like `https://indian-supreme-court-judgments.s3.amazonaws.com/data/zip/year=2023/english.zip`


See the AWS [tutorials](/opendata/tutorials/README.md) for more detailed examples of:
* Downloading and extracting judgment data
* Querying metadata using AWS Athena [here](/opendata/tutorials/ATHENA.md)

#### Local development

- install [uv](https://docs.astral.sh/uv/getting-started/installation/)
- install dependencies: `uv sync`
- `source .venv/bin/activate`
- `python3 download.py`
- VS Code extensions: `Python`, `Pylance`, `ruff`
