PDF judgements and the metadata can be downloaded from [Kaggle](https://www.kaggle.com/datasets/vangap/indian-supreme-court-judgments/data) which gets updated weekly.

## Data Schema in parquet

The dataset is stored in Parquet format with the following columns:

| Column                | Type    | Description                       |
| --------------------- | ------- | --------------------------------- |
| `title`               | string  | Case title                        |
| `petitioner`          | string  | Petitioner name(s)                |
| `respondent`          | string  | Respondent name(s)                |
| `description`         | string  | Case description                  |
| `judge`               | string  | Judge(s) presiding                |
| `author_judge`        | string  | Authoring judge                   |
| `citation`            | string  | Legal citation                    |
| `case_id`             | string  | Unique case identifier            |
| `cnr`                 | string  | Case Number Register              |
| `decision_date`       | string  | Date of judgment                  |
| `disposal_nature`     | string  | Nature of disposal                |
| `court`               | string  | Court name                        |
| `available_languages` | string  | Available languages (CSV format)  |
| `raw_html`            | string  | Raw HTML                          |
| `path`                | string  | File path to PDF                  |
| `nc_display`          | string  | Display name                      |
| `scraped_at`          | string  | Timestamp when scraped            |
| `year`                | string  | Year                              |

#### Local development

- install [uv](https://docs.astral.sh/uv/getting-started/installation/)
- install dependencies: `uv sync`
- `source .venv/bin/activate`
- `python3 download.py`
- VS Code extensions: `Python`, `Pylance`, `ruff`
