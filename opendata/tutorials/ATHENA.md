# AWS Athena Tutorial for Indian Supreme Court Judgments

This guide walks you through querying Indian Supreme Court judgment metadata stored in Parquet format on Amazon S3 using AWS Athena.

---

## 🏗️ Setup

### Create a Database

```sql
CREATE DATABASE supreme_court_cases;
```

### Create a Table for Parquet Files with Partitioning

```sql
CREATE EXTERNAL TABLE supreme_court_cases.judgments (
  title STRING,
  petitioner STRING,
  respondent STRING,
  description STRING,
  judge STRING,
  author_judge STRING,
  citation STRING,
  case_id STRING,
  cnr STRING,
  decision_date STRING,
  disposal_nature STRING,
  court STRING,
  available_languages STRING,
  raw_html STRING,
  path STRING,
  nc_display STRING,
  scraped_at STRING
)
PARTITIONED BY (year STRING)
STORED AS PARQUET
LOCATION 's3://indian-supreme-court-judgments/metadata/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'projection.enabled'='true',
  'projection.year.type'='integer',
  'projection.year.range'='1950,2025',
  'storage.location.template'='s3://indian-supreme-court-judgments/metadata/year=${year}/metadata.parquet'
)
```

## 🔍 Query Examples

### Level 1: Basic Selection

```sql
SELECT 
  title,
  petitioner,
  respondent,
  judge,
  decision_date,
  year
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
LIMIT 10;
```

### Level 2: Sorted Results

```sql
SELECT 
  title,
  decision_date,
  judge,
  author_judge
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
ORDER BY 
  decision_date DESC
LIMIT 10;
```

### Level 3: Pattern Matching with `LIKE`

```sql
SELECT 
  title,
  petitioner,
  respondent,
  judge,
  decision_date
FROM 
  supreme_court_cases.judgments
WHERE 
  judge LIKE '%CHANDRACHUD%'
  AND year = '2025'
LIMIT 10;
```

### Level 4: Searching by Citation

```sql
SELECT 
  title,
  petitioner,
  respondent,
  citation,
  decision_date
FROM 
  supreme_court_cases.judgments
WHERE 
  citation LIKE '%(2025)%'
LIMIT 10;
```

### Level 5: Aggregation with `GROUP BY`

```sql
SELECT 
  judge,
  COUNT(*) AS total_cases
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
GROUP BY 
  judge
ORDER BY 
  total_cases DESC
LIMIT 5;
```

### Level 6: Finding Most Active Author Judges

```sql
SELECT 
  author_judge,
  COUNT(*) AS authored_judgments
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
  AND author_judge != ''
GROUP BY 
  author_judge
ORDER BY 
  authored_judgments DESC
LIMIT 10;
```

### Level 7: Cases by Disposal Nature

```sql
SELECT 
  disposal_nature,
  COUNT(*) AS case_count
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
GROUP BY 
  disposal_nature
ORDER BY 
  case_count DESC;
```

### Level 8: Multi-language Judgments

```sql
SELECT 
  title,
  available_languages,
  decision_date
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
  AND array_size(split(available_languages, ',')) > 1
ORDER BY 
  decision_date DESC
LIMIT 10;
```

### Level 9: Subquery in `WHERE` Clause

```sql
SELECT 
  title,
  petitioner,
  respondent,
  judge,
  decision_date
FROM 
  supreme_court_cases.judgments
WHERE 
  judge IN (
    SELECT 
      judge
    FROM 
      supreme_court_cases.judgments
    WHERE 
      year = '2025'
    GROUP BY 
      judge
    HAVING 
      COUNT(*) > 10
  )
AND year = '2025'
LIMIT 20;
```

### Level 10: Window Function (RANK)

```sql
SELECT 
  title,
  judge,
  author_judge,
  decision_date,
  RANK() OVER (PARTITION BY author_judge ORDER BY decision_date DESC) AS recent_rank
FROM 
  supreme_court_cases.judgments
WHERE 
  year = '2025'
  AND author_judge != ''
LIMIT 20;
```

### Level 11: Finding Related Cases

```sql
WITH case_parties AS (
  SELECT 
    title,
    petitioner,
    respondent,
    decision_date,
    year
  FROM 
    supreme_court_cases.judgments
)

SELECT 
  a.title AS case_1,
  b.title AS case_2,
  a.decision_date,
  b.decision_date
FROM 
  case_parties a
JOIN 
  case_parties b
ON 
  a.petitioner = b.petitioner
  AND a.title != b.title
WHERE 
  a.year = '2025'
  AND b.year = '2025'
LIMIT 10;
```

### Level 12: Cross-Year Analysis

```sql
SELECT 
  year,
  COUNT(*) AS judgment_count
FROM 
  supreme_court_cases.judgments
WHERE 
  year BETWEEN '2020' AND '2025'
GROUP BY 
  year
ORDER BY 
  year;
```

---

### The year starts from 1950 to July 2025

## 📊 Data Schema

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
| `year`                | string  | Year (partition column)           |

## 📁 Data Organization

The Supreme Court judgments data is organized in the following structure on S3:

```
s3://indian-supreme-court-judgments/metadata/year=YYYY/metadata.parquet
```

Each year has its own parquet file, making it efficient to query data for specific time periods.

---