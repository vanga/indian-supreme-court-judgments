# Tutorial: Using the Indian Supreme Court Judgments Dataset on AWS

This tutorial demonstrates how to use the Indian Supreme Court Judgments dataset on AWS to perform some basic analyses and extract valuable insights from the legal data.

## Prerequisites

- Basic knowledge of AWS services
- An AWS account (for services beyond simple S3 access)
- Basic knowledge of Python (for the analysis examples)
- Familiarity with SQL (for Athena queries)

## 1. Accessing the Dataset

### Using AWS CLI

First, let's explore what's available in the dataset using the AWS CLI:

```bash
# List the top-level directories in the dataset
aws s3 ls s3://indian-supreme-court-judgments/ --no-sign-request

# List all files in the data directory
aws s3 ls s3://indian-supreme-court-judgments/data/ --no-sign-request

# List files for a specific year (example: 2023)
aws s3 ls s3://indian-supreme-court-judgments/data/ --no-sign-request | grep 2023
```

### Downloading and Examining Index Files

Index files provide information about the contents of each zip file without having to download the entire archive:

```bash
# Download an index file for English judgments from 2023
aws s3 cp s3://indian-supreme-court-judgments/data/zip/year=2023/english/english.index.json . --no-sign-request

# Examine the contents using jq (if installed)
jq . english.index.json

# Or using Python
python -m json.tool english.index.json
```

### Using Python with boto3

You can also access the data programmatically using Python:

```python
import boto3
import json
import zipfile
import io
from botocore import UNSIGNED
from botocore.client import Config

# Create an S3 client without requiring AWS credentials
s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# List all files in the data directory
response = s3_client.list_objects_v2(
    Bucket='indian-supreme-court-judgments',
    Prefix='data/'
)

# Print the first 5 keys
for i, obj in enumerate(response['Contents']):
    if i >= 5:
        break
    print(obj['Key'])

# Download and read an index file
obj = s3_client.get_object(
    Bucket='indian-supreme-court-judgments',
    Key='data/zip/year=2023/english/english.index.json'
)
index_content = json.loads(obj['Body'].read().decode('utf-8'))
print(f"Index content structure: {index_content}")
print(f"Number of files in archive: {index_content.get('file_count', 'Unknown')}")
if 'files' in index_content:
    print(f"First 5 files: {index_content['files'][:5]}")

# Download a zip file and extract its contents
obj = s3_client.get_object(
    Bucket='indian-supreme-court-judgments',
    Key='data/zip/year=2023/english/english.zip'
)
zip_content = obj['Body'].read()

# Create a zipfile object from the binary content
z = zipfile.ZipFile(io.BytesIO(zip_content))
print(f"Files in zip: {len(z.namelist())}")
print(f"First 5 files: {z.namelist()[:5]}")

# Extract and read a specific file (adjust the filename as needed)
sample_file = z.namelist()[0]
with z.open(sample_file) as f:
    content = f.read().decode('utf-8')
    print(f"Sample content from {sample_file} (first 500 chars):")
    print(content[:500])
```

## 2. Analyzing Judgments by Year using Amazon Athena

AWS Athena allows you to run SQL queries against the dataset. Here's how to set up tables and run useful analytical queries:

### Setting up Tables in Athena

```sql
-- Create database
CREATE DATABASE supreme_court_judgments;

-- Create external table for Supreme Court judgments
CREATE EXTERNAL TABLE supreme_court_judgments.judgments (
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

### Example Queries

```sql
-- Count judgments by year
SELECT
  year,
  COUNT(*) as judgment_count
FROM
  supreme_court_judgments.judgments
GROUP BY
  year
ORDER BY
  year DESC;

-- Find the most active judges in 2024
SELECT
  judge,
  COUNT(*) as judgment_count
FROM
  supreme_court_judgments.judgments
WHERE
  year = '2024'
GROUP BY
  judge
ORDER BY
  judgment_count DESC
LIMIT 10;

-- Analyze disposal nature trends over time
SELECT
  year,
  disposal_nature,
  COUNT(*) as count
FROM
  supreme_court_judgments.judgments
WHERE
  year BETWEEN '2020' AND '2025'
  AND disposal_nature IS NOT NULL
GROUP BY
  year, disposal_nature
ORDER BY
  year DESC, count DESC;

-- Find cases with specific petitioners
SELECT
  title,
  petitioner,
  respondent,
  decision_date,
  citation
FROM
  supreme_court_judgments.judgments
WHERE
  petitioner LIKE '%Union of India%'
  AND year = '2025'
LIMIT 20;

-- Analyze case distribution by month in 2025
SELECT
  SUBSTR(decision_date, 1, 7) as month,
  COUNT(*) as case_count
FROM
  supreme_court_judgments.judgments
WHERE
  year = '2025'
  AND decision_date IS NOT NULL
GROUP BY
  SUBSTR(decision_date, 1, 7)
ORDER BY
  month;
```

## Conclusion

This tutorial has shown you how to:

1. Access and explore the Indian Supreme Court Judgments dataset
2. Analyze judgment metadata using AWS Athena

These examples demonstrate the fundamental ways to work with this legal corpus, but there are many more advanced analyses possible, including:

- Constitutional law trend analysis
- Citation network analysis
- Judgment sentiment and complexity scoring
- Comparative studies between Indian Supreme Court and High Court judgments

For more information and resources, visit the dataset's page in the Registry of Open Data on AWS.
