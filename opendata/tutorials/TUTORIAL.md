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
aws s3 cp s3://indian-supreme-court-judgments/data/sc-judgments-2023-english.index.json . --no-sign-request

# Examine the contents using jq (if installed)
jq . sc-judgments-2023-english.index.json

# Or using Python
python -m json.tool sc-judgments-2023-english.index.json
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
    Key='data/sc-judgments-2023-english.index.json'
)
index_content = json.loads(obj['Body'].read().decode('utf-8'))
print(f"Index content structure: {index_content}")
print(f"Number of files in archive: {index_content.get('file_count', 'Unknown')}")
if 'files' in index_content:
    print(f"First 5 files: {index_content['files'][:5]}")

# Download a zip file and extract its contents
obj = s3_client.get_object(
    Bucket='indian-supreme-court-judgments', 
    Key='data/sc-judgments-2023-english.zip'
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
<!-- TODO: REVIEW NEEDED -->
<!-- ## 2. Analyzing Judgments by Year using Amazon Athena

AWS Athena allows you to run SQL queries against the dataset. First, we'll need to set up tables to analyze the metadata:

### Setting up Tables in Athena

```sql
-- Create database
CREATE DATABASE supreme_court_judgments;

-- Create external table for metadata indexes
CREATE EXTERNAL TABLE supreme_court_judgments.metadata_indexes (
  year STRING,
  files ARRAY<
    STRUCT<
      filename: STRING,
      case_number: STRING,
      judgment_date: STRING,
      bench: ARRAY<STRING>,
      petitioner: STRING,
      respondent: STRING
    >
  >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://indian-supreme-court-judgments/data/'
TBLPROPERTIES ('has_encrypted_data'='false');
```

### Example Queries

```sql
-- Count judgments by year
WITH flattened AS (
  SELECT 
    SUBSTR(key, 21, 4) as year,
    json_array_length(json_extract(index_content, '$')) as judgment_count
  FROM 
    s3_objects
  WHERE 
    bucket = 'indian-supreme-court-judgments' AND
    key LIKE '%metadata.index.json'
)
SELECT 
  year,
  judgment_count
FROM 
  flattened
ORDER BY 
  year DESC;

-- Find the most common case types (requires parsing the case numbers)
-- This is a simplified example as actual implementation would depend on the data format
WITH case_numbers AS (
  SELECT 
    SUBSTR(file.case_number, 1, POSITION(' ' IN file.case_number)) as case_type
  FROM 
    metadata_indexes
    CROSS JOIN UNNEST(files) as t(file)
)
SELECT 
  case_type, 
  COUNT(*) as count
FROM 
  case_numbers
GROUP BY 
  case_type
ORDER BY 
  count DESC
LIMIT 10;
``` -->

## 3. Creating a Simple Dashboard with QuickSight

You can visualize the judgment data using Amazon QuickSight:

1. Create an Athena data source in QuickSight
2. Connect to the tables you created
3. Create visualizations such as:
   - Trend line of judgments per year
   - Distribution of judgments by language
   - Top judges by number of judgments



## Conclusion

This tutorial has shown you how to:

1. Access and explore the Indian Supreme Court Judgments dataset
2. Analyze judgment metadata using AWS Athena
3. Create visualizations using Amazon QuickSight
4. Perform text analysis using Amazon Comprehend
5. Build a simple search application using Elasticsearch

These examples demonstrate the fundamental ways to work with this legal corpus, but there are many more advanced analyses possible, including:

- Constitutional law trend analysis
- Citation network analysis
- Judgment sentiment and complexity scoring
- Comparative studies between Indian Supreme Court and High Court judgments

For more information and resources, visit the dataset's page in the Registry of Open Data on AWS.