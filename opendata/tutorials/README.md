# Indian Supreme Court Judgments 
## Overview

This repository contains judgments from the Supreme Court of India, made available as part of the AWS Open Data Sponsorship Program. This dataset aims to make Indian legal information more accessible to researchers, legal professionals, and the general public.

## Dataset Description

The Indian Supreme Court Judgments dataset contains court judgments delivered by the Supreme Court of India. The dataset covers judgments from 1950 to 2025 and is regularly updated.

### Data Structure

The dataset is organized as follows:

```
s3://indian-supreme-court-judgments/
└── data/
    ├── README.md
    ├── sc-judgments-YYYY-english.zip
    ├── sc-judgments-YYYY-english.index.json
    ├── sc-judgments-YYYY-regional.zip
    ├── sc-judgments-YYYY-regional.index.json
    ├── sc-judgments-YYYY-metadata.zip
    └── sc-judgments-YYYY-metadata.index.json
```

Where YYYY represents the year (1950-2025).

### Data Files

Each year's data consists of three main components:

1. **English Judgments** (`sc-judgments-YYYY-english.zip`):
   - Contains judgments in English language for the specified year
   - Each zip file has a corresponding index file (`sc-judgments-YYYY-english.index.json`) that provides information about the contained judgments

2. **Regional Language Judgments** (`sc-judgments-YYYY-regional.zip`):
   - Contains judgments in various regional Indian languages for the specified year
   - Each zip file has a corresponding index file (`sc-judgments-YYYY-regional.index.json`) that provides information about the contained judgments

3. **Metadata** (`sc-judgments-YYYY-metadata.zip`):
   - Contains metadata about judgments for the specified year
   - Each zip file has a corresponding index file (`sc-judgments-YYYY-metadata.index.json`) that provides information about the contained metadata

### Index Files

Each zip file is accompanied by an index JSON file that contains information about the files within the zip archive. These index files can be used to quickly determine the contents without downloading the complete zip file.

## Usage

### Accessing the Data

This dataset is available in the AWS Open Data Registry and can be accessed via:

1. **Direct S3 Access**:
   ```
   aws s3 ls s3://indian-supreme-court-judgments/ --no-sign-request
   ```

2. **Listing Available Years**:
   ```
   aws s3 ls s3://indian-supreme-court-judgments/data/ --no-sign-request
   ```

3. **Downloading Index Files** (to preview content):
   ```
   aws s3 cp s3://indian-supreme-court-judgments/data/sc-judgments-2023-english.index.json . --no-sign-request
   ```

4. **Downloading Judgment Files**:
   ```
   aws s3 cp s3://indian-supreme-court-judgments/data/sc-judgments-2023-english.zip . --no-sign-request
   ```

### Example Use Cases

This dataset can be used for:

1. **Legal Research**: Analyze trends in legal decisions and precedents set by India's highest court
2. **Natural Language Processing**: Train and test NLP models on legal text in multiple languages
3. **Legal Education**: Access historical judgments for educational purposes
4. **Socio-legal Analysis**: Study the evolution of legal reasoning and social perspectives through judgments
5. **Comparative Law Studies**: Compare Indian Supreme Court judgments with those from other jurisdictions

## License

This dataset is made available under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See the LICENSE file for details.

## Citation

If you use this dataset in your research, please cite it as follows:

```
Vanga (2025). Indian Supreme Court Judgments. [Data set]. AWS Open Data Registry. https://registry.opendata.aws/indian-supreme-court-judgments/
```

## Contact

For questions or feedback about this dataset, please contact Pradeep Vanga (pradeep@dattam.in).

## Acknowledgments

We thank the AWS Open Data Sponsorship Program for hosting this dataset and making it freely accessible to the public.