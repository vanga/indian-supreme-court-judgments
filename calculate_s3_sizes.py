import boto3
import re
import csv
from collections import defaultdict
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 configuration
BUCKET_NAME = "indian-supreme-court-judgments"
S3_PREFIX = "data/"

def convert_size_to_gb(size_bytes):
    return size_bytes / (1024 * 1024 * 1024)

def extract_year_from_filename(filename):
    match = re.search(r'sc-judgments-(\d{4})-', filename)
    if match:
        return int(match.group(1))
    return None

def calculate_s3_sizes():
    
    # Initialize S3 client for anonymous access (equivalent to --no-sign-request)
    from botocore import UNSIGNED
    from botocore.config import Config
    
    s3_client = boto3.client('s3', 
                            region_name='us-east-1',
                            config=Config(signature_version=UNSIGNED))
    
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=S3_PREFIX
    )
    
    year_sizes = defaultdict(int)  # Store size in bytes
    total_size = 0
    
    logger.info("Processing S3 objects...")
    
    # Process all objects
    while True:
        if 'Contents' in response:
            for obj in response['Contents']:
                filename = obj['Key']
                size_bytes = obj['Size']
                
                # Skip index files and README, only count .zip files
                if filename.endswith('.zip'):
                    year = extract_year_from_filename(filename)
                    if year:
                        year_sizes[year] += size_bytes
                        total_size += size_bytes
                        logger.debug(f"Added {filename}: {size_bytes} bytes to year {year}")
        
        # Check if there are more objects to retrieve
        if response.get('IsTruncated', False):
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=S3_PREFIX,
                ContinuationToken=response['NextContinuationToken']
            )
        else:
            break
    
    # Convert to GB and sort by year
    year_sizes_gb = {year: convert_size_to_gb(size) for year, size in year_sizes.items()}
    sorted_years = sorted(year_sizes_gb.keys())
    
    # Generate CSV output
    csv_output_path = Path("dataset_sizes_by_year.csv")
    with open(csv_output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Year', 'Size_GB'])
        for year in sorted_years:
            writer.writerow([year, f"{year_sizes_gb[year]:.3f}"])
    
    logger.info(f"CSV output written to {csv_output_path}")
    
    # Generate markdown table
    markdown_table = "| Year | Size (GB) |\n|------|-----------|\n"
    for year in sorted_years:
        markdown_table += f"| {year} | {year_sizes_gb[year]:.3f} |\n"
    
    total_size_gb = convert_size_to_gb(total_size)
    
    # Write markdown table to file
    markdown_output_path = Path("dataset_sizes_by_year.md")
    with open(markdown_output_path, 'w') as f:
        f.write("## Dataset Size by Year\n\n")
        f.write(markdown_table)
        f.write(f"\n**Total Size Across All Years: {total_size_gb:.3f} GB**\n")
    
    logger.info(f"Markdown table written to {markdown_output_path}")
    print(f"Total files processed: {len(year_sizes)} years")
    print(f"Total dataset size: {total_size_gb:.3f} GB")
    print(f"Year range: {min(sorted_years)} - {max(sorted_years)}")
    return year_sizes_gb, total_size_gb

if __name__ == "__main__":
    calculate_s3_sizes()
