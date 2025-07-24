import boto3
import re
import csv
import json
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

def get_local_judgment_counts():
    """Get judgment counts from local index files"""
    packages_dir = Path("./packages")
    year_counts = defaultdict(lambda: defaultdict(int))
    
    if not packages_dir.exists():
        logger.warning("Local packages directory not found, judgment counts will be unavailable")
        return {}
    
    # Find all index files
    index_files = list(packages_dir.glob("*.index.json"))
    
    for index_path in index_files:
        try:
            # Extract year and type from filename (e.g., sc-judgments-2023-english.index.json)
            parts = index_path.stem.split("-")
            if len(parts) >= 4:
                year = int(parts[2])
                archive_type = parts[3]
                
                with open(index_path, "r") as f:
                    index_data = json.load(f)
                    count = len(index_data.get("files", []))
                    year_counts[year][archive_type] = count
                    
        except Exception as e:
            logger.warning(f"Error reading index file {index_path}: {e}")
    
    # Calculate total judgments per year (english + regional, excluding metadata)
    year_totals = {}
    for year, types in year_counts.items():
        # Count english and regional PDFs only
        total = types.get('english', 0) + types.get('regional', 0)
        if total > 0:
            year_totals[year] = total
    
    return year_totals

def calculate_s3_sizes():
    # Initialize S3 client for anonymous access
    from botocore import UNSIGNED
    from botocore.config import Config
    
    s3_client = boto3.client('s3', 
                            region_name='us-east-1',
                            config=Config(signature_version=UNSIGNED))
    
    # First, try to get sizes from a head request on known zip files
    # If we have local index files, we can predict the zip file names
    packages_dir = Path("./packages")
    year_sizes = defaultdict(int)
    
    if packages_dir.exists():
        # Try to get sizes for known files based on local index files
        index_files = list(packages_dir.glob("*.index.json"))
        known_years = set()
        
        for index_path in index_files:
            parts = index_path.stem.split("-")
            if len(parts) >= 4:
                year = int(parts[2])
                archive_type = parts[3]
                known_years.add(year)
                
                # Try to get size of corresponding zip file on S3
                zip_filename = f"sc-judgments-{year}-{archive_type}.zip"
                s3_key = f"{S3_PREFIX}{zip_filename}"
                
                try:
                    response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                    size_bytes = response['ContentLength']
                    year_sizes[year] += size_bytes
                    logger.debug(f"Got size for {zip_filename}: {size_bytes} bytes")
                except Exception as e:
                    logger.debug(f"Could not get size for {s3_key}: {e}")
        
        if known_years:
            logger.info(f"Retrieved sizes for {len(known_years)} years using index-based approach")
        else:
            logger.info("No local index files found, falling back to full S3 listing")
    
    # If we don't have complete data, fall back to listing objects
    if not year_sizes:
        logger.info("Using full S3 object listing...")
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)
        
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
    
    # Get local judgment counts
    year_judgment_counts = get_local_judgment_counts()
    
    # Convert to GB and sort by year
    year_sizes_gb = {year: convert_size_to_gb(size) for year, size in year_sizes.items()}
    all_years = sorted(set(list(year_sizes_gb.keys()) + list(year_judgment_counts.keys())))
    
    # Generate CSV output with both size and judgment count
    csv_output_path = Path("dataset_sizes_by_year.csv")
    with open(csv_output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Year', 'Size_GB', 'Judgment_Count'])
        
        total_size_gb = 0
        total_judgments = 0
        
        for year in all_years:
            size_gb = year_sizes_gb.get(year, 0)
            judgment_count = year_judgment_counts.get(year, 0)
            
            writer.writerow([year, f"{size_gb:.3f}", judgment_count])
            total_size_gb += size_gb
            total_judgments += judgment_count
        
        # Add total row
        writer.writerow(['TOTAL', f"{total_size_gb:.3f}", total_judgments])
    
    logger.info(f"CSV output written to {csv_output_path}")
    print(f"Total years processed: {len(all_years)}")
    print(f"Total dataset size: {total_size_gb:.3f} GB")
    print(f"Total judgments: {total_judgments}")
    print(f"Year range: {min(all_years)} - {max(all_years)}")
    
    return year_sizes_gb, total_size_gb, year_judgment_counts

if __name__ == "__main__":
    calculate_s3_sizes()
