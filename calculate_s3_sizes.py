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

def get_local_judgment_counts_and_files():
    """Get judgment counts and expected file names from local index files"""
    packages_dir = Path("./packages")
    year_counts = defaultdict(lambda: defaultdict(int))
    expected_files = set()
    
    if not packages_dir.exists():
        logger.warning("Local packages directory not found, judgment counts will be unavailable")
        return {}, set()
    
    # Find all index files
    index_files = list(packages_dir.glob("*.index.json"))
    
    if not index_files:
        logger.warning("No index files found in packages directory")
        return {}, set()
    
    logger.info(f"Found {len(index_files)} index files")
    
    for index_path in index_files:
        try:
            # Extract year and type from filename (e.g., sc-judgments-2023-english.index.json)
            filename_parts = index_path.stem.replace('.index', '').split("-")
            if len(filename_parts) >= 4 and filename_parts[0] == 'sc' and filename_parts[1] == 'judgments':
                year = int(filename_parts[2])
                archive_type = filename_parts[3]
                
                # Add expected zip file name
                zip_filename = f"sc-judgments-{year}-{archive_type}.zip"
                expected_files.add(zip_filename)
                
                with open(index_path, "r", encoding='utf-8') as f:
                    index_data = json.load(f)
                    count = len(index_data.get("files", []))
                    year_counts[year][archive_type] = count
                    logger.debug(f"Year {year}, type {archive_type}: {count} judgments")
                    
        except Exception as e:
            logger.warning(f"Error reading index file {index_path}: {e}")
    
    return year_counts, expected_files

def calculate_s3_sizes():
    # Initialize S3 client for anonymous access
    from botocore import UNSIGNED
    from botocore.config import Config
    
    s3_client = boto3.client('s3', 
                            region_name='us-east-1',
                            config=Config(signature_version=UNSIGNED))
    
    # Get local judgment counts and expected files
    year_counts, expected_files = get_local_judgment_counts_and_files()
    year_sizes = defaultdict(int)
    
    # Try index-based approach first
    if expected_files:
        logger.info(f"Using index-based approach with {len(expected_files)} expected files")
        successful_files = 0
        
        for zip_filename in expected_files:
            s3_key = f"{S3_PREFIX}{zip_filename}"
            
            try:
                response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                size_bytes = response['ContentLength']
                
                year = extract_year_from_filename(zip_filename)
                if year:
                    year_sizes[year] += size_bytes
                    successful_files += 1
                    logger.debug(f"Got size for {zip_filename}: {size_bytes:,} bytes")
                    
            except Exception as e:
                logger.warning(f"Could not get size for {s3_key}: {e}")
        
        logger.info(f"Successfully retrieved sizes for {successful_files}/{len(expected_files)} files")
        
        # If we got most files, use this approach
        if successful_files >= len(expected_files) * 0.8:  # 80% success rate threshold
            logger.info("Index-based approach successful")
        else:
            logger.warning("Index-based approach had low success rate, falling back to full listing")
            year_sizes.clear()
    
    # Fall back to full S3 listing if index approach failed or no index files
    if not year_sizes:
        logger.info("Using full S3 object listing...")
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)
            
            file_count = 0
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        filename = obj['Key']
                        size_bytes = obj['Size']
                        
                        # Skip index files and README, only count .zip files
                        if filename.endswith('.zip'):
                            year = extract_year_from_filename(filename)
                            if year:
                                year_sizes[year] += size_bytes
                                file_count += 1
                                logger.debug(f"Added {filename}: {size_bytes:,} bytes to year {year}")
            
            logger.info(f"Processed {file_count} zip files from S3 listing")
            
        except Exception as e:
            logger.error(f"Error during S3 listing: {e}")
            return {}, 0, {}
    
    # Calculate total judgments per year (english + regional, excluding metadata)
    year_judgment_counts = {}
    for year, types in year_counts.items():
        # Count english and regional PDFs only
        total = types.get('english', 0) + types.get('regional', 0)
        if total > 0:
            year_judgment_counts[year] = total
    
    # Convert to GB and sort by year
    year_sizes_gb = {year: convert_size_to_gb(size) for year, size in year_sizes.items()}
    all_years = sorted(set(list(year_sizes_gb.keys()) + list(year_judgment_counts.keys())))
    
    if not all_years:
        logger.error("No data found. Check S3 access and file structure.")
        return {}, 0, {}
    
    # Generate CSV output with size and judgment count
    csv_output_path = Path("dataset_sizes_by_year.csv")
    with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
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
    print(f"Total judgments: {total_judgments:,}")
    if all_years:
        print(f"Year range: {min(all_years)} - {max(all_years)}")
    if total_judgments > 0:
        print(f"Average size per judgment: {(total_size_gb * 1024 / total_judgments):.2f} MB")
    else:
        print("Judgment counts unavailable (no local index files found)")
    
    return year_sizes_gb, total_size_gb, year_judgment_counts

if __name__ == "__main__":
    calculate_s3_sizes()
