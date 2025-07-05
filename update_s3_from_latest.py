import os
import re
import zipfile
import json
from datetime import datetime, timedelta
import subprocess
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime, timezone
from tqdm import tqdm
import concurrent.futures

S3_BUCKET = "indian-supreme-court-judgments-test"
S3_PREFIX = "data/"
LOCAL_DIR = "./local_sc_judgments_data"
PACKAGES_DIR = "./packages"
DOWNLOAD_SCRIPT = "./download.py"

def sync_latest_metadata_zip(force_refresh=True):
    """
    Download the current year's metadata zip file from S3, or latest available.
    If force_refresh is True, always download a fresh copy.
    """
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # First try to get current year's metadata
    current_year = datetime.now().year
    current_year_key = f"{S3_PREFIX}sc-judgments-{current_year}-metadata.zip"
    
    # Check if current year metadata exists
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=current_year_key)
        latest_zip_key = current_year_key
        print(f"Found current year ({current_year}) metadata")
    except:
        # Fall back to finding the latest available year
        print(f"Current year metadata not found, finding latest available...")
        zips = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if re.match(rf"{S3_PREFIX}sc-judgments-\d{{4}}-metadata\.zip", key):
                    zips.append(key)
        if not zips:
            raise Exception("No metadata zip files found")
        # Sort by year in filename descending
        zips.sort(key=lambda k: int(re.search(r"(\d{4})", k).group(1)), reverse=True)
        latest_zip_key = zips[0]
    
    local_path = os.path.join(LOCAL_DIR, os.path.basename(latest_zip_key))
    
    # Force a fresh download if requested
    if force_refresh and os.path.exists(local_path):
        print(f"Removing cached metadata zip to force refresh...")
        os.remove(local_path)
        
    if not os.path.exists(local_path):
        print(f"Downloading {latest_zip_key} ...")
        s3.download_file(S3_BUCKET, latest_zip_key, local_path)
    else:
        print(f"Using cached metadata zip: {local_path}")
        
    return local_path

def extract_decision_date_from_json(json_obj):
    raw_html = json_obj.get("raw_html", "")
    # Try to find DD-MM-YYYY after 'Decision Date'
    m = re.search(r"Decision Date\s*:\s*<font[^>]*>\s*(\d{2}-\d{2}-\d{4})\s*</font>", raw_html)
    if not m:
        # Fallback: try to find any date pattern
        m = re.search(r"(\d{2}-\d{2}-\d{4})", raw_html)
        # print(m.group(1))
    if m:
        try:
            # print(datetime.strptime(m.group(1), "%d-%m-%Y"))
            return datetime.strptime(m.group(1), "%d-%m-%Y")
        except Exception:
            pass
    return None

def find_latest_decision_date_in_zip(zip_path):
    latest_date = None
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if not name.endswith('.json'):
                continue
            with z.open(name) as f:
                try:
                    data = json.load(f)
                    decision_date = extract_decision_date_from_json(data)
                    if decision_date and (latest_date is None or decision_date > latest_date):
                        latest_date = decision_date
                except Exception:
                    continue
    if latest_date:
        print(f"[INFO] Latest decision date in metadata zip: {latest_date.date()}")
    else:
        print("[WARN] No decision date found in metadata zip, falling back to ZIP entry date.")
        # fallback (not recommended)
        with zipfile.ZipFile(zip_path, "r") as z:
            latest_date = max(datetime(*zi.date_time[:3]) for zi in z.infolist())
    return latest_date

def run_downloader(start_date, end_date):
    print(f"Fetching new data from {start_date} to {end_date} ...")
    subprocess.run([
        "python", DOWNLOAD_SCRIPT,
        "--start_date", (start_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        "--end_date", end_date.strftime("%Y-%m-%d")
    ], check=True)

def upload_new_zips_to_s3():
    """
    Upload new packages to S3 by appending to existing zip files and updating index.json files.
    Specifically handles the three types: regional, English, and metadata packages.
    """
    import shutil
    import threading
    import tempfile
    from concurrent.futures import ThreadPoolExecutor
    
    # Define progress tracking class once
    class ProgressPercentage:
        def __init__(self, filename, desc=None):
            self._filename = os.path.basename(filename)
            self._size = os.path.getsize(filename)
            self._seen_so_far = 0
            self._lock = threading.Lock()
            self._pbar = tqdm(
                total=self._size, 
                unit='B', 
                unit_scale=True, 
                desc=desc or f"Uploading {self._filename}"
            )

        def __call__(self, bytes_amount):
            with self._lock:
                self._seen_so_far += bytes_amount
                self._pbar.update(bytes_amount)
                if self._seen_so_far >= self._size:
                    self._pbar.close()
    
    # Merge zip helper function
    def merge_zip_files(s3_path, local_path, output_path, new_files=None):
        """Efficiently merge zip files, prioritizing newer versions of files"""
        with zipfile.ZipFile(s3_path, 'r') as s3_zip:
            with zipfile.ZipFile(local_path, 'r') as local_zip:
                with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as merged_zip:
                    # Get file info from both zips
                    s3_files_info = {info.filename: info.date_time for info in s3_zip.infolist()}
                    local_files_info = {info.filename: info.date_time for info in local_zip.infolist()}
                    
                    # Copy files from S3 zip (skipping if local has newer version)
                    for item in tqdm(s3_zip.namelist(), desc=f"Merging files from S3", unit="file"):
                        if item in local_files_info and local_files_info[item] > s3_files_info[item]:
                            continue
                        merged_zip.writestr(item, s3_zip.read(item))
                    
                    # Copy new/updated files from local zip
                    files_to_add = new_files if new_files else local_zip.namelist()
                    for item in tqdm(files_to_add, desc=f"Adding new/updated files", unit="file"):
                        try:
                            if item not in s3_files_info or local_files_info[item] >= s3_files_info[item]:
                                merged_zip.writestr(item, local_zip.read(item))
                        except KeyError as e:
                            print(f"Warning: {item} not found in local zip but listed in index")
    
    # Process a single package
    def process_package(local_zip_name, package_type):
        try:
            if not local_zip_name.endswith(".zip"):
                return package_type, False
                
            local_index = local_zip_name.replace(".zip", ".index.json")
            local_zip_path = os.path.join(PACKAGES_DIR, local_zip_name)
            local_index_path = os.path.join(PACKAGES_DIR, local_index)
            
            # Check if both files exist locally
            if not os.path.exists(local_index_path):
                print(f"Skipping {local_zip_name}: Missing index file")
                return package_type, False
            
            # Load local index
            with open(local_index_path, 'r') as f:
                local_index_data = json.load(f)
            
            # Determine S3 keys
            s3_zip_key = f"{S3_PREFIX}{local_zip_name}"
            s3_index_key = f"{S3_PREFIX}{local_index}"
            
            # Temp paths for this package
            temp_subdir = os.path.join(temp_dir, package_type)
            os.makedirs(temp_subdir, exist_ok=True)
            s3_index_path = os.path.join(temp_subdir, local_index)
            s3_zip_path = os.path.join(temp_subdir, local_zip_name)
            merged_zip_path = os.path.join(temp_subdir, f"merged_{local_zip_name}")
            
            try:
                # Try to download existing index file
                print(f"Downloading index file {s3_index_key}...")
                s3.download_file(S3_BUCKET, s3_index_key, s3_index_path)
                
                with open(s3_index_path, 'r') as f:
                    s3_index_data = json.load(f)
                
                # Merge file lists
                s3_files = set(s3_index_data.get("files", []))
                local_files = set(local_index_data.get("files", []))
                all_files = sorted(list(s3_files | local_files))
                new_files = list(local_files - s3_files)
                
                # Update index with new data
                merged_index = s3_index_data.copy()
                merged_index["file_count"] = len(all_files)
                merged_index["files"] = all_files
                merged_index["created_at"] = s3_index_data.get("created_at", datetime.now().isoformat())
                merged_index["updated_at"] = datetime.now().isoformat()
                
                # Save merged index
                with open(s3_index_path, 'w') as f:
                    json.dump(merged_index, f, indent=2)
                
                if not new_files:
                    print(f"No new files to add to {local_zip_name}")
                    print(f"Uploading updated index with new timestamp to {s3_index_key}")
                    
                    # Upload updated index file even if there are no new zip contents
                    s3.upload_file(
                        s3_index_path, 
                        S3_BUCKET, 
                        s3_index_key,
                        Callback=ProgressPercentage(s3_index_path)
                    )
                    return package_type, True
                
                # Download existing zip
                print(f"Downloading existing zip {s3_zip_key}...")
                s3.download_file(S3_BUCKET, s3_zip_key, s3_zip_path)
                
                # Merge zip files
                merge_zip_files(s3_zip_path, local_zip_path, merged_zip_path, new_files)
                
                # Upload merged files
                print(f"Uploading merged {local_zip_name} with {len(new_files)} new files to {s3_zip_key}")
                s3.upload_file(
                    merged_zip_path, 
                    S3_BUCKET, 
                    s3_zip_key,
                    Callback=ProgressPercentage(merged_zip_path)
                )
                
                print(f"Uploading updated {local_index} to {s3_index_key}")
                s3.upload_file(
                    s3_index_path, 
                    S3_BUCKET, 
                    s3_index_key,
                    Callback=ProgressPercentage(s3_index_path)
                )
                return package_type, True
                
            except s3.exceptions.ClientError as e:
                if 'HeadObject' in str(e) or '404' in str(e):
                    # File doesn't exist in S3, upload directly
                    print(f"Uploading new {local_zip_name} to {s3_zip_key}")
                    
                    # Upload with progress bar
                    s3.upload_file(
                        local_zip_path, 
                        S3_BUCKET, 
                        s3_zip_key,
                        Callback=ProgressPercentage(local_zip_path)
                    )
                    
                    print(f"Uploading new {local_index} to {s3_index_key}")
                    s3.upload_file(
                        local_index_path, 
                        S3_BUCKET, 
                        s3_index_key,
                        Callback=ProgressPercentage(local_index_path)
                    )
                    return package_type, True
                else:
                    # Some other error occurred
                    print(f"Error processing {local_zip_name}: {str(e)}")
                    return package_type, False
        except Exception as e:
            print(f"Error processing package {local_zip_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            return package_type, False
    
    # MAIN FUNCTION BODY STARTS HERE
    temp_dir = "temp_merge"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Track which types we've processed
    processed_types = {
        "regional": False,
        "english": False,
        "metadata": False
    }
    
    # Check if packages directory exists and has files
    if not os.path.exists(PACKAGES_DIR):
        print(f"ERROR: Packages directory '{PACKAGES_DIR}' not found")
        return
    
    package_files = [f for f in os.listdir(PACKAGES_DIR) if f.endswith('.zip')]
    if not package_files:
        print(f"WARNING: No zip files found in '{PACKAGES_DIR}'")
        return
    
    try:
        # First, check if AWS credentials are configured
        try:
            # Try to get credentials - this will raise an exception if none are available
            session = boto3.Session(profile_name="dattam-supreme")
            credentials = session.get_credentials()
            if credentials is None:
                print("ERROR: No AWS credentials found.")
                return
            
            # Create S3 client with credentials from profile
            s3 = session.client('s3')
            print("Successfully authenticated with AWS")
        except Exception as e:
            print(f"Failed to initialize S3 client: {e}")
            return
            
        # Prepare package tasks
        package_tasks = []
        for local_zip in os.listdir(PACKAGES_DIR):
            if not local_zip.endswith(".zip"):
                continue
                
            # Determine package type
            if "regional" in local_zip:
                package_type = "regional"
            elif "english" in local_zip:
                package_type = "english"
            elif "metadata" in local_zip:
                package_type = "metadata"
            else:
                print(f"Warning: Unknown package type for {local_zip}, processing anyway")
                package_type = "unknown"
                
            package_tasks.append((local_zip, package_type))
        
        # Process packages with progress tracking
        with tqdm(total=len(package_tasks), desc="Processing packages") as pbar:
            # For smaller datasets, process sequentially
            if len(package_tasks) <= 3:
                for local_zip, package_type in package_tasks:
                    pkg_type, success = process_package(local_zip, package_type)
                    processed_types[pkg_type] = success
                    pbar.update(1)
            else:
                # For larger datasets, process in parallel
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [
                        executor.submit(process_package, local_zip, package_type)
                        for local_zip, package_type in package_tasks
                    ]
                    
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            pkg_type, success = future.result()
                            if success:
                                processed_types[pkg_type] = True
                        except Exception as e:
                            print(f"Error in worker thread: {e}")
                        pbar.update(1)
    
    except Exception as e:
        print(f"Error during upload process: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Report on which types were processed
        for pkg_type, was_processed in processed_types.items():
            if was_processed:
                print(f"✅ {pkg_type.capitalize()} package processed successfully")
            else:
                print(f"⚠️ No {pkg_type} package was processed")
                
        # Clean up temporary files
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("Upload process completed")
        
        # Delete packages directory after successful upload
        if all(processed_types.values()):
            print("All package types processed successfully. Cleaning up packages directory...")
            shutil.rmtree(PACKAGES_DIR, ignore_errors=True)
            print(f"✅ Packages directory {PACKAGES_DIR} deleted")
        else:
            print("⚠️ Not all package types were processed. Keeping packages directory for further processing.")

def get_latest_date_from_metadata(force_check_files=False):
    """
    Get the latest decision date from metadata, preferring index.json if available.
    Falls back to parsing individual files if needed or if force_check_files=True.
    """
    # First try to download the index.json file from S3
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    current_year = datetime.now().year
    index_path = os.path.join(LOCAL_DIR, f"sc-judgments-{current_year}-metadata.index.json")
    index_key = f"{S3_PREFIX}sc-judgments-{current_year}-metadata.index.json"
    
    if not force_check_files:
        try:
            # Try to get current year index
            s3.download_file(S3_BUCKET, index_key, index_path)
            with open(index_path, 'r') as f:
                index_data = json.load(f)
                
            # Check if updated_at is available
            if "updated_at" in index_data:
                updated_at = datetime.fromisoformat(index_data["updated_at"])
                print(f"[INFO] Found updated_at in index.json: {updated_at}")
                return updated_at
            
            ## TODO needs review
            # If no updated_at but there's created_at
            # if "created_at" in index_data:
            #     created_at = datetime.fromisoformat(index_data["created_at"])
            #     print(f"[INFO] Using created_at from index.json: {created_at}")
            #     return created_at
                
        except Exception as e:
            print(f"[INFO] Could not use index.json for date detection: {e}")
    
    # Fall back to the original method - parsing individual files
    print("[INFO] Falling back to parsing individual files for decision dates...")
    latest_zip = sync_latest_metadata_zip()
    return find_latest_decision_date_in_zip(latest_zip)

def main():
    latest_date = get_latest_date_from_metadata()
    print(f"Latest date in metadata: {latest_date.date() if latest_date else 'Unknown'}")
    today = datetime.now().date()
    if latest_date.date() < today:
        run_downloader(latest_date - timedelta(days=1), today)
        print("Download and packaging complete. Ready to upload new packages.")
        print("All done. New packages are ready in ./packages. Upload is starting.")
        upload_new_zips_to_s3()
    else:
        print("No new data to fetch.")
    
    # Clean up LOCAL_DIR after processing
    if os.path.exists(LOCAL_DIR):
        print(f"Cleaning up local data directory {LOCAL_DIR}...")
        import shutil
        shutil.rmtree(LOCAL_DIR, ignore_errors=True)
        print(f"✅ Local data directory deleted")

if __name__ == "__main__":
    main()