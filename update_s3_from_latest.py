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

S3_BUCKET = "indian-supreme-court-judgments-test"
S3_PREFIX = "data/"
LOCAL_DIR = "./local_sc_judgments_data"
PACKAGES_DIR = "./packages"
DOWNLOAD_SCRIPT = "./download.py"

def sync_latest_metadata_zip(force_refresh=True):
    """
    Download the latest metadata zip file from S3.
    If force_refresh is True, always download a fresh copy.
    """
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
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
    import sys
    import threading
    
    # First, check if AWS credentials are configured
    try:
        # Try to get credentials - this will raise an exception if none are available
        session = boto3.Session(profile_name="dattam-supreme")
        credentials = session.get_credentials()
        if credentials is None:
            print("ERROR: No AWS credentials found in profile 'dattam-supreme'.")
            print("You can configure credentials by:")
            print("1. Using the AWS CLI command 'aws configure --profile dattam-supreme'")
            print("2. Check that the profile exists in ~/.aws/credentials")
            return
        
        # Create S3 client with credentials from profile
        s3 = session.client('s3')
        print("Successfully authenticated with AWS using profile 'dattam-supreme'")
    except Exception as e:
        print(f"Failed to initialize S3 client: {e}")
        return
    
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
        # Process each zip file in packages directory with progress bar
        for local_zip in tqdm(os.listdir(PACKAGES_DIR), desc="Processing packages"):
            if not local_zip.endswith(".zip"):
                continue
            
            # Check which type of package this is
            if "regional" in local_zip:
                package_type = "regional"
            elif "english" in local_zip:
                package_type = "english"
            elif "metadata" in local_zip:
                package_type = "metadata"
            else:
                print(f"Warning: Unknown package type for {local_zip}, processing anyway")
                package_type = "unknown"
                
            local_index = local_zip.replace(".zip", ".index.json")
            
            # Check if both files exist locally
            if not os.path.exists(os.path.join(PACKAGES_DIR, local_index)):
                print(f"Skipping {local_zip}: Missing index file")
                continue
            
            # Load local index
            with open(os.path.join(PACKAGES_DIR, local_index), 'r') as f:
                local_index_data = json.load(f)
            
            # Determine S3 keys
            s3_zip_key = f"{S3_PREFIX}{local_zip}"
            s3_index_key = f"{S3_PREFIX}{local_index}"
            
            # Check if files exist in S3
            try:
                # Try to download existing index file
                s3_index_path = os.path.join(temp_dir, local_index)
                print(f"Downloading index file {s3_index_key}...")
                s3.download_file(S3_BUCKET, s3_index_key, s3_index_path)
                
                with open(s3_index_path, 'r') as f:
                    s3_index_data = json.load(f)
                
                # Merge file lists
                s3_files = set(s3_index_data.get("files", []))
                local_files = set(local_index_data.get("files", []))
                all_files = sorted(list(s3_files | local_files))
                new_files = local_files - s3_files
                
                # Update index regardless of whether there are new files
                merged_index = s3_index_data.copy()
                merged_index["file_count"] = len(all_files)
                merged_index["files"] = all_files
                merged_index["created_at"] = s3_index_data.get("created_at", datetime.now().isoformat())  # Keep original creation date
                merged_index["updated_at"] = datetime.now().isoformat()  # Add updated_at timestamp
                
                # Save merged index
                with open(s3_index_path, 'w') as f:
                    json.dump(merged_index, f, indent=2)
                
                if not new_files:
                    print(f"No new files to add to {local_zip}")
                    print(f"Uploading updated index with new timestamp to {s3_index_key}")
                    
                    # Create a progress callback for upload
                    class ProgressPercentage:
                        def __init__(self, filename):
                            self._filename = os.path.basename(filename)
                            self._size = os.path.getsize(filename)
                            self._seen_so_far = 0
                            self._lock = threading.Lock()
                            self._pbar = tqdm(total=self._size, unit='B', unit_scale=True, 
                                             desc=f"Uploading {self._filename}")

                        def __call__(self, bytes_amount):
                            with self._lock:
                                self._seen_so_far += bytes_amount
                                self._pbar.update(bytes_amount)
                                if self._seen_so_far >= self._size:
                                    self._pbar.close()
                    
                    # Upload updated index file even if there are no new zip contents
                    s3.upload_file(
                        s3_index_path, 
                        S3_BUCKET, 
                        s3_index_key,
                        Callback=ProgressPercentage(s3_index_path)
                    )
                    processed_types[package_type] = True
                    continue
                
                # Update index
                merged_index = s3_index_data.copy()
                merged_index["file_count"] = len(all_files)
                merged_index["files"] = all_files
                merged_index["created_at"] = s3_index_data.get("created_at", datetime.now().isoformat())  # Keep original creation date
                merged_index["updated_at"] = datetime.now().isoformat()  # Add updated_at timestamp
                
                # Save merged index
                with open(s3_index_path, 'w') as f:
                    json.dump(merged_index, f, indent=2)
                
                # Download existing zip
                s3_zip_path = os.path.join(temp_dir, local_zip)
                merged_zip_path = os.path.join(temp_dir, f"merged_{local_zip}")
                
                print(f"Downloading existing zip {s3_zip_key}...")
                s3.download_file(S3_BUCKET, s3_zip_key, s3_zip_path)
                
                # Define a custom callback for tqdm
                file_sizes = {}
                
                # Merge zip files with progress bars
                with zipfile.ZipFile(s3_zip_path, 'r') as s3_zip:
                    with zipfile.ZipFile(os.path.join(PACKAGES_DIR, local_zip), 'r') as local_zip_file:
                        with zipfile.ZipFile(merged_zip_path, 'w', zipfile.ZIP_DEFLATED) as merged_zip:
                            # Get existing filenames and their modification times
                            s3_files_info = {info.filename: info.date_time for info in s3_zip.infolist()}
                            local_files_info = {info.filename: info.date_time for info in local_zip_file.infolist()}
                            
                            # Copy all files from S3 zip that aren't in the local zip or are older versions
                            s3_items = s3_zip.namelist()
                            for item in tqdm(s3_items, desc=f"Copying files from existing {local_zip}", unit="file"):
                                # Skip if local version is newer
                                if item in local_files_info and local_files_info[item] > s3_files_info[item]:
                                    print(f"Skipping {item} from S3 as local version is newer")
                                    continue
                                merged_zip.writestr(item, s3_zip.read(item))
                            
                            # Add new files from local zip with progress bar
                            for item in tqdm(new_files, desc=f"Adding new files to {local_zip}", unit="file"):
                                try:
                                    # Skip if we already added this file from S3 and it's newer
                                    if item in s3_files_info and s3_files_info[item] > local_files_info.get(item, (1980,1,1,0,0,0)):
                                        print(f"Skipping {item} from local as S3 version is newer")
                                        continue
                                    merged_zip.writestr(item, local_zip_file.read(item))
                                except KeyError:
                                    print(f"Warning: {item} not found in local zip but listed in index")
                
                # Create a custom callback class for upload progress
                class ProgressPercentage:
                    def __init__(self, filename):
                        self._filename = os.path.basename(filename)
                        self._size = os.path.getsize(filename)
                        self._seen_so_far = 0
                        self._lock = threading.Lock()
                        self._pbar = tqdm(total=self._size, unit='B', unit_scale=True, 
                                         desc=f"Uploading {self._filename}")

                    def __call__(self, bytes_amount):
                        with self._lock:
                            self._seen_so_far += bytes_amount
                            self._pbar.update(bytes_amount)
                            if self._seen_so_far >= self._size:
                                self._pbar.close()
                
                # Upload merged files with progress bar
                print(f"Uploading merged {local_zip} with {len(new_files)} new files to {s3_zip_key}")
                import threading
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
                processed_types[package_type] = True
                
            except s3.exceptions.ClientError as e:
                if 'HeadObject' in str(e) or '404' in str(e):
                    # File doesn't exist in S3, upload directly
                    print(f"Uploading new {local_zip} to {s3_zip_key}")
                    
                    # Create a progress callback for direct upload
                    import threading
                    class ProgressPercentage:
                        def __init__(self, filename):
                            self._filename = os.path.basename(filename)
                            self._size = os.path.getsize(filename)
                            self._seen_so_far = 0
                            self._lock = threading.Lock()
                            self._pbar = tqdm(total=self._size, unit='B', unit_scale=True, 
                                             desc=f"Uploading {self._filename}")

                        def __call__(self, bytes_amount):
                            with self._lock:
                                self._seen_so_far += bytes_amount
                                self._pbar.update(bytes_amount)
                                if self._seen_so_far >= self._size:
                                    self._pbar.close()
                    
                    # Upload with progress bar
                    s3.upload_file(
                        os.path.join(PACKAGES_DIR, local_zip), 
                        S3_BUCKET, 
                        s3_zip_key,
                        Callback=ProgressPercentage(os.path.join(PACKAGES_DIR, local_zip))
                    )
                    
                    print(f"Uploading new {local_index} to {s3_index_key}")
                    s3.upload_file(
                        os.path.join(PACKAGES_DIR, local_index), 
                        S3_BUCKET, 
                        s3_index_key,
                        Callback=ProgressPercentage(os.path.join(PACKAGES_DIR, local_index))
                    )
                    processed_types[package_type] = True
                else:
                    # Some other error occurred
                    print(f"Error processing {local_zip}: {str(e)}")
    
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

def main():
    latest_zip = sync_latest_metadata_zip()
    latest_date = find_latest_decision_date_in_zip(latest_zip)
    print(f"Latest date in metadata: {latest_date.date() if latest_date else 'Unknown'}")
    today = datetime.now().date()
    if latest_date.date() < today:
        run_downloader(latest_date - timedelta(days=1), today)
        print("Download and packaging complete. Ready to upload new packages.")
        # Uncomment the next line to automatically upload to S3 (requires credentials)
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