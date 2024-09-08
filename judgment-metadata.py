import os, sys
import json
import requests
from datetime import datetime
import calendar
import time
from pathlib import Path


base_url = "https://scourtapp.nic.in"
output_dir = Path("./data/metadata/raw/")
# read auth token from env
AUTH_TOKEN = os.environ.get("AUTH_TOKEN")
assert AUTH_TOKEN, "AUTH_TOKEN not found in environment variables"


def get_year_intervals():
    starting_date = datetime(1950, 1, 1)
    intervals = [("01-01-1900", "31-12-1949")]
    # create interval for each 10 years until 2009
    for i in range(1950, 2010, 10):
        start = datetime(i, 1, 1).strftime("%d-%m-%Y")
        end = datetime(i + 9, 12, 31).strftime("%d-%m-%Y")
        intervals.append((start, end))

    # 1 year interval from 2010 on till 2023
    for i in range(2010, 2024):
        start = datetime(i, 1, 1).strftime("%d-%m-%Y")
        end = datetime(i, 12, 31).strftime("%d-%m-%Y")
        intervals.append((start, end))

    # 1 month interval from 2024 till current date
    current_year = datetime.now().year
    for i in range(2024, current_year + 1):
        if i == current_year:
            end_month = datetime.now().month
        else:
            end_month = 12
        for j in range(1, end_month + 1):
            start = datetime(i, j, 1).strftime("%d-%m-%Y")
            # gets end date of the month accounting of leap years, feb month etc
            end = datetime(i, j, calendar.monthrange(i, j)[1]).strftime("%d-%m-%Y")
            intervals.append((start, end))

    return intervals


def basic_clean(metadata: dict):
    # delete slno as it is a transient data that changes based on new case additions or even changes in query time period
    for item in metadata["data"]:
        del item["slno"]
    return metadata


def get_judgment_metadata(from_date: str, to_date: str):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "rpt_type": "A",
        "from_date": from_date,
        "to_date": to_date,
        "token": AUTH_TOKEN,
        "judgename": "99999",
    }

    response = requests.post(base_url + "/?pageid=100001", headers=headers, data=data)
    if response.status_code != 200:
        print(
            f"Failed to get metadata for {from_date} to {to_date}, err: {response.text}"
        )
        return
    file_name = f"{from_date}-{to_date}.json"
    out_path = output_dir / file_name
    try:
        metadata = json.loads(response.text)
    except Exception as e:
        print(f"Failed to parse response: {response.text}")
        raise e
    if not metadata["data"]:
        print(
            f"No metadata found for {from_date} to {to_date}, response: {response.text}"
        )
        return
    with open(out_path, "w") as f:
        metadata = basic_clean(metadata)
        json.dump(metadata, f, indent=4)
        print(f"Metadata saved to {file_name}")
    return response.text


def run():
    output_dir.mkdir(parents=True, exist_ok=True)
    intervals = get_year_intervals()
    print(intervals)
    for start, end in intervals:
        print(f"Getting metadata for {start} to {end}")
        # be a good citizen and wait for 1 second before making next request
        time.sleep(1)
        get_judgment_metadata(start, end)
    print("Finished getting metadata")


if __name__ == "__main__":
    run()
