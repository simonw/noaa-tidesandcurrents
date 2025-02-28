#!/usr/bin/env python
import json
import os
import gzip
import time
import httpx
import tqdm
from urllib.parse import urlencode
from pathlib import Path
from datetime import datetime

# Configuration
BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
YEARS = range(2025, 2030)  # 2025 through 2029
REQUEST_PARAMS = {
    "product": "predictions",
    "datum": "mllw",
    "time_zone": "lst_ldt",
    "units": "english",
    "format": "json",
}
# Add a delay between requests to avoid hitting rate limits
DELAY_SECONDS = 2.0

def ensure_directory(path):
    """Create directory if it doesn't exist"""
    Path(path).mkdir(parents=True, exist_ok=True)

def fetch_and_save_tide_data(station_id, year):
    """Fetch tide data for a station and year, save as compressed JSON"""
    # Create start and end dates for the year
    begin_date = f"{year}0101"
    end_date = f"{year+1}0101"
    
    # Prepare request parameters
    params = {
        **REQUEST_PARAMS,
        "begin_date": begin_date,
        "end_date": end_date,
        "station": station_id,
    }
    
    # Build URL
    url = f"{BASE_URL}?{urlencode(params)}"
    
    # Create output directory
    station_dir = os.path.join("stations", station_id)
    ensure_directory(station_dir)
    
    # Output filename
    output_file = os.path.join(station_dir, f"{year}.json.gz")
    
    # Skip if file already exists
    if os.path.exists(output_file):
        return True
    
    try:
        # Fetch data (silently)
        response = httpx.get(url, timeout=60)
        
        if response.status_code != 200:
            # Write error to log file
            with open("error_log.txt", "a") as log:
                log.write(f"Error: Station {station_id}, year {year}: HTTP {response.status_code}\n")
            return False
        
        # Parse JSON to validate it
        data = response.json()
        
        # Check if we have predictions data
        if "predictions" not in data or not data["predictions"]:
            with open("error_log.txt", "a") as log:
                log.write(f"Error: Station {station_id}, year {year}: No predictions data\n")
            return False
        
        # Compress and save
        with gzip.open(output_file, 'wt', encoding='utf-8') as f:
            json.dump(data, f)
        
        return True
    
    except Exception as e:
        # Write exception to log file
        with open("error_log.txt", "a") as log:
            log.write(f"Exception: Station {station_id}, year {year}: {str(e)}\n")
        return False

def main():
    # Create base directory
    ensure_directory("stations")
    
    # Load California stations
    try:
        with open("california.json", "r") as f:
            stations = json.load(f)
    except Exception as e:
        print(f"Error loading california.json: {e}")
        return
    
    print(f"Found {len(stations)} stations in California")
    
    # Track statistics
    total_requests = len(stations) * len(YEARS)
    successful_requests = 0
    failed_requests = 0
    
    # Start time
    start_time = datetime.now()
    print(f"Started at {start_time}")
    
    # Create a progress bar for all requests
    with tqdm.tqdm(total=total_requests, desc="Fetching tide data", unit="file") as pbar:
        # Process each station
        for station in stations:
            station_id = station["id"]
            station_name = station["name"]
            
            # Update progress bar description with current station
            pbar.set_description(f"Station: {station_id} - {station_name[:20]}")
            
            # Process each year for this station
            for year in YEARS:
                result = fetch_and_save_tide_data(station_id, year)
                
                if result:
                    successful_requests += 1
                else:
                    failed_requests += 1
                
                # Update progress bar
                pbar.update(1)
                
                # Add delay between requests
                if year != YEARS[-1] or station != stations[-1]:  # Skip delay after last request
                    time.sleep(DELAY_SECONDS)
    
    # Calculate statistics
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n=== Summary ===")
    print(f"Total stations processed: {len(stations)}")
    print(f"Years processed: {min(YEARS)} through {max(YEARS)}")
    print(f"Successful requests: {successful_requests}/{total_requests}")
    print(f"Failed requests: {failed_requests}/{total_requests}")
    print(f"Duration: {duration}")
    print(f"Finished at {end_time}")

if __name__ == "__main__":
    main()