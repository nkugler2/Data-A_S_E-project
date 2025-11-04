# Importing the necessary libraries

import zipfile                      # For extracting the ZIP files
import requests                     # For downloading the files
import os                           # For creating directories, and joining file paths
from datetime import datetime       # For naming the files with the current date
from pathlib import Path            # For joining file paths
import csv                          # For creating CSV files for logging 
import json                         # For normalizing values for CSV writing


# Step 0: Anchor this script to the root directory to use my folder structure
## Without this, I wouldn't be able to use the folder structure I have ##

THIS_FILE = Path(__file__).resolve() # Locate the path of this file
ROOT_DIR = THIS_FILE.parents[2] # Go up two levels to the root directory

# Defining the data directories under the projects root directory
RAW_DIR = ROOT_DIR / '01_data' / '01_sampleData' / '01_raw'
BRONZE_DIR = ROOT_DIR / '01_data' / '01_sampleData' / '02_bronze'
LOG_DIR = ROOT_DIR / '06_logs'

def download_one_quarter(year, quarter, output_dir='01_data/01_sampleData/01_raw', extract_dir='01_data/01_sampleData/02_bronze'):
    """
    Downloads a specific quarter's zip file to the raw directory from the SEC websiite and extracts it
    into the data/bronze directory.

    Args:
        year (int): The year of the data
        quarter (int): The quarter of the data (1, 2, 3, 4)
        output_dir (str): The directory to save the zip file
        extract_dir (str): The directory to extract the zip file
    
    Returns:
    - dict with download metadata (status, size, timing)
    """

    # Step 1: Define the base URL and filename
    ## Base URL is so we know where to pull from ##
    ## Filename is so we know what to name the file ##
    ## URL is the full path to the file ##
    base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
    filename = f'{year}q{quarter}.zip'
    url = base_url + filename

    # Step 2: Set up paths
    ## This is so we know where to save the file and where to extract it ##
    zip_path = os.path.join(output_dir, filename)
    quarter_extract_path = os.path.join(extract_dir, f'{year}q{quarter}')
    
    # Step 3: Create directories if they don't exist
    ## This is so we don't have to worry about the directories existing ##
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)


    # Step 4: Make my header
    headers = {
    'User-Agent': 'Noah Kugler nkugler2@asu.edu'
    }

    # Print the download process
    print("\n" + "="*50)
    print("Downloading Process")
    print("="*50)
    print(f"Starting download: {filename}")
    print(f"URL: {url}")


    # Step 5: Download the file
    ## This is so we can get the file from the SEC ##

    # Start the timer
    start_time = datetime.now()

    try:
        # Make the request with stream=True to handle large files
        response = requests.get(url, headers=headers, stream=True)

        # Check if the request was successful
        ## This raises error for 4xx and 5xx status codes ##
        response.raise_for_status()

        # Step 6: Write the ZIP file in chunks (for memory efficiency)
        ## This is so we don't have to load the entire file into memory at once ##
        ## This is where we actually write the file to the raw directory ##
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
            
        # Get the file size
        file_size = os.path.getsize(zip_path)

        # Print the download time
        end_time = datetime.now()
        download_time = end_time - start_time
        print(f"✓ Download complete: {filename}")
        print(f"Download Size: {file_size / 1024 / 1024:.2f} MB")
        print(f"Time taken: {download_time}")

        # Step 7: Extract the ZIP file
        ## Now that we downloaded the file, we can extract it ##
        
        # Output the extraction path and start the timer
        print(f"Extracting to: {quarter_extract_path}")
        extraction_start_time = datetime.now()
        
        # Extract the ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(quarter_extract_path)

        #Step 8: Verify the extraction
        extracted_files = os.listdir(quarter_extract_path)
        expected_files = ['sub.txt', 'num.txt', 'tag.txt', 'pre.txt']

        # Note: SEC files might be uppercase or lowercase, so check case-insensitive
        extracted_lower = [f.lower() for f in extracted_files]
        
        missing_files = [f for f in expected_files if f not in extracted_lower]

        if missing_files:
            print(f"⚠ Warning: Missing expected files: {missing_files}")
        else:
            print(f"✓ All expected files extracted successfully")

        extraction_end_time = datetime.now()
        extraction_time = extraction_end_time - extraction_start_time
        print(f"Time taken to extract: {extraction_time}")

        # Step 9: Return metadata
        return {
            'status': 'success',
            'year': year,
            'quarter': quarter,
            'url': url,
            'zip_path': zip_path,
            'extract_path': quarter_extract_path,
            'file_size_mb': file_size / (1024*1024),
            'download_start': start_time.isoformat(" "),
            'download_time': f"{download_time.total_seconds()} seconds",
            'extract_start': extraction_start_time.isoformat(" "),
            'extract_time': f"{extraction_time.total_seconds()} seconds",
            'extracted_files': extracted_files,
            'http_status': response.status_code
        }

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'year': year,
            'quarter': quarter,
            'url': url
        }
    
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'year': year,
            'quarter': quarter,
            'url': url
        }

# Creating a function to log the results to a CSV file
def log_download_dynamic(metadata: dict, log_path):
    """
    Creates a .csv file with the logging information gathered from the 
    main download/extraction function.

    Args:
        metadata (dict): The dictionary of metadata returned in the return block 
            from the main download/extraction function
        log_path (Path): The path to the CSV file to be created

    """
    
    # Step 1: Define the path to the CSV file
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Step 2: Check if the file exists
    write_header = not log_path.exists()

    # Step 3: Normalize values so CSV writes cleanly
    def normalize(v):
        if isinstance(v, (list, dict, set, tuple)):
            return json.dumps(v)
        return str(v)

    # Step 4: Write the header to the file
    if write_header:
        fieldnames = list(metadata.keys())
        with log_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({k: normalize(v) for k, v in metadata.items()})
        return

    # Step 5: For existing file, keep original header order
    with log_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            existing_header = next(reader)
        except StopIteration:
            existing_header = list(metadata.keys())

    # Step 6: Create a row of the metadata
    row = {col: normalize(metadata.get(col, "")) for col in existing_header}

    # Step 7: Write the row to the file
    with log_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=existing_header)
        writer.writerow(row)



# Test it with one quarter
if __name__ == "__main__":
    # Download 2024 Q4 as a test
    result = download_one_quarter(year=2025, quarter=1)
    
    # Log the results to the CSV file
    log_download_dynamic(result, LOG_DIR / 'downloads.csv')

    # Output the results to the terminal
    print("\n" + "="*50)
    print("Download Summary:")
    print("="*50)
    for key, value in result.items():
        print(f"{key}: {value}")