# Testing extraction of ZIP files

The goal of this file is to simply test how to extract a ZIP file from the [SEC Financial Statement Data Sets](https://catalog.data.gov/dataset/financial-statement-data-sets) website.

The data will:

1. Be extracted from the site
2. Placed in the data/raw folder
3. Be prepared to be sent to the bronze zone

```python
# Importing the necessary libraries

import zipfile                      # For extracting the ZIP files
import requests                     # For downloading the files
import os                           # For creating directories, and joining file paths
from datetime import datetime       # For naming the files with the current date
from pathlib import Path            # For joining file paths
import json                         # For normalizing values for CSV writing
```

## Understanding the SEC URL Pattern

If I am going to download zip files from the website, I need to understand the URL that is used to download the file. I wont be able to see this from the original website, because that just has the catalog of all of the data files. If I try to click download, it will just download the file and not show me the URL. Instead, I need to inspect element to show the format of the URL for the ZIP file download.

`<a href="https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q2.zip" class="btn btn-primary" data-format="zip" data-organization="Securities and Exchange Commission"> <i class="fa fa-download"></i>Download </a>`

This is the HTML element that has the download link. I can see now the format:

- `https://www.sec.gov/files/dera/data/financial-statement-data-sets/` - shows that I am pulling from there files/dera/data/*the whole dataset*
- `2025q2` shows one example of what I am pulling, namely the data for year 2025, quarter 2 (lower case `q`, number `2`, no spaces)
- `.zip`   shows that I am pulling the ZIP file, which is what I want

So now, I can use this information to *request* the data file itself.

## User-Header - Part of SEC Requirements

SEC wants to know (apparently) who is pulling their data. So, I need to have a `header` that shows my name and my email address. An example would be:

```python
headers = {
    'User-Agent': 'John Smith jsmith@university.edu'
}
```

The SEC has another rule: **A Maximum of 10 requests per second.** This is not an issue for this file, as I will only be downloading one file as a test, but will be important to keep in mind when I make a script to download multiple files as I build this project up

So my setup is:

```python
headers = {
    'User-Agent': 'Noah Kugler nkugler2@asu.edu'
}
```

## Creating the download script

**What this does:** Downloads one quarter's zip file and extracts it

**Why I am creating this:** To test that I can download one file, and put it where it needs to be

**When will I need this:** Only for the test of downloading one file

Lets break it down step by step:

### Step 0.1: Setup for my project directory

I want the data that I download to go into my own pre configured project directories. To do this, I use pathlib's `Path` function.

1. Locate the path of the current file
2. Go up two levels to the root directory
    - Go from `01_extract` -> `02_src` -> `Data A_S_E Project` directory, which is going up two levels
3. Use the `ROOT_DIR` variable to define what is the directory for RAW files and the directory for Bronze files, as well as create the path for a log file to be generated later.

```python
THIS_FILE = Path(__file__).resolve()
ROOT_DIR = THIS_FILE.parents[2]


RAW_DIR = ROOT_DIR / '01_data' / '01_sampleData' / '01_raw'
BRONZE_DIR = ROOT_DIR / '01_data' / '01_sampleData' / '02_bronze'
LOG_DIR = ROOT_DIR / '06_logs'
```

### Step 0.2: Create the main function that will be used for the extraction of a single file

This will be the function that we can call to download the file to the RAW directory and extract the data to the Bronze directory.

`output_dir` and `extract_dir` are hardcoded for the directories that I set up that I can now use because of Step 0.1. `year` and `quarter` can be defined at the end when the function is called.

This function returns information about the downloading process in the terminal, including the status of the process, the time it takes, the size of the file, and where the file is downloaded and extracted to.

```python
def download_one_quarter(year, quarter, output_dir='01_data/01_raw', extract_dir='01_data/02_bronze'):
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
```

### Step 1: Define the URL and the filename

This builds the download URL by taking the base URL and combining it with the `year` and `quarter` that I define when I call the function. This allows me to download whatever file that I want by simply changing the `year` and `quarter` variables located at the end of the file.

```python
base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
filename = f'{year}q{quarter}.zip'
url = base_url + filename
```

### Step 2: Setting up the file paths

When we download a file, we want it to go to the correct directory. `os.path.join` creates the correct path for our zip file (for the RAW zone) and the extracted data (for the bronze zone). For example, the name created for the ZIP data for `year` 2025 and `quarter` 2 is: `01_data/01_raw/2025q2.zip`.

```python
zip_path = os.path.join(output_dir, filename)
quarter_extract_path = os.path.join(extract_dir, f'{year}q{quarter}')
```

### Step 3: Ensure that the correct directories exist

This may not be needed, as the directories should be created, but it is just to make sure are there.

```python
os.makedirs(output_dir, exist_ok=True)
os.makedirs(extract_dir, exist_ok=True)
```

### Step 4: Make the header

As mentioned earlier, this is needed to comply with SEC rules about pulling data.

```pyton
headers = {
'User-Agent': 'Noah Kugler nkugler2@asu.edu'
}
```

### Logging: Outputting the start of the process to the terminal

We want to know that the download process has started, and get some basic information when our download process starts.

```python
print("\n" + "="*50)
print("Downloading Process")
print("="*50)
print(f"Starting download: {filename}")
print(f"URL: {url}")
```

Here is an example of what will be seen from the code above:

```zsh
==================================================
Downloading Process
==================================================
Starting download: 2025q2.zip
URL: https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q2.zip
```

### Step 5: Download the file

This is where the real process of downloading the file starts. We begin by starting a timer so that we can know when we started the download and use that data later to know how long our download started.

```python
start_time = datetime.now()
```

Next, we make the request. We use the URL, the header, and set stream to true to handle how large these files are.

```python
response = requests.get(url, headers=headers, stream=True)
```

We also use `raise_for_status()` so that we can check for 4xx and 5xx error codes (for example, a 404 error)

```python
response.raise_for_status()
```

### Step 6: Managing our ZIP - writting the ZIP file in chunks, getting the final size, and printing how long downloading took

First, we want to chunk the writting of our ZIP file so that it is not all put into memory at the same time. This allows the process to be more efficent, and allows us to work with very large files. We use `'wb'` mode to write binary, which is required of ZIP files.

```python
with open(zip_path, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
```

We then get the file size so we can output that later.

```python
file_size = os.path.getsize(zip_path)
```

Finally, we get the `end_time`, or when the download finished, calculate how long the process took, and the output a confirmation that the download finished as well as the file size and the overall time taken to download.

```python
end_time = datetime.now()
download_time = end_time - start_time
print(f"✓ Download complete: {filename}")
print(f"Download Size: {file_size / 1024 / 1024:.2f} MB")
print(f"Time taken: {download_time}")
```

### Step 7: Extracting the ZIP file

Now that we have the file downloaded and in our RAW directory, we can extract the ZIP file to access the data.

First, we output where we are extracting the file to in the terminal, and start a timer so we know how long the extraction process took. Both of these are for logging.

```python
print(f"Extracting to: {quarter_extract_path}")
extraction_start_time = datetime.now()
```

Then, we do the actual extraction of the ZIP into the Bronze zone directory. `'r'` mode is used to as the "read mode" for ZIP files.

```python
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(quarter_extract_path)
```

### Step 8: Verifying the extraction

We want to make sure that we successfully extracted the files. We grab the files that were extracted, and hardcode the files that we want to make sure are there.

```python
extracted_files = os.listdir(quarter_extract_path)
expected_files = ['sub.txt', 'num.txt', 'tag.txt', 'pre.txt']
```

Lets make sure that the files are set to lower case, just in case some are not, so that they are all consistent.

```python
extracted_lower = [f.lower() for f in extracted_files]
```

And then lets check if any of the files are actually missing, and print out if some are missing.

```python
missing_files = [f for f in expected_files if f not in extracted_lower]

if missing_files:
    print(f"⚠ Warning: Missing expected files: {missing_files}")
else:
    print(f"✓ All expected files extracted successfully")
```

### Logging: Additonal information to the terminal

Lets get the time the logging ended, calculate how long the extraction took, and then print out the amount of time that it took.

```python
extraction_end_time = datetime.now()
extraction_time = extraction_end_time - extraction_start_time
print(f"Time taken to extract: {extraction_time}")
```

### Step 9: Returning our metadata and checking for errors

`return` and `except` are used to output, or "return" information if our process was successful, and output an error message if something went wrong.

The following outputs a lot of the logging, in a dictonary, that we have been gathering, as well as messages including 'success' if our process worked, the year and quarter we requested, the url, paths, file size, and even the `http_status` just to make sure we got the right thing.

```python
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
```

Now we need to have some `except` code, which outputs things if our code does not work.

Our first block outputs error code if there is an http error, like a 404.

```python
except requests.exceptions.HTTPError as e:
    print(f"❌ HTTP Error: {e}")
    return {
        'status': 'failed',
        'error': str(e),
        'year': year,
        'quarter': quarter,
        'url': url
    }
```

Our second block outputs error messages if something else went wrong, and metadata we can use to understand what went wrong.

```python
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    return {
        'status': 'failed',
        'error': str(e),
        'year': year,
        'quarter': quarter,
        'url': url
    }
```

### Logging: Creating a log file in the `06_logs` directory

When we scale this solution up to download many files, we will want to have sufficient logging that is not just in the terminal. Lets create that as a seperate function outside of our downloading function.

First, we create the function. This function takes the dictionary of metadata that is used in the `return` box in the original download function, and then generates a log_path for where the log should go. Doing it this way, making a function and grabbing the dictionary, means that if I change the metadata from the `return` block, the logging function still works. If I change the metadata, or add new metadata, I don't need to change the logging logic.

It is important to remember that to use this log_path in my current file structure, we have to do two things:

1. We have to put `LOG_DIR = ROOT_DIR / '06_logs'` so that our program knows that we should put the file in the `06_logs` directory in our main project.
2. We have to pass `LOG_DIR` into the call of our function, which is where the function knows what the `log_path` is.

```python
def log_download_dynamic(metadata: dict, log_path):
    """
    Creates a .csv file with the logging information gathered from the main download/extraction function.

    Args:
        metadata (dict): The dictionary of metadata returned in the return block from the main download/extraction function
        log_path (Path): The path to the CSV file to be created

    """

    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

```

This is where we write the header (the column names) to the CSV *if* it does not always exist. It uses the dictionary keys to define the CSV schema. It basically asks "does the file already exsit?"

```python
    write_header = not log_path.exists()
```

Now, we normalize the values going to the CSV. We need to convert the Python object (a dictionary) to "clean" text that we can actually put it in a CSV. So we will make a function that:

- takes Python object `v`
  - checks if `v` is one of the common container types that we can't put in a CSV without this process. We need a string of words, not a nested structure.
  - then it serializes the container to a json string, which can be parsed later
- and returns a string that can be put into a CSV for anything that is not a container. In my script there shouldn't be anything there, but it is nice to have just in case.
  - If, for example, I change the use of `isoformat()` to just a string, this would capture that data.

```python
    def normalize(v):
        if isinstance(v, (list, dict, set, tuple)):
            return json.dumps(v)
        return str(v)
```

Next, we create the header from the dictionary's keys and write the row

- we make a list of the keys of our metadata (so things like `status`, `year`, and `quarter` without their associated values) and use those to make column headers.
- `.open` opens the file, uses `newline` to avoid extra blank lines, `encoding` to ensure non-ASCII characters are preserved, and `w` to write a new file. the `with... as f` syntax means that we ipen the file and close it afterwards so nothing gets lost.
- `fieldnames` is just the list of column names that we want to use
- `DictWriter` is what is used to map the keys to the columns
- `writeheader()` writes a single heder row with the column names
- `writerow()` writes one data row by mapping keys to columns and normalizing values

```python
    if write_header:
        fieldnames = list(metadata.keys())
        with log_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({k: normalize(v) for k, v in metadata.items()})
        return
```

Then, we use the same `with... as f` syntax to use the open file, read it, and ensures that we keep the original header order and that it is stable. If the file exists but is empty, we fall back to the current metadata keys.

```python
    with log_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            existing_header = next(reader)
        except StopIteration:
            existing_header = list(metadata.keys())
```

Before we can insert the row, we need to build it. For each column in the existing header, grab the corresponding value from metadata, or use an empty string if the new metadata doesn't have that key (so if we cange our metadata later, we dont have "schema drift" where we add/remove fields over time). `normalize` ensures that the value is a string (or JSON string) before writting

```python
    row = {col: normalize(metadata.get(col, "")) for col in existing_header}
```

Finally, we can acutally write (in this case append with the `"a"` parameter) to the file. Appending allows us to add something to the bottom without changing what we already have, assuming we have information in the log file. `newline="", encoding="utf-8"` is again used to make sure this works across platforms.

```python
    with log_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=existing_header)
        writer.writerow(row)
```

All we need to do now is call the function when the file is used. That will be shown below when we run the code, but what we need to make sure we have is:

```python
log_download_dynamic(result, LOG_DIR / 'downloads.csv')
```

### Running the code

Finally, we can run the code!

`if __name__ == "__main__":` is used to run the current file. `__name__` is a variable (a "dunder") created automatically by Python when a file is made. It is automatically assigned to `"__main__"` when the code is run as a script rather than a module. This confused me so much when I was first learning Python!

`result` is where we define what `year` and what `quarter` we want to download.

The last line of code outputs "Download Summary" with a lot of equal signs above and below so that it is visually distinct in the terminal.

Finally, we use the dictionary of logging information we made in step 9, and iterate through the `key` and `value` pairs to output that useful information. If the file has an error for any reason, `result.items()` doesn't output, and instead we see the error information.

```python
if __name__ == "__main__":
    # Download 2024 Q4 as a test
    result = download_one_quarter(year=2025, quarter=2)
    
    log_download_dynamic(result, LOG_DIR / 'downloads.csv')

    print("\n" + "="*50)
    print("Download Summary:")
    print("="*50)
    for key, value in result.items():
        print(f"{key}: {value}")
```

Now we are done with our test of extracting one file! We should have a zip file in the `01_raw` folder, and 4 .txt files (as well as a readme) in the `02_bronze` folder.

Here is an example of the output in the terminal:

```zsh
==================================================
Downloading Process
==================================================
Starting download: 2025q2.zip
URL: https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q2.zip
✓ Download complete: 2025q2.zip
Download Size: 75.32 MB
Time taken: 0:00:01.844909
Extracting to: 01_data/02_bronze/2025q2
✓ All expected files extracted successfully
Time taken to extract: 0:00:00.598719

==================================================
Download Summary:
==================================================
status: success
year: 2025
quarter: 2
url: https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q2.zip
zip_path: 01_data/01_raw/2025q2.zip
extract_path: 01_data/02_bronze/2025q2
file_size_mb: 75.31525421142578
download_start: 2025-10-30 21:46:13.240340
download_time: 1.844909 seconds
extract_start: 2025-10-30 21:46:15.085308
extract_time: 0.598719 seconds
extracted_files: ['tag.txt', 'pre.txt', 'sub.txt', 'readme.htm', 'num.txt']
http_status: 200
```

## What I learned

This code gave me the opprotunity to learn a lot about how to ingest data. Some of the things that I learned is:

1. Anchoring a script to the root directroy: When I tried to run the script originally, it created new directorires within the folder that the script exsits in. I wanted the files to go into the directory structure that I already laid out. using `Path(__file__).resolve()` (which utlizes one of the "dunders"), `THIS_FILE.parents[2]`, and then defining the Raw, Bronze, and Log directories allowed me to place the files where I wanted.

2. Combining URL: When you have a URL that is standard, but the very end changes based on what file you are downloading, you can create that unique URL by combining the base URL (what is common among any time you download a file) and variables that denote what you are specifically looking for (in my case `year` and `quarter`), I am able to download exactly the file that I want by indicating what `year` and `quarter` I want when calling the funciton, and let the code do the combining.

3. Setting up unique paths of where to save using `os.path.join`: I can use the combined `url` and `filename` variables, along with the paths to the root directory that I made, to join paths so that I can download/write to a specific file location. This allowed me to have things like `02_bronze/2025q2`, so the file goes where I want it.

4. Ensuring that a directory is made, so nothing happens if it is there, and a directory is made if it isn't there.

5. Headers: Headers are common for HTTP requests, to let the site know who is asking for the data. The SEC requires that header, as do many sites.

6. Using `response`: I can request a URL, grab the status to see if there were any errors or if my request was successful, and chunk the file for memory efficiency.

7. Chunking: I can download files in chunks to be more memory efficient.

8. Extracting a ZIP file: Using `zipfile.ZipFile`, I can `extractall` the information that I need, and do that to the path that I want.

9. Using `try`, `except`, and `return`: I knew about these, and have used them in smaller situations, but this is the first time I had a serious example.

- It is interesting that the `return` can be used later, as I do in the log file, and you don't need to set up a new variable to house that information. The function does what it needs to (download and extract), and the `return` (in this case of a dictionary), can just be used in my logging function call statement by simply passing a variable contaning the use of my function.
- It is also interesting that the `except` can use multiple different versions based on the issue. So I used `requests.exceptions.HTTPError` to see if there was an HTTP error code, and the other except block in case something else happened.

1. Logging with a CSV: I have always logged with the terminal, but it is nice to know the basics of logging to a CSV so that I can store that information for later. I am curious if it was right to log this to a CSV, or if it makes more sense to use a database. I would imagine that it depends on the use case.
