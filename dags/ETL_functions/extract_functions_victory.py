import urllib.parse
import requests
from bs4 import BeautifulSoup
import time
import os
import gzip
import shutil

def extract_xml_from_gz(gz_filepath):
    """
    Extracts a .gz file to an .xml file.

    Args:
    gz_filepath (str): The path to the .gz file.

    Returns:
    str: The path to the extracted .xml file.
    """
    xml_filename = os.path.splitext(os.path.basename(gz_filepath))[0]  # Remove .gz extension
    extract_filepath = os.path.join('/usr/local/airflow/dags/xml_files_victory', xml_filename)
    
    with gzip.open(gz_filepath, 'rb') as f_in, open(extract_filepath, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    
    return extract_filepath

def download_and_extract_file(url):
    """
    Downloads a file from a URL and extracts it if it's a .gz file.

    Args:
    url (str): The URL of the file to download.

    Returns:
    str: The path to the downloaded (and possibly extracted) file.
    """
    try:
        url = url.replace('\\', '/')  # Ensure URL uses forward slashes
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful

        parsed_url = urllib.parse.urlparse(url)
        filename = urllib.parse.unquote(os.path.basename(parsed_url.path))  # Decode URL encoded characters
        os.makedirs("/usr/local/airflow/dags/xml_files_victory", exist_ok=True)
        local_filepath = os.path.join('/usr/local/airflow/dags/xml_files_victory', filename)

        # Save the file
        with open(local_filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extract if it's a .gz file
        if filename.endswith('.gz'):
            extract_filepath = extract_xml_from_gz(local_filepath)
            os.remove(local_filepath)  # Remove the .gz file after extraction
            return extract_filepath

        return local_filepath

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {url}: {e}")
        return None

def extract_data():
    print("Extracting data from Victory website...")
    base_url = "https://laibcatalog.co.il/"
    max_pages = 1

    all_branches = []

    for page_number in range(1, max_pages + 1):
        url = base_url if page_number == 1 else f"{base_url}/?page={page_number}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving data from {url}: {e}")
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")

        if table:
            rows = table.find_all("tr")[1:]
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 8:
                    branch_name = cells[2].text.strip()
                    download_link = "https://laibcatalog.co.il/"+cells[7].find('a')['href']
                    if "PriceFull" in download_link:
                        print(f'Downloading file for branch: {branch_name}')
                        xml_file_path = download_and_extract_file(download_link)
                        if xml_file_path:
                            print(f'File saved as: {xml_file_path}')
                            all_branches.append(branch_name)
                            time.sleep(1)
                    else:
                        print(f'Skipping file for branch: {branch_name} (not a price category)')
                else:
                    print("Row does not contain enough cells:", row)
        else:
            print("Table with class 'webgrid' not found on page:", url)

    return all_branches

