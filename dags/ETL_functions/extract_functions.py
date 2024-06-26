import urllib.parse
import requests
from bs4 import BeautifulSoup
import time
import os
import gzip
import shutil

def extract_xml_from_gz(gz_filepath):
    xml_filename = os.path.splitext(os.path.basename(gz_filepath))[0] + '.xml'
    extract_filepath = os.path.join('/usr/local/airflow/dags/xml_files', xml_filename)
    
    with gzip.open(gz_filepath, 'rb') as f_in, open(extract_filepath, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    
    return extract_filepath

def download_and_extract_file(url):
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            parsed_url = urllib.parse.urlparse(url)
            filename = urllib.parse.unquote(os.path.basename(parsed_url.path))  # Decode URL encoded characters
            os.makedirs("/usr/local/airflow/dags/xml_files", exist_ok=True)
            local_filepath = os.path.join('/usr/local/airflow/dags/xml_files', filename)

            with open(local_filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if filename.endswith('.gz'):
            extract_filepath = extract_xml_from_gz(local_filepath)
            os.remove(local_filepath)
            return extract_filepath

        return local_filepath

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {url}: {e}")
        return None

def extract_data():
    print("Extracting data from Shufersal website...")
    base_url = "https://prices.shufersal.co.il/"
    max_pages = 103

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
        table = soup.find("table", class_="webgrid")

        if table:
            rows = table.find_all("tr")[1:]
            for row in rows:
                row_classes = row.get("class", [])
                if "webgrid-row-style" in row_classes or "webgrid-alternating-row" in row_classes:
                    cells = row.find_all("td")
                    if len(cells) >= 6:
                        branch_name = cells[5].text.strip()
                        download_link = cells[0].find('a')['href']
                        if "/pricefull/" in download_link:
                            print(f'Downloading file for branch: {branch_name}')
                            xml_file_path = download_and_extract_file(download_link)
                            if xml_file_path:
                                print(f'File saved as: {xml_file_path}')
                                all_branches.append(branch_name)
                                time.sleep(1)  # Optional: pause briefly to avoid overwhelming the server with requests
                        else:
                            print(f'Skipping file for branch: {branch_name} (not a price category)')
                    else:
                        print("Row does not contain enough cells:", row)
        else:
            print("Table with class 'webgrid' not found on page:", url)

    return all_branches
