import requests
from bs4 import BeautifulSoup
import re
import csv
import os
from urllib.parse import urlparse
import concurrent.futures
import threading
from tenacity import retry, stop_after_attempt, wait_fixed
import time
import json

file_lock = threading.Lock()

url = "https://unpkg.com/codes-postaux@4.0.0/codes-postaux.json"

response = requests.get(url)

data = response.json()

postal_codes = {item['codePostal']: item['nomCommune'] for item in data}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}

keys = ["url", "commune", "postal_code", "phone_numbers", "email_addresses"]

file_exists = os.path.isfile('scraped_data_new.csv')

seen_domains = set()
seen_domains_lock = threading.Lock()


# Define a retry decorator for requests.get
@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
def get_with_retry(url, headers=None, verify=False):
    return requests.get(url, headers=headers, verify=verify, timeout=5)


# Function to process each link
def process_link(link_url, commune_name, postal_code):
    if link_url.endswith('.pdf'):
        return []
    domain = urlparse(link_url).netloc
    with seen_domains_lock:
        if domain in seen_domains:
            return []
        seen_domains.add(domain)
    print(link_url)

    for attempt in range(3):  # Try up to 3 times
        try:
            link_response = get_with_retry(link_url, headers=headers, verify=False)
            break  # If the request was successful, break out of the loop
        except requests.exceptions.RequestException as e:
            print(f"Une erreur s'est produite lors de la récupération de l'URL {link_url}: {e}")
            if attempt < 2:  # Don't sleep after the last attempt
                time.sleep(3)  # Wait for 3 seconds before trying again
            else:
                return []  # If all attempts failed, return an empty list

    link_soup = BeautifulSoup(link_response.text, 'html.parser')

    link_content = link_soup.prettify()

    phone_numbers = list(set(re.findall(r'0\d(?: \d{2}){4}', link_content)))

    email_addresses = list(set(re.findall(r'[\w\.-]+(?:@|\[a\])[\w\.-]+\.\w+', link_content)))

    print(email_addresses)

    if phone_numbers or email_addresses:
        data = {
            "url": link_url,
            "commune": commune_name,
            "postal_code": postal_code,
            "phone_numbers": ', '.join(phone_numbers),
            "email_addresses": ', '.join(email_addresses)
        }
        with file_lock:  # Lock the file for writing
            file_exists = os.path.isfile('scraped_data_new.csv')
            with open('scraped_data_new.csv', 'a', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                if not file_exists:
                    dict_writer.writeheader()
                dict_writer.writerow(data)
        return [data]
    else:
        return []


# Function to process each postal code
def process_postal_code(postal_code, commune_name):
    print(postal_code)

    url = "https://google.serper.dev/search"
    payload = json.dumps({
        "q": f'"réalisé par solocal" "{postal_code}"',
        "gl": "fr",
        "hl": "fr",
        "autocorrect": False
    })
    headers_serper = {
        'X-API-KEY': '',
        'Content-Type': 'application/json'
    }

    for attempt in range(3):  # Try up to 3 times
        try:
            response = requests.request("POST", url, headers=headers_serper, data=payload)
            if response.status_code != 200:
                raise requests.exceptions.HTTPError(f'Unexpected status code: {response.status_code}')
            break  # If the request was successful, break out of the loop
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            print(f"Une erreur s'est produite lors de la récupération de l'URL avec params {payload}: {e}")
            if attempt < 2:  # Don't sleep after the last attempt
                time.sleep(3)  # Wait for 3 seconds before trying again
            else:
                return []  # If all attempts failed, return an empty list

    # Parse the response to get the links
    results = response.json().get('organic', [])
    links = [result.get('link') for result in results if result.get('link')]

    print(links)

    data_list = []

    # Use a ThreadPoolExecutor to process multiple links in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_link = {executor.submit(process_link, link, commune_name, postal_code): link
                          for link in links}

        for future in concurrent.futures.as_completed(future_to_link):
            link = future_to_link[future]
            try:
                link_data_list = future.result()
            except Exception as exc:
                print(f'{link} generated an exception: {exc}')
            else:
                data_list.extend(link_data_list)

    return data_list


with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    future_to_postal_code = {executor.submit(process_postal_code, postal_code, commune_name): postal_code
                             for postal_code, commune_name in postal_codes.items()}

    for future in concurrent.futures.as_completed(future_to_postal_code):
        postal_code = future_to_postal_code[future]
        try:
            data_list = future.result()
        except Exception as exc:
            print(f'{postal_code} generated an exception: {exc}')
        else:
            # No need to write to the file here since it's done in process_link
            pass
