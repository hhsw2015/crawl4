import requests
from bs4 import BeautifulSoup
import csv
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
import subprocess
import logging
import random

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
]

csv_file = "xxxclub_data.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5
COMMIT_INTERVAL = 1000
MAX_WORKERS = 3  # 减少并发

def init_csv():
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["id", "title", "size", "magnet"])
        logging.info("Initialized new CSV file")
    else:
        logging.info(f"CSV file '{csv_file}' already exists, skipping initialization")

def configure_git_lfs():
    try:
        subprocess.run(["git", "lfs", "track", csv_file], check=True)
        logging.info(f"Configured Git LFS to track {csv_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error configuring Git LFS: {e.stderr}")
        raise

def git_commit(message):
    try:
        configure_git_lfs()
        subprocess.run(["git", "add", csv_file], check=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"No changes to commit: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")
        pass

def extract_magnet_prefix(magnet_link):
    if magnet_link.startswith("magnet:?xt=urn:btih:"):
        end_index = magnet_link.find("&")
        return magnet_link[:end_index] if end_index != -1 else magnet_link
    return "N/A"

def crawl_page(t_id, retries=0):
    url = f"https://xxxclub.to/torrents/details/{t_id}"
    headers = {"User-Agent": random.choice(user_agents)}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find("h1")
        title = title_tag.text.strip() if title_tag else "N/A"

        details = soup.find("div", class_="detailsdescr")
        size = "N/A"
        magnet = "N/A"
        if details:
            ul = details.find("ul")
            if ul:
                for li in ul.find_all("li"):
                    spans = li.find_all("span")
                    if len(spans) >= 3 and spans[0].text.strip() == "Size":
                        size = spans[2].text.strip()
                    if "downloadboxlist" in li.get("class", []):
                        magnet_link = li.find("a", class_="mg-link btn b-o-s")
                        if magnet_link and "href" in magnet_link.attrs:
                            magnet = extract_magnet_prefix(magnet_link["href"])

        logging.info(f"Scraped ID {t_id}: {title}, Size: {size}, Magnet: {magnet}")
        time.sleep(random.uniform(1, 3))  # 随机延迟
        return {"id": t_id, "title": title, "size": size, "magnet": magnet}

    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}, Response: {e.response.text if e.response else 'No response'}")
        if retries < MAX_RETRIES:
            logging.info(f"Retrying {url} ({retries + 1}/{MAX_RETRIES}) after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return crawl_page(t_id, retries + 1)
        else:
            logging.error(f"Max retries ({MAX_RETRIES}) reached for {url}. Returning N/A.")
            return {"id": t_id, "title": "N/A", "size": "N/A", "magnet": "N/A"}
    except Exception as e:
        logging.error(f"Unexpected error on {url}: {e}")
        return {"id": t_id, "title": "N/A", "size": "N/A", "magnet": "N/A"}

def crawl_torrent_pages(start_id, end_id):
    init_csv()
    tasks = list(range(start_id, end_id + 1))
    total_tasks = len(tasks)
    logging.info(f"Total tasks to process: {total_tasks}")

    for batch_start in range(0, total_tasks, COMMIT_INTERVAL):
        batch_end = min(batch_start + COMMIT_INTERVAL, total_tasks)
        batch_tasks = tasks[batch_start:batch_end]
        results = [None] * len(batch_tasks)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_index = {executor.submit(crawl_page, t_id): i for i, t_id in enumerate(batch_tasks)}
            for future in tqdm(as_completed(future_to_index), total=len(batch_tasks), desc=f"Crawling IDs {batch_tasks[0]} to {batch_tasks[-1]}"):
                index = future_to_index[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    logging.error(f"Error in future for ID {batch_tasks[index]}: {e}")
                    results[index] = {"id": batch_tasks[index], "title": "N/A", "size": "N/A", "magnet": "N/A"}

        with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for result in results:
                if result:
                    writer.writerow([result["id"], result["title"], result["size"], result["magnet"]])

        git_commit(f"Update data for IDs {batch_tasks[0]} to {batch_tasks[-1]}")
        logging.info(f"Processed and committed batch: IDs {batch_tasks[0]} to {batch_tasks[-1]}")
        time.sleep(random.uniform(5, 10))  # 批次间延迟

if __name__ == "__main__":
    logging.info("Starting crawl...")
    start_id = int(os.getenv("START_ID", 1))
    end_id = int(os.getenv("END_ID", 1000))
    crawl_torrent_pages(start_id, end_id)
    logging.info(f"Data saved to {csv_file}")
