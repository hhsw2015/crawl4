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

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

csv_file = "xxxclub_data.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5
COMMIT_INTERVAL = 1000  # 每 1000 条提交一次
MAX_WORKERS = 10  # 线程数

def init_csv():
    """初始化 CSV 文件，如果文件不存在则创建并写入表头"""
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["id", "title", "size", "magnet"])
        logging.info("Initialized new CSV file")
    else:
        logging.info(f"CSV file '{csv_file}' already exists, skipping initialization")

def configure_git_lfs():
    """配置 Git LFS 来跟踪 CSV 文件"""
    try:
        subprocess.run(["git", "lfs", "track", csv_file], check=True)
        logging.info(f"Configured Git LFS to track {csv_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error configuring Git LFS: {e.stderr}")
        raise

def git_commit(message):
    """提交 CSV 文件到 Git 仓库，始终使用 LFS"""
    try:
        # 始终使用 Git LFS
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
        # 如果提交失败，记录错误但不中断程序
        pass

def extract_magnet_prefix(magnet_link):
    """提取磁力链接的前缀，只保留 magnet:?xt=urn:btih:hash 部分"""
    if magnet_link.startswith("magnet:?xt=urn:btih:"):
        # 找到第一个 & 的位置，截取前面的部分
        end_index = magnet_link.find("&")
        if end_index != -1:
            return magnet_link[:end_index]
        return magnet_link  # 如果没有 &，返回整个链接（理论上不会发生）
    return "N/A"

def crawl_page(t_id, retries=0):
    """爬取单个页面并提取数据，带重试机制"""
    url = f"https://xxxclub.to/torrents/details/{t_id}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # 提取标题
        title_tag = soup.find("h1")
        title = title_tag.text.strip() if title_tag else "N/A"

        # 提取 detailsdescr 内的数据
        details = soup.find("div", class_="detailsdescr")
        size = "N/A"
        magnet = "N/A"
        if details:
            ul = details.find("ul")
            if ul:
                for li in ul.find_all("li"):
                    spans = li.find_all("span")
                    if len(spans) >= 3:
                        key = spans[0].text.strip()
                        value = spans[2].text.strip()
                        if key == "Size":
                            size = value
                    # 提取磁力链接并只保留前缀
                    if "downloadboxlist" in li.get("class", []):
                        magnet_link = li.find("a", class_="mg-link btn b-o-s")
                        if magnet_link and "href" in magnet_link.attrs:
                            magnet = extract_magnet_prefix(magnet_link["href"])

        logging.info(f"Scraped ID {t_id}: {title}, Size: {size}, Magnet: {magnet}")
        return {"id": t_id, "title": title, "size": size, "magnet": magnet}

    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
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
    """主爬取逻辑，使用多线程并按 ID 顺序写入 CSV"""
    init_csv()
    tasks = list(range(start_id, end_id + 1))  # 从 start_id 到 end_id
    total_tasks = len(tasks)
    logging.info(f"Total tasks to process: {total_tasks}")

    # 分批处理，每 COMMIT_INTERVAL 一批
    for batch_start in range(0, total_tasks, COMMIT_INTERVAL):
        batch_end = min(batch_start + COMMIT_INTERVAL, total_tasks)
        batch_tasks = tasks[batch_start:batch_end]
        results = [None] * len(batch_tasks)  # 预分配结果列表

        # 多线程爬取当前批次
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_index = {executor.submit(crawl_page, t_id): i for i, t_id in enumerate(batch_tasks)}
            for future in tqdm(as_completed(future_to_index), total=len(batch_tasks), desc=f"Crawling IDs {batch_tasks[0]} to {batch_tasks[-1]}"):
                index = future_to_index[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    logging.error(f"Error in future for ID {batch_tasks[index]}: {e}")
                    results[index] = {"id": batch_tasks[index], "title": "N/A", "size": "N/A", "magnet": "N/A"}

        # 按 ID 顺序写入 CSV
        with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for result in results:
                if result:
                    writer.writerow([result["id"], result["title"], result["size"], result["magnet"]])

        # 提交当前批次
        git_commit(f"Update data for IDs {batch_tasks[0]} to {batch_tasks[-1]}")
        logging.info(f"Processed and committed batch: IDs {batch_tasks[0]} to {batch_tasks[-1]}")

if __name__ == "__main__":
    logging.info("Starting crawl...")
    start_id = int(os.getenv("START_ID", 1))
    end_id = int(os.getenv("END_ID", 1000))
    crawl_torrent_pages(start_id, end_id)
    logging.info(f"Data saved to {csv_file}")
