#!/usr/bin/env python3
"""
爬取《人民网-社会法制-频道首页》的所有内容，
频道入口链接：http://society.people.com.cn/GB/index.html；
爬取时过滤掉外链和其它频道的链接。
"""
import concurrent.futures
import json
import os
import urllib.parse as urlp

from bs4 import BeautifulSoup
import requests
from requests.exceptions import HTTPError

image_urls = set()

# Constants defined here
# 入口地址
ENTRY_URL = "http://society.people.com.cn/GB/index.html"
# 限定爬取的 Domain（人民网通过 Domain 前缀区分频道）
ALLOWED_DOMAINS = {
    'society.people.com.cn',  # 社会
    'legal.people.com.cn',   # 法制
}
MAX_THREADS = 30
IMAGE_DIR = "images"


def process_page(page_url: str) -> set[str]:
    """
    需要提取的内容为：新闻的标题、内容、链接、发布时间、新闻图片（需下载原图）

    :param page_url str: 文章链接
    """
    html = requests.get(page_url)
    soup = BeautifulSoup(html.content, features='lxml')
    page = {}
    # TODO：根据年份决定对应的 CSS Selector
    try:
        page['link'] = page_url
        page['title'] = soup.select_one('.rm_txt .col-1 h1').text.strip()
        date, source = soup.select_one('.channel .col-1-1').text.split('|')
        page['metadata'] = {
            'date': date.strip(),
            'source': source.strip()
        }
        page['content'] = soup.select_one(
            '.col.col-1 div.rm_txt_con.cf').text.strip()
    except (AttributeError, KeyError):
        print(f"\tPaser error occur when procssing: {page_url}")
        return set()

    # 单独处理图片
    try:
        page['images'] = [img.attrs['src'] for img in soup.select(
            '.rm_txt_con.cf img[src]') if img.attrs['src'].startswith('/NMediaFile')]
        res = urlp.urlparse(page_url)
        prefix = f"{res.scheme}://{res.hostname}"
        image_urls.update([prefix + src for src in page['images']])
    except (AttributeError):
        page['images'] = []

    print(f"\tTitle: {page['title']}\n\tImage: {page['images']}")

    # 保存到本地
    filename = ''.join((ch if ch.isalnum() else '_')
                       for ch in page_url) + 'html.json'
    with open(filename, 'w', encoding='utf8') as f:
        json.dump(page, f, ensure_ascii=False)

    # write file in json
    return select_internal_link(soup, page_url)


def select_internal_link(soup: BeautifulSoup, url: str) -> set[str]:
    """
    选择网页中的内链

    :param soup BeautifulSoup: 网页的 DOM 树
    :param url str: 网页的 URL
    :rtype set[str]: 包含内链的集合
    """
    links = set()

    res = urlp.urlparse(url)
    # 用于将相对路径组合成绝对路径
    prefix = f"{res.scheme}://{res.hostname}"

    for link in soup.select('a[href]'):
        href: str = link['href']

        # `#` 这个语法只是指向同一页面的不同位置，要去除
        href = href.split('#')[0] if '#' in href else href

        # 如果是绝对路径
        if href.startswith('http'):
            # 解析出 Domain
            domain = urlp.urlparse(href).hostname
            # 如果主题是社会或法制
            if domain in ALLOWED_DOMAINS:
                links.add(href)
        # 如果是相对路径
        elif href.startswith('/'):
            # 构造绝对路径
            absolute_path = prefix + href
            links.add(absolute_path)

    return links


def download_image(image_url: str):
    """
    通过 URL 下载图片，并保存在 IMAGE_DIR

    :param image_url str: 图片的 URL
    """
    print(f"Downloading: {image_url}")
    image = requests.get(image_url)
    filename = urlp.urlparse(image_url).path.replace('/', '_')
    store_path = f"{IMAGE_DIR}/{filename}"
    with open(store_path, 'wb') as f:
        f.write(image.content)


if __name__ == "__main__":
    response = requests.get(ENTRY_URL)

    if not response.ok or response.content is None:
        raise HTTPError

    soup = BeautifulSoup(response.content, features='lxml')
    unvisited_urls = select_internal_link(soup, ENTRY_URL)

    visited_urls = set(ENTRY_URL)
    # 执行 BFS
    total = 0
    try:
        while unvisited_urls:
            unvisited_url = unvisited_urls.pop()
            print(f"{unvisited_url}")

            internal_urls = process_page(unvisited_url)
            visited_urls.add(unvisited_url)
            unvisited_urls.update(internal_urls - visited_urls)
            total += 1
    except KeyboardInterrupt:
        print("Detech keyboard interruption, exit...")

    # 处理图片下载
    if not os.path.exists(IMAGE_DIR):
        os.makedirs(IMAGE_DIR)

    # 使用多线程加速下载
    threads = min(MAX_THREADS, len(image_urls))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_image, image_urls)

    print(f"Total: {total}")
