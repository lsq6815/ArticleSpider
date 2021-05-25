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
import pika
import requests

import settings

Image_Urls = set()
Channel = None # 消息通道


def process_page(page_url: str) -> set[str]:
    """
    对网页进行提取。需要提取的内容为：新闻的标题、内容、链接、发布时间、新闻图片（需下载原图）

    :param page_url str: 文章链接
    """
    try:
        html = requests.get(page_url)
        html.raise_for_status()
    except requests.exceptions.RequestException:
        print(f"\tHttp Request Failed for: {page_url}")
        return set()

    soup = BeautifulSoup(html.content, features='lxml')
    page = {}

    # TODO：根据年份决定对应的 CSS Selector
    try:
        # 数据提取
        page['link'] = page_url
        page['title'] = soup.select_one('.rm_txt .col-1 h1').text.strip()
        # use ,
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

    process_page_images(soup, page, page_url)

    # Log info
    print(f"\tTitle: {page['title']}\n\tImage: {page['images']}")

    Channel.basic_publish(exchange='',
                          routing_key=settings.QUEUE_NAME,
                          body=json.dumps(page),
                          properties=pika.BasicProperties(
                              delivery_mode=2
                          ))
    # # 保存到本地
    # filename = ''.join((ch if ch.isalnum() else '_')
    #                    for ch in page_url) + 'html.json'
    # # write file in json
    # with open(filename, 'w', encoding='utf8') as f:
    #     json.dump(page, f, ensure_ascii=False)

    return select_internal_link(soup, page_url)


def process_page_images(soup: BeautifulSoup, page: dict, page_url: str):
    """
    处理文章中的图片

    :param soup BeautifulSoup: 网页的 DOM 树
    :param page dict: 保存提取出的信息的字典
    :param page_url str: 网页的 URL
    """
    # 单独处理图片
    try:
        # 抓取文章图片
        page['images'] = [img.attrs['src'] for img in soup.select(
            '.rm_txt_con.cf img[src]') if img.attrs['src'].startswith('/NMediaFile')]

        # 更新 image_urls
        res = urlp.urlparse(page_url)
        prefix = f"{res.scheme}://{res.hostname}"
        Image_Urls.update([prefix + src for src in page['images']])
    except (AttributeError):
        print(f"\tImage Paser error occur when procssing: {page_url}")
        page['images'] = []


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
            if domain in settings.ALLOWED_DOMAINS:
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
    store_path = f"{settings.IMAGE_DIR}/{filename}"
    with open(store_path, 'wb') as f:
        f.write(image.content)


if __name__ == "__main__":
    # connect to channel
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    Channel = connection.channel()
    # declare a queue
    Channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)

    response = requests.get(settings.ENTRY_URL)
    soup = BeautifulSoup(response.content, features='lxml')
    unvisited_urls = select_internal_link(soup, settings.ENTRY_URL)

    visited_urls = set(settings.ENTRY_URL)
    # 执行 BFS
    total = 0
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    try:
        while unvisited_urls:
            unvisited_url = unvisited_urls.pop()
            print(f"{unvisited_url}")

            internal_urls = process_page(unvisited_url)
            visited_urls.add(unvisited_url)
            unvisited_urls.update(internal_urls - visited_urls)
            total += 1

            # 失败的并发尝试
            # works = {}
            # for _ in range(5):
            #     try: 
            #         unvisited_url = unvisited_urls.pop()
            #     except Exception:
            #         break

            #     works[executor.submit(process_page, unvisited_url)] = unvisited_url
            #     print(f"{unvisited_url}")

            # for future in concurrent.futures.as_completed(works):
            #     url = works[future]
            #     try:
            #         internal_urls = future.result()
            #         visited_urls.add(url)
            #         unvisited_urls.update(internal_urls - visited_urls)
            #     except Exception:
            #         pass

    except KeyboardInterrupt:
        print("Detech keyboard interruption, exit...")

    print("Close Connection...")
    connection.close()

    # 处理图片下载
    if not os.path.exists(settings.IMAGE_DIR):
        os.makedirs(settings.IMAGE_DIR)

    # 使用多线程加速下载
    threads = min(settings.MAX_THREADS, len(Image_Urls))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_image, Image_Urls)

    print(f"Total: {total}, Image downloaded: {len(Image_Urls)}")
