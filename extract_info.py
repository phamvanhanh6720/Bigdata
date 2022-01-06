import re
import sys
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession


def extract_info(body: str):
    soup = BeautifulSoup(body, "lxml")
    title = soup.title.text.replace('\n', '').replace('\t', '')

    location = soup.find('div', {'class': 'address'}).find('span', class_='value').text.strip()
    raw_price = soup.find('span', class_='price').find('span', class_='value').text.strip().lower()
    words = raw_price.replace(',', '.').split()
    price = 0
    if 'tỷ' in words:
        price = float(words[0])
    elif 'triệu' in words:
        price = float(words[0]) / 1000

    raw_area = soup.find('span', class_='square').find('span', class_='value').text.strip().lower()
    words = raw_area.replace(',', '.').split()
    area = float(words[0])

    return price, area, title, location


def clean_data(x):
    body = x.body
    price, area, title, location = extract_info(body)
    url = x.url
    district = x.district
    province = x.province
    realestate_type = x.realestate_type
    unit = (price / area) * 1000

    return (url, title, realestate_type, area, price, unit, location, district, province)
