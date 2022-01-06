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
    price = None
    if 'tỷ' in words:
        try:
            price = float(words[0])
        except:
            pass
    elif 'triệu' in words:
        try:
            price = float(words[0]) / 1000
        except:
            pass

    raw_area = soup.find('span', class_='square').find('span', class_='value').text.strip().lower()
    words = raw_area.replace(',', '.').split()
    area = None
    try:
        area = float(words[0])
    except:
        pass

    return price, area, title, location


def clean_data(x):
    body = x.body
    unit = None
    price = None
    area = None
    title = None
    location = None
    url = x.url
    district = x.district
    province = x.province
    realestate_type = x.realestate_type
    if isinstance(body, str):
        try:
            price, area, title, location = extract_info(body)

            if price is not None and area is not None:
                unit = (price / area) * 1000
        except:
            pass
    return (url, title, realestate_type, area, price, unit, location, district, province)


spark = SparkSession.builder\
    .appName('CleanData')\
    .config("spark.mongodb.output.uri", "mongodb+srv://manhtruong:123456aA@cloudcluster.kq0xr.mongodb.net/myfirstDatabase:realestate.cleaned_data")\
    .getOrCreate()

columns = ["Url", "Title", "Type", "Area (m2)", "Price (B)", "m/m2","Location", "District", "Province"]
raw_df = spark.read.json("hdfs://namenode:9000/alonhadatnews_json")
rdd_raw = raw_df.rdd

cleaned_rdd = rdd_raw.map(lambda x: clean_data(x))
cleaned_df = cleaned_rdd.toDF(columns)
# cleaned_df.write.mode('overwrite').csv('/opt/spark-data/cleaned_data.csv')
cleaned_df.write.format("mongo").mode("append").save()
