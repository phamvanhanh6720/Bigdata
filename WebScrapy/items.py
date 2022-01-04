from typing import Optional, List
from datetime import datetime
from dataclasses import dataclass
from scrapy import Item, Field

from WebScrapy.utils import normalize_text


@dataclass
class ChoTotRawNewsItem:
    raw_title: Optional[str]
    raw_price: Optional[str]
    raw_square: Optional[str]
    raw_description: Optional[str]

    raw_upload_time: Optional[List[str]]
    raw_location: Optional[str]
    raw_upload_person: Optional[str]
    raw_phone_number: Optional[str]

    raw_info: Optional[List[str]]
    url: Optional[str]

    def __post_init__(self):
        self.raw_title = normalize_text(self.raw_title)
        self.raw_price = normalize_text(self.raw_price)
        self.raw_square = normalize_text(self.raw_square)
        self.raw_description = normalize_text(self.raw_description)

        self.raw_upload_time = [normalize_text(_) for _ in self.raw_upload_time]
        self.raw_location = normalize_text(self.raw_location)
        self.raw_upload_person = normalize_text(self.raw_upload_person)
        self.raw_phone_number = normalize_text(self.raw_phone_number)

        self.raw_info = [normalize_text(_) for _ in self.raw_info]


@dataclass
class HomedyRawNewsItem:
    title: Optional[str]
    raw_price: Optional[str]
    raw_area: Optional[str]
    description: Optional[str]

    raw_upload_time: Optional[str]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]

    expire_time: Optional[str]
    furniture: Optional[List[str]]
    project: Optional[str]
    investor: Optional[str]

    convenient: Optional[List[str]]
    status: Optional[str]
    url: Optional[str]

    def __post_init__(self):
        self.title = normalize_text(self.title)
        self.raw_price = normalize_text(self.raw_price)
        self.raw_area = normalize_text(self.raw_area)
        self.description = normalize_text(self.description)

        self.raw_upload_time = normalize_text(self.raw_upload_time)
        self.location = normalize_text(self.location)
        self.upload_person = normalize_text(self.upload_person)
        self.phone_number = normalize_text(self.phone_number)

        self.expire_time = normalize_text(self.expire_time)
        self.furniture = [normalize_text(_) for _ in self.furniture]
        self.convenient = [normalize_text(_) for _ in self.convenient]
        self.project = normalize_text(self.project)
        self.investor = normalize_text(self.investor)
        self.status = normalize_text(self.status)


@dataclass
class AlonhadatItem:
    url: Optional[str]

    realestate_type: Optional[str]
    province: Optional[str]
    district: Optional[str]

    status_code: Optional[str]
    # request_headers: Optional[str]
    body: Optional[str]
    encoding: Optional[str]

    def __post_init__(self):
        if not isinstance(self.url, str):
            self.url = self.url.decode('utf-8')

        if not isinstance(self.realestate_type, str):
            self.realestate_type = self.realestate_type.decode('utf-8')

        if not isinstance(self.province, str):
            self.province = self.province.decode('utf-8')

        if not isinstance(self.district, str):
            self.district = self.district.decode('utf-8')

        if not isinstance(self.body, str):
            self.body = self.body.decode('utf-8')

        if not isinstance(self.encoding, str):
            self.encoding = self.encoding.decode('utf-8')

@dataclass
class AlonhadatRawNewsItem:
    title: Optional[str]
    raw_price: Optional[str]
    raw_area: Optional[str]
    raw_description: Optional[str]

    raw_upload_time: Optional[str]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]

    project: Optional[str]
    raw_info: Optional[List[str]]
    url: Optional[str]

    def __post_init__(self):
        self.title = normalize_text(self.title)
        self.raw_price = normalize_text(self.raw_price)
        self.raw_area = normalize_text(self.raw_area)
        self.raw_description = normalize_text(self.raw_description)

        self.raw_upload_time = normalize_text(self.raw_upload_time)
        self.location = normalize_text(self.location)
        self.upload_person = normalize_text(self.upload_person)
        self.phone_number = normalize_text(self.phone_number)

        self.project = normalize_text(self.project)
        self.raw_info = [normalize_text(_) for _ in self.raw_info]


@dataclass
class BatDongSanRawNewsItem:
    title: Optional[str]
    raw_price: Optional[str]
    raw_area: Optional[str]
    raw_room_number: Optional[str]
    description: Optional[str]
    raw_duration_time: Optional[str]
    raw_upload_time: Optional[str]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]

    url: Optional[str]

    def __post_init__(self):
        self.title = normalize_text(self.title)
        self.raw_price = normalize_text(self.raw_price)
        self.raw_area = normalize_text(self.raw_area)
        self.raw_room_number = normalize_text(self.raw_room_number)
        self.description = normalize_text(self.description)

        self.raw_duration_time = normalize_text(self.raw_duration_time)
        self.raw_upload_time = normalize_text(self.raw_upload_time)
        self.location = normalize_text(self.location)
        self.upload_person = normalize_text(self.upload_person)
        self.phone_number = normalize_text(self.phone_number)


@dataclass
class ChototNewsItem:
    title: Optional[str]
    price: Optional[float]
    area_m2: Optional[str]
    description: Optional[str]
    upload_time: Optional[datetime]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]
    # 'ca nhan' or 'moi gioi'
    news_type: Optional[str]
    url: Optional[str]


@dataclass
class HomedyNewsItem:
    title: Optional[str]
    price: Optional[float]
    area_m2: Optional[str]
    description: Optional[str]

    upload_time: Optional[datetime]
    expire_time: Optional[str]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]

    furniture: Optional[List[str]]
    convenient: Optional[List[str]]
    project: Optional[str]
    investor: Optional[str]
    status: Optional[str]

    url: Optional[str]


@dataclass
class AlonhadatNewsItem:
    title: Optional[str]
    price: Optional[float]
    area_m2: Optional[str]
    description: Optional[str]

    upload_time: Optional[datetime]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]

    project: Optional[str]
    url: Optional[str]

      
@dataclass
class BatDongSanNewsItem:
    title: Optional[str]
    price: Optional[float]
    area_m2: Optional[str]
    room_number: Optional[str]
    description: Optional[str]

    raw_duration_time: Optional[datetime]
    upload_time: Optional[datetime]
    location: Optional[str]
    upload_person: Optional[str]
    phone_number: Optional[str]
    url: Optional[str]
