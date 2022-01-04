import re
import unicodedata
from typing import Optional

from datetime import datetime, timedelta


def normalize_text(text: Optional[str]) -> Optional[str]:

    if isinstance(text, str):
        text = text.lower()
        norm_text: str = unicodedata.normalize('NFC', text)

        return norm_text
    else:
        return None


def process_upload_time(raw_upload_time: str) -> datetime:

    upload_time: datetime = datetime.now()

    if 'hôm qua' in raw_upload_time:
        upload_time = upload_time - timedelta(days=1)
    else:
        num_list = list(map(int, re.findall(r'\d+', raw_upload_time)))
        previous_time: int = num_list[0] if len(num_list) else None

        if 'phút' in raw_upload_time:
            upload_time = upload_time - timedelta(minutes=previous_time)
        elif 'giờ' in raw_upload_time:
            upload_time = upload_time - timedelta(hours=previous_time)
        elif 'ngày' in raw_upload_time:
            upload_time = upload_time - timedelta(days=previous_time)
        elif 'tuần' in raw_upload_time:
            upload_time = upload_time - timedelta(weeks=previous_time)
        elif 'tháng' in raw_upload_time:
            upload_time = upload_time - timedelta(days=previous_time * 30)
        elif 'năm' in raw_upload_time:
            upload_time = upload_time - timedelta(days=previous_time * 365)

    return upload_time
