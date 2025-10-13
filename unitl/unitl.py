import json
import random
import time
from datetime import datetime


# 从字典列表中提取 'text' 字段并拼接为字符串
def get_text_from_items(items):
    result = ''
    for item in items:
        if isinstance(item, dict) and 'text' in item:
            result += item['text']
    return result

# 计算 Content-Length
def calculate(params):
    json_string = json.dumps(params, ensure_ascii=False, separators=(',', ':'))
    return len(json_string.encode('utf-8'))

# 获取日期
def get_month_range():
    # 获取当前日期
    today = datetime.now()

    # 获取当月的第一天
    date_begin = today.replace(day=1).strftime('%Y-%m-%d')

    # 获取下个月的第一天
    if today.month == 12:
        # 如果是12月，下个月是1月，年份加1
        next_month = today.replace(year=today.year + 1, month=1, day=1)
    else:
        # 其他月份，月份加1
        next_month = today.replace(month=today.month + 1, day=1)
        date_end = next_month.strftime('%Y-%m-%d')

    return {
        'dateBegin': date_begin,
        'dateEnd': date_end,
    }

# 随机暂停时间
def random_sleep(self, min_seconds=1, max_seconds=5):
    """随机等待一段时间，避免被反爬"""
    time.sleep(random.uniform(min_seconds, max_seconds))