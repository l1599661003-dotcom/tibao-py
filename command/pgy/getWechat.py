import json
import logging
import random
import re
import time
import uuid

import requests
import schedule
import urllib3

from service.feishu_service import update_token_info, search_feishu_record, update_record, read_table_content
from service.pgy_service import get_reply_message, get_vx_parameter
from unitl.unitl import get_text_from_items

logging.captureWarnings(True)
logger = logging.getLogger('urllib3')
logger.setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 待建联博主再用
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbliGz3IRUgjz5Jg'
view_id = 'vewFjbwzKs'
# token表
app_token2 = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id2 = 'tblDO9VqC6EMHGiY'
view_id2 = 'vewJ9nETr9' #1
#view_id2 = 'vewseFTzpf' #2
# view_id2 = 'vewsBw8bqh' #3
# view_id2 = 'vewISzDhNy' #4
# view_id2 = 'vewaWVrpzP' #5
# view_id2 = 'vewGCmuZ6O' #6
# view_id2 = 'vew4h9hl27' #7
# view_id2 = 'vewsJpJbIG' #8
# view_id2 = 'vewfIwp7is' #9
# view_id2 = 'vew7Ub1b7k' #10
# view_id2 = 'vewxDmcxcd' #11
# view_id2 = 'vewKdieV2D' #12
# view_id2 = 'vewHLbaddn' #13
# view_id2 = 'vewOvmHCGy' #14
# view_id2 = 'vewXQ0yclF' #15
# view_id2 = 'vewpoCBmMe' #16
# view_id2 = 'vew9Glo5Hz' #17
# view_id2 = 'vew4MxhuUh' #18

def get_table_content():
    data = read_table_content(app_token2, table_id2, view_id2)

    if data is None or len(data) == 0:
        print('获取蒲公英 Token 数据失败或格式不正确')
        return

    for item in data:
        token = get_text_from_items(item['fields'].get('蒲公英token', []))
        cooperateBrandName = item['fields']['账号简称'][0]['text']
        cooperateBrandId = item['fields']['账号id'][0]['text']
        res = get_reply_message(token)
        if res is None:
            continue
        total = res.get('data', {}).get('total', 0)
        total_pages = - (- total // 100)
        for i in range(1, total_pages):

            sleep_time = random.uniform(3, 6)
            time.sleep(sleep_time)
            res = get_reply_message(token, i)
            if res is None:
                continue

            if res.get('code') == 902 or res.get('code') == 903 or res.get('code') == 906:
                fields = {
                    '账号简称': str(cooperateBrandName),
                    'token过期信息': str(token),
                    '账号id': str(cooperateBrandId),
                }
                update_token_info(fields)
                break

            for message in res.get('data', {}).get('messageList', []):

                sleep_time = random.uniform(3, 6)
                time.sleep(sleep_time)
                kol = get_vx_parameter(token, message.get('content', ''))

                if kol is None:
                    continue

                fields = {
                    '博主是否预留联系方式': '否'
                }

                if isinstance(kol['fpweixin'], dict) and kol['fpweixin'].get('text'):
                    fields['方片微信号'] = str(kol['fpweixin'].get('text'))

                # 检查 wechat 是否有值
                if isinstance(kol['wechat'], dict) and kol['wechat'].get('text'):
                    fields['微信号'] = str(kol['wechat'].get('text'))
                    fields['博主是否预留联系方式'] = '是'

                # 检查 phone 是否有值
                if isinstance(kol['phone'], dict) and kol['phone'].get('text'):
                    fields['博主手机号'] = str(kol['phone'].get('text'))
                    fields['博主是否预留联系方式'] = '是'

                result = search_feishu_record(app_token, table_id, view_id, '博主id', kol['kolId'])
                try:
                    if result.get('msg') == 'success' and result.get('data', {}).get('items'):
                        for item in result.get('data', {}).get('items', []):
                            if item.get('博主是否预留联系方式'):
                                break
                        record_id = result['data']['items'][0]['record_id']
                        update_record(app_token, table_id, record_id, fields)
                except Exception as e:
                    continue
            if i > total_pages:
                break

if __name__ == '__main__':
    get_table_content()
    # schedule.every().day.at('04:00').do(get_table_content)
    #
    # # 如果你想在程序运行时一直保持调度
    # print("调度器已启动，按 Ctrl+C 退出...")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)