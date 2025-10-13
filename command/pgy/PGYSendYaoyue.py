import json
import logging
import random
import time
import uuid
import certifi
import requests
import urllib3
from datetime import datetime

"""
    自动发送邀约接口
"""

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
# 邀约蒲公英账号表
table_id = 'tblDO9VqC6EMHGiY'
view_id = 'vewJK6XVP4'
# 微信信息表
table_id1 = 'tbl69wH9DNnlNrTT'
view_id1 = 'vewW4ixEC6'
# 大于2000表
table_id2 = 'tbliGz3IRUgjz5Jg'
view_id2 = 'vewFjbwzKs'

# 处理发送邀约逻辑的主入口
def get_table_content():
    # 获取蒲公英账号信息
    pgy_message = read_table_content(app_token, table_id, view_id)
    # 获取微信账号信息
    vx_messages = read_table_content(app_token, table_id1, view_id1)

    # 处理每个蒲公英账号
    for item in pgy_message:
        pgy_total = item.get('可用次数').get('value', [])[0]
        if pgy_total < 1:
            continue

        pgy_total1 = item.get('剩余邀约次数', 0)
        cooperateBrandName = item['账号简称'][0]['text']
        cooperateBrandId = item['账号id'][0]['text']
        token = get_text_from_items(item.get('蒲公英token', []))

        # 处理每个微信号
        for vx_message in vx_messages:
            # 获取微信加密信息
            contactInfoCiphertext = get_text_from_items(vx_message.get('微信号加密', []))
            if '微信号' not in vx_message or not vx_message['微信号']:
                continue
            contactInfo = vx_message.get('微信号', [])[0]['text']
            expectedPublishTimeStart = datetime.fromtimestamp(vx_message.get('期望开始时间', 0) / 1000).strftime('%Y-%m-%d')
            expectedPublishTimeEnd = datetime.fromtimestamp(vx_message.get('期望结束时间', 0) / 1000).strftime('%Y-%m-%d')

            # 搜索大于2000表中需要发邀约的博主信息
            sends = search_feishu_record(app_token, table_id2, view_id2, '区分列', contactInfo)
            if sends.get('code') != 0:
                continue

            for send in sends['data']['items']:
                # 准备发邀约参数
                productName = send['fields']['发送邀约产品'][0]['text'] + '小红书报备合作'
                inviteContent = send['fields']['发送邀约产品'][0]['text'] + '小红书报备合作，辛苦留个联系方式，加我时请备注博主名'
                kol_id = send['fields']['博主id'][0]['text']
                record_id = send['record_id']

                # 构建邀约数据
                data = {
                    'kolId': kol_id,
                    'cooperateBrandName': cooperateBrandName,
                    'cooperateBrandId': cooperateBrandId,
                    'productName': productName,
                    'inviteType': 1,
                    'expectedPublishTimeStart': expectedPublishTimeStart,
                    'expectedPublishTimeEnd': expectedPublishTimeEnd,
                    'inviteContent': inviteContent,
                    'contactType': 2,
                    'contactInfo': contactInfo,
                    'contactInfoCiphertext': contactInfoCiphertext,
                    'kolType': 0,
                    'brandUserId': cooperateBrandId
                }

                print(data)
                try:
                    # 发送邀约
                    sleep_time = random.uniform(5, 10)
                    time.sleep(sleep_time)
                    content_length = calculate(data)
                    send_success = send_yaoyue(content_length, token, kol_id, data)
                except Exception as e:
                    logging.error(f"发送邀约时发生异常：{e}")
                    continue

                # 更新飞书记录
                if send_success == 0:
                    try:
                        # 标记为已邀约
                        fields = {
                            '是否发送邀约': '已邀约'
                        }
                        update_record(app_token, table_id2, record_id, fields)

                        # 更新剩余邀约次数
                        pgys = search_feishu_record(app_token, table_id, view_id, '账号id', cooperateBrandId)
                        if pgys.get('code') != 0:
                            logging.warning(f"未找到匹配邀约蒲公英账号表的飞书记录：账号id为={cooperateBrandId}")
                            continue
                        record_id2 = ''
                        for pgy in pgys['data']['items']:
                            record_id2 = pgy['record_id']
                        pgy_total1 -= 1
                        fields2 = {
                            '剩余邀约次数': pgy_total1
                        }
                        update_record(app_token, table_id, record_id2, fields2)

                        pgy_total -= 1
                        if pgy_total < 1:
                            break
                    except Exception as e:
                        logging.error(f"更新飞书记录时发生异常：{e}")
                        continue
                else:
                    update_fields = {
                        '备注': send_success
                    }
                    try:
                        update_record(app_token, table_id2, record_id, update_fields)
                    except Exception as e:
                        logging.error(f"更新飞书记录时发生异常：{e}")
                        continue
            else:
                continue
            break

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

# 发送邀约
def send_yaoyue(content_length, token, kol_id, data):
    url = 'https://pgy.xiaohongshu.com/api/solar/invite/initiate_invite'
    trace_id = str(uuid.uuid4())
    headers = {
        'Content-Length': str(content_length),
        'Content-Type': 'application/json;charset=UTF-8',
        'Referer': f'https://pgy.xiaohongshu.com/solar/pre-trade/invite-form?id={kol_id}',
        'Cookie': token,
        'X-B3-Traceid': trace_id[:16]
    }
    # 发送 POST 请求
    try:
        response = requests.post(
            url,
            headers=headers,
            json=data,
            verify=False  # 禁用 SSL 验证（不推荐在生产环境中使用）
        )
        response.raise_for_status()
        response_json = response.json()
        print("Response:", response_json)
        if response_json.get('code') == 0 and response_json.get('success') == True and response_json.get('data')['hint'] == '有邀约权限':
            return 0
        else:
            return response_json.get('data')['hint']
    except requests.RequestException as e:
        print(f"发送邀约请求失败: {e}")

# 再飞书表格修改
def update_record(app_token, table_id, record_id, fields):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + get_feishu_token()
    }
    body = {
        'fields': fields
    }
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    try:
        response = requests.put(url, headers=headers, json=body, verify=certifi.where(), timeout=30)
        response_data = response.json()

        if response_data.get('msg') == 'success':
            print(f'飞书表格修改成功: {fields}')
    except requests.RequestException as e:
        print(f"更新飞书修改失败: {e}")

# 查询飞书表格信息
def read_table_content(app, table, view):
    result = []
    headers = {
        'Content-Type'  : 'application/json; charset=utf-8',
        'Authorization' : f"Bearer {get_feishu_token()}",
    }
    while True:
        try:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/search?page_size=500"
            response = requests.post(url, headers=headers, data=json.dumps({'view_id': view}), verify=False)
            response.raise_for_status()
            data = response.json()

            if 'data' not in data or 'items' not in data['data']:
                break

            for item in data['data']['items']:
                result.append(item['fields'])

            if 'page_token' not in data['data'] or not data['data']['page_token']:
                break

            page_token = data['data']['page_token']
        except requests.RequestException as e:
            print(f"查询飞书表格信息失败: {e}")
        break

    return result

# 查询飞书单条信息
def search_feishu_record(app_token, table_id, view_id, field_name, field_value):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": field_name,
                    "operator": "is",
                    "value": [field_value]
                }
            ]
        }
    }
    try:
        response = requests.post(url, headers=headers, json=data, verify=False)
        return response.json()
    except requests.RequestException as e:
        print(f"查询飞书单条信息失败: {e}")
        return None

# 更新飞书token过期表格
def update_token_info(fields):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/PJISbPC5OaihG8sCfMpc4Wohnyb/tables/tblytxdwmyqhJt7D/records"
    headers = {
        'Authorization': 'Bearer ' + get_feishu_token(),
        'Content-Type': 'application/json; charset=utf-8'
    }
    body = {
        'fields': fields
    }
    try:
        response = requests.post(url, headers=headers, json=body, verify=False)
        response_data = response.json()
        if response_data.get('msg') == 'success':
            message = f"账号简称为:{fields['账号简称']},账号id为:{fields['账号id']},得token已经过期，请及时更换，链接为：https://gxumrig3imz.feishu.cn/base/PJISbPC5OaihG8sCfMpc4Wohnyb?table=tblDO9VqC6EMHGiY&view=vewJK6XVP4"
            feishu_send_message(message)
    except requests.RequestException as e:
        print(f"飞书token过期表新增失败: {e}")

# 飞书发送消息
def feishu_send_message(message, key='5929796a-713a-4a67-bab9-15a2c7ebc8d7'):
    """飞书发送信息"""
    url = f'https://open.feishu.cn/open-apis/bot/v2/hook/{key}'
    data = {
        'msg_type': 'text',
        'content': {
            'text': message
        }
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
    except Exception as e:
        print(f"飞书消息发送失败: {e}")

# 获取飞书token
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    headers = {
        'Content-Type': 'application/json; charset=utf-8'
    }
    data = {
        'app_id': 'cli_a6e824d4363b500d',
        'app_secret': 'nW4ff1Mviwr0ZuYkF1BBhciZGOyDeBP5'
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        response_data = response.json()
        return response_data.get('app_access_token', '')
    except requests.RequestException as e:
        print(f"获取飞书Token失败: {e}")
        return None

if __name__ == '__main__':
    get_table_content()