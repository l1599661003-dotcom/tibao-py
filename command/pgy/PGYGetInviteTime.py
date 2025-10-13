import json
import random
import time
import uuid
from datetime import datetime
import certifi
import requests
import urllib3

"""
    自动获取蒲公英邀约次数接口
"""

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# 邀约蒲公英账号表
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tblDO9VqC6EMHGiY'
view_id = 'vewJK6XVP4'

# 获取飞书请求头
def get_feishu_headers():
    return {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
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
# 查询飞书表格信息
def read_table_content(app, table, view):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/search?page_size=500"
    headers = {
        'Content-Type'  : 'application/json; charset=utf-8',
        'Authorization' : f"Bearer {get_feishu_token()}",
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps({'view_id': view}), verify=False)
        response.raise_for_status()
        response_data = response.json()

        # 获取总记录数和第一页数据
        total = response_data.get("data", {}).get("total", 0)
        items = response_data.get("data", {}).get("items", [])
        page_token = response_data.get("data", {}).get("page_token", "")

        # 如果总记录数大于 500，继续分页查询
        if total > 500:
            page = (total + 499) // 500  # 计算总页数
            for i in range(1, page):
                # 添加 page_token 参数
                paginated_url = f"{url}&page_token={page_token}"
                paginated_response = requests.post(paginated_url, headers=headers, data=json.dumps({'view_id': view}), verify=False)
                paginated_data = paginated_response.json()

                # 合并当前页数据
                items.extend(paginated_data.get("data", {}).get("items", []))
                page_token = paginated_data.get("data", {}).get("page_token", "")

                # 如果没有下一页，退出循环
                if not page_token:
                    break
        return items
    except requests.RequestException as e:
        print(f"查询飞书表格信息失败: {e}")
        return None
# 再飞书表格修改
def update_record(app_token, table_id, record_id, fields):
    headers = get_feishu_headers()
    body = {
        'fields': fields
    }
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    try:
        response = requests.put(url, headers=headers, json=body, verify=certifi.where(), timeout=30)
        response_data = response.json()
        if response_data.get('msg') == 'success':
            print(f'飞书表格修改成功: {fields}')
        else:
            print(response_data)
    except requests.RequestException as e:
        print(f"更新飞书修改失败: {e}")
# 从字典列表中提取 'text' 字段并拼接为字符串
def get_text_from_items(items):
    result = ''
    for item in items:
        if isinstance(item, dict) and 'text' in item:
            result += item['text']
    return result
def search_feishu_record(app_token, table_id, view_id, field_name, field_value):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
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
        # 第一次查询
        response = requests.post(url, headers=headers, json=data, verify=False)
        response_data = response.json()

        # 获取总记录数和第一页数据
        total = response_data.get("data", {}).get("total", 0)
        items = response_data.get("data", {}).get("items", [])
        page_token = response_data.get("data", {}).get("page_token", "")

        # 如果总记录数大于 500，继续分页查询
        if total > 500:
            page = (total + 499) // 500  # 计算总页数
            for i in range(1, page):
                # 添加 page_token 参数
                paginated_url = f"{url}&page_token={page_token}"
                paginated_response = requests.post(paginated_url, headers=headers, json=data, verify=False)
                paginated_data = paginated_response.json()

                # 合并当前页数据
                items.extend(paginated_data.get("data", {}).get("items", []))
                page_token = paginated_data.get("data", {}).get("page_token", "")

                # 如果没有下一页，退出循环
                if not page_token:
                    break

        return items

    except requests.RequestException as e:
        print(f"查询飞书单条信息失败: {e}")
        return None
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
# 获取蒲公英邀约次数
def get_yaoyue_total(token, name, pgy_id):
    url = "https://pgy.xiaohongshu.com/api/solar/invite/get_invites_overview"
    trace_id = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': token,
        'X-B3-Traceid': trace_id[:16]
    }
    data = get_month_range()
    try:
        sleep_time = random.uniform(1, 3)
        time.sleep(sleep_time)
        response = requests.post(url, headers=headers, json=data, verify=False)
        response_json = response.json()
        print(response_json)

        if response_json.get('code') == 0:
            return 300 - response_json['data']['total']
        elif response_json.get('code') == 903 or response_json.get('code') == 906 or response_json.get('code') == 902:
            fields = {
                '账号简称':str(name),
                'token过期信息': str(token),
                '账号id': str(pgy_id),
            }
            # update_token_info(fields)
            return None
        else:
            print(f"获取邀约次数消息失败，失败信息：{response_json}")
            return None
    except requests.RequestException as e:
        print(f"获取消息失败: {e}")
        return
# 发送加急电话
def send_message(message):
    # receive_id = "ou_76bb47b72c5dd6efe1b9d1729e111c16"
    receive_id = "ou_27bc9427b2a6e3fea32fc4988d90644f"
    receive_id_type = {"receive_id_type": "open_id"}
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    payload = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({
            "text": message,
        }),
    }
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    try:
        response = requests.post(url, headers=headers, params=receive_id_type, json=payload, verify=certifi.where(), timeout=30)
        response_data = response.json()
        response.raise_for_status()
    except requests.RequestException as e:
        return f"发送消息失败：{e}"
# 发送加急电话
def send_phone(receive_id, message_id):
    receive_id = [receive_id]
    user_id_type = {"user_id_type": "open_id"}
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    payload = {
        "user_id_list": receive_id,
    }
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/urgent_phone"
    try:
        response = requests.patch(url, headers=headers, params=user_id_type, json=payload, verify=certifi.where(), timeout=30)
        response_data = response.json()
        print(response_data)
        response.raise_for_status()
    except requests.RequestException as e:
        return f"发送消息失败：{e}"
# 修改飞书表格的剩余邀约次数
def update_yaoyue_total():
    data = read_table_content(app_token, table_id, view_id)

    if not data:
        print("获取蒲公英 Token 数据失败或格式不正确")
        return
    message = ''
    for item in data:
        name = item['fields']['账号简称'][0]['text']
        pgy_id = item['fields']['账号id'][0]['text']
        token = get_text_from_items(item['fields'].get('蒲公英token', []))
        total = get_yaoyue_total(token, name, pgy_id)
        result = search_feishu_record(app_token, table_id, view_id, '账号id', pgy_id)
        try:
            record_id = result[0]['record_id']
            if total is None:
                message.join(result[0]['fields']['账号简称'][0]['text']+'\n')
                fields = {
                    '是否过期': '已过期'
                }
                update_record(app_token, table_id, record_id, fields)
                continue
            fields = {
                '剩余邀约次数': total,
                '是否过期': ''
            }
            update_record(app_token, table_id, record_id, fields)
        except Exception as e:
            continue
    send_message(message)

if __name__ == '__main__':
    update_yaoyue_total()