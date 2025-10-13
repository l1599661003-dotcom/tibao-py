import json
import math
import random
import time
import certifi
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbliGz3IRUgjz5Jg'
view_id = 'vew971aOKw' #1
#view_id = 'vew3W6dzSh' #2
#view_id = 'vewa3SeTaG' #3
#view_id = 'vewRlXg7S7'  #4
#view_id = 'vewiOAsvXE' #5
#view_id = 'vewJuBaDnQ' #6
#view_id = 'vewV0q7DUz'  #7
#view_id = 'vewimHmpmU' #8
#view_id = 'vewRbxnnX0' #9
table_id1 = 'tblDO9VqC6EMHGiY'
view_id1 = 'vewJK6XVP4'
conditions = [
        {
            "field_name": '使用电脑',
            "operator": "is",
            "value": [11]
        }
    ]
def get_feishu_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    headers = {
        'Content-Type': 'application/json; charset=utf-8'
    }
    data = {
        'app_id': 'cli_a6e824d4363b500d',
        'app_secret': 'nW4ff1Mviwr0ZuYkF1BBhciZGOyDeBP5'
    }
    response = requests.post(url, headers=headers, json=data)
    response_data = response.json()
    return response_data.get('app_access_token', '')

def search_feishu_record(app_token, table_id, view_id, conditions):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": conditions
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
        print(f"查询飞书单条信息失败：{e}")
        return None

def notes_rate(user_id, token):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'Cookie': token,
        'Authorization':get_feishu_token()
    }

    url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v3/notes_rate?userId={user_id}&business=0&noteType=3&dateType=1&advertiseSwitch=1"
    response = requests.get(url, headers=headers, verify=certifi.where(), timeout=30)
    print(response.json())
    if response.json():
        result = response.json()
        if 'data' in result:
            note_type = result['data'].get('noteType', [])
            note_type_top_two = note_type[:2]
            if result['data']['pagePercentVo'] != None:
                exposure = result['data']['pagePercentVo'].get('impHomefeedPercent', 0)
                reads = result['data']['pagePercentVo'].get('readHomefeedPercent', 0)
            else:
                exposure = 0
                reads = 0

            return {
                'noteTypeTopTwo': note_type_top_two,
                'reads': reads,
                'exposure': exposure,
            }
    return None

def fans_overall_new_history(user_id, token):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'Cookie': token,
        'Authorization': get_feishu_token()
    }
    url = f"https://pgy.xiaohongshu.com/api/solar/kol/data/{user_id}/fans_overall_new_history?dateType=1&increaseType=1"
    response = requests.get(url, headers=headers, verify=certifi.where(), timeout=30)
    if response.json():
        result = response.json()
        if 'data' in result and 'list' in result['data']:
            fans_data = result['data']['list']
            if len(fans_data) >= 30:
                key_points = [
                    fans_data[0]['num'],
                    fans_data[4]['num'],
                    fans_data[9]['num'],
                    fans_data[14]['num'],
                    fans_data[19]['num'],
                    fans_data[24]['num'],
                    fans_data[29]['num']
                ]
                for i in range(len(key_points) - 1):
                    if key_points[i] <= key_points[i + 1]:
                        return 0
                return 1
    return None

# 修改飞书多维表格数据
def update_record(app_token, table_id, record_id, fields):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Bearer ' + get_feishu_token()
    }
    body = {
        'fields' : fields
    }
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    response = requests.put(url, headers=headers, json=body, verify=certifi.where(), timeout=30)
    print(response.json())

def get_table_info(app_token, table_id, view):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}',
    }

    # 构建请求体
    payload = {
        'view_id': view
    }
    # 第一次请求
    response = requests.post(
        f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500',
        headers=headers,
        data=json.dumps(payload),
        verify=certifi.where()  # 如果需要，您可以将其设置为 True
    )

    response_body = response.json()
    items = response_body['data']['items']
    total = response_body['data']['total']

    # 如果总记录数超过500，进行分页请求
    if total > 500:
        page_token = response_body['data']['page_token']
        page_count = math.ceil(total / 500)

        for i in range(1, page_count):
            payload1 = {
                'view_id': view
            }

            # 发送分页请求
            response1 = requests.post(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500&page_token={page_token}',
                headers=headers,
                data=json.dumps(payload1),
                verify=certifi.where()  # 如果需要，您可以将其设置为 True
            )

            response_body1 = response1.json()
            items.extend(response_body1['data']['items'])
            if i < page_count - 1:
                page_token = response_body1['data']['page_token']

    return response_body['data']
# 从字典列表中提取 'text' 字段并拼接为字符串
def get_text_from_items(items):
    result = ''
    for item in items:
        if isinstance(item, dict) and 'text' in item:
            result += item['text']
    return result
def week_account_13():
    tokens = search_feishu_record(app_token, table_id1, view_id1, conditions)
    token = ''
    for item in tokens:
        token = get_text_from_items(item['fields'].get('蒲公英token', []))

    info = get_table_info(app_token, table_id, view_id)
    for items in info['items']:
        user_id = items['fields'].get('博主id', [{}])[0].get('text')
        record_id = items.get('record_id')
        if user_id is None or record_id is None :
            continue
        sleep_time = random.uniform(3, 6)
        time.sleep(sleep_time)
        notes = notes_rate(user_id, token)
        time.sleep(sleep_time)
        fans = fans_overall_new_history(user_id, token)
        note_type_top_two = notes.get('noteTypeTopTwo', [])
        reads = notes.get('reads', 0)
        exposure = notes.get('exposure', 0)

        # 提取内容标签和百分比
        content_tag1 = note_type_top_two[0].get('contentTag', '') if len(note_type_top_two) > 0 else ''
        content_tag2 = note_type_top_two[1].get('contentTag', '') if len(note_type_top_two) > 1 else ''
        content_percent1 = f"{note_type_top_two[0].get('percent', '')}%" if len(note_type_top_two) > 0 else ''
        content_percent2 = f"{note_type_top_two[1].get('percent', '')}%" if len(note_type_top_two) > 1 else ''

        # 构造字段字典
        fields = {
            '连续30天负增长': str(fans),
            '阅读量来源的【发现页】占比': float(reads),
            '曝光量来源的【发现页】占比': float(exposure),
            '内容类目1': content_tag1,
            '内容占比1': content_percent1,
            '内容类目2': content_tag2,
            '内容占比2': content_percent2
        }
        if reads < 0.55 and exposure < 0.55:
            fields['备注'] = '水号'
        print(fields)
        update_record(app_token, table_id, record_id, fields)

if __name__ == '__main__':
    week_account_13()