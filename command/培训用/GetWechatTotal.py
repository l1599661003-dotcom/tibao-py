import json
import random
import re
import time
import uuid

import certifi
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 待建联博主再用
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbliGz3IRUgjz5Jg'
view_id = 'vewFjbwzKs'
# token表
app_token2 = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id2 = 'tblDO9VqC6EMHGiY'
#view_id2 = 'vewJ9nETr9' #1
#view_id2 = 'vewseFTzpf' #2
#view_id2 = 'vewsBw8bqh' #3
view_id2 = 'vewISzDhNy' #4
#view_id2 = 'vewaWVrpzP' #5
#view_id2 = 'vewGCmuZ6O' #6
# view_id2 = 'vew4h9hl27' #7
# view_id2 = 'vewsJpJbIG' #8
# view_id2 = 'vewfIwp7is' #9
# view_id2 = 'vew7Ub1b7k' #10
# view_id2 = 'vewxDmcxcd' #11
# view_id2 = 'vewKdieV2D' #12
#view_id2 = 'vewHLbaddn' #13
# view_id2 = 'vewOvmHCGy' #14
# view_id2 = 'vewXQ0yclF' #15
# view_id2 = 'vewpoCBmMe' #16
# view_id2 = 'vew9Glo5Hz' #17
# view_id2 = 'vew4MxhuUh' #18

# 从字典列表中提取 'text' 字段并拼接为字符串
def get_text_from_items(items):
    result = ''
    for item in items:
        if isinstance(item, dict) and 'text' in item:
            result += item['text']
    return result

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

# 获取飞书请求头
def get_feishu_headers():
    return {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }

# 查询飞书表格信息
def read_table_content(app, table, view):
    result = []
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

# 获取蒲公英收件箱信息
def get_reply_message(token, page_num=None):
    url = "https://pgy.xiaohongshu.com/api/solar/message/all/message/list"
    trace_id = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': token,
        'X-B3-Traceid': trace_id[:16]
    }
    data = {
        'keywords': '合作邀约回复',
        'messageTopic': None,
        'pageNum': page_num,
        'pageSize': 100,
        'platform': 2
    }
    try:
        sleep_time = random.uniform(1, 2)
        time.sleep(sleep_time)
        response = requests.post(url, headers=headers, json=data, verify=False)
        return response.json()
    except requests.RequestException as e:
        print(f"获取消息失败: {e}")
        return None

def get_vx_parameter(token, content):
    trace_id = str(uuid.uuid4())
    match = re.search(r'https?://[^"\s]+', content)
    if not match:
        return None

    url = match.group(0)
    query_params = requests.utils.urlparse(url).query
    params = {}
    for param in query_params.split('&'):
        if '=' in param:
            key, value = param.split('=', 1)  # 限制 split 只分割一次
            params[key] = value

    id_param = params.get('id', '')

    invite_url = f"https://pgy.xiaohongshu.com/api/solar/invite/get_invite_info?invite_id={id_param}"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Cookie': token,
        'Referer' : f"https://pgy.xiaohongshu.com/solar/pre-trade/mcn/invite-detail?id={id_param}",
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'X-B3-Traceid': trace_id[:16]
    }
    try:
        response = requests.get(invite_url, headers=headers, verify=False)
        res = response.json()

        if res.get('msg') == '成功':
            if res['data']['invite']['kolIntention'] == 2:
                result = search_feishu_record1(app_token, table_id, view_id, '博主id', res['data']['invite']['kolId'])
                if not result:
                    return None

                record_id = result[0]['record_id']
                fields = {
                    '备注': '拒绝邀约'
                }
                update_record(app_token, table_id, record_id, fields)
                return None
            kol_vx = None
            phone = None
            fp_phone = None

            if 'wechatNoCiphertext' in res['data']['invite']:
                kol_vx = get_kol_vx(token, res['data']['invite']['wechatNoCiphertext'], id_param)
                time.sleep(3)

            if 'phoneNoCiphertext' in res['data']['invite']:
                phone = get_kol_vx(token, res['data']['invite']['phoneNoCiphertext'], id_param)
                time.sleep(3)

            if 'contactInfoCiphertext' in res['data']['invite']:
                fp_phone = get_kol_vx(token, res['data']['invite']['contactInfoCiphertext'], id_param)
                time.sleep(3)

            return {
                'wechat': kol_vx,
                'phone': phone,
                'fpweixin': fp_phone,
                'kolId': res['data']['invite']['kolId']
            }
        else:
            # print('获取vx接口参数失败')
            return None
    except requests.RequestException as e:
        # print(f"获取vx参数失败: {e}")
        return None

def get_kol_vx(token, contact_info_ciphertext, id_param):
    url = "https://pgy.xiaohongshu.com/api/solar/common/sensitive_info_view"
    trace_id = str(uuid.uuid4())
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Cookie': token,
        'Referer': f"https://pgy.xiaohongshu.com/solar/pre-trade/mcn/invite-detail?id={id_param}",
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'X-B3-Traceid': trace_id[:16]
    }
    data = {'ciphertext': contact_info_ciphertext}
    try:
        # 添加超时设置
        response = requests.post(url, headers=headers, json=data, verify=False, timeout=30)
        res = response.json()

        if res.get('msg') == '成功':
            return res['data']
        else:
            # print(f"获取博主联系方式失败: {res.get('msg')}")
            return '获取博主联系方式失败'
    except requests.Timeout:
        # print("请求超时，跳过当前处理")
        return None
    except requests.RequestException as e:
        # print(f"获取vx参数失败: {e}")
        return None

# 查询飞书单条信息
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
                },{
                    "field_name": '微信号',
                    "operator": "isEmpty",
                    "value": []
                },
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
        print(f"查询飞书单条信息失败：{e}")
        return None
def search_feishu_record1(app_token, table_id, view_id, field_name, field_value):
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
def search_feishu_record4(app_token, table_id, view_id, field_name, field_value):
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
                },
                {
                    "field_name": '金虎VX',
                    "operator": 'isNotEmpty',
                    "value": []
                },
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
        print(f"查询飞书单条信息失败：{e}")
        return None
def get_table_content():
    results = []
    try:
        data = read_table_content(app_token2, table_id2, view_id2)
        if not data:
            print('获取蒲公英 Token 数据失败或格式不正确')
            return results

        for item in data:
            try:
                # 检查必要字段是否存在
                if not item.get('fields'):
                    continue
                    
                token = get_text_from_items(item['fields'].get('蒲公英token', []))
                if not token:
                    print(f"Token为空，跳过处理")
                    continue

                # 获取账号信息，添加错误处理
                cooperateBrandName = item['fields'].get('账号简称', [{'text': '未知'}])[0]['text']
                cooperateBrandId = item['fields'].get('账号id', [{'text': '未知'}])[0]['text']

                # 检查是否已经抓取完成
                # if item['fields'].get('是否抓完') == '是':
                #     print(f"账号 {cooperateBrandName} 已抓取完成，跳过")
                #     continue

                print(f"开始处理账号: {cooperateBrandName}")

                # 获取总页数
                res = get_reply_message(token)
                if not res:
                    print(f"获取消息列表失败，跳过账号 {cooperateBrandName}")
                    continue

                if res.get('code') in (902, 903, 906):
                    handle_token_expired(token, cooperateBrandName, cooperateBrandId)
                    continue

                total = res.get('data', {}).get('total', 0)
                total_pages = (total + 99) // 100  # 更简洁的页数计算

                # 分页处理
                for page in range(1, total_pages + 1):
                    try:
                        print(f"处理第 {page}/{total_pages} 页")
                        process_page(token, page, cooperateBrandName, cooperateBrandId)
                        time.sleep(random.uniform(2, 4))  # 添加随机延迟
                    except Exception as e:
                        print(f"处理页面 {page} 时出错: {e}")
                        continue

                # 更新处理状态
                update_processing_status(token, total_pages)
                print(f"账号 {cooperateBrandName} 处理完成")

            except Exception as e:
                print(f"处理账号时出错: {e}")
                continue

    except Exception as e:
        print(f"执行过程中出现错误: {e}")
    
    return results

def process_page(token, page_num, cooperateBrandName, cooperateBrandId):
    """处理单个页面的数据"""
    res = get_reply_message(token, page_num)
    if not res:
        return

    if res.get('code') in (902, 903, 906):
        handle_token_expired(token, cooperateBrandName, cooperateBrandId)
        return

    for message in res.get('data', {}).get('messageList', []):
        try:
            process_message(token, message, cooperateBrandName)
        except Exception as e:
            print(f"处理消息时出错: {e}")
            continue

def process_message(token, message, cooperateBrandName):
    """处理单条消息"""
    time.sleep(random.uniform(2, 4))
    kol = get_vx_parameter(token, message.get('content', ''))
    if not kol:
        return

    fields = prepare_fields(kol)
    if not fields:
        return
    result = search_feishu_record(app_token, table_id, view_id, '博主id', kol['kolId'])
    if not result:
        return
    record_id = result[0]['record_id']
    try:
        update_record(app_token, table_id, record_id, fields)
        print(f"博主id:{kol['kolId']},修改数据{fields}")
    except Exception as e:
        print(f"更新记录时出错: {e}")

    if fields.get('微信号'):
        wechat_number = fields['微信号']

        # **1. 获取该微信号的所有记录**
        duplicate_records = search_feishu_record1(app_token, table_id, view_id, '微信号', wechat_number)

        # 如果查不到数据，或者条数 ≤ 1，不需要处理
        if not duplicate_records or len(duplicate_records) <= 1:
            return

        print(f"\n微信号 {wechat_number} 共有 {len(duplicate_records)} 条记录")

        # **2. 检查该微信号是否已经有 "金虎VX" 不为空的记录**
        vx_filled_records = search_feishu_record4(app_token, table_id, view_id, '微信号', wechat_number)

        vx_filled_record_ids = set()
        if vx_filled_records:
            for record in vx_filled_records:
                vx_filled_record_ids.add(record['record_id'])
            print(f"发现该微信号存在 '金虎VX' 不为空的记录，将所有其他记录标记为 '重复号'")

            for record in duplicate_records:
                record_id = record['record_id']
                # 只对不在vx_filled_record_ids中的记录进行修改
                if record_id not in vx_filled_record_ids:
                    update_record(app_token, table_id, record_id, {'金虎VX': '重复号'})
                    print(f"微信号 {wechat_number} 记录 {record_id} 被标记为 '重复号'")
                    time.sleep(1)

        else:
            # **3. 如果 "金虎VX" 全部为空，随机保留一条**
            print(f"该微信号无 '金虎VX' 记录，随机保留一条，其余标记为 '重复号'")

            keep_record = random.choice(duplicate_records)

            for record in duplicate_records:
                record_id = record['record_id']
                if record_id != keep_record['record_id']:
                    update_record(app_token, table_id, record_id, {'金虎VX': '重复号'})
                    print(f"微信号 {wechat_number} 记录 {record_id} 被标记为 '重复号'")
                    time.sleep(1)

def prepare_fields(kol):
    """准备要更新的字段"""
    fields = {
        '博主是否预留联系方式': '否'
    }

    if isinstance(kol.get('fpweixin'), dict) and kol['fpweixin'].get('text'):
        fields['方片微信号'] = str(kol['fpweixin'].get('text'))

    if isinstance(kol.get('wechat'), dict) and kol['wechat'].get('text'):
        fields['微信号'] = str(kol['wechat'].get('text'))
        fields['博主是否预留联系方式'] = '是'

    if isinstance(kol.get('phone'), dict) and kol['phone'].get('text'):
        fields['博主手机号'] = str(kol['phone'].get('text'))
        fields['博主是否预留联系方式'] = '是'

    return fields

def handle_token_expired(token, cooperateBrandName, cooperateBrandId):
    """处理token过期情况"""
    fields = {
        '账号简称': str(cooperateBrandName),
        'token过期信息': str(token),
        '账号id': str(cooperateBrandId),
    }
    update_token_info(fields)

def update_processing_status(token, total_pages):
    """更新处理状态"""
    try:
        result = search_feishu_record1(app_token, table_id2, view_id2, '蒲公英token', token)
        if not result:
            return

        record_id = result[0]['record_id']
        fields = {
            '是否抓完': '是',
            '总页数': total_pages
        }
        update_record(app_token, table_id2, record_id, fields)
    except Exception as e:
        print(f"更新处理状态时出错: {e}")

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
        print(response_data)
        if response_data.get('msg') == 'success':
            print(f'飞书表格修改成功: {fields}')
        else:
            print(response_data)
    except requests.RequestException as e:
        print(f"更新飞书修改失败: {e}")

# 更新飞书token过期表格
def update_token_info(fields):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/PJISbPC5OaihG8sCfMpc4Wohnyb/tables/tblytxdwmyqhJt7D/records"
    headers = get_feishu_headers()
    body = {
        'fields': fields
    }
    try:
        response = requests.post(url, headers=headers, json=body, verify=False)
        response_data = response.json()
        if response_data.get('msg') == 'success':
            print(f'飞书token过期表新增成功: {body}')
    except requests.RequestException as e:
        print(f"飞书token过期表新增失败: {e}")

if __name__ == '__main__':
    get_table_content()