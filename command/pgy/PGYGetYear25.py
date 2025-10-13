import json
import math
import time
import certifi
import requests

from service.feishu_service import get_feishu_token

view_id = 'vewKt6xYit'
app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
table_id = 'tbliGz3IRUgjz5Jg'

"""
    获取活跃粉丝以及25-34岁粉丝占比
"""

def notes_rate(user_id):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'Cookie': 'abRequestId=b43b0375-a680-549b-802f-a5ad99896ecd; a1=195406449fdxtqwjh51u1p6d7mgplq0uhf3hg4rfp50000105650; webId=ffc4f1682f0baf7ebc3eb0b29650b1a1; gid=yj248K4Kyixfyj248K44ji4FifC9AEVx2y7yqiUUKfW6kx28U1A87k888y82K288jyfYq4Dj; web_session=0400698efe0fc8579efee3d88b354ba1a1fe35; webBuild=4.58.0; acw_tc=0a00071517405599276892038e56046c201bf48c6470051df9f534c6a6956c; customer-sso-sid=68c51747564801076689657081514d02a65f6d86; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; customerClientId=876055260878386; solar.beaker.session.id=AT-68c517475648015059450176jpfba2mpakt6700t; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517475648015059450176jpfba2mpakt6700t; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517475648015059450176jpfba2mpakt6700t; loadts=1740561400256; websectiga=6169c1e84f393779a5f7de7303038f3b47a78e47be716e7bec57ccce17d45f99; sec_poison_id=33af29ac-ddbf-4f59-8253-6c1451ba1490; xsecappid=ratlin',
        'Authorization':get_feishu_token()
    }

    url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v3/fans_summary?userId={user_id}"
    response = requests.get(url, headers=headers, verify=certifi.where(), timeout=30)
    result = response.json()
    if result['data']:
        fans = result['data']['activeFansRate']
        return fans
    else:
        return None

def fans_overall_new_history(user_id):
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
        'Cookie': 'abRequestId=b43b0375-a680-549b-802f-a5ad99896ecd; a1=195406449fdxtqwjh51u1p6d7mgplq0uhf3hg4rfp50000105650; webId=ffc4f1682f0baf7ebc3eb0b29650b1a1; gid=yj248K4Kyixfyj248K44ji4FifC9AEVx2y7yqiUUKfW6kx28U1A87k888y82K288jyfYq4Dj; web_session=0400698efe0fc8579efee3d88b354ba1a1fe35; webBuild=4.58.0; acw_tc=0a00071517405599276892038e56046c201bf48c6470051df9f534c6a6956c; customer-sso-sid=68c51747564801076689657081514d02a65f6d86; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; customerClientId=876055260878386; solar.beaker.session.id=AT-68c517475648015059450176jpfba2mpakt6700t; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517475648015059450176jpfba2mpakt6700t; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517475648015059450176jpfba2mpakt6700t; loadts=1740561400256; websectiga=6169c1e84f393779a5f7de7303038f3b47a78e47be716e7bec57ccce17d45f99; sec_poison_id=33af29ac-ddbf-4f59-8253-6c1451ba1490; xsecappid=ratlin',
        'Authorization': get_feishu_token()
    }
    url = f"https://pgy.xiaohongshu.com/api/solar/kol/data/{user_id}/fans_profile"
    response = requests.get(url, headers=headers, verify=certifi.where(), timeout=30)
    if response.json():
        result = response.json()
        if 'data' in result and 'ages' in result['data']:
            fans_data = result['data']['ages']
            for fan in fans_data:
                if fan['group']== '25-34':
                    return fan
        else:
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

def get_table_info(app_token, table_id, view, field_names=None, where=None):
    token = get_feishu_token()
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {token}',
    }

    # 构建请求体
    payload = {
        'view_id': view
    }
    if field_names:
        payload['field_names'] = field_names
    if where:
        payload['filter'] = where

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
            if field_names:
                payload1['field_names'] = field_names
            if where:
                payload1['filter'] = where

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

def week_account_13():
    info = get_table_info(app_token, table_id, view_id)
    for items in info['items']:
        user_id = items['fields'].get('博主id', [{}])[0].get('text')
        record_id = items.get('record_id')
        if (user_id) :
            time.sleep(6)
            notes = notes_rate(user_id)
            time.sleep(6)
            fans = fans_overall_new_history(user_id)

            # 构造字段字典
            fields = {
                '活跃粉丝占比': str(notes),
                '粉丝年龄25-34岁占比': float(fans['percent']),
            }

            update_record(app_token, table_id, record_id, fields)



if __name__ == '__main__':
    week_account_13()