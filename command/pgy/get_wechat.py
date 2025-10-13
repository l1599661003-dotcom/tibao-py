import json
import random
import re
import time
import uuid

import certifi
import requests
import schedule
import urllib3

from service.feishu_service import search_feishu_record, update_record

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 待建联博主再用
app_token = 'O5uNbHYLdaWQV1sPn8FcdsbSnmb'
table_id = 'tbleBTQoU3Uq9tCL'
view_id = 'vewSV3ZG35'
token = 'acw_tc=0a42442417496069156418632e804ce6bde4ecf5e639686da5a6b3a6701c9b; xsecappid=ratlin; a1=1975cb2db64kmuru7spt00ftx6pgjhu3a5lxhq14150000427506; webId=08df22df98cb60b14369ff2d23a35616; gid=yjW2SDJdSf00yjW2SDJfD2AyK4T67F7WMU9814y8i9CKUC28kVx7qx8884JW28K8WD8JK8dq; customerClientId=390102905002751; websectiga=59d3ef1e60c4aa37a7df3c23467bd46d7f1da0b1918cf335ee7f2e9e52ac04cf; sec_poison_id=ce00f455-2ee3-49f9-a625-2e7925f0fe88; customer-sso-sid=68c517514510790414062555d1y8sqktbr9v0elo; x-user-id-pgy.xiaohongshu.com=67d562790000000006012f35; solar.beaker.session.id=AT-68c51751451079041449989987jc8lndb9ovtsrc; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c51751451079041449989987jc8lndb9ovtsrc; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c51751451079041449989987jc8lndb9ovtsrc'
cooperateBrandName = '树洞先生'
cooperateBrandId = '67d82984000000000e02cf5a'

def get_table_content():
    try:

        total_pages = 3

        # 分页处理
        for page in range(1, total_pages + 1):
            print(f"处理第 {page}/{total_pages} 页")
            res = get_reply_message(token, page)
            if res.get('code') in (902, 903, 906):
                print("token过期")
                return
            for message in res.get('data', {}).get('messageList', []):
                try:
                    process_message(token, message, cooperateBrandName)
                except Exception as e:
                    print(f"处理消息时出错: {e}")
                    continue
            time.sleep(random.uniform(2, 4))
        print(f"账号 {cooperateBrandName} 处理完成")

    except Exception as e:
        print(f"处理账号时出错: {e}")

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

        # print(res)
        if res.get('msg') == '成功':
            if res['data']['invite']['kolIntention'] == 2:
                result = search_feishu_record(app_token, table_id, view_id, '博主id', res['data']['invite']['kolId'])
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
        print(f"获取vx参数失败: {e}")
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

def process_message(token, message, cooperateBrandName):
    """处理单条消息"""
    time.sleep(random.uniform(2, 4))
    kol = get_vx_parameter(token, message.get('content', ''))
    if not kol:
        return

    fields = {}

    if isinstance(kol.get('wechat'), dict) and kol['wechat'].get('text'):
        fields['微信号'] = str(kol['wechat'].get('text'))

    if isinstance(kol.get('phone'), dict) and kol['phone'].get('text'):
        fields['博主手机号'] = str(kol['phone'].get('text'))
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


if __name__ == '__main__':
    get_table_content()