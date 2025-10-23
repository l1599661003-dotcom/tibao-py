import logging
import random
import re
import time
import uuid
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    "cookie": 'a1=19918aa122a4aiicm59fhz57ovnd0oo438xblh8ws50000173718; webId=0b1228fa555218955d9ec8a20123a6c3; customerClientId=439504990393203; x-user-id-pgy.xiaohongshu.com=65da95910000000005031083; solar.beaker.session.id=AT-68c517560298324197867522ozlmbsv8mfcuioms; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517560298324197867522ozlmbsv8mfcuioms; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517560298324197867522ozlmbsv8mfcuioms; xsecappid=ratlin; acw_tc=0a00076417606091261846932edf90693d62a7e997ed72617c6053ee4eb67d; loadts=1760609872788',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
}

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
        sleep_time = random.uniform(3, 6)
        time.sleep(sleep_time)
        response = requests.post(url, headers=headers, json=data, verify=False)
        return response.json()
    except requests.RequestException as e:
        print(f"获取消息失败: {e}")
        return None

# 获取蒲公英联系方式
def get_vx_parameter(token, content):
    trace_id = str(uuid.uuid4())
    match = re.search(r'https?://[^"\s]+', content)
    if not match:
        return {}

    url = match.group(0)
    query_params = requests.utils.urlparse(url).query
    params = {}
    for param in query_params.split('&'):
        if '=' in param:
            key, value = param.split('=', 1)  # 限制 split 只分割一次
            params[key] = value
        else:
            # 如果没有等号，跳过或记录日志
            logging.warning(f"Invalid query parameter: {param}")

    id_param = params.get('id', '')

    invite_url = f"https://pgy.xiaohongshu.com/api/solar/invite/get_invite_info?invite_id={id_param}"
    headers = {
        'Cookie': token,
        'Content-Type': 'application/json',
        'X-B3-Traceid': trace_id[:16]
    }
    try:
        response = requests.get(invite_url, headers=headers, verify=False)
        res = response.json()

        if res.get('msg') == '成功':
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
            print('获取vx接口参数失败')
            return None
    except requests.RequestException as e:
        print(f"获取vx参数失败: {e}")
        return None

def get_kol_vx(token, contact_info_ciphertext, id_param):
    url = "https://pgy.xiaohongshu.com/api/solar/common/sensitive_info_view"
    trace_id = str(uuid.uuid4())
    headers = {
        'Cookie': token,
        'Referer' : f"https://pgy.xiaohongshu.com/solar/pre-trade/mcn/invite-detail?id={id_param}",
        'Content-Type': 'application/json',
        'X-B3-Traceid': trace_id[:16]
    }
    data = {'ciphertext': contact_info_ciphertext}
    try:
        response = requests.post(url, headers=headers, json=data, verify=False)
        res = response.json()

        if res.get('msg') == '成功':
            return res['data']
        else:
            return '获取博主联系方式失败'
    except requests.RequestException as e:
        print(f"获取vx参数失败: {e}")
        return None

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

def get_fans_profile(user_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data/{user_id}/fans_profile"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            print(f"获取博主{user_id}粉丝信息成功, 数据为{result}")
            return result
        else:
            print(f"获取博主{user_id}报价信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}报价信息出错: {str(e)}")
        return {}

def get_fans_overall_new_history(user_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data/{user_id}/fans_overall_new_history?dateType=1&increaseType=1"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            print(f"获取博主{user_id}粉丝信息成功, 数据为{result}")
            return result
        else:
            print(f"获取博主{user_id}报价信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}报价信息出错: {str(e)}")
        return {}

def get_blogger_info(user_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/{user_id}"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            return result
        else:
            print(f"获取博主{user_id}报价信息失败: {data.get('msg')}")
            return []
    except Exception as e:
        print(f"获取博主{user_id}报价信息出错: {str(e)}")
        return 0, 0

def get_fans_summary(user_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v3/fans_summary?userId={user_id}"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            print(f"获取博主{user_id}粉丝信息成功")
            return result
        else:
            print(f"获取博主{user_id}报价信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}报价信息出错: {str(e)}")
        return {}

def get_notes_detail(user_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        # url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v2/notes_detail?advertiseSwitch=1&orderType=1&pageNumber=1&pageSize=999&userId={user_id}&noteType=4&isThirdPlatform=0"
        url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/{user_id}"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        print(response.json())
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {}).get('list', [])
            return result
        else:
            print(f"获取博主{user_id}笔记信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}笔记信息出错: {str(e)}")
        return {}

def get_data_summary(user_id, business=0, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/pgy/kol/data/data_summary?userId={user_id}&business={business}"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(response.json())

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            return result
        else:
            print(f"获取博主{user_id}笔记信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}笔记信息出错: {str(e)}")
        return {}

def get_notes_rate(user_id, business, noteType, dateType, advertiseSwitch=1, header=None):
    """
    获取博主笔记指标
    :param user_id: 博主ID
    :param business: 0-日常笔记, 1-合作笔记
    :return: 阅读中位数, 互动中位数
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v3/notes_rate?userId={user_id}&business={business}&noteType={noteType}&dateType={dateType}&advertiseSwitch={advertiseSwitch}"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {})
            interactionRate = result.get("interactionRate", '0')
            videoFullViewRate = result.get("videoFullViewRate", '0')
            picture3sViewRate = result.get("picture3sViewRate", '0')
            thousandLikePercent = result.get("thousandLikePercent", '0')
            hundredLikePercent = result.get("hundredLikePercent", '0')
            likeMedian = result.get("likeMedian", '0')
            print(result)
            return result
        else:
            print(f"获取博主{user_id}笔记指标失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id}笔记指标出错: {str(e)}")
        return {0, 0}

def get_core_data(user_id, business, noteType, advertiseSwitch, dateType=1, header=None):
    """
    获取博主的报价信息
    :param header:
    :param user_id: 博主ID
    :param business: 0=认知类 1=种草类
    :param noteType: 内容形式 3=视频
    :return: core_data 字典
    """
    data = {
        'userId': user_id,
        'business': business,
        'dateType': dateType,
        'advertiseSwitch': advertiseSwitch,
        'noteType': noteType,
    }
    try:
        if header is None:
            header = headers
        url = "https://pgy.xiaohongshu.com/api/pgy/kol/data/core_data"
        response = requests.post(url, headers=header, data=data, verify=False, timeout=30)
        response.raise_for_status()
        resp_data = response.json()

        if resp_data.get("code") == 0 and resp_data.get("success"):
            print(f"获取博主{user_id} CPM 信息成功: {resp_data.get('data')}")
            return resp_data.get("data", {})
        else:
            print(f"获取博主{user_id} 报价信息失败: {resp_data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主{user_id} 报价信息出错: {str(e)}")
        return {}

def get_mcn_detail(mcn_id, header=None):
    """
    获取博主的报价信息
    :param user_id: 博主ID
    :return: 视频报价和图文报价
    """
    try:
        if header is None:
            header = headers
        url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/mcn/{mcn_id}/blogger/v1?column=&sort=&pageNum=1&pageSize=999999"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0 and data.get("success"):
            result = data.get("data", {}).get('kols', [])
            return result
        else:
            print(f"获取博主笔记信息失败: {data.get('msg')}")
            return {}
    except Exception as e:
        print(f"获取博主笔记信息出错: {str(e)}")
        return {}