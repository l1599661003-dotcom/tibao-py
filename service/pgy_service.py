import logging
import random
import re
import time
import uuid
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    "cookie": 'a1=19a5ead69f4zkdfll2met4caij5qsa4u5h4kzfkqg50000152304; webId=a482ce8dfc97a466250424206503fa86; customerClientId=505888866352470; x-user-id-pgy.xiaohongshu.com=68f9ddc3155e000000000001; xsecappid=ratlin; customer-sso-sid=68c5175735989617408737285jrenefomiqvidas; solar.beaker.session.id=AT-68c5175735989617408737295hcvofvvuloyuyns; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c5175735989617408737295hcvofvvuloyuyns; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c5175735989617408737295hcvofvvuloyuyns; acw_tc=0a4269db17636189803563777e61ac015f8cd8cf3920c3d5df210db85f65ef; loadts=1763618984928',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
}

headers1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    "cookie": 'a1=19882f48bcap04qq1r777p9ggafrdrroz4abocygo50000339517; webId=e7111fec356dc781ca1d14d236afda9a; customerClientId=016216997022661; abRequestId=e7111fec356dc781ca1d14d236afda9a; x-user-id-creator.xiaohongshu.com=634cc30badd08a00019ee4e3; gid=yjYSKK8dd0uiyjYYJi4YD4SvS0U84AAyFWWW0klUjkk0iD28FfFFll888qqj2yW8DY2KKijK; x-user-id-ad-market.xiaohongshu.com=67bbea69000000000d009ec6; access-token-ad-market.xiaohongshu.com=customer.ad_market.AT-68c517561719769394855947z3wtukxckssgow61; web_session=0400698efe0fc8579efeac34ce3a4bbb01a2bd; customer-sso-sid=68c517568811894271950853phdjccj3sgroalhn; x-user-id-pgy.xiaohongshu.com=68fb0f1c1558000000000001; solar.beaker.session.id=AT-68c517569793964314001413gtpd0jlij5tprk24; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517569793964314001413gtpd0jlij5tprk24; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517569793964314001413gtpd0jlij5tprk24; webBuild=4.84.4; xsecappid=ratlin; acw_tc=0a422b7a17625225228367977e4b55ef4bd6ea5a352e0eedb0de4305d22287; loadts=1762523551948',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://pgy.xiaohongshu.com/',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://pgy.xiaohongshu.com',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Authorization': '',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Ch-Ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Ch-Ua-Mobile': '?0',
    'X-S': 'ZBOUs21LsYZBO6TW1g5GsiZ6sj9bOl5l16MG12OvsBT3',
    'X-S-Common': '2UQAPsHC+aIjqArjwjHjNsQhPsHCH0rjNsQhPaHCH0c1PahFHjIj2eHjwjQgynEDJ74AHjIj2ePjwjQUGgzVynhTq9YSJBIjNsQh+sHCH0Z1PshlPeHEHjIj2eLjwjHlw/WhPfGFwBQ0GgZI+oblPgH7+A4Iwn4dGn8U8oQUJ7iFGnQ6G7SdJALIPeZIPAPE+/r7HjIj2eGjwjHjNsQh+UHCHjHVHdWhH0ija/PhqDYD87+xJ7mdag8Sq9zn494QcUT6aLpPJLQy+nLApd4G/B4BprShLA+jqg4bqD8S8gYDPBp3Jf+m2DMBnnEl4BYQyrkSL98+zrTM4bQQPFTAnnRUpFYc4r4UGSGILeSg8DSkN9pgGA8SngbF2pbmqbmQPA4Sy9Ma+SbPtApQy/8A8BES8p+fqpSHqg4VPdbF+LQmzrQQ2sTczFzkN7+n4BTQ2BzA2op7q0zl4BSQyopYaLLA8/+Pp0mQPM8LaLP78/mM4BIUcLzTqFl98Lz/a7+/LoqMaLp9q9Sn4rkOqgqhcdp78SmI8BpLzS4OagWFprSk4/8yLo4ULopF+LS9JBbPGf4AP7bF2rSh8gPlpd4HanTMJLS3agSSyf4AnaRgpB4S+9p/qgzSNFc7qFz0qBSI8nzSngQr4rSe+fprpdqUaLpwqM+l4Bl1Jb+M/fkn4rS9J9p3qgcAGMi7qM86+B4Qzp+EanYbaDW7/bpQ2BMc/7kTJFSkGMYSLo4Bag8kcAQsPo+r204A2b8F8rS9+fpD+A4SnnkMtFSk+nL9p9Qr/db7yDSiadPAJ9pA+Dz98pzc4rE0p/8Szop7+LS9nnE0NFbS8ob7yrls8g+fqg46a/+VaFShJnlAqgzAGpm72g+AaBQNze4A8rQDq9TM4BpQynRS8sRm8/+0JrMQzLRS8BQ9qM86zpQQy9pSpb8F/LS3zBzF4g4I/7b7cFDAafpL4gzBanVAq9Sl4B8Qy/+SpMm7zDkl4FIh4gztaL+3+LSe+BEc8dknadiA8/bM49YQyBYoanVI8nzM49RynfTFcSLMqM4mLLzQyF4PadbFa9bl47pQ4fzS+S8F2bbIa9LAaLbSPLlCqDSbad+3cDRS+Sm7NMmn4BEQ2B4ALM8FnrSbPo+hzdkoa/+tqFcIa7PILoqAagGA8/+l4BkzGLlYanSjJLShafpLG9zAPFr7qFzy+9pxqgqUaLL3NFS9pM+Pqg4wagYH+DSkP9p/G9lLaFzt8nkl4FTQy7QI/ob7y7zP/9p3n/pAngpFJdkM4eQQP7bpaLpb+oQc4sTz4g4FagY6qAmCPBL9qg4oG7pFJLSb//zypdcIagYBc9F6nDkQy/+AnncIqM4U4fpDqg4Lq7pFyLDAarpQy/pAydbF2LSe+9L98BM0anY+4DuItFzQzpQDaLpm8n8n474opdcAJM8FwrSha/+QzLbAP/mO8nTn4FpIqD8Fag8PJDTM4B+ApdzUagYl+LShLg+QypD9aLpN8nSr8gP9+AYB4b87zFSeLBSIqg4lJ7b7pLSb+9ph8opOanSBzfr7+fp/pdzVGS8FJnpVarYyJDlhanTTysRl47SQy/pAPb48aDS3L7S6Lo49anTat7zM4b+sLozTanYQyDS9pepQyokwq7b7LDSkp0QQzg8Ay7p74npg/LpQyr86ag8Oq7Y1+7+3pd4fag83pDSbGML6G7Q9aL+CpezmqdkQz/8Ay94w8p4l49pQz/+AyMmF4FSbpA+QznpApdb7PFSbzpz7Lo41anYOqMSyzAz1pdzVqSmFaFS9N9p8qgz6anY9qFzI2d8QzLTALFMOqAbc4BMz2d8A8S8749QUad+kLozoagWIq7YV+npDLozzaLpgcFSkwsTjLo4t4r8baUHVHdWEH0ilPeLF+/DF+eqMNsQhP/Zjw0ZVHdWlPaHCHflk4BLjKc==',
    'X-T': str(int(time.time() * 1000))
}

headers2 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    "cookie": 'a1=19882f48bcap04qq1r777p9ggafrdrroz4abocygo50000339517; webId=e7111fec356dc781ca1d14d236afda9a; customerClientId=016216997022661; abRequestId=e7111fec356dc781ca1d14d236afda9a; x-user-id-creator.xiaohongshu.com=634cc30badd08a00019ee4e3; gid=yjYSKK8dd0uiyjYYJi4YD4SvS0U84AAyFWWW0klUjkk0iD28FfFFll888qqj2yW8DY2KKijK; x-user-id-ad-market.xiaohongshu.com=67bbea69000000000d009ec6; access-token-ad-market.xiaohongshu.com=customer.ad_market.AT-68c517561719769394855947z3wtukxckssgow61; web_session=0400698efe0fc8579efeac34ce3a4bbb01a2bd; customer-sso-sid=68c517568811894271950853phdjccj3sgroalhn; x-user-id-pgy.xiaohongshu.com=68fb0f1c1558000000000001; solar.beaker.session.id=AT-68c517569793964314001413gtpd0jlij5tprk24; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517569793964314001413gtpd0jlij5tprk24; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517569793964314001413gtpd0jlij5tprk24; webBuild=4.84.4; xsecappid=ratlin; acw_tc=0a422b7a17625225228367977e4b55ef4bd6ea5a352e0eedb0de4305d22287; loadts=1762523551948',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://pgy.xiaohongshu.com',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Authorization': '',
    'Pragma': 'no-cache',
    'priority': 'u=1, i',
    'Referer': 'https://pgy.xiaohongshu.com/',
    'Sec-Ch-Ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'X-S': 'OjVBZgZksiFK16sbsB5W1ia60gVkOj9G0jwB0YTCslA3',
    'X-S-Common': '2UQAPsHC+aIjqArjwjHjNsQhPsHCH0rjNsQhPaHCH0c1PahFHjIj2eHjwjQgynEDJ74AHjIj2ePjwjQUGgzVynhTq9YSJBIjNsQh+sHCH0Z1PshlPeHEHjIj2eLjwjHlw/WhPfGFwBQ0GgZI+oblPgH7+A4Iwn4dGn8U8oQUJ7iFGnQ6G7SdJALIPeZIPAPE+/r7HjIj2eGjwjHjNsQh+UHCHjHVHdWhH0ija/PhqDYD87+xJ7mdag8Sq9zn494QcUT6aLpPJLQy+nLApd4G/B4BprShLA+jqg4bqD8S8gYDPBp3Jf+m2DMBnnEl4BYQyrkSL98+zrTM4bQQPFTAnnRUpFYc4r4UGSGILeSg8DSkN9pgGA8SngbF2pbmqbmQPA4Sy9Ma+SbPtApQy/8A8BES8p+fqpSHqg4VPdbF+LQmzrQQ2sTczFzkN7+n4BTQ2BzA2op7q0zl4BSQyopYaLLA8/+Pp0mQPM8LaLP78/mM4BIUcLzTqFl98Lz/a7+/LoqMaLp9q9Sn4rkOqgqhcdp78SmI8BpLzS4OagWFprSk4/8yLo4ULopF+LS9JBbPGf4AP7bF2rSh8gPlpd4HanTMJLS3agSSyf4AnaRgpB4S+9p/qgzSNFc7qFz0qBSI8nzSngQr4rSe+fprpdqUaLpwqM+l4Bl1Jb+M/fkn4rS9J9p3qgcAGMi7qM86+B4Qzp+EanYbaDW7/bpQ2BMc/7kTJFSkGMYSLo4Bag8kcAQsPo+r204A2b8F8rS9+fpD+A4SnnkMtFSk+nL9p9Qr/db7yDSiadPAJ9pA+Dz98pzc4rE0p/8Szop7+LS9nnE0NFbS8ob7yrlfad+rpd4Aa/+aGLShJnlAqgzAGpm72g+AaBQNze4A8rQDq9TM4BpQynRS8sRm8/+0JrMQzLRS8BQ9qM86zpQQy9pSpb8F/LS3zBzF4g4I/7b7cFDAafpL4gzBanVAq9Sl4B8Qy/+SpMm7zDkl4FIh4gztaL+3+LSe+BEc8dknadiA8/bM49YQyBYoanVI8nzM49RUJBMFNFrI8nkxqe8QyF4cz7pFa9Rl4A+QPFRS2obFySbP+npkGFbAP7mi4FSicgPAcDRAP7pFq7+M4sTQyURAzbmFPFSb8o+n8SbtanSmq9kA87+/Lo4fag8O8nTl4o8cPrq3anSa8LShafp3JemAPppmqM+8PBpxLo46anYTnLS9G9b0Loq3a/+lPLS3/9pnpMSTqnzw8nzn4M4QyFI6G7p7LFR+89p/2d8ApbmFGjRc4BpQ2b8eaLL3Lo+n4sTHLo4AagWIqM48afpnLoqF87pFNFSiweYspdzpagY0qBRQnfEQyLRAp94DqM+dafL9pd4BcdbFnrShnS+Qy9RALM8F/FSeJ9LA80Y6aLpCzrEpwr+QysTBaLpm8n8M474oqgze8gpFGLSk4pkQzLEAPMLM8pzc49l0LFznaL+g40Yc4rTj4gzwagYnPrDApBRQy9QeanYw8nSI/7+LqeW7JMm7PDS9zSL6Lo4zndb7pFSb89L9cS83anTywrl6N9px4gzGcS8F+BQgLrkS+9SkanSbpaRl4FEQyFEAPgkCwLSkLgmyqg4BanV32Smn4b4yqgzPaLprnLSenLMQc7kH/7p74DS3/sRQz/+A2ob7PBpTtFbQyr81aL+9q9zDJ7+x4g4aag8IJrSbpSYP2S4eaL+acnbg4LYQyrbA+f4D8gYc498Qz/4AP7bFaFSiqpmQ408AP7p7PrSbJpH3qg4NanW6qMD6LBTPqgc9LpmF+rSb+fp8pdz7anY6qM4UpBYQyo8ApFztqAbM4Bbwcg8A8db7yoQ/PoP9qgzta/+9qA+Vafpn4gzFag8PnLSkL9+s4g4dtFS/GFSe89p8pAFRHjIj2eDjw0qMweWAP/rUwaIj2erIH0iINsQhP/rjwjQVygzSHdF=',
    'X-T': '1762523552378'
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
        data = response.json()
        print(data)

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
            header = headers1
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v2/notes_detail?advertiseSwitch=1&orderType=1&pageNumber=1&pageSize=8&userId={user_id}&noteType=4&isThirdPlatform=0"
        response = requests.get(url, headers=header, verify=False, timeout=30)
        data = response.json()
        print(data)
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
            header = headers2
        url = f"https://pgy.xiaohongshu.com/api/pgy/kol/data/data_summary?userId={user_id}&business={business}"
        response = requests.get(url, headers=header, verify=False, timeout=30)
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
        data = response.json()
        print(data)

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