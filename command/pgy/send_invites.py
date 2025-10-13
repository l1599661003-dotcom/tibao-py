import time
import uuid
import requests
from service.feishu_service import read_table_content, update_record
from unitl.unitl import calculate

"""
    自动发送邀约接口
"""
app_token = 'O5uNbHYLdaWQV1sPn8FcdsbSnmb'
table_id = 'tbl5hkz3Evnt7YE6'
view_id = 'vew2J0SGvI'
cooperateBrandName = '烟台烟霞网络科技有限公司'
token = 'a1=1978767f364htqdow1m4jnuhpliopzawm8o3hjr9f50000222882; webId=b26253878c867794e4cfbc1fe1f42755; gid=yjWYWKY4q46jyjWYWKWiqYVFK4x9AflEy64VljiI7xU13q28lUu0Ex888JJJYYJ80JKWqf2S; customer-sso-sid=68c51751758433353112444258xby3oa4gcwl2sd; x-user-id-pgy.xiaohongshu.com=67d580eb0000000008014d85; customerClientId=630481399870715; abRequestId=b26253878c867794e4cfbc1fe1f42755; web_session=030037af6087be106b5f86952b2f4a61f3f5d1; solar.beaker.session.id=AT-68c517519027579886274599hczzr4pgki86uuim; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517519027579886274599hczzr4pgki86uuim; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517519027579886274599hczzr4pgki86uuim; webBuild=4.68.0; xsecappid=ratlin; acw_tc=0a0d099e17509234946368713e3b497bc4afead80d197c3ba4987aac4fc321; loadts=1750923494970; websectiga=16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134; sec_poison_id=0a6ea091-b1de-4137-b8e7-598d7b116bd3'
# contactInfoCiphertext = 'pgy_sens_encrypt:gAllwlHgBZi+vMIGAd8mGqOORK+zqA36puYNAdq+iMPkcUlXKxE+PRNLRRqpoCfUxWTTdaR9cxxUtJSWkEtilDDU8OCJzX1+9LCZs4Vty52KIbHXLmxnSIUpJ5Ic7EseioZ0S+TIbzohkN/BQrfT7F6qy1nfKmx3GvXYKP1NrsY='
# llmm19897
contactInfoCiphertext = 'pgy_sens_encrypt:QW+l9yG7ebFQ3MCmello9i1VGpCwctPvrCIY35KU0Q9Vwkuss2M35aRx7V3hx9o4eEKEbtwCtRGAuuTU3IAWRvkQhp/D7doAVPDiGeQmBeIPT5z2tCAEu1r0b4l7ixt09dbMJAAAGoEk3TAC3udcMml+CNcMiurEuYdUKU1NeQs=' #llmm19897
contactInfo = 'llmm19897'
# 13002729752
# contactInfoCiphertext = 'pgy_sens_encrypt:gAllwlHgBZi+vMIGAd8mGqOORK+zqA36puYNAdq+iMPkcUlXKxE+PRNLRRqpoCfUxWTTdaR9cxxUtJSWkEtilLWgCIV6TVAr5/VdsHsgpv0fuutUQIT69i1UvdqZd0tZMael+rBIvkR5l7fbP/pheTvILebP5q5Xdvzbsz3gMYs='
# contactInfo = '13002729752'
cooperateBrandId = '67d7d540000000000e02d7e0'
productName = 'bc山茶花纸尿裤'
inviteContent = 'bc山茶花纸尿裤 项目二核'
# 期望开始时间和期望结束时间
expectedPublishTimeStart = '2025-07-05'
expectedPublishTimeEnd = '2025-07-10'

# 处理发送邀约逻辑的主入口
def get_table_content():
    print("开始执行自动发送邀约...")
    records = read_table_content(app_token, table_id, view_id)
    for record in records:
        record_id = record.get("record_id")
        fields = record.get("fields", {})
        kol_id = fields.get("博主id")['value'][0]['text']

        print(f"正在处理账号: {cooperateBrandName}")
        try:

            # 构建邀约数据
            data = {
                'kolId': kol_id,
                'cooperateBrandName': cooperateBrandName,
                'cooperateBrandId': cooperateBrandId,
                'productName': productName,
                'inviteType': 2,
                'expectedPublishTimeStart': expectedPublishTimeStart,
                'expectedPublishTimeEnd': expectedPublishTimeEnd,
                'inviteContent': inviteContent,
                'contactType': 2,
                'contactInfo': contactInfo,
                'contactInfoCiphertext': contactInfoCiphertext,
                'kolType': 0,
                'brandUserId': cooperateBrandId
            }

            print(f"准备发送邀约给博主: {kol_id}")

            # 发送邀约
            time.sleep(6)
            content_length = calculate(data)
            remainTimes = send_yaoyue(content_length, token, kol_id, data, record_id, cooperateBrandName)

            if remainTimes == 10040000:
                print(f"账号 {cooperateBrandId} 发送邀约失败，kol信息有误")
                continue

            if remainTimes is None:
                print(f"账号 {cooperateBrandId} 发送邀约失败，强制切换下一个账号")
                break

            # 更新飞书记录
            fields = {
                '是否发送邀约': '已邀约'
            }
            update_record(app_token, table_id, record_id, fields)
            print(f"博主: {kol_id}已发送邀约")

            if remainTimes == 0:
                return

        except Exception as e:
            print(f"处理微信号时出错：{str(e)}")
    print("发送邀约执行完成")

"""
    蒲公英接口
"""

# 发送邀约
def send_yaoyue(content_length, token, kol_id, data, record_id, name):
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
        response_json = response.json()
        print("Response:", response_json)
        if response_json.get('code') == 0 and response_json.get('success') == True and response_json.get('data')[
            'hint'] == '有邀约权限':
            return response_json.get('data')['remainTimes']
        elif response_json.get('code') in (902, 903, 906, -1):
            print(f"token过期信息,过期账号名:{name}")
            return None
        elif response_json.get('code') == -10040000:
            print(kol_id)
            update_fields = {
                '备注': '博主账号异常无法发送邀约',
            }
            try:
                update_record(app_token, table_id, record_id, update_fields)
                return 10040000
            except Exception as e:
                print(f"更新飞书记录时发生异常：{e}")
        else:
            update_fields = {
                '备注': response_json.get('data')['hint']
            }
            try:
                update_record(app_token, table_id, record_id, update_fields)
                return None
            except Exception as e:
                print(f"更新飞书记录时发生异常：{e}")
    except requests.RequestException as e:
        print(f"更新飞书记录时发生异常：{e}")

get_table_content()