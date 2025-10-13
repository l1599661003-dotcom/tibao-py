import json

import certifi
import requests

from api import session
from models.models import BoZhu, JiGou, PaiMing
from service.feishu_service import get_feishu_token

"""
    删除指定的机构的博主信息
"""
def institution_kol_del():
    # 第一个表（有数据）
    # bozhus = session.query(BoZhu).all()
    # for bozhu in bozhus:
    #     print(bozhu.pgy_id)
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tblm6SMpbaYiPAHF'

    # 获取现有记录的蒲公英链接
    existing_links = set()
    response = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers={'Authorization': f'Bearer {get_feishu_token()}'}
    )
    if response.status_code == 200:
        existing_data = response.json()
        print(existing_data)
        for record in existing_data.get('data', {}).get('items', []):
            if '蒲公英链接' in record['fields']:
                existing_links.add(record['fields']['蒲公英链接'])

    # 收集要插入的数据
    array_total1 = []
    jigous = session.query(JiGou).all()
    paimings = session.query(PaiMing).all()
    for jigou in jigous:
        for paiming in paimings:
            if jigou.jigou == paiming.达人所属机构:
                link = paiming.博主id
                if link not in existing_links:  # 只插入不存在的链接
                    array_total1.append({
                        'fields': {
                            '蒲公英链接': link,
                        }
                    })
                    existing_links.add(link)  # 添加到已插入链接集合

    # 批量插入数据
    chunk_size = 1000  # 根据飞书 API 的限制调整
    chunks = [array_total1[i:i + chunk_size] for i in range(0, len(array_total1), chunk_size)]
    for chunk in chunks:
        response = requests.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': f'Bearer {get_feishu_token()}'
            },
            data=json.dumps({'records': chunk}),
            verify=certifi.where()  # 确保 SSL 证书验证
        )
        response_body = response.json()
        print(response_body)  # 打印响应以便调试

"""
    删除指定的博主信息
"""
def kol_del():
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tblm6SMpbaYiPAHF'

    # 获取现有记录的蒲公英链接
    existing_links = set()
    response = requests.get(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
        headers={'Authorization': f'Bearer {get_feishu_token()}'}
    )
    if response.status_code == 200:
        existing_data = response.json()
        for record in existing_data.get('data', {}).get('items', []):
            if '蒲公英链接' in record['fields']:
                existing_links.add(record['fields']['蒲公英链接'])

    # 收集要插入的数据
    array_total1 = []
    bozhus = session.query(BoZhu).all()
    for bozhu in bozhus:
        blogger_id = bozhu.pgy_id.split('/')[-1].split('?')[0]
        if blogger_id not in existing_links:  # 只插入不存在的链接
            array_total1.append({
                'fields': {
                    '蒲公英链接': blogger_id,
                }
            })
            existing_links.add(blogger_id)  # 更新已插入链接集合

    # 批量插入数据
    chunk_size = 1000  # 根据飞书 API 的限制调整
    chunks = [array_total1[i:i + chunk_size] for i in range(0, len(array_total1), chunk_size)]
    for chunk in chunks:
        response = requests.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': f'Bearer {get_feishu_token()}'
            },
            data=json.dumps({'records': chunk}),
            verify=certifi.where()  # 确保 SSL 证书验证
        )
        response_body = response.json()
        print(response_body)  # 打印响应以便调试

institution_kol_del()