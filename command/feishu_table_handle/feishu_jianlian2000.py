import json

import certifi
import requests
from sqlalchemy import or_, and_

from api import session
from models.models import FeiShuToken, KolMediaAccountsJianlian
from service.feishu_service import get_feishu_token, get_table_info

"""
    对飞书多维表格进行新增和修改数据
    id小于等于44000
"""
def feishu_0():
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tbliGz3IRUgjz5Jg'
    # 获取飞书 Token
    token = get_feishu_token()
    if not token:
        print("Failed to get Feishu token.")
        return

    # 获取已有 record_id 的记录
    kols = session.query(KolMediaAccountsJianlian).filter(
        KolMediaAccountsJianlian.monthly_income >= 2000,
        and_(
            KolMediaAccountsJianlian.record_id.isnot(None),
            KolMediaAccountsJianlian.record_id != ''
        )
    ).all()

    # 获取 record_id 为空字符串或为 None 的记录，按 id 升序排序，限制 44000 条
    kol1s = session.query(KolMediaAccountsJianlian).filter(
        KolMediaAccountsJianlian.id <= 44000,
        KolMediaAccountsJianlian.monthly_income >= 2000,
        or_(
            KolMediaAccountsJianlian.record_id == '',
            KolMediaAccountsJianlian.record_id == None
        )
    ).order_by(KolMediaAccountsJianlian.id.asc()).all()
    print(len(kol1s))
   # 处理已有  record_id 的记录
   #  array_total = []
   #  for kol in kols:
   #      pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{kol.pgy_id}"
   #      array_total.append({
   #          'record_id': kol.record_id,
   #          'fields': {
   #              'id': kol.id,
   #              '博主id': str(kol.pgy_id),
   #              '蒲公英链接': pgy_url,
   #              '达人昵称': kol.nickname,
   #              '微信号': kol.contact_info,
   #          }
   #      })
   #
   #  # 分批更新飞书记录
   #  chunk_size = 500  # 根据飞书 API 的限制调整
   #  chunks = [array_total[i:i + chunk_size] for i in range(0, len(array_total), chunk_size)]
   #  for chunk in chunks:
   #      response = requests.post(
   #          f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
   #          headers={
   #              'Content-Type': 'application/json; charset=utf-8',
   #              'Authorization': f'Bearer {token}'
   #          },
   #          data=json.dumps({'records': chunk}),
   #          verify=certifi.where()  # 确保 SSL 证书验证
   #      )

    # 处理没有 record_id 的记录
    array_total1 = []
    for kol in kol1s:
        pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{kol.pgy_id}"
        array_total1.append({
            'fields': {
                'id': kol.id,
                '博主id': str(kol.pgy_id),
                '蒲公英链接': pgy_url,
                '达人昵称': kol.nickname,
                '微信号': kol.contact_info,
            }
        })
    print(len(array_total1))
    # 分批插入飞书记录
    chunk_size = 1000  # 根据飞书 API 的限制调整
    chunks = [array_total1[i:i + chunk_size] for i in range(0, len(array_total1), chunk_size)]
    print(chunks, 1111)
    for chunk in chunks:
        response = requests.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': f'Bearer {token}'
            },
            data=json.dumps({'records': chunk}),
            verify=certifi.where()  # 确保 SSL 证书验证
        )
        response_body = response.json()

        if 'data' in response_body and 'records' in response_body['data']:
            for record in response_body['data']['records']:
                if 'record_id' in record:
                    record_id = record['record_id']
                    pgy_id = record['fields']['博主id']
                    session.query(KolMediaAccountsJianlian).filter_by(pgy_id=pgy_id).update(
                        {'record_id': record_id})
                    session.commit()

"""
    对飞书多维表格进行新增和修改数据
    id大于44000 小于等于88000
"""
def feishu_44000():
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tbl76BOeADIJALUd'
    # 获取飞书 Token
    token = get_feishu_token()
    if not token:
        print("Failed to get Feishu token.")
        return

    # 获取已有 record_id 的记录
    kols = session.query(KolMediaAccountsJianlian).filter(
        KolMediaAccountsJianlian.monthly_income >= 2000,
        and_(
            KolMediaAccountsJianlian.record_id.isnot(None),
            KolMediaAccountsJianlian.record_id != ''
        )
    ).all()

    # 获取 record_id 为空字符串或为 None 的记录，按 id 升序排序，限制 44000 条
    kol1s = session.query(KolMediaAccountsJianlian).filter(
        KolMediaAccountsJianlian.monthly_income >= 2000,
        and_(
            KolMediaAccountsJianlian.id > 44000,
            KolMediaAccountsJianlian.id <= 88000
        ),
        or_(
            KolMediaAccountsJianlian.record_id == '',
            KolMediaAccountsJianlian.record_id == None
        )
    ).order_by(KolMediaAccountsJianlian.id.asc()).all()

    # 处理已有 record_id 的记录
    if len(kols) > 0:
        array_total = []
        for kol in kols:
            pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{kol.pgy_id}"
            array_total.append({
                'record_id': kol.record_id,
                'fields': {
                    'id': kol.id,
                    '博主id': str(kol.pgy_id),
                    '蒲公英链接': pgy_url,
                    '达人昵称': kol.nickname,
                    '微信号': kol.contact_info,
                }
            })

        # 分批更新飞书记录
        chunk_size = 500  # 根据飞书 API 的限制调整
        chunks = [array_total[i:i + chunk_size] for i in range(0, len(array_total), chunk_size)]
        for chunk in chunks:
            response = requests.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
                headers={
                    'Content-Type': 'application/json; charset=utf-8',
                    'Authorization': f'Bearer {token}'
                },
                data=json.dumps({'records': chunk}),
                verify=certifi.where()  # 确保 SSL 证书验证
            )

    # 处理没有 record_id 的记录
    if len(kol1s) > 0:
        array_total1 = []
        for kol in kol1s:
            pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{kol.pgy_id}"
            array_total1.append({
                'fields': {
                    'id': kol.id,
                    '博主id': str(kol.pgy_id),
                    '蒲公英链接': pgy_url,
                    '达人昵称': kol.nickname,
                    '微信号': kol.contact_info,
                }
            })

        # 分批插入飞书记录
        chunk_size = 1000  # 根据飞书 API 的限制调整
        chunks = [array_total1[i:i + chunk_size] for i in range(0, len(array_total1), chunk_size)]
        for chunk in chunks:
            response = requests.post(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
                headers={
                    'Content-Type': 'application/json; charset=utf-8',
                    'Authorization': f'Bearer {token}'
                },
                data=json.dumps({'records': chunk}),
                verify=certifi.where()  # 确保 SSL 证书验证
            )
            response_body = response.json()

            if 'data' in response_body and 'records' in response_body['data']:
                for record in response_body['data']['records']:
                    if 'record_id' in record:
                        record_id = record['record_id']
                        pgy_id = record['fields']['博主id']
                        session.query(KolMediaAccountsJianlian).filter_by(pgy_id=pgy_id).update(
                            {'record_id': record_id})
                        session.commit()

"""
    对飞书多维表格进行新增和修改数据
    id大于88000
"""
def feishu_88000():
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tblIyL4UkR8wpYz2'

    # 获取飞书 Token
    feishu_token = session.query(FeiShuToken).filter_by(id=1).first()
    token = feishu_token.token if feishu_token else None
    if not token:
        print("Failed to get Feishu token.")
        return

    # 获取已有 record_id 的记录
    kols = session.query(KolMediaAccountsJianlian).filter(
        KolMediaAccountsJianlian.monthly_income > 5000,
        and_(
            KolMediaAccountsJianlian.record_id.isnot(None),
            KolMediaAccountsJianlian.record_id != ''
        )
    ).all()

    # 获取 record_id 为空字符串或为 None 的记录，按 id 升序排序，限制 44000 条
    kol1s = session.query(KolMediaAccountsJianlian).filter(
        KolMediaAccountsJianlian.id > 88000,
        KolMediaAccountsJianlian.monthly_income > 2000,
        or_(
            KolMediaAccountsJianlian.record_id == '',
            KolMediaAccountsJianlian.record_id == None
        )
    ).order_by(KolMediaAccountsJianlian.id.asc()).all()


    # 处理已有 record_id 的记录
    array_total = []
    for kol in kols:
        pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{kol.pgy_id}"
        array_total.append({
            'record_id': kol.record_id,
            'fields': {
                'id': kol.id,
                '博主id': str(kol.pgy_id),
                '蒲公英链接': pgy_url,
                '达人昵称': kol.nickname,
            }
        })

    # 分批更新飞书记录
    chunk_size = 500  # 根据飞书 API 的限制调整
    chunks = [array_total[i:i + chunk_size] for i in range(0, len(array_total), chunk_size)]
    for chunk in chunks:
        response = requests.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': f'Bearer {token}'
            },
            data=json.dumps({'records': chunk}),
            verify=certifi.where()  # 确保 SSL 证书验证
        )
    # 处理没有 record_id 的记录
    array_total1 = []
    for kol in kol1s:
        pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{kol.pgy_id}"
        array_total1.append({
            'fields': {
                'id': kol.id,
                '博主id': str(kol.pgy_id),
                '蒲公英链接': pgy_url,
                '达人昵称': kol.nickname,
            }
        })

    # 分批插入飞书记录
    chunk_size = 1000  # 根据飞书 API 的限制调整
    chunks = [array_total1[i:i + chunk_size] for i in range(0, len(array_total1), chunk_size)]
    for chunk in chunks:
        response = requests.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create",
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': f'Bearer {token}'
            },
            data=json.dumps({'records': chunk}),
            verify=certifi.where()  # 确保 SSL 证书验证
        )
        response_body = response.json()

        if 'data' in response_body and 'records' in response_body['data']:
            for record in response_body['data']['records']:
                if 'record_id' in record:
                    record_id = record['record_id']
                    pgy_id = record['fields']['博主id']
                    session.query(KolMediaAccountsJianlian).filter_by(pgy_id=pgy_id).update(
                        {'record_id': record_id})
                    session.commit()

"""
    获取飞书多维表格数据,并将微信号传入数据库
"""
def get_feishu_jianlian():

    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tbliGz3IRUgjz5Jg'
    view_id = 'vewJq9KS7s'

    # 获取飞书表格数据
    data = get_table_info(app_token, table_id, view_id)
    if not data or 'items' not in data:
        print("Failed to get data from Feishu table.")
        return

    # 获取本地数据库中需要更新的记录
    kols = session.query(KolMediaAccountsJianlian).filter(
        or_(
            KolMediaAccountsJianlian.contact_info == '',
            KolMediaAccountsJianlian.contact_info == None
        )
    ).all()

    # 创建一个字典以便快速查找
    feishu_data = {
        int(item['fields']['id']): next((record['text'] for record in item['fields']['微信号'] if 'text' in record), None)
        for item in data['items'] if 'id' in item['fields'] and '微信号' in item['fields']
    }
    # 更新本地数据库中的记录
    for kol in kols:
        if kol.id in feishu_data:
            kol.contact_info = feishu_data[kol.id]

    # 提交更新
    session.commit()


# get_feishu_jianlian()
feishu_0()