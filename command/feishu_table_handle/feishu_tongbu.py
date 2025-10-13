import html
from datetime import datetime

from sqlalchemy import func

from api import session
from models.models import PaiMing25_1, PaiMing, KolMediaAccountsWaicai
from service.feishu_service import get_table_info

"""
    将一月份的数据同步到排名总表
"""
def important_sheet1_insert():
    try:
        # 1月份表
        datas = session.query(PaiMing25_1).all()

        # 找出不在 paimings 中的博主 ID 进行插入
        for data in datas:
            new_entry = PaiMing(
                达人昵称=data.达人昵称,
                达人粉丝量=data.达人粉丝量,
                达人所属机构=data.达人所属机构,
                标签=data.标签,
                图文商单数量=data.图文商单数量,
                视频商单数量=data.视频商单数量,
                图文营收=data.图文营收,
                视频营收=data.视频营收,
                月总营收=data.月总营收,
                博主id=data.博主id,
                图文价格=data.图文价格,
                视频价格=data.视频价格,
                所属月份=data.所属月份,
                小红书账号=data.小红书账号,
                简介=data.简介,
            )
            session.add(new_entry)
        session.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()  # 回滚事务
    finally:
        session.close()

"""
    将大于2000的数据同步到KolMediaAccountsWaicai表
"""
def jianlian_update():
    app_token1 = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id1 = 'tbliGz3IRUgjz5Jg'
    view_id1 = 'vew7UDFBUD'

    data = get_table_info(app_token1, table_id1, view_id1)
    if not data or 'items' not in data:
        print("Failed to get data from Feishu table.")
        return

    blogger_to_pgy = {}
    for item in data['items']:
        if '博主id' in item['fields'] and item['fields']['博主id']:
            blogger_to_pgy[html.unescape(item['fields']['博主id'][0]['text'])] = item

    kols = session.query(KolMediaAccountsWaicai).filter(
        func.length(KolMediaAccountsWaicai.pgy_id) > 5
    ).all()

    matching_records = {}
    for kol in kols:
        pgy_id = kol.pgy_id
        if pgy_id in blogger_to_pgy:
            matching_records[pgy_id] = blogger_to_pgy[pgy_id]
        else:
            kol.delete_type = 1

    existing_ids = set(matching_records.keys())
    for blogger_id, item in blogger_to_pgy.items():
        if blogger_id not in existing_ids:
            # 创建一个新的 KolMediaAccountsWaicai 实例
            new_kol = KolMediaAccountsWaicai(
                pgy_id=html.unescape(item['fields']['博主id'][0]['text']),  # 确保字段存在
                nickname=html.unescape(item['fields']['达人昵称'][0]['text']),  # 确保字段存在
                created_at=datetime.now(),
                updated_at=datetime.now(),
                delete_type=0
            )
            session.add(new_kol)

    try:
        session.commit()
    except Exception as e:
        print(f"Error committing to the database: {e}")
important_sheet1_insert()