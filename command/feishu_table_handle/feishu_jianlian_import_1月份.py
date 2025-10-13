import urllib3
from sqlalchemy import func, and_

from core.localhost_fp_project import session
from models.models import  PaiMing25_1
from service.feishu_service import search_feishu_record, update_record, insert_record

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
    将该月份的数据导入到待建联博主-在用表，有的修改，没有的新增
    数据库操作去掉重复数据，取日期最近的那一条
"""
def jianlian_insert():
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tbliGz3IRUgjz5Jg'
    view_id = 'vewFjbwzKs'

    try:
        total_count = session.query(PaiMing25_1).filter(
            and_(PaiMing25_1.月总营收 < 2000, PaiMing25_1.月总营收 >= 1000)
        ).count()
        print(f"📊 总共 {total_count} 条数据符合条件")

        subquery = (
            session.query(
                PaiMing25_1.博主id,
                func.max(PaiMing25_1.所属月份).label('max_month')
            )
            .filter(and_(PaiMing25_1.月总营收 < 2000, PaiMing25_1.月总营收 >= 1000))
            .group_by(PaiMing25_1.博主id)
        ).subquery()

        paimings = (
            session.query(PaiMing25_1)
            .join(subquery, (PaiMing25_1.博主id == subquery.c.博主id) & (PaiMing25_1.所属月份 == subquery.c.max_month))
            .all()
        )
        processed_count = 0

        # 找出不在 blogger_to_pgy 中的博主 ID 进行插入
        for paiming in paimings:
            pgy_id = paiming.博主id
            data = search_feishu_record(app_token, table_id, view_id, '博主id', pgy_id)

            fields = {
                '博主id': pgy_id,
                '达人昵称': paiming.达人昵称,
                '蒲公英链接': f'https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{pgy_id}',
                '达人粉丝量': int(paiming.达人粉丝量) if paiming.达人粉丝量 else 0,
                '达人所属机构': paiming.达人所属机构,
                '标签': paiming.标签,
                '图文商单数量': int(paiming.图文商单数量) if paiming.图文商单数量 else 0,
                '视频商单数量': int(paiming.视频商单数量) if paiming.视频商单数量 else 0,
                '图文营收': int(paiming.图文营收) if paiming.图文营收 else 0,
                '视频营收': int(paiming.视频营收) if paiming.视频营收 else 0,
                '月总营收': int(paiming.月总营收) if paiming.月总营收 else 0,
                '图文价格': int(paiming.图文价格) if paiming.图文价格 else 0,
                '视频价格': int(paiming.视频价格) if paiming.视频价格 else 0,
                '所属月份': paiming.所属月份,
                '简介': paiming.简介,
                '内容类目1': paiming.标签,
            }

            if data is None or len(data) == 0:
                insert_record(app_token, table_id, fields)
            else:
                record_id = data[0]['record_id']
                fields['标签'] = paiming.标签.split(',') if isinstance(paiming.标签, str) else paiming.标签
                update_record(app_token, table_id, record_id, fields)

            processed_count += 1
            print(f"✅ 处理进度: {processed_count}/{total_count}")

    finally:
        session.close()

if __name__ == '__main__':
    jianlian_insert()