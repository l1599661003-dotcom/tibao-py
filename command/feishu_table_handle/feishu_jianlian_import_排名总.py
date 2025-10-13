import html
import json
import certifi
import requests
from sqlalchemy import func
from sqlalchemy.dialects.oracle import NUMBER

from api import session
from models.models import PaiMing
from service.feishu_service import get_table_info, get_feishu_token
"""
    将 总排名数据表 导入到待建联博主多维表格
"""
def important_sheet1():
    app_token1 = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id1 = 'tbl76BOeADIJALUd'
    view_id1 = 'vewFjbwzKs'

    # 获取第二个表的数据
    data = get_table_info(app_token1, table_id1, view_id1)
    if not data or 'items' not in data:
        print("Failed to get data from Feishu table.")
        return

    blogger_to_pgy = {}
    for item in data['items']:
        if '博主id' in item['fields'] and item['fields']['博主id']:
            blogger_to_pgy[html.unescape(item['fields']['博主id'][0]['text'])] = item

    subquery = (
        session.query(
            PaiMing.博主id,
            func.max(PaiMing.所属月份).label('max_month')
        )
        .filter(PaiMing.月总营收 >= 2000)
        .group_by(PaiMing.博主id)
    ).subquery()

    # 然后，使用联接查询来获取完整的记录
    paimings = (
        session.query(PaiMing)
        .join(subquery, (PaiMing.博主id == subquery.c.博主id) & (PaiMing.所属月份 == subquery.c.max_month))
        .all()
    )
    print(len(paimings))

    # 找到匹配的记录
    matching_records = {}
    for paiming in paimings:
        pgy_id = paiming.博主id  # 确保获取正确的 pgy_id 属性
        if pgy_id in blogger_to_pgy:
            matching_records[pgy_id] = blogger_to_pgy[pgy_id]

    # 打印匹配的记录
    updates = []
    for pgy_id, blogger_item in matching_records.items():
        # 在 paimings 中找到对应的项
        first_table_item = next((p for p in paimings if p.博主id == pgy_id), None)

        if first_table_item is None:
            print(f"No matching record found for pgy_id: {pgy_id}")
            continue
        second_table_item = blogger_item
        # 更新字段
        second_table_item['fields'].update({
            '达人粉丝量': int(first_table_item.达人粉丝量) if first_table_item.达人粉丝量 else 0,
            '标签': first_table_item.标签.split(',') if isinstance(first_table_item.标签,
                                                                   str) else first_table_item.标签,
            '达人所属机构': first_table_item.达人所属机构,
            '图文商单数量': int(first_table_item.图文商单数量) if first_table_item.图文商单数量 else 0,
            '视频商单数量': int(first_table_item.视频商单数量) if first_table_item.视频商单数量 else 0,
            '图文营收': int(first_table_item.图文营收) if first_table_item.图文营收 else 0,
            '视频营收': int(first_table_item.视频营收) if first_table_item.视频营收 else 0,
            '月总营收': int(first_table_item.月总营收) if first_table_item.月总营收 else 0,
            '图文价格': int(first_table_item.图文价格) if first_table_item.图文价格 else 0,
            '视频价格': int(first_table_item.视频价格) if first_table_item.视频价格 else 0,
            '所属月份': first_table_item.所属月份,
            '简介': first_table_item.简介,
        })

        # 删除多余字段
        for field_to_remove in ['SourceID', '博主id', '达人昵称', '父记录 2', '流转倒计时', '经纪人添加时间', '蒲公英链接',
                                '是否添加成功', '微信号', '返点登记', '区分列', '环比', '是否发送邀约',
                                '母婴所属分类二级', '家居所属分类二级', '时尚所属分类二级', '方片所属分类二级', '方片所属分类二级',
                                '去重1', '去重', '发送邀约时间', '商务自打一级', '商务自打二级', '最新图文价格', '最新视频价格',
                                '金虎vx填写时间', '微信号登记时间', '是否添加成功', '父记录', '文本 13', '经纪人', '是否添加微信',
                                '重复号博主名', '签约反馈', '博主在意点', '流量组微信核对时间', '流量组金虎VX登记人 13', '反馈']:
            second_table_item['fields'].pop(field_to_remove, None)

        # 准备批量更新的数据
        updates.append({
            'record_id': second_table_item['record_id'],
            'fields': second_table_item['fields']
        })

    # 批量更新飞书记录
    chunk_size = 1000  # 根据飞书 API 的限制调整
    chunks = [updates[i:i + chunk_size] for i in range(0, len(updates), chunk_size)]
    for chunk in chunks:
        response = requests.post(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token1}/tables/{table_id1}/records/batch_update",
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'Authorization': f'Bearer {get_feishu_token()}'
            },
            data=json.dumps({'records': chunk}),
            verify=certifi.where()
        )
        print(response.json())

important_sheet1()