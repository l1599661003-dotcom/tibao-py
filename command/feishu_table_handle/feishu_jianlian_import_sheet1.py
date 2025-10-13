import html
import json

import certifi
import requests
import urllib3

from service.feishu_service import get_table_info, get_feishu_token

"""
    将sheet1数据导入到待建联博主多为表格
"""
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def important_sheet1():
    # 第一个表（有数据）
    app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id = 'tblUYL1HJsBLGqMj'
    view_id = 'vewHoFwq3A'

    app_token1 = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
    table_id1 = 'tbliGz3IRUgjz5Jg'
    view_id1 = 'vewFjbwzKs'

    # 获取第一个表的数据
    data = get_table_info(app_token, table_id, view_id)
    if not data or 'items' not in data:
        print("Failed to get data from Feishu table.")
        return

    # 获取第二个表的数据
    data1 = get_table_info(app_token1, table_id1, view_id1)
    if not data1 or 'items' not in data1:
        print("Failed to get data1 from Feishu table.")
        return

    # 创建一个字典以便快速查找
    pgy_to_blogger = {}
    for item in data['items']:
        if '博主id' in item['fields'] and item['fields']['博主id']:
            pgy_to_blogger[html.unescape(item['fields']['博主id'][0]['text'])] = item

    blogger_to_pgy = {}
    for item in data1['items']:
        if '蒲公英链接' in item['fields'] and item['fields']['蒲公英链接']:
            blogger_to_pgy[html.unescape(item['fields']['蒲公英链接'][0]['text'])] = item

    # 找到匹配的记录
    matching_records = {}
    for pgy_id, item in pgy_to_blogger.items():
        if pgy_id in blogger_to_pgy:
            matching_records[pgy_id] = blogger_to_pgy[pgy_id]

    # 打印匹配的记录
    updates = []
    for pgy_id, blogger_item in matching_records.items():
        first_table_item = pgy_to_blogger[pgy_id]
        second_table_item = blogger_item

        # 检查微信号是否是列表类型
        weixin = first_table_item['fields'].get('微信号', '')
        if isinstance(weixin, list) and len(weixin) > 0:
            vx = weixin[0].get('text', '')  # 如果是列表，获取第一个字典的文本
        else:
            vx = weixin

        # 确保经纪人字段中 ID 的有效性
        jingji = first_table_item['fields'].get('经纪人', [{'id': ''}])[0]['id']  # 获取经纪人的 ID
        if jingji:
            # 如果经纪人 ID 有效，将其格式化为字典
            jjr = [{'id': str(jingji)}]  # 确保 ID 是字符串
        else:
            jjr = []  # 如果没有有效 ID，设置为空列表

        quchong1 = first_table_item['fields'].get('去重1', [''])[0]  # 获取去重1字段的值
        qc1 = quchong1['text'] if isinstance(quchong1, dict) and 'text' in quchong1 else str(quchong1)

        quchong = first_table_item['fields'].get('去重', [''])[0]  # 获取去重1字段的值
        qc = quchong['text'] if isinstance(quchong, dict) and 'text' in quchong else str(quchong)

        # 提取需要的字段的 text 值
        def extract_text(field):
            if isinstance(field, list) and len(field) > 0 and isinstance(field[0], dict):
                return field[0].get('text', '')
            return ''

        second_table_item['fields'].update({
            '蒲公英链接': first_table_item['fields'].get('博主id', [''])[0]['text'],
            '是否发送邀约': first_table_item['fields'].get('是否发送邀约', ''),
            '达人粉丝量': int(first_table_item['fields'].get('达人粉丝量', 0)) if '达人粉丝量' in first_table_item[
                'fields'] else 0,  # 确保是数字
            '微信号': vx,
            # '所属分类': first_table_item['fields'].get('所属分类', []),
            '金虎VX': str(first_table_item['fields'].get('金虎VX', '')),
            '返点登记': extract_text(first_table_item['fields'].get('返点登记', [])),
            '添加微信核对人': str(first_table_item['fields'].get('核对人', '')),
            '标签': first_table_item['fields'].get('标签', []),
            '区分列': first_table_item['fields'].get('区分列', [''])[0]['text'] if isinstance(
                first_table_item['fields'].get('区分列', []), list) and len(
                first_table_item['fields'].get('区分列', [])) > 0 else '',
            '达人所属机构': extract_text(first_table_item['fields'].get('达人所属机构', [])),
            '备注': first_table_item['fields'].get('备注', ''),
            '发送邀约时间': int(first_table_item['fields'].get('发送邀约时间', 0)),
            '去重1': qc1,
            '去重': qc,
            '商务自打一级': first_table_item['fields'].get('商务自打一级', []),  # 确保是字符串
            '商务自打二级': first_table_item['fields'].get('商务自打二级', []),  # 确保是字符串列表
            '图文商单数量': int(first_table_item['fields'].get('图文商单数量', 0)) if '图文商单数量' in
                                                                                      first_table_item['fields'] else 0,
            '视频商单数量': int(first_table_item['fields'].get('视频商单数量', 0)),
            '图文营收': int(first_table_item['fields'].get('图文营收', 0)),
            '视频营收': int(first_table_item['fields'].get('视频营收', 0)),
            '月总营收': int(first_table_item['fields'].get('月总营收', 0)),
            '图文价格': int(first_table_item['fields'].get('图文价格', 0)),
            '视频价格': int(first_table_item['fields'].get('视频价格', 0)),
            '所属月份': extract_text(first_table_item['fields'].get('所属月份', [])),  # 确保是字符串
            '环比': extract_text(first_table_item['fields'].get('环比', [])),  # 确保是字符串
            '金虎vx填写时间': int(first_table_item['fields'].get('金虎vx填写时间', 0)),
            '微信号登记时间': int(first_table_item['fields'].get('微信号登记时间', 0)),  # 确保是 Unix 时间戳
            '是否添加成功': extract_text(first_table_item['fields'].get('是否添加成功', [])),  # 确保是字符串
            '文本 13': first_table_item['fields'].get('文本 13', ''),
            '经纪人': jjr,
        })

        for field_to_remove in ['SourceID', '博主id', '达人昵称', '父记录 2', '流转倒计时', '经纪人添加时间', '内容类目1'
            , '内容占比1', '内容类目2', '内容占比2', '博主是否预留联系方式', '博主手机号', '方片微信号', '连续30天负增长'
            , '阅读量来源的【发现页】占比', '简介', '微信号', '微信号 副本', '进群核对人', '所属分类', '返点备注']:
            if field_to_remove in second_table_item['fields']:
                del second_table_item['fields'][field_to_remove]

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
            verify=certifi.where()  # 确保 SSL 证书验证
        )
        print(response.json())


important_sheet1()