import html
import json
import certifi
import requests
from service.feishu_service import get_table_info, get_feishu_token

"""
    将7-10月份榜单博主导入到待建联博主多为表格
"""
def important_7_10():
    # 第一个表（有数据）
    app_token = 'ZNxTblPcuaP0oIsf5SccodhinLe'
    table_id = 'tbl2IThX7L9L7be8'
    view_id = 'vew1RI8frh'

    app_token1 = 'OFRyb2P6Aa85Z1sdS96cOnQnnqh'
    table_id1 = 'tbl3y4wPdCDT4HA7'
    view_id1 = 'vewKC6d950'

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
        if '蒲公英链接' in item['fields'] and item['fields']['蒲公英链接']:
            pgy_to_blogger[html.unescape(item['fields']['蒲公英链接'][0]['text'])] = item

    # 准备插入的数据
    inserts = []
    for item in data1['items']:
        if '蒲公英链接' in item['fields'] and item['fields']['蒲公英链接']:
            pgy_id = html.unescape(item['fields']['蒲公英链接'][0]['text'])
            # 如果表一中没有这个蒲公英链接，则准备插入
            if pgy_id not in pgy_to_blogger:
                内容类目及占比 = item['fields'].get('内容类目及占比', [])
                if 内容类目及占比 and isinstance(内容类目及占比, str):
                    内容类目及占比 = 内容类目及占比.split(',')
                垂类标签 = item['fields'].get('垂类标签', [])
                if 垂类标签 and isinstance(垂类标签, str):
                    垂类标签 = 垂类标签.split(',')

                已合作品牌 = item['fields'].get('已合作品牌', [])
                if isinstance(已合作品牌, list):
                    # 如果是列表，将列表元素用逗号分隔成一个字符串
                    已合作品牌 = ', '.join(
                        [brand['text'] for brand in 已合作品牌 if isinstance(brand, dict) and 'text' in brand])
                elif isinstance(已合作品牌, str):
                    # 如果已经是字符串，直接使用
                    已合作品牌 = 已合作品牌
                else:
                    # 如果字段不存在或值为空，设置为空字符串
                    已合作品牌 = ''

                培训返点 = item['fields'].get('培训返点', '')
                if isinstance(培训返点, list):
                    # 如果是列表，尝试提取第一个元素的文本值
                    培训返点 = 培训返点[0].get('text', '') if 培训返点 else ''
                elif isinstance(培训返点, dict):
                    # 如果是字典，尝试提取 'text' 键的值
                    培训返点 = 培训返点.get('text', '')
                else:
                    # 如果字段值已经是字符串，直接使用
                    培训返点 = str(培训返点) if 培训返点 is not None else ''
                # 准备新记录
                new_record = {
                    'fields': {
                        '昵称': item['fields'].get('昵称', [{'text': ''}])[0].get('text', ''),
                        '编导': item['fields'].get('编导', [{'text': ''}])[0].get('text', ''),
                        '性别': item['fields'].get('性别', ''),
                        '博主分级': item['fields'].get('博主分级', ''),
                        '新晋博主': item['fields'].get('新晋博主', [])[0].get('text', '') if item['fields'].get('新晋博主', []) else '',
                        '博主类型': item['fields'].get('博主类型', ''),
                        '账号等级': item['fields'].get('账号等级', ''),
                        '小红书ID': item['fields'].get('小红书ID', [{'text': '0'}])[0].get('text', '0'),
                        '内容类目及占比':内容类目及占比,
                        '预估阅读单价（图文）': item['fields'].get('预估阅读单价（图文）', 0),  # 改为0
                        '预估阅读单价（视频）': item['fields'].get('预估阅读单价（视频）', 0),  # 改为0
                        '修哥组商务标签': ', '.join([tag['text'] for tag in item['fields'].get('修哥组商务标签', []) if isinstance(tag, dict) and 'text' in tag]),
                        '小红书主页链接': item['fields'].get('小红书主页链接', [{'text': ''}])[0].get('text', ''),
                        '粉丝数': int(item['fields'].get('粉丝数', 0)),
                        '赞藏': int(item['fields'].get('赞藏', 0)),
                        '图文报备': int(item['fields'].get('图文报备', 0)),
                        '培训返点': 培训返点,
                        '视频报备': int(item['fields'].get('视频报备', 0)),
                        '所在地区': item['fields'].get('所在地区', ''),
                        '收货地址': item['fields'].get('收货地址', ''),
                        '日常视频笔记数': int(item['fields'].get('日常视频笔记数', 0)),
                        '日常图文笔记数': int(item['fields'].get('日常图文笔记数', 0)),
                        '合作图文笔记数': int(item['fields'].get('合作图文笔记数', 0)),
                        '合作视频笔记数': int(item['fields'].get('合作视频笔记数', 0)),
                        '系统订单总数': int(item['fields'].get('系统订单总数', 0)),
                        '30天内系统订单数': int(item['fields'].get('30天内系统订单数', 0)),
                        '90天内系统订单数': int(item['fields'].get('90天内系统订单数', 0)),
                        '蒲公英商单总数': int(item['fields'].get('蒲公英商单总数', 0)),
                        '30天蒲公英商单数': int(item['fields'].get('30天蒲公英商单数', 0)),
                        '90天蒲公英商单数': int(item['fields'].get('90天蒲公英商单数', 0)),
                        '博主机构': item['fields'].get('博主机构', [{'text': ''}])[0].get('text', ''),
                        '签约类型': '培训',
                        '签约状态': item['fields'].get('签约状态', ''),
                        '垂类标签': 垂类标签,
                        '垂类细分标签': '\n'.join(item['fields'].get('垂类细分标签', [])),
                        '职业标签': item['fields'].get('职业标签', []),
                        '人设标签': item['fields'].get('人设标签', []),
                        '分组': item['fields'].get('分组', [{'text': ''}])[0].get('text', ''),
                        '蒲公英链接': pgy_id,
                        '年框返点比例': item['fields'].get('年框返点比例', 0),  # 改为0
                        '非年框返点比例': item['fields'].get('非年框返点比例', 0),  # 改为0
                        '已合作品牌': 已合作品牌,
                        '签约人': item['fields'].get('签约人', ''),
                        '日常图文+视频曝光中位数': item['fields'].get('日常图文+视频曝光中位数', 0),  # 改为0
                        '日常图文+视频阅读中位数': item['fields'].get('日常图文+视频阅读中位数', 0),  # 改为0
                        '日常图文+视频互动中位数': item['fields'].get('日常图文+视频互动中位数', 0),  # 改为0
                        '日常图文+视频互动率': item['fields'].get('日常图文+视频互动率', 0),  # 改为0
                        '日常图文+视频千赞笔记比例': item['fields'].get('日常图文+视频千赞笔记比例', 0),  # 改为0
                        '日常图文+视频完播率': item['fields'].get('日常图文+视频完播率', 0),  # 改为0
                        '日常图文+视频3秒阅读率': item['fields'].get('日常图文+视频3秒阅读率', 0),  # 改为0
                        '日常图文+视频-视频CPE': item['fields'].get('日常图文+视频-视频CPE', 0),  # 改为0
                        '日常图文+视频-图文CPE': item['fields'].get('日常图文+视频-图文CPE', 0),  # 改为0
                        '日常图文+视频-视频CPM': item['fields'].get('日常图文+视频-视频CPM', 0),  # 改为0
                        '日常图文+视频-图文CPM': item['fields'].get('日常图文+视频-图文CPM', 0),  # 改为0
                        '日常图文+视频-视频CPC': item['fields'].get('日常图文+视频-视频CPC', 0),  # 改为0
                        '日常图文+视频-图文CPC': item['fields'].get('日常图文+视频-图文CPC', 0),  # 改为0
                        '日常图文+视频-视频CPR': item['fields'].get('日常图文+视频-视频CPR', 0),  # 改为0
                        '日常图文+视频-图文CPR': item['fields'].get('日常图文+视频-图文CPR', 0),  # 改为0
                        '合作图文+视频曝光中位数': item['fields'].get('合作图文+视频曝光中位数', 0),  # 改为0
                        '合作图文+视频阅读中位数': item['fields'].get('合作图文+视频阅读中位数', 0),  # 改为0
                        '合作图文+视频互动中位数': item['fields'].get('合作图文+视频互动中位数', 0),  # 改为0
                        '合作图文+视频互动率': item['fields'].get('合作图文+视频互动率', 0),  # 改为0
                        '合作图文+视频千赞笔记比例': item['fields'].get('合作图文+视频千赞笔记比例', 0),  # 改为0
                        '合作图文+视频完播率': item['fields'].get('合作图文+视频完播率', 0),  # 改为0
                        '合作图文+视频3秒阅读率': item['fields'].get('合作图文+视频3秒阅读率', 0),  # 改为0
                        '合作图文+视频-视频CPE': item['fields'].get('合作图文+视频-视频CPE', 0),  # 改为0
                        '合作图文+视频-图文CPE': item['fields'].get('合作图文+视频-图文CPE', 0),  # 改为0
                        '合作图文+视频-视频CPM': item['fields'].get('合作图文+视频-视频CPM', 0),  # 改为0
                        '合作图文+视频-图文CPM': item['fields'].get('合作图文+视频-图文CPM', 0),  # 改为0
                        '合作图文+视频-视频CPC': item['fields'].get('合作图文+视频-视频CPC', 0),  # 改为0
                        '合作图文+视频-图文CPC': item['fields'].get('合作图文+视频-图文CPC', 0),  # 改为0
                        '合作图文+视频-视频CPR': item['fields'].get('合作图文+视频-视频CPR', 0),  # 改为0
                        '合作图文+视频-图文CPR': item['fields'].get('合作图文+视频-图文CPR', 0),  # 改为0
                        '日常视频曝光中位数': item['fields'].get('日常视频曝光中位数', 0),  # 改为0
                        '日常视频阅读中位数': item['fields'].get('日常视频阅读中位数', 0),  # 改为0
                        '日常视频互动中位数': item['fields'].get('日常视频互动中位数', 0),  # 改为0
                        '日常视频互动率': item['fields'].get('日常视频互动率', 0),  # 改为0
                        '日常视频千赞笔记比例': item['fields'].get('日常视频千赞笔记比例', 0),  # 改为0
                        '日常视频完播率': item['fields'].get('日常视频完播率', 0),  # 改为0
                        '日常视频3秒阅读率': item['fields'].get('日常视频3秒阅读率', 0),  # 改为0
                        '日常视频CPE': item['fields'].get('日常视频CPE', 0),  # 改为0
                        '日常视频CPM': item['fields'].get('日常视频CPM', 0),  # 改为0
                        '日常视频CPC': item['fields'].get('日常视频CPC', 0),  # 改为0
                        '日常视频CPR': item['fields'].get('日常视频CPR', 0),  # 改为0
                        '日常图文曝光中位数': item['fields'].get('日常图文曝光中位数', 0),  # 改为0
                        '日常图文阅读中位数': item['fields'].get('日常图文阅读中位数', 0),  # 改为0
                        '日常图文互动中位数': item['fields'].get('日常图文互动中位数', 0),  # 改为0
                        '日常图文互动率': item['fields'].get('日常图文互动率', 0),  # 改为0
                        '日常图文千赞笔记比例': item['fields'].get('日常图文千赞笔记比例', 0),  # 改为0
                        '日常图文完播率': item['fields'].get('日常图文完播率', 0),  # 改为0
                        '日常图文3秒阅读率': item['fields'].get('日常图文3秒阅读率', 0),  # 改为0
                        '日常图文CPE': item['fields'].get('日常图文CPE', 0),  # 改为0
                        '日常图文CPM': item['fields'].get('日常图文CPM', 0),  # 改为0
                        '日常图文CPC': item['fields'].get('日常图文CPC', 0),  # 改为0
                        '日常图文CPR': item['fields'].get('日常图文CPR', 0),  # 改为0
                        '合作图文曝光中位数': item['fields'].get('合作图文曝光中位数', 0),  # 改为0
                        '合作图文阅读中位数': item['fields'].get('合作图文阅读中位数', 0),  # 改为0
                        '合作图文互动中位数': item['fields'].get('合作图文互动中位数', 0),  # 改为0
                        '合作图文互动率': item['fields'].get('合作图文互动率', 0),  # 改为0
                        '合作图文千赞笔记比例': item['fields'].get('合作图文千赞笔记比例', 0),  # 改为0
                        '合作图文完播率': item['fields'].get('合作图文完播率', 0),  # 改为0
                        '合作图文3秒阅读率': item['fields'].get('合作图文3秒阅读率', 0),  # 改为0
                        '合作图文CPE': item['fields'].get('合作图文CPE', 0),  # 改为0
                        '合作图文CPM': item['fields'].get('合作图文CPM', 0),  # 改为0
                        '合作图文CPC': item['fields'].get('合作图文CPC', 0),  # 改为0
                        '合作图文CPR': item['fields'].get('合作图文CPR', 0),  # 改为0
                        '合作视频曝光中位数': item['fields'].get('合作视频曝光中位数', 0),  # 改为0
                        '合作视频阅读中位数': item['fields'].get('合作视频阅读中位数', 0),  # 改为0
                        '合作视频互动中位数': item['fields'].get('合作视频互动中位数', 0),  # 改为0
                        '合作视频互动率': item['fields'].get('合作视频互动率', 0),  # 改为0
                        '合作视频千赞笔记比例': item['fields'].get('合作视频千赞笔记比例', 0),  # 改为0
                        '合作视频完播率': item['fields'].get('合作视频完播率', 0),  # 改为0
                        '合作视频3秒阅读率': item['fields'].get('合作视频3秒阅读率', 0),  # 改为0
                        '合作视频CPE': item['fields'].get('合作视频CPE', 0),  # 改为0
                        '合作视频CPM': item['fields'].get('合作视频CPM', 0),  # 改为0
                        '合作视频CPC': item['fields'].get('合作视频CPC', 0),  # 改为0
                        '合作视频CPR': item['fields'].get('合作视频CPR', 0),  # 改为0
                        '年龄<18': item['fields'].get('年龄<18', 0),  # 改为0
                        '年龄18_24': item['fields'].get('年龄18_24', 0),
                        '年龄25_34': item['fields'].get('年龄25_34', 0),
                        '年龄35_44': item['fields'].get('年龄35_44', 0),
                        '年龄>44': item['fields'].get('年龄>44', 0),
                        '女粉丝占比': item['fields'].get('女粉丝占比', 0),
                        '用户兴趣top1': item['fields'].get('用户兴趣top1', [{'text': ''}])[0].get('text', ''),
                        '用户兴趣top2': item['fields'].get('用户兴趣top2', [{'text': ''}])[0].get('text', ''),
                        '用户兴趣top3': item['fields'].get('用户兴趣top3', [{'text': ''}])[0].get('text', ''),
                        '用户兴趣top4': item['fields'].get('用户兴趣top4', [{'text': ''}])[0].get('text', ''),
                        '用户兴趣top5': item['fields'].get('用户兴趣top5', [{'text': ''}])[0].get('text', ''),
                        '省份top1': item['fields'].get('省份top1', [{'text': ''}])[0].get('text', ''),
                        '省份top2': item['fields'].get('省份top2', [{'text': ''}])[0].get('text', ''),
                        '省份top3': item['fields'].get('省份top3', [{'text': ''}])[0].get('text', ''),
                        '城市top1': item['fields'].get('城市top1', [{'text': ''}])[0].get('text', ''),
                        '城市top2': item['fields'].get('城市top2', [{'text': ''}])[0].get('text', ''),
                        '城市top3': item['fields'].get('城市top3', [{'text': ''}])[0].get('text', ''),
                        '设备top1': item['fields'].get('设备top1', [{'text': ''}])[0].get('text', ''),
                        '设备top2': item['fields'].get('设备top2', [{'text': ''}])[0].get('text', ''),
                        '设备top3': item['fields'].get('设备top3', [{'text': ''}])[0].get('text', ''),
                        '发布笔记数量': int(item['fields'].get('发布笔记数量', 0)),
                        '粉丝增量': int(item['fields'].get('粉丝增量', 0)),
                        '粉丝量变化幅度': item['fields'].get('粉丝量变化幅度', 0),  # 改为0
                        '活跃粉丝占比': item['fields'].get('活跃粉丝占比', 0),  # 改为0
                        '返点': item['fields'].get('返点', 0),  # 改为0
                        '签约时间': int(item['fields'].get('签约时间', 0)),
                        '解约时间': int(item['fields'].get('解约时间', 0)),
                        '可回签时间': int(item['fields'].get('可回签时间', 0)),
                        '孩子1年龄': item['fields'].get('孩子1年龄', [{'text': '0.0'}])[0].get('text', '0.0'),
                        '孩子2年龄': item['fields'].get('孩子2年龄', [{'text': '0.0'}])[0].get('text', '0.0'),
                        '孩子3年龄': item['fields'].get('孩子3年龄', [{'text': '0.0'}])[0].get('text', '0.0'),
                        '孩子1性别': item['fields'].get('孩子1性别', ''),
                        '孩子2性别': item['fields'].get('孩子2性别', ''),
                        '孩子3性别': item['fields'].get('孩子3性别', ''),
                        '能否接空调挂机': item['fields'].get('能否接空调挂机', ''),
                        '能否挂空调外机': item['fields'].get('能否挂空调外机', ''),
                        '是否有冰箱的全嵌入场景': item['fields'].get('是否有冰箱的全嵌入场景', ''),
                        '冰箱的全嵌入场景尺寸': ', '.join(item['fields'].get('冰箱的全嵌入场景尺寸', [])),
                        '是否洗衣机全嵌入场景': item['fields'].get('是否洗衣机全嵌入场景', ''),
                        '洗衣机全嵌入场景尺寸': item['fields'].get('洗衣机全嵌入场景尺寸', []),
                        '是否有洗碗机全嵌入场景': item['fields'].get('是否有洗碗机全嵌入场景', ''),
                        '洗碗机全嵌入场景尺寸': item['fields'].get('洗碗机全嵌入场景尺寸', []),
                        '是否有蒸烤箱的全嵌入场景': item['fields'].get('是否有蒸烤箱的全嵌入场景', ''),
                        '蒸烤箱的全嵌入场景尺寸': item['fields'].get('蒸烤箱的全嵌入场景尺寸', []),
                        '能否通电通水': item['fields'].get('能否通电通水', ''),
                        '能否接受打孔': item['fields'].get('能否接受打孔', ''),
                        '接受净饮机上墙': item['fields'].get('接受净饮机上墙', ''),
                        '接受电视上墙': item['fields'].get('接受电视上墙', ''),
                        '线下探店': item['fields'].get('线下探店', ''),
                        '修改时间': int(item['fields'].get('修改时间', 0)),
                        '创建时间': int(item['fields'].get('创建时间', 0)),
                        '系统最后修改时间': int(item['fields'].get('系统最后修改时间', 0)),
                        '大运组商务标签': ', '.join(item['fields'].get('大运组商务标签', [])),
                        '沐远组商务标签': ', '.join(item['fields'].get('沐远组商务标签', [])),
                        '是否拉培训群': item['fields'].get('是否拉培训群', [{'text': ''}])[0].get('text', ''),
                        '是否拉运营群': item['fields'].get('是否拉运营群', [{'text': ''}])[0].get('text', ''),
                        '阅读中位数': int(item['fields'].get('阅读中位数', 0)),
                        '互动中位数': int(item['fields'].get('互动中位数', 0)),
                    }
                }
                inserts.append(new_record)

    # 批量插入飞书记录
    chunk_size = 500  # 根据飞书 API 的限制调整
    chunks = [inserts[i:i + chunk_size] for i in range(0, len(inserts), chunk_size)]
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
        print(response.json())

important_7_10()