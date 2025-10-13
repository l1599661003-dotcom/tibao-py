import random
import time
import uuid
import requests
import urllib3
from datetime import datetime, timedelta

from service.feishu_service import read_table_content, update_record, update_token_info, send_message, \
    search_feishu_record2, search_feishu_record3, get_feishu_token, search_feishu_record4
from unitl.unitl import get_text_from_items, get_month_range, calculate, random_sleep

"""
    自动化发送邀约
"""

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app_token = 'PJISbPC5OaihG8sCfMpc4Wohnyb'
# 邀约蒲公英账号表
table_id = 'tblDO9VqC6EMHGiY'
view_id = 'vewJK6XVP4'
# 发邀约
view_id4 = 'vewjCdNdpJ'
# 微信信息表
table_id1 = 'tbl69wH9DNnlNrTT'
view_id1 = 'vewW4ixEC6'
# 大于2000表
table_id2 = 'tbliGz3IRUgjz5Jg'
view_id2 = 'vewFjbwzKs'
# 标签表
table_id3 = 'tblnCPaLeLLyWJ5P'
view_id3 = 'vewtXRS3K8'

"""
    自动发送邀约接口
"""

# 处理发送邀约逻辑的主入口
def get_table_content():
    results = []
    try:
        print("开始执行自动发送邀约...")
        results.append("开始执行自动发送邀约...")
        # 获取蒲公英账号信息
        pgy_message = read_table_content(app_token, table_id, view_id4)
        if not pgy_message:
            results.append("获取蒲公英账号信息失败")
            return results

        # 获取微信账号信息
        vx_messages = read_table_content(app_token, table_id1, view_id1)
        if not vx_messages:
            results.append("获取微信账号信息失败")
            return results

        # 处理每个蒲公英账号
        for item in pgy_message:
            try:
                pgy_total = item['fields'].get('可用次数', {}).get('value', [0])[0]
                no_pgy_total = item['fields'].get('不可用次数')

                if pgy_total < 1:
                    continue

                cooperateBrandName = item['fields']['账号简称'][0]['text']
                cooperateBrandId = item['fields']['账号id'][0]['text']
                token = get_text_from_items(item['fields'].get('蒲公英token', []))

                results.append(f"正在处理账号: {cooperateBrandName}")
                print(f"正在处理账号: {cooperateBrandName}")

                # 处理每个微信号
                for vx_message in vx_messages:
                    try:
                        # 获取微信加密信息
                        contactInfoCiphertext = get_text_from_items(vx_message['fields'].get('微信号加密', []))
                        if '微信号' not in vx_message['fields'] or not vx_message['fields']['微信号']:
                            continue
                        contactInfo = vx_message['fields'].get('微信号', [])[0]['text']

                        # 期望开始时间和期望结束时间
                        expectedPublishTimeStart = (datetime.now() + timedelta(days=10)).strftime('%Y-%m-%d')
                        expectedPublishTimeEnd = (datetime.now() + timedelta(days=50)).strftime('%Y-%m-%d')

                        conditions = [
                            {
                                "field_name": '区分列',
                                "operator": "is",
                                "value": [contactInfo]
                            },
                            {
                                "field_name": '是否发送邀约',
                                "operator": "isEmpty",
                                "value": []
                            }
                        ]

                        # 搜索大于2000表中需要发邀约的博主信息
                        sends = search_feishu_record(app_token, table_id2, conditions, view_id2)
                        if sends is None or len(sends) == 0:
                            print(f"账号: {cooperateBrandName},没有邀约次数")
                            results.append(f"账号: {cooperateBrandName},没有邀约次数")
                            continue

                        for send in sends:
                            try:
                                # 准备发邀约参数
                                # productName = send['fields']['发送邀约产品'][0]['text'] + '小红书报备合作'
                                # inviteContent = send['fields']['发送邀约产品'][0]['text'] + '小红书报备合作，辛苦留个联系方式，加我时请备注博主名'
                                productName = '小红书商务合作'
                                inviteContent = '看到您近期笔记内容和我们项目非常契合，希望尽快得到您的联系方式'
                                kol_id = send['fields']['博主id'][0]['text']
                                kol_jigou = get_kol_blogger(kol_id, token)
                                if kol_jigou == 1:
                                    print(f"博主 {kol_id} 属于方片机构，跳过处理")
                                    results.append(f"博主 {kol_id} 属于方片机构，跳过处理")
                                    continue
                                record_id = send['record_id']

                                # 构建邀约数据
                                data = {
                                    'kolId': kol_id,
                                    'cooperateBrandName': cooperateBrandName,
                                    'cooperateBrandId': cooperateBrandId,
                                    'productName': productName,
                                    'inviteType': 1,
                                    'expectedPublishTimeStart': expectedPublishTimeStart,
                                    'expectedPublishTimeEnd': expectedPublishTimeEnd,
                                    'inviteContent': inviteContent,
                                    'contactType': 2,
                                    'contactInfo': contactInfo,
                                    'contactInfoCiphertext': contactInfoCiphertext,
                                    'kolType': 0,
                                    'brandUserId': cooperateBrandId
                                }

                                results.append(f"准备发送邀约给博主: {kol_id}")
                                print(f"准备发送邀约给博主: {kol_id}")

                                # 发送邀约
                                # random_sleep(1, 3)
                                time.sleep(6)
                                content_length = calculate(data)
                                remainTimes = send_yaoyue(content_length, token, kol_id, data, record_id,
                                                          cooperateBrandName)

                                if remainTimes == 10040000:
                                    results.append(f"账号 {cooperateBrandId} 发送邀约失败，kol信息有误")
                                    continue

                                if remainTimes is None:
                                    results.append(f"账号 {cooperateBrandId} 发送邀约失败，强制切换下一个账号")
                                    break

                                # 更新飞书记录
                                fields = {
                                    '是否发送邀约': '已邀约'
                                }
                                update_record(app_token, table_id2, record_id, fields)
                                results.append(f"博主: {kol_id}已发送邀约")
                                print(f"博主: {kol_id}已发送邀约")

                                # 更新剩余邀约次数
                                conditions = [
                                    {
                                        "field_name": '账号id',
                                        "operator": "is",
                                        "value": [cooperateBrandId]
                                    }
                                ]
                                pgys = search_feishu_record(app_token, table_id, conditions, view_id)
                                if pgys is None or 'record_id' not in pgys[0] or len(pgys) == 0:
                                    results.append(f"账号 {cooperateBrandId} 查找飞书记录失败，跳过更新")
                                    continue

                                record_id2 = pgys[0]['record_id']
                                fields2 = {
                                    '剩余邀约次数': remainTimes
                                }
                                update_record(app_token, table_id, record_id2, fields2)
                                pgy_total = remainTimes
                                results.append(f"邀约成功，剩余次数: {remainTimes}")
                                print(f"邀约成功，剩余次数: {remainTimes}")

                                if pgy_total == no_pgy_total or pgy_total < 1:
                                    results.append(f"账号 {cooperateBrandName} 邀约次数已用完")
                                    break

                            except Exception as e:
                                results.append(f"处理博主时出错：{str(e)}")
                                continue
                        else:
                            continue
                        break

                    except Exception as e:
                        results.append(f"处理微信号时出错：{str(e)}")
                        continue

            except Exception as e:
                results.append(f"处理账号时出错：{str(e)}")
                continue

        results.append("发送邀约执行完成")

    except Exception as e:
        results.append(f"执行过程中出现错误: {str(e)}")

    return results

"""
    获取邀约次数
"""

# 获取蒲公英邀约次数
def get_yaoyue_total(token, name, pgy_id):
    url = "https://pgy.xiaohongshu.com/api/solar/invite/get_invites_overview"
    trace_id = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': token,
        'X-B3-Traceid': trace_id[:16]
    }
    data = get_month_range()
    try:
        random_sleep(3, 6)
        response = requests.post(url, headers=headers, json=data, verify=False)
        response_json = response.json()

        if response_json.get('code') == 0:
            return 300 - response_json['data']['total']
        elif response_json.get('code') in (902, 903, 906):
            send_message(f"token过期信息,过期账号名:{name}")
            fields = {
                '账号简称': str(name),
                'token过期信息': str(token),
                '账号id': str(pgy_id),
            }
            update_token_info(fields)
            return None
        else:
            return None
    except requests.RequestException as e:
        return None

# 修改飞书表格的剩余邀约次数
def update_yaoyue_total():
    results = []
    try:
        results.append("自动获取邀约次数脚本已经启动")
        data = read_table_content(app_token, table_id, view_id)

        if data is None:
            results.append("获取蒲公英 Token 数据失败或格式不正确")
            return results

        for item in data:
            try:
                name = item['fields']['账号简称'][0]['text']
                pgy_id = item['fields']['账号id'][0]['text']
                token = get_text_from_items(item['fields'].get('蒲公英token', []))

                if not token:
                    results.append(f"账号 {name} 的蒲公英 token 为空，跳过处理")
                    continue

                conditions = [
                    {
                        "field_name": '账号id',
                        "operator": "is",
                        "value": [pgy_id]
                    },
                ]

                total = get_yaoyue_total(token, name, pgy_id)
                result = search_feishu_record(app_token, table_id, conditions, view_id)
                if result is None or 'record_id' not in result[0] or len(result) == 0:
                    results.append(f"账号 {name} 查找飞书记录失败，跳过更新")
                    continue

                record_id = result[0]['record_id']
                if total is None:
                    fields1 = {'是否过期': '已过期', '剩余邀约次数': None}
                    update_result = update_record(app_token, table_id, record_id, fields1)
                    results.append(f"账号 {name} 获取邀约次数失败，请检查网络或 token 状态")
                    continue
                fields = {'剩余邀约次数': total, '是否过期': None}
                update_result = update_record(app_token, table_id, record_id, fields)
                print(f"账号 {name} 更新成功: {fields}")
                results.append(f"账号 {name} 更新成功: {fields}")

            except Exception as e:
                results.append(f"处理账号时出错：{str(e)}")
                continue

        results.append("获取邀约次数执行完成")

    except Exception as e:
        results.append(f"执行过程中出现错误: {str(e)}")

    return results

"""
    自动打区分列以及发送邀约产品
"""

def automatic_product():
    results = []
    try:
        results.append("自动打区分列以及发送邀约产品脚本已经启动")
        messages = read_table_content(app_token, table_id1, view_id1)
        if messages is None:
            results.append("获取微信账号数据失败")
            return results

        for message in messages:
            try:
                if message['fields'].get('微信号') is None:
                    continue
                wx = message['fields'].get('微信号')[0]['text']

                if message['fields'].get('对应大类') is None:
                    continue
                big_tag = message['fields'].get('对应大类')

                results.append(f"开始处理微信号: {wx}")

                conditions = [
                    {
                        "field_name": '垂类大类',
                        "operator": "is",
                        "value": [big_tag]
                    }
                ]

                tags = search_feishu_record(app_token, table_id3, conditions, view_id3)
                if tags is None or len(tags) == 0:
                    results.append(f"未找到对应标签数据，跳过处理")
                    continue

                update_count = 0  # 初始化当前微信号的更新次数计数器

                # 第一次处理 tags
                for tag in tags:
                    try:
                        if tag['fields'].get('产品') is None or tag['fields'].get('小类') is None:
                            continue
                        # product = tag['fields'].get('产品')[0]['text']
                        small_tags = tag['fields'].get('小类')

                        for small_tag in small_tags:
                            datas = search_feishu_record2(app_token, table_id2, view_id2, '内容类目1', small_tag)
                            if not datas:
                                continue

                            for data in datas:
                                if update_count >= 90:
                                    results.append(f"微信号 {wx} 已达到66次更新限制")
                                    break

                                record_id = data['record_id']
                                fields = {
                                    '区分列': str(wx),
                                    '区分列-流量组用': str(wx),
                                    # '发送邀约产品': str(product),
                                }
                                update_result = update_record(app_token, table_id2, record_id, fields)
                                update_count += 1
                                results.append(f"更新成功 - 微信号: {wx}, 更新次数: {update_count}")

                            if update_count >= 90:
                                break

                        if update_count >= 90:
                            break

                    except Exception as e:
                        results.append(f"处理标签时出错：{str(e)}")
                        continue

                # 如果当前微信号未更新66次，继续查找对应大类的其他小类产品
                if update_count < 90:
                    results.append(f"继续处理微信号 {wx} 的其他小类产品")
                    for tag in tags:
                        try:
                            if tag['fields'].get('产品') is None or tag['fields'].get('小类') is None:
                                continue
                            # product = tag['fields'].get('产品')[0]['text']
                            small_tags = tag['fields'].get('小类')

                            for small_tag in small_tags:
                                datas = search_feishu_record2(app_token, table_id2, view_id2, '内容类目1', small_tag)
                                if not datas:
                                    continue

                                for data in datas:
                                    if update_count >= 90:
                                        results.append(f"微信号 {wx} 已达到75次更新限制")
                                        break

                                    record_id = data['record_id']
                                    fields = {
                                        '区分列': str(wx),
                                        '区分列-流量组用': str(wx),
                                        # '发送邀约产品': str(product),
                                    }
                                    update_result = update_record(app_token, table_id2, record_id, fields)
                                    update_count += 1
                                    results.append(f"更新成功 - 微信号: {wx}, 更新次数: {update_count}")

                                if update_count >= 90:
                                    break

                            if update_count >= 90:
                                break

                        except Exception as e:
                            results.append(f"处理标签时出错：{str(e)}")
                            continue

            except Exception as e:
                results.append(f"处理微信号时出错：{str(e)}")
                continue

        results.append("自动打标签执行完成")

    except Exception as e:
        results.append(f"执行过程中出现错误: {str(e)}")

    return results

"""
    丁葳自动打区分列以及发送邀约产品
"""
def automatic_product_01():
    results = []
    try:
        results.append("自动打区分列以及发送邀约产品脚本已经启动")
        messages = read_table_content(app_token, table_id1, view_id1)
        if messages is None:
            results.append("获取微信账号数据失败")
            return results

        for message in messages:
            try:
                if message['fields'].get('丁葳微信号') is None:
                    continue
                wx = message['fields'].get('丁葳微信号')[0]['text']

                if message['fields'].get('对应大类') is None:
                    continue
                big_tag = message['fields'].get('对应大类')

                results.append(f"开始处理微信号: {wx}")

                conditions = [
                    {
                        "field_name": '垂类大类',
                        "operator": "is",
                        "value": [big_tag]
                    }
                ]

                tags = search_feishu_record(app_token, table_id3, conditions, view_id3)
                if tags is None or len(tags) == 0:
                    results.append(f"未找到对应标签数据，跳过处理")
                    continue

                update_count = 0  # 初始化当前微信号的更新次数计数器

                # 第一次处理 tags
                for tag in tags:
                    try:
                        if tag['fields'].get('产品') is None or tag['fields'].get('小类') is None:
                            continue
                        # product = tag['fields'].get('产品')[0]['text']
                        small_tags = tag['fields'].get('小类')

                        for small_tag in small_tags:
                            datas = search_feishu_record2(app_token, table_id2, view_id2, '内容类目1', small_tag)
                            if not datas:
                                continue

                            for data in datas:
                                if update_count >= 90:
                                    results.append(f"微信号 {wx} 已达到91次更新限制")
                                    break

                                record_id = data['record_id']
                                fields = {
                                    '区分列': str(wx),
                                    '区分列-流量组用': str(wx),
                                    # '发送邀约产品': str(product),
                                }
                                update_result = update_record(app_token, table_id2, record_id, fields)
                                update_count += 1
                                results.append(f"更新成功 - 微信号: {wx}, 更新次数: {update_count}")

                            if update_count >= 90:
                                break

                        if update_count >= 90:
                            break

                    except Exception as e:
                        results.append(f"处理标签时出错：{str(e)}")
                        continue

                # 如果当前微信号未更新91次，继续查找对应大类的其他小类产品
                if update_count < 90:
                    results.append(f"继续处理微信号 {wx} 的其他小类产品")
                    for tag in tags:
                        try:
                            if tag['fields'].get('产品') is None or tag['fields'].get('小类') is None:
                                continue
                            # product = tag['fields'].get('产品')[0]['text']
                            small_tags = tag['fields'].get('小类')

                            for small_tag in small_tags:
                                datas = search_feishu_record2(app_token, table_id2, view_id2, '内容类目1', small_tag)
                                if not datas:
                                    continue

                                for data in datas:
                                    if update_count >= 90:
                                        results.append(f"微信号 {wx} 已达到75次更新限制")
                                        break

                                    record_id = data['record_id']
                                    fields = {
                                        '区分列': str(wx),
                                        '区分列-流量组用': str(wx),
                                        # '发送邀约产品': str(product),
                                    }
                                    update_result = update_record(app_token, table_id2, record_id, fields)
                                    update_count += 1
                                    results.append(f"更新成功 - 微信号: {wx}, 更新次数: {update_count}")

                                if update_count >= 90:
                                    break

                            if update_count >= 90:
                                break

                        except Exception as e:
                            results.append(f"处理标签时出错：{str(e)}")
                            continue

            except Exception as e:
                results.append(f"处理微信号时出错：{str(e)}")
                continue

        results.append("自动打标签执行完成")

    except Exception as e:
        results.append(f"执行过程中出现错误: {str(e)}")

    return results

"""
    自动打水号
"""

def automatic_water_horn():
    searchs = search_feishu_record3(app_token, table_id2, view_id2)
    for search in searchs:
        # Get the discovery page percentages
        read_discovery = search['fields'].get('阅读量来源的【发现页】占比', 0)
        exposure_discovery = search['fields'].get('曝光量来源的【发现页】占比', 0)
        bozhu = search['fields'].get('博主id', 0)[0]['text']

        # Check if both percentages are greater than 50
        if read_discovery <= 0.55 and exposure_discovery <= 0.55:
            record_id = search['record_id']
            fields = {
                '备注': '水号'
            }
            # Update the record in Feishu
            result = update_record(app_token, table_id2, record_id, fields)
            print(result)
            print(f"{bozhu}修改成功")

"""
    自动打重复号
"""

def automatic_wechat_duplicates():
    try:
        print("开始处理微信号查重...")

        # 1. 获取金虎VX为空的重复微信号记录
        empty_data = search_feishu_record4(app_token, table_id2, view_id2, "isEmpty")
        if not empty_data:
            print('获取数据失败')
            return

        print(f"\n总共找到 {len(empty_data)} 条金虎VX为空的重复微信号记录")

        # 2. 按微信号分组
        wechat_groups = {}
        for item in empty_data:
            fields = item.get('fields', {})
            wechat = get_text_from_items(fields.get('微信号', []))
            if wechat and wechat.strip():
                if wechat not in wechat_groups:
                    wechat_groups[wechat] = []
                wechat_groups[wechat].append(item)

        # 3. 处理每组重复记录
        processed = 0
        for wechat, records in wechat_groups.items():
            print(f"\n处理微信号: {wechat}")
            print(f"重复次数: {len(records)}条记录")

            # 查询该微信号是否存在金虎VX不为空的记录
            has_value_data = search_feishu_record4(app_token, table_id2, view_id2, "isNotEmpty")
            has_value_records = [item for item in has_value_data if
                                 get_text_from_items(item['fields'].get('微信号', [])) == wechat]

            if has_value_records:
                # 存在金虎VX不为空的记录，将当前所有记录标记为重复号
                print(f"发现该微信号存在金虎VX不为空的记录，将所有空记录标记为重复号")
                for record in records:
                    fields = {
                        '金虎VX': '重复号'
                    }
                    update_record(app_token, table_id2, record['record_id'], fields)
                    processed += 1
                    time.sleep(1)
            else:
                # 不存在金虎VX不为空的记录，随机保留一条
                print(f"该微信号无金虎VX不为空的记录，随机保留一条")
                keep_record = random.choice(records)
                for record in records:
                    if record['record_id'] != keep_record['record_id']:
                        fields = {
                            '金虎VX': '重复号'
                        }
                        update_record(app_token, table_id2, record['record_id'], fields)
                        processed += 1
                        time.sleep(1)

        print(f"\n处理完成! 共处理 {processed} 条记录")
        print("\n处理结果:")
        print(f"1. 发现 {len(wechat_groups)} 个重复微信号")
        print(f"2. 处理了 {processed} 条记录")
        print("3. 已根据金虎VX是否存在进行相应处理")

    except Exception as e:
        print(f"处理过程出错: {e}")

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
            send_message(f"token过期信息,过期账号名:{name}")
            fields = {
                '账号简称': str(name),
                'token过期信息': str(token),
                '账号id': str(kol_id),
            }
            update_token_info(fields)
            return None
        elif response_json.get('code') == -10040000:
            print(kol_id)
            update_fields = {
                '备注': '博主账号异常无法发送邀约',
                '区分列': '',
                '发送邀约产品': ''
            }
            try:
                update_record(app_token, table_id2, record_id, update_fields)
                return 10040000
            except Exception as e:
                print(f"更新飞书记录时发生异常：{e}")
        else:
            update_fields = {
                '备注': response_json.get('data')['hint']
            }
            try:
                update_record(app_token, table_id2, record_id, update_fields)
                return None
            except Exception as e:
                print(f"更新飞书记录时发生异常：{e}")
    except requests.RequestException as e:
        print(f"更新飞书记录时发生异常：{e}")

# 获取博主信息
def get_kol_blogger(kol_id, token):
    url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/{kol_id}"
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': token,
    }
    try:
        response = requests.get(url, headers=headers, verify=False)
        response_json = response.json()
        note_sign_name = ''
        if response_json.get('code') == 0 and response_json.get('success') == True:
            if response_json.get('data', {}).get('noteSign', {}):
                note_sign_name = response_json.get('data', {}).get('noteSign', {}).get('name', '')
            else:
                return 0

            # 只有当name包含"方片"时才处理
            if '方片' in note_sign_name:
                conditions = [
                    {
                        "field_name": '博主id',
                        "operator": "is",
                        "value": [kol_id]
                    }
                ]
                sends = search_feishu_record(app_token, table_id2, conditions, view_id2)
                # 修正条件判断
                if sends is not None and len(sends) > 0:
                    for send in sends:
                        record_id = send['record_id']
                        fields = {
                            '备注': '机构商务，不加不拉群',
                            '区分列': None,
                            '达人所属机构': note_sign_name,
                        }
                        update_result = update_record(app_token, table_id2, record_id, fields)
                        if update_result:  # 确保更新成功
                            return 1
            return 0  # 不是方片或没有找到记录

    except requests.RequestException as e:
        print(f"请求发生异常：{e}")
        return 0

    return 0  # 确保所有路径都有返回值

"""
    处理飞书接口业务层
"""
def search_feishu_record(app_token, table_id, conditions, view_id):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": conditions
        }
    }

    try:
        # 第一次查询
        response = requests.post(url, headers=headers, json=data, verify=False)
        response_data = response.json()

        # 获取总记录数和第一页数据
        total = response_data.get("data", {}).get("total", 0)
        items = response_data.get("data", {}).get("items", [])
        page_token = response_data.get("data", {}).get("page_token", "")

        # 如果总记录数大于 500，继续分页查询
        if total > 500:
            page = (total + 499) // 500  # 计算总页数
            for i in range(1, page):
                # 添加 page_token 参数
                paginated_url = f"{url}&page_token={page_token}"
                paginated_response = requests.post(paginated_url, headers=headers, json=data, verify=False)
                paginated_data = paginated_response.json()

                # 合并当前页数据
                items.extend(paginated_data.get("data", {}).get("items", []))
                page_token = paginated_data.get("data", {}).get("page_token", "")

                # 如果没有下一页，退出循环
                if not page_token:
                    break

        return items

    except requests.RequestException as e:
        print(f"查询飞书单条信息失败：{e}")
        return None

# update_yaoyue_total()
# get_table_content()
# automatic_product_01()
# automatic_wechat_duplicates()
# automatic_water_horn()