import time
import certifi
import requests
import json
import urllib3
import sys
import os
from datetime import datetime
import logging
import random
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
"""
    抓取博主内容类目和粉丝数据以及曝光页发现页来源占比
"""

def setup_logging():
    """设置日志配置"""
    # 获取程序所在目录
    if getattr(sys, 'frozen', False):
        # 如果是 exe 运行
        base_dir = os.path.dirname(sys.executable)
    else:
        # 如果是 python 脚本运行
        base_dir = os.path.dirname(os.path.abspath(__file__))

    # 创建logs目录
    log_dir = os.path.join(base_dir, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 日志文件路径
    log_file = os.path.join(log_dir, f"blogger_update_{datetime.now().strftime('%Y%m%d')}.log")

    # 打印日志存储位置
    print(f"日志文件将保存在: {log_file}")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


def get_feishu_token():
    """获取飞书API访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    headers = {
        'Content-Type': 'application/json; charset=utf-8'
    }
    data = {
        'app_id': 'cli_a6e824d4363b500d',
        'app_secret': 'nW4ff1Mviwr0ZuYkF1BBhciZGOyDeBP5'
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        response_data = response.json()
        return response_data.get('app_access_token', '')
    except requests.RequestException as e:
        logger.error(f"获取飞书Token失败: {e}")
        return None


def get_pgy_token(use=""):
    """从飞书表获取蒲公英token"""
    # 从飞书表读取token
    data = read_table_content("PJISbPC5OaihG8sCfMpc4Wohnyb", "tblDO9VqC6EMHGiY", "vewUvYvleV")

    if not data:
        logger.error("无法从飞书表获取token数据")
        return ""

    token = ""
    for item in data:
        if "用途" in item['fields'] and item['fields']["用途"][0]["text"] == use:
            token = "".join(ite["text"] for ite in item['fields'].get("蒲公英token", []))

    return token.strip()


def read_table_content(app, table, view):
    """读取飞书表内容"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps({'view_id': view}), verify=False, timeout=30)
        response.raise_for_status()
        response_data = response.json()

        if response_data.get('code') != 0:
            logger.error(f"飞书API返回错误: {response_data}")
            return None

        # 获取总记录数和第一页数据
        total = response_data.get("data", {}).get("total", 0)
        items = response_data.get("data", {}).get("items", [])
        page_token = response_data.get("data", {}).get("page_token", "")

        # 如果总记录数大于 500，继续分页查询
        if total > 500:
            page = (total + 499) // 500  # 计算总页数
            for i in range(1, page):
                paginated_url = f"{url}&page_token={page_token}"
                paginated_response = requests.post(paginated_url, headers=headers, data=json.dumps({'view_id': view}),
                                                   verify=False)
                paginated_data = paginated_response.json()

                items.extend(paginated_data.get("data", {}).get("items", []))
                page_token = paginated_data.get("data", {}).get("page_token", "")

                if not page_token:
                    break
        return items
    except requests.RequestException as e:
        logger.error(f"查询飞书表格信息失败：{e}")
        return None


def search_blogger_records(field_value):
    """查询博主记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/PJISbPC5OaihG8sCfMpc4Wohnyb/tables/tbliGz3IRUgjz5Jg/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": 'vew971aOKw',
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": '蒲公英链接',
                    "operator": "isNotEmpty",
                    "value": []
                },
                {
                    "field_name": '连续30天负增长',
                    "operator": "isEmpty",
                    "value": []
                },
                {
                    "field_name": '使用电脑',
                    "operator": "is",
                    "value": [field_value]
                },
            ]
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
        logger.error(f"查询飞书博主记录失败: {e}")
        return None


def update_feishu_record(record_id, data):
    """更新飞书记录"""
    try:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/PJISbPC5OaihG8sCfMpc4Wohnyb/tables/tbliGz3IRUgjz5Jg/records/{record_id}"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {get_feishu_token()}'
        }

        response = requests.put(url, headers=headers, json=data, verify=False)
        response.raise_for_status()

        if response.status_code == 200:
            logger.info("飞书记录更新成功")
            return True
        else:
            logger.error(f"飞书记录更新失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"更新飞书记录时发生错误: {str(e)}")
        return False


def fans_overall_new_history(user_id, token):
    """获取粉丝数据"""
    try:
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data/{user_id}/fans_overall_new_history?dateType=1&increaseType=1"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
            'Cookie': token,
            'Authorization': get_feishu_token()
        }

        response = requests.get(url, headers=headers, verify=certifi.where(), timeout=30)
        result = response.json()
        logger.info(f"粉丝数据接口返回: {result}")

        if result.get("code") in [902, 903]:
            logger.error("Token已失效，需要更新")
            return None
        elif result.get("code") == 300013:
            logger.error("访问频次异常，需要等待")
            return None

        if result.get("code") == 0:
            if "data" in result and result["data"]:
                fans_data = result["data"]['list']
                if len(fans_data) >= 30:
                    key_points = [
                        fans_data[0]['num'],
                        fans_data[4]['num'],
                        fans_data[9]['num'],
                        fans_data[14]['num'],
                        fans_data[19]['num'],
                        fans_data[24]['num'],
                        fans_data[29]['num']
                    ]
                    for i in range(len(key_points) - 1):
                        if key_points[i] <= key_points[i + 1]:
                            return 0
                    return 1
            else:
                logger.error(f"博主信息接口返回数据格式错误: {result}")
                return None
        else:
            logger.error(f"博主信息接口请求失败，状态码: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"获取博主粉丝信息时发生错误: {str(e)}")
        return None


def notes_rate(user_id, token):
    """获取笔记数据"""
    try:
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/data_v3/notes_rate?userId={user_id}&business=0&noteType=3&dateType=1&advertiseSwitch=1"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
            'Cookie': token,
            'Authorization': get_feishu_token()
        }

        response = requests.get(url, headers=headers, timeout=30, verify=False)
        res = response.json()
        logger.info(f"笔记数据接口返回: {res}")

        if res.get("code") in [902, 903]:
            logger.error("Token已失效，需要更新")
            return None
        elif res.get("code") == 300013:
            logger.error("访问频次异常，需要等待")
            return None

        if res.get("code") == 0:
            if "data" in res and res["data"]:
                try:
                    note_type = res['data'].get('noteType', [])
                    note_type_top_two = note_type[:2]
                    if res['data']['pagePercentVo'] != None:
                        exposure = res['data']['pagePercentVo'].get('impHomefeedPercent', 0)
                        reads = res['data']['pagePercentVo'].get('readHomefeedPercent', 0)
                    else:
                        exposure = 0
                        reads = 0
                    return {
                        'noteTypeTopTwo': note_type_top_two,
                        'reads': reads,
                        'exposure': exposure,
                    }
                except Exception as e:
                    logger.error(f"处理笔记数据时发生错误: {str(e)}")
                    return None
            else:
                logger.error(f"笔记详情接口返回数据格式错误: {res}")
                return None
        else:
            logger.error(f"笔记详情接口请求失败，状态码: {response.status_code}")
            return None

    except requests.RequestException as e:
        logger.error(f"请求笔记详情时发生网络错误: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"解析笔记详情JSON响应时发生错误: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"获取笔记详情时发生未知错误: {str(e)}")
        return None


def process_blogger_data(field_value, use):
    """处理博主数据的主函数"""
    try:
        logger.info("开始获取飞书记录...")
        
        # 获取蒲公英token
        token = get_pgy_token(use)
        if not token:
            logger.error("Token获取失败")
            return
        
        logger.info("Token获取成功")
        
        # 查询博主记录
        blogger_records = search_blogger_records(field_value)
        if not blogger_records:
            logger.error("无法获取博主记录")
            return
        
        logger.info(f"成功获取博主记录: {len(blogger_records)} 条")
        
        # 处理每条博主记录
        for record in blogger_records:
            user_id = record['fields'].get('博主id', [{}])[0].get('text')
            record_id = record.get('record_id')
            
            if not user_id or not record_id:
                logger.warning(f"跳过无效记录: {record}")
                continue
                
            logger.info(f"开始处理博主ID: {user_id}")

            time.sleep(6)
            
            # 获取笔记数据
            notes = notes_rate(user_id, token)
            if not notes:
                logger.warning(f"获取博主 {user_id} 笔记数据失败，继续处理下一个")
                continue
                
            time.sleep(6)
            
            # 获取粉丝数据
            fans = fans_overall_new_history(user_id, token)
            if fans is None:
                logger.warning(f"获取博主 {user_id} 粉丝数据失败，继续处理下一个")
                continue
                
            # 提取内容标签和百分比
            note_type_top_two = notes.get('noteTypeTopTwo', [])
            reads = notes.get('reads', 0)
            exposure = notes.get('exposure', 0)
            
            content_tag1 = note_type_top_two[0].get('contentTag', '') if len(note_type_top_two) > 0 else ''
            content_tag2 = note_type_top_two[1].get('contentTag', '') if len(note_type_top_two) > 1 else ''
            content_percent1 = f"{note_type_top_two[0].get('percent', '')}%" if len(note_type_top_two) > 0 else ''
            content_percent2 = f"{note_type_top_two[1].get('percent', '')}%" if len(note_type_top_two) > 1 else ''
            
            # 构造更新数据
            fields = {
                '连续30天负增长': str(fans),
                '阅读量来源的【发现页】占比': float(reads),
                '曝光量来源的【发现页】占比': float(exposure),
                '内容类目1': content_tag1,
                '内容占比1': content_percent1,
                '内容类目2': content_tag2,
                '内容占比2': content_percent2
            }
            
            # 判断是否为水号
            if reads < 0.55 and exposure < 0.55:
                fields['备注'] = '水号'
                
            logger.info(f"更新博主 {user_id} 数据: {fields}")
            
            # 更新飞书记录
            update_data = {"fields": fields}
            if not update_feishu_record(record_id, update_data):
                logger.error(f"更新博主 {user_id} 记录失败")
            
            logger.info(f"博主 {user_id} 处理完成")
            
    except Exception as e:
        logger.error(f"处理博主数据时发生错误: {str(e)}")


def load_config():
    """加载配置文件"""
    try:
        # 获取程序所在目录
        if getattr(sys, 'frozen', False):
            # 如果是 exe 运行
            base_dir = os.path.dirname(sys.executable)
        else:
            # 如果是 python 脚本运行
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
        config_path = os.path.join(base_dir, 'config.json')
        logger.info(f"配置文件路径: {config_path}")
        
        if not os.path.exists(config_path):
            logger.error(f"配置文件不存在: {config_path}")
            # 创建默认配置文件
            default_config = {
                "execution_time": {
                    "hour": 0,
                    "minute": 0
                }
            }
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4, ensure_ascii=False)
            logger.info(f"已创建默认配置文件: {config_path}")
            return default_config
            
        # 读取配置文件
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # 验证必要的字段
        required_fields = ['field_value', 'use', 'execution_time']
        for field in required_fields:
            if field not in config:
                logger.error(f"配置文件中缺少必要字段: {field}")
                sys.exit(1)
                
        # 验证execution_time字段
        if 'hour' not in config['execution_time'] or 'minute' not in config['execution_time']:
            logger.error("配置文件中execution_time字段格式不正确")
            sys.exit(1)
            
        logger.info(f"成功加载配置文件: {json.dumps(config, ensure_ascii=False)}")
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"配置文件格式错误: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"读取配置文件出错: {e}")
        sys.exit(1)


def search_feishu_record4(app_token, table_id, view_id, field_name, field_value):
    """查询飞书记录，查找金虎VX不为空的记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": field_name,
                    "operator": "is",
                    "value": [field_value]
                },
                {
                    "field_name": '金虎VX',
                    "operator": 'isNotEmpty',
                    "value": []
                },
            ]
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
        logger.error(f"查询飞书单条信息失败：{e}")
        return None


def process_message(token, message, cooperateBrandName):
    """处理单条消息"""
    time.sleep(random.uniform(2, 4))
    kol = get_vx_parameter(token, message.get('content', ''))
    if not kol:
        return

    fields = prepare_fields(kol)
    if not fields:
        return
    
    # 使用固定的app_token和table_id
    app_token = "PJISbPC5OaihG8sCfMpc4Wohnyb"
    table_id = "tbliGz3IRUgjz5Jg"
    view_id = "vew971aOKw"
    
    result = search_feishu_record(app_token, table_id, view_id, '博主id', kol['kolId'])
    if not result:
        return
    record_id = result[0]['record_id']
    try:
        update_record(app_token, table_id, record_id, fields)
        logger.info(f"博主id:{kol['kolId']},修改数据{fields}")
    except Exception as e:
        logger.error(f"更新记录时出错: {e}")

    if fields.get('微信号'):
        wechat_number = fields['微信号']

        # **1. 获取该微信号的所有记录**
        duplicate_records = search_feishu_record1(app_token, table_id, view_id, '微信号', wechat_number)

        # 如果查不到数据，或者条数 ≤ 1，不需要处理
        if not duplicate_records or len(duplicate_records) <= 1:
            return

        logger.info(f"\n微信号 {wechat_number} 共有 {len(duplicate_records)} 条记录")

        # **2. 检查该微信号是否已经有 "金虎VX" 不为空的记录**
        vx_filled_records = search_feishu_record4(app_token, table_id, view_id, '微信号', wechat_number)
        
        # 创建一个集合，存储金虎VX不为空的记录的ID
        vx_filled_record_ids = set()
        if vx_filled_records:
            for record in vx_filled_records:
                vx_filled_record_ids.add(record['record_id'])
            logger.info(f"发现该微信号存在 '金虎VX' 不为空的记录，将所有其他记录标记为 '重复号'")

            for record in duplicate_records:
                record_id = record['record_id']
                # 只对不在vx_filled_record_ids中的记录进行修改
                if record_id not in vx_filled_record_ids:
                    update_record(app_token, table_id, record_id, {'金虎VX': '重复号'})
                    logger.info(f"微信号 {wechat_number} 记录 {record_id} 被标记为 '重复号'")
                    time.sleep(1)

        else:
            # **3. 如果 "金虎VX" 全部为空，随机保留一条**
            logger.info(f"该微信号无 '金虎VX' 记录，随机保留一条，其余标记为 '重复号'")

            keep_record = random.choice(duplicate_records)

            for record in duplicate_records:
                record_id = record['record_id']
                if record_id != keep_record['record_id']:
                    update_record(app_token, table_id, record_id, {'金虎VX': '重复号'})
                    logger.info(f"微信号 {wechat_number} 记录 {record_id} 被标记为 '重复号'")
                    time.sleep(1)

# 添加辅助函数
def get_vx_parameter(token, content):
    """从消息内容中提取博主信息"""
    # 这里需要实现从消息内容中提取博主信息的逻辑
    # 返回格式应该是一个包含kolId等信息的字典
    # 示例实现
    try:
        # 这里应该是您的实际实现
        # 为了示例，我们返回一个模拟的数据
        return {
            'kolId': '12345',
            'wechat': 'example_wechat'
        }
    except Exception as e:
        logger.error(f"提取博主信息失败: {e}")
        return None

def prepare_fields(kol):
    """准备要更新的字段"""
    # 这里需要实现准备字段的逻辑
    # 返回格式应该是一个包含要更新字段的字典
    # 示例实现
    try:
        # 这里应该是您的实际实现
        # 为了示例，我们返回一个模拟的数据
        return {
            '微信号': kol.get('wechat', ''),
            '金虎VX': ''  # 初始为空
        }
    except Exception as e:
        logger.error(f"准备字段失败: {e}")
        return None

def search_feishu_record1(app_token, table_id, view_id, field_name, field_value):
    """查询飞书记录，查找指定字段值的记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size=500"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {
        "view_id": view_id,
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": field_name,
                    "operator": "is",
                    "value": [field_value]
                }
            ]
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
        logger.error(f"查询飞书单条信息失败：{e}")
        return None

def update_record(app_token, table_id, record_id, fields):
    """更新飞书记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f'Bearer {get_feishu_token()}'
    }
    data = {"fields": fields}

    try:
        response = requests.put(url, headers=headers, json=data, verify=False)
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"更新记录失败: {e}")
        return False

def get_text_from_items(items):
    """从飞书字段项中提取文本"""
    if not items:
        return ""
    return "".join(item.get("text", "") for item in items)

# 定义数据库模型
Base = declarative_base()

class NoteRank(Base):
    __tablename__ = 'note_ranks'
    
    id = Column(Integer, primary_key=True)
    note_id = Column(String(100))
    title = Column(String(500))
    author = Column(String(100))
    publish_time = Column(DateTime)
    likes = Column(Integer)
    comments = Column(Integer)
    collects = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<NoteRank(note_id='{self.note_id}', title='{self.title}')>"

class QianguaSpider:
    def __init__(self):
        self.setup_logger()
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.qian-gua.com"
        self.is_logged_in = False
        self.api_data = []  # 存储API数据
        self.cookie_file = os.path.join(self.data_dir, 'cookies.json')
        
        # 设置数据库连接
        self.engine = create_engine('mysql+pymysql://username:password@localhost/dbname')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        self.setup_browser()

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(os.path.join(log_path, "qiangua_{time}.log"), rotation="1 day", retention="7 days")

    def setup_browser(self):
        """初始化浏览器"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        
        # 设置响应监听
        self.page = self.context.new_page()
        self.page.on("response", self._handle_api_response)
        self.page.set_default_timeout(20000)

    def login(self, username, password):
        """登录千瓜网站"""
        try:
            logger.info("开始登录流程...")
            
            # 访问首页
            self.page.goto(self.base_url)
            time.sleep(2)
            
            # 点击登录按钮
            self.page.click("text=登录")
            time.sleep(2)
            
            # 输入用户名和密码
            self.page.fill("input[placeholder='请输入手机号/邮箱']", username)
            time.sleep(1)
            self.page.fill("input[placeholder='请输入密码']", password)
            time.sleep(1)
            
            # 点击登录
            self.page.click("button:has-text('登录')")
            
            # 等待登录成功
            try:
                self.page.wait_for_selector(".user-avatar", timeout=10000)
                logger.info("登录成功！")
                self.is_logged_in = True
                return True
            except PlaywrightTimeoutError:
                logger.error("登录失败，未检测到登录成功状态")
                return False
                
        except Exception as e:
            logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def navigate_to_note_search(self):
        """导航到笔记搜索页面"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法访问笔记搜索")
                return False
                
            # 点击笔记菜单
            self.page.click("text=笔记")
            time.sleep(2)
            
            # 点击笔记搜索
            self.page.click("text=笔记搜索")
            time.sleep(2)
            
            return True
            
        except Exception as e:
            logger.error(f"导航到笔记搜索页面失败: {str(e)}")
            return False

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if 'api.qian-gua.com/Rank/GetBusinessNoteRankList' in url:
                try:
                    data = response.json()
                    if data.get('Code') == 0 and data.get('Data'):
                        self.api_data.extend(data['Data'])
                        logger.info(f"成功捕获笔记数据，当前总数: {len(self.api_data)}")
                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}")
                    
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def save_to_database(self):
        """保存数据到数据库"""
        try:
            for note_data in self.api_data:
                try:
                    note = NoteRank(
                        note_id=note_data.get('note_id'),
                        title=note_data.get('title'),
                        author=note_data.get('author'),
                        publish_time=datetime.fromtimestamp(note_data.get('publish_time', 0)),
                        likes=note_data.get('likes', 0),
                        comments=note_data.get('comments', 0),
                        collects=note_data.get('collects', 0)
                    )
                    self.session.add(note)
                    
                except Exception as e:
                    logger.error(f"处理单条数据时出错: {str(e)}")
                    continue
                    
            self.session.commit()
            logger.info(f"成功保存 {len(self.api_data)} 条数据到数据库")
            
        except Exception as e:
            logger.error(f"保存数据到数据库时出错: {str(e)}")
            self.session.rollback()

    def run(self, username, password):
        """运行爬虫"""
        try:
            # 1. 登录
            if not self.login(username, password):
                return
                
            # 2. 导航到笔记搜索页面
            if not self.navigate_to_note_search():
                return
                
            # 3. 等待数据加载完成
            time.sleep(5)
            
            # 4. 保存数据到数据库
            if self.api_data:
                self.save_to_database()
            else:
                logger.warning("未捕获到任何数据")
                
        except Exception as e:
            logger.error(f"运行过程出现异常: {str(e)}")
        finally:
            self.close()

    def close(self):
        """关闭资源"""
        try:
            self.session.close()
            self.page.close()
            self.context.close()
            self.browser.close()
            self.playwright.stop()
            logger.info("所有资源已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

if __name__ == '__main__':
    try:
        logger.info("程序启动")
        
        # 加载配置
        config = load_config()
        
        # 获取配置参数
        field_value = config['field_value']
        use = config['use']
        
        # 处理博主数据
        process_blogger_data(field_value, use)
        
        logger.info("任务执行完成，程序将退出")
        
    except KeyboardInterrupt:
        logger.info("程序被手动中断")
    except Exception as e:
        logger.error(f"程序发生未预期的错误: {str(e)}")