import re
import time
import requests
import json
import schedule
import urllib3
import sys
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Text, BigInteger, DECIMAL
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
import logging
from pathlib import Path
import subprocess
import signal

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
"""
    抓每个月变现博主数据
"""

# 数据库连接配置
DATABASE_URL = 'mysql+pymysql://fpdev:fpdev@47.104.13.93:3306/tibao_2'

# 创建数据库引擎
engine = create_engine(DATABASE_URL, isolation_level="READ UNCOMMITTED")

# 创建会话工厂
Session = sessionmaker(bind=engine)
ScopedSession = scoped_session(Session)

# 创建会话
session = Session()
Base = declarative_base()


class TrainingBloggerDetails(Base):
    __tablename__ = 'training_blogger_details'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_id = Column(String(244), nullable=True)  # 小红书ID
    nickname = Column(String(255), nullable=True)  # 昵称
    blogger_dandelion_id = Column(String(100), nullable=False)  # 荷公英ID
    followers_count = Column(Integer, nullable=False, default=0)  # 粉丝量
    organization_name = Column(String(255), nullable=True)  # 机构名称
    graphic_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 图文报价
    video_price = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 视频报价
    current_note_count = Column(Integer, nullable=False, default=0)  # 当前笔记总数
    graphic_orders_count = Column(Integer, nullable=False, default=0)  # 图文商单数量
    video_orders_count = Column(Integer, nullable=False, default=0)  # 视频商单数量
    tags = Column(String(255), nullable=True)  # 标签
    graphic_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 图文总营收
    video_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 视频总营收
    total_revenue = Column(DECIMAL(20, 4), nullable=False, default=0.0000)  # 总营收
    page = Column(Integer, nullable=True)  # 页数
    month = Column(String(7), nullable=True)  # 月份（格式如2024-06）
    created_at = Column(TIMESTAMP, nullable=False)  # 创建时间
    updated_at = Column(TIMESTAMP, nullable=False)  # 更新时间
    is_updated = Column(Integer, nullable=False, default=0)  # 是否更新
    intro = Column(Text, nullable=True)  # 简介
    type = Column(Integer, nullable=False, default=0)  # 1=图文2000-5000, 2=图文5001+, 3=视频2000-5000, 4=视频5001+


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


def run_with_retry(config, max_retries=3, retry_delay=60):
    """
    包装器函数，处理程序崩溃后的重试逻辑
    """
    retries = 0
    while retries < max_retries:
        try:
            logger.info("开始执行更新博主详情任务...")
            update_blogger_details(config)
            logger.info("任务执行完成")
            break
        except Exception as e:
            retries += 1
            logger.error(f"程序发生错误: {str(e)}")
            logger.info(f"这是第 {retries} 次重试，{retry_delay}秒后重试...")
            time.sleep(retry_delay)

    if retries == max_retries:
        logger.error("达到最大重试次数，本次任务结束")


def update_blogger_details(config):
    try:
        logger.info("开始获取飞书记录...")
        feishu_config = config['feishu_config']
        search = search_feishu_record(
            feishu_config['app_token'],
            feishu_config['table_id'],
            feishu_config['view_id'],
            feishu_config['field_name'],
            config['field_value']
        )
        if not search:
            logger.error("无法获取飞书记录")
            return

        logger.info(f"成功获取飞书记录: {len(search)} 条")
        record_id = search[0]['record_id']  # 获取记录ID
        use = search[0]['fields']['用途'][0]['text']

        token = get_pgy_token(use)
        if not token:
            logger.error("Token获取失败")
            return

        logger.info("Token获取成功")
        batch_size = 1000
        processed = 0

        while True:
            search = search_feishu_record(
                feishu_config['app_token'],
                feishu_config['table_id'],
                feishu_config['view_id'],
                feishu_config['field_name'],
                config['field_value']
            )
            if not search:
                logger.error("无法获取飞书记录")
                time.sleep(300)  # 如果获取飞书记录失败，等待5分钟后重试
                continue
                
            start_value = search[0]['fields']['开始取值范围']
            end_value = search[0]['fields']['结束取值范围']
            logger.info(f"参数信息: 用途={use} 范围={start_value}-{end_value}")

            query = session.query(TrainingBloggerDetails).filter(
                TrainingBloggerDetails.is_updated == 0
            )

            query = query.filter(TrainingBloggerDetails.id > start_value)
            query = query.filter(TrainingBloggerDetails.id <= end_value)

            blogger_list = query.limit(batch_size).all()

            if not blogger_list:
                logger.info("当前没有需要处理的博主数据，等待10分钟后重新检查...")
                # 更新飞书状态为已完成
                data = {
                    "fields": {
                        "是否抓取完毕": "是"
                    }
                }
                update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
                time.sleep(600)  # 等待10分钟后继续检查
                continue

            logger.info(f"本批次需要处理 {len(blogger_list)} 个博主")

            for blogger in blogger_list:
                blogger_id = blogger.blogger_dandelion_id
                try:
                    logger.info(f"开始处理博主ID: {blogger_id}")
                    time.sleep(6)

                    # 获取博主信息
                    blogger_data = blogger_info(blogger_id, token, feishu_config, record_id)
                    if blogger_data is None:
                        logger.warning(f"获取博主 {blogger_id} 信息失败，继续处理下一个")
                        continue

                    logger.info("获取博主基本信息成功")
                    time.sleep(6)

                    # 获取笔记信息
                    notes_data = notes_detail(blogger_id, blogger.video_price, blogger.graphic_price, token, feishu_config, record_id)
                    
                    # 准备更新数据
                    data = blogger_data.copy()  # 创建博主基本信息的副本
                    data['updated_at'] = datetime.now()
                    data['is_updated'] = 1  # 标记为已更新，无论是否有笔记数据

                    # 如果有笔记数据，则添加到更新数据中
                    if notes_data is not None:
                        data.update(notes_data)
                        logger.info("获取博主笔记信息成功")
                    else:
                        logger.info("博主没有笔记数据，仅更新基本信息")

                    # 获取简介（如果需要）
                    if not blogger.intro:
                        intro = get_pgy_intro(blogger_id, token, feishu_config, record_id)
                        if intro and "intro" in intro:  # 添加 intro 不为 None 的判断
                            data["intro"] = intro["intro"]
                            logger.info("成功获取博主简介")

                    # 更新数据库
                    session.query(TrainingBloggerDetails).filter_by(id=blogger.id).update(data)
                    session.commit()
                    processed += 1
                    logger.info(f"成功更新博主记录: {blogger.id}, 已处理: {processed}")

                except Exception as e:
                    logger.error(f"处理博主 {blogger.id} 时出错: {str(e)}")
                    session.rollback()  # 防止事务锁住
                    continue

            logger.info(f"本批次完成，共处理 {processed} 条记录")

    except Exception as e:
        logger.error(f"更新博主详情时发生错误: {str(e)}")
        time.sleep(300)  # 发生错误时等待5分钟后重试
        raise  # 向上抛出异常，触发重试机制


def get_pgy_token(use=""):
    """获取 Feishu Token"""
    data = read_table_content("PJISbPC5OaihG8sCfMpc4Wohnyb", "tblDO9VqC6EMHGiY", "vewUvYvleV")

    if not data:
        return ""

    token = ""

    for item in data:
        if "用途" in item['fields'] and item['fields']["用途"][0]["text"] == use:
            token = "".join(ite["text"] for ite in item['fields'].get("蒲公英token", []))

    return token.strip()


def blogger_info(user_id, token, feishu_config=None, record_id=None):
    try:
        url = f"https://pgy.xiaohongshu.com/api/solar/cooperator/user/blogger/{user_id}"
        headers = {"Cookie": token}

        response = requests.get(url, headers=headers, timeout=30, verify=False)
        res = response.json()
        logger.error(response.json())
        
        # 检查错误码，更新飞书状态后等待6小时再重启
        if res.get("code") in [902, 903]:
            logger.error("Token已失效，程序将在6小时后重启")
            if feishu_config and record_id:
                data = {
                    "fields": {
                        "抓取异常": "Token过期"
                    }
                }
                update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
            if 'session' in globals():
                try:
                    session.close()
                except:
                    pass
            time.sleep(21600)  # 等待6小时
            restart_program()
        elif res.get("code") == 300013:
            logger.error("访问频次异常，程序将在6小时后重启")
            if feishu_config and record_id:
                data = {
                    "fields": {
                        "抓取异常": "访问频次异常"
                    }
                }
                update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
            if 'session' in globals():
                try:
                    session.close()
                except:
                    pass
            time.sleep(21600)  # 等待6小时
            restart_program()
            
        # 其他逻辑保持不变
        if res.get("code") == 0:
            if "data" in res and res["data"]:
                data = res["data"]
                note_sign = data.get("noteSign", {}).get("name", "") if data.get("noteSign") else ""

                insert_data = {
                    "nickname": data.get("name", ""),  # 昵称
                    "organization_name": note_sign,  # 博主机构
                    "followers_count": data.get("fansCount", 0),  # 粉丝数量
                    "account_id": data.get("redId", ""),  # 账号ID
                }
                return insert_data
            else:
                logger.error(f"博主信息接口返回数据格式错误: {res}")
                return None
        else:
            logger.error(f"博主信息接口请求失败，状态码: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"获取博主信息时发生错误: {str(e)}")
        return None


def notes_detail(user_id, video_price, graphic_price, token, feishu_config=None, record_id=None):
    try:
        url = f"https://pgy.xiaohongshu.com/api/solar/kol/dataV2/notesDetail?advertiseSwitch=1&orderType=1&pageNumber=1&pageSize=999&userId={user_id}&noteType=3&withComponent=false"
        headers = {"Cookie": token}

        response = requests.get(url, headers=headers, timeout=30, verify=False)
        res = response.json()
        logger.error(response.json())
        
        # 检查错误码，更新飞书状态后等待6小时再重启
        if res.get("code") in [902, 903]:
            logger.error("Token已失效，程序将在6小时后重启")
            if feishu_config and record_id:
                data = {
                    "fields": {
                        "抓取异常": "Token过期"
                    }
                }
                update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
            if 'session' in globals():
                try:
                    session.close()
                except:
                    pass
            time.sleep(21600)  # 等待6小时
            restart_program()
        elif res.get("code") == 300013:
            logger.error("访问频次异常，程序将在6小时后重启")
            if feishu_config and record_id:
                data = {
                    "fields": {
                        "抓取异常": "访问频次异常"
                    }
                }
                update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
            if 'session' in globals():
                try:
                    session.close()
                except:
                    pass
            time.sleep(21600)  # 等待6小时
            restart_program()
            
        # 其他逻辑保持不变
        if res.get("code") == 0:
            res = response.json()
            if "data" in res and res["data"]:
                data = res["data"]
                notes_list = data.get("list", [])

                try:
                    # 获取上个月的第一天
                    last_month_start = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime(
                        '%Y-%m-%d')
                    this_month_start = datetime.today().replace(day=1).strftime('%Y-%m-%d')

                    # 筛选上个月的订单
                    last_order = [note for note in notes_list if
                                  last_month_start <= note.get("date", "") < this_month_start]

                    # 计算视频和图文订单数量
                    video_orders = [note for note in last_order if note.get("isVideo", False)]
                    video_orders_count = len(video_orders)
                    graphic_orders_count = len(last_order) - video_orders_count

                    insert_data = {
                        "current_note_count": data.get("total", 0),  # 当前笔记总数
                        "video_orders_count": video_orders_count,  # 视频商单数量
                        "graphic_orders_count": graphic_orders_count,  # 图文商单数量
                        "graphic_revenue": graphic_orders_count * graphic_price,  # 图文总营收
                        "video_revenue": video_orders_count * video_price,  # 视频总营收
                        "total_revenue": (graphic_orders_count * graphic_price) + (video_orders_count * video_price),
                        # 总营收
                        "is_updated": 1,  # 是否更新
                    }
                    return insert_data
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


def get_pgy_intro(user_id, token, feishu_config=None, record_id=None):
    time.sleep(6)  # 避免请求过快

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Cookie': token
    }

    url = f"https://www.xiaohongshu.com/user/profile/{user_id}"

    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()  # 检查是否请求成功
        response_body = response.text

        # 检查是否包含错误码
        if "code" in response_body:
            try:
                res = json.loads(response_body)
                if res.get("code") in [902, 903]:
                    logger.error("Token已失效，程序将在6小时后重启")
                    if feishu_config and record_id:
                        data = {
                            "fields": {
                                "抓取异常": "Token过期"
                            }
                        }
                        update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
                    if 'session' in globals():
                        try:
                            session.close()
                        except:
                            pass
                    time.sleep(21600)  # 等待6小时
                    restart_program()
                elif res.get("code") == 300013:
                    logger.error("访问频次异常，程序将在6小时后重启")
                    if feishu_config and record_id:
                        data = {
                            "fields": {
                                "抓取异常": "访问频次异常"
                            }
                        }
                        update_feishu_status(feishu_config['app_token'], feishu_config['table_id'], record_id, data)
                    if 'session' in globals():
                        try:
                            session.close()
                        except:
                            pass
                    time.sleep(21600)  # 等待6小时
                    restart_program()
            except json.JSONDecodeError:
                pass  # 如果不是JSON格式，继续处理HTML内容

        # 使用 BeautifulSoup 提取简介（推荐方式）
        soup = BeautifulSoup(response_body, "html.parser")
        user_desc_tag = soup.find("div", class_="user-desc")
        if user_desc_tag:
            user_desc = user_desc_tag.get_text(strip=True)
            return {"intro": user_desc}

        # 备用方案：使用正则匹配
        match = re.search(r'<div class="user-desc"[^>]*>(.*?)</div>', response_body, re.S)
        if match:
            user_desc = match.group(1).strip()
            return {"intro": user_desc}

        # 没找到简介
        return None

    except requests.RequestException as e:
        logger.error(f"获取博主简介时发生网络错误: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"获取博主简介时发生未知错误: {str(e)}")
        return None


def get_feishu_token():
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
        print(f"获取飞书Token失败: {e}")
        return None


def read_table_content(app, table, view):
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
            print(f"飞书API返回错误: {response_data}")
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
        print(f"查询飞书表格信息失败：{e}")
        return None


# 查询飞书单条信息
def search_feishu_record(app_token, table_id, view_id, field_name, field_value):
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
        print(f"查询飞书单条信息失败: {e}")
        return None


# 添加配置文件读取函数
def load_config():
    config_path = os.path.join(os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__),
                               'config.json')
    default_config = {
        'feishu_config': {
            'app_token': 'PJISbPC5OaihG8sCfMpc4Wohnyb',
            'table_id': 'tbl4buWQgvOt6bjs',
            'view_id': 'vewysOdkI7',
            'field_name': '使用电脑'
        },
        'execution_time': {
            'hour': 0,
            'minute': 15
        },
        'database': {
            'url': 'mysql+pymysql://fpdev:fpdev@47.104.13.93:3306/tibao_2',
            'isolation_level': 'READ UNCOMMITTED'
        }
    }

    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 检查是否存在必需的 field_value
                if 'field_value' not in config:
                    logger.error("配置文件中缺少 field_value 值，请在配置文件中指定要处理的电脑编号")
                    sys.exit(1)
                # 合并默认配置和用户配置
                return {**default_config, **config}
        else:
            logger.error("配置文件不存在，请创建配置文件并指定 field_value 值")
            # 创建配置文件模板，但不包含 field_value
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4, ensure_ascii=False)
            sys.exit(1)
    except Exception as e:
        logger.error(f"读取配置文件出错: {e}")
        sys.exit(1)


def init_database(config):
    """初始化数据库连接"""
    try:
        global engine, session
        engine = create_engine(
            config['database']['url'],
            isolation_level=config['database']['isolation_level']
        )
        Session = sessionmaker(bind=engine)
        session = Session()
        # 测试连接
        session.query(TrainingBloggerDetails).first()
        logger.info("数据库连接初始化成功")
        return True
    except Exception as e:
        error_message = str(e).lower()
        if "远程主机强迫关闭了一个现有的连接" in error_message or \
           "an existing connection was forcibly closed by the remote host" in error_message:
            logger.error("数据库连接被远程主机关闭，30分钟后将自动重启程序")
            if 'session' in globals():
                try:
                    session.close()
                except:
                    pass
            time.sleep(1800)  # 等待30分钟
            restart_program()
        else:
            logger.error(f"数据库连接初始化失败: {e}")
            return False


# 修改主程序部分
def update_feishu_status(app_token, table_id, record_id, data):
    """更新飞书记录状态"""
    try:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {get_feishu_token()}'
        }

        # 更新数据

        response = requests.put(url, headers=headers, json=data, verify=False)
        response.raise_for_status()

        if response.status_code == 200:
            logger.info("飞书记录状态更新成功")
            return True
        else:
            logger.error(f"飞书记录状态更新失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"更新飞书记录状态时发生错误: {str(e)}")
        return False


def check_running():
    """检查程序是否已经在运行（Windows版本）"""
    lock_file = 'blogger_update.lock'
    try:
        # 获取程序所在目录
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
        lock_path = os.path.join(base_dir, lock_file)
        
        try:
            if os.path.exists(lock_path):
                # 检查文件是否可以被删除（如果可以删除，说明没有程序正在运行）
                try:
                    os.remove(lock_path)
                except PermissionError:
                    logger.error("程序已经在运行中")
                    return False
            
            # 创建新的锁文件
            global lock_fd
            lock_fd = open(lock_path, 'w')
            lock_fd.write(str(os.getpid()))
            lock_fd.flush()
            return True
            
        except IOError as e:
            logger.error(f"创建锁文件失败: {e}")
            return False
            
    except Exception as e:
        logger.error(f"检查程序运行状态时发生错误: {str(e)}")
        return False


def restart_program():
    """重启程序的安全方法（Windows版本）"""
    logger.info("准备重启程序...")
    
    # 清理锁文件
    if 'lock_fd' in globals():
        try:
            lock_fd.close()
            # 获取锁文件路径
            if getattr(sys, 'frozen', False):
                base_dir = os.path.dirname(sys.executable)
            else:
                base_dir = os.path.dirname(os.path.abspath(__file__))
            lock_path = os.path.join(base_dir, 'blogger_update.lock')
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except Exception as e:
            logger.error(f"清理锁文件时发生错误: {str(e)}")

    # 使用subprocess重启程序
    try:
        if getattr(sys, 'frozen', False):
            # 如果是exe运行
            subprocess.Popen([sys.executable] + sys.argv)
        else:
            # 如果是python脚本运行
            subprocess.Popen([sys.executable, sys.argv[0]] + sys.argv[1:])
        sys.exit(0)  # 退出当前进程
    except Exception as e:
        logger.error(f"重启程序时发生错误: {str(e)}")
        sys.exit(1)


def signal_handler(signum, frame):
    """处理程序退出信号"""
    logger.info(f"收到信号 {signum}，准备退出程序")
    cleanup()
    sys.exit(0)

def cleanup():
    """清理函数"""
    try:
        if 'session' in globals():
            session.close()
            logger.info("数据库连接已关闭")
        
        if 'lock_fd' in globals():
            try:
                lock_fd.close()
                if getattr(sys, 'frozen', False):
                    base_dir = os.path.dirname(sys.executable)
                else:
                    base_dir = os.path.dirname(os.path.abspath(__file__))
                lock_path = os.path.join(base_dir, 'blogger_update.lock')
                if os.path.exists(lock_path):
                    os.remove(lock_path)
            except Exception as e:
                logger.error(f"清理锁文件时发生错误: {str(e)}")
    except Exception as e:
        logger.error(f"清理时发生错误: {str(e)}")


# 修改主程序部分
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    try:
        logger.info("程序启动")
        
        # 检查程序是否已经在运行
        if not check_running():
            logger.error("另一个实例已经在运行，程序将退出")
            sys.exit(1)
            
        logger.info(
            f"日志文件位置: {os.path.join(os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__), 'logs')}")

        # 加载配置
        config = load_config()
        logger.info(f"加载配置成功: {json.dumps(config, ensure_ascii=False)}")

        # 初始化数据库（添加重试机制）
        retry_count = 0
        max_retries = 3  # 最大重试次数
        while retry_count < max_retries:
            if init_database(config):
                break
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"数据库连接失败，10分钟后进行第 {retry_count + 1} 次重试")
                time.sleep(600)  # 等待10分钟
        
        if retry_count == max_retries:
            logger.error("数据库连接重试次数达到上限，程序退出")
            sys.exit(1)

        # 从配置文件获取执行时间并转换为整数
        execution_hour = int(config['execution_time']['hour'])
        execution_minute = int(config['execution_time']['minute'])

        # 获取今天的执行时间
        target_time = datetime.now().replace(
            hour=execution_hour,
            minute=execution_minute,
            second=0,
            microsecond=0
        )

        # 如果当前时间已经超过了今天的执行时间，就立即执行
        current_time = datetime.now()
        if current_time > target_time:
            logger.info("当前时间已超过今天的执行时间，将立即执行任务")
            run_with_retry(config)
        else:
            logger.info(f"程序将在今天 {target_time.strftime('%H:%M:%S')} 执行任务")

            # 计算需要等待的秒数
            wait_seconds = (target_time - current_time).total_seconds()

            if wait_seconds > 0:
                logger.info(f"等待执行，还需 {wait_seconds / 60:.1f} 分钟")
                time.sleep(wait_seconds)

            # 执行任务
            logger.info("开始执行任务...")
            run_with_retry(config)

        logger.info("任务执行完成，程序将退出")

    except KeyboardInterrupt:
        logger.info("程序被手动中断")
    except Exception as e:
        logger.error(f"程序发生未预期的错误: {str(e)}")
    finally:
        # 清理工作
        cleanup()