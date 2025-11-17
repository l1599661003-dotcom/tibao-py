"""
 小红书博主简介抓取脚本
 从API获取博主列表，访问小红书主页抓取简介，并同步到数据API
 """
import logging
import re
import time
import traceback
from typing import Dict, List, Optional
from datetime import datetime

import requests
from requests import RequestException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 配置常量
class Config:
    # API URLs
    BLOGGER_LIST_API = "https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator"
    SYNC_API_URL = "http://47.104.76.46:19000/api/v1/sync/spider/data"

    # 请求参数
    BLOGGER_TYPE = 3
    PAGE = 1
    PAGE_SIZE = 9999
    REQUEST_TIMEOUT = 30
    REQUEST_DELAY = 1.0  # 请求间隔（秒）

    # 重试配置
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 0.5

    # 日志配置
    LOG_DIR = "command/pgy_playwright/logs"
    LOG_LEVEL = logging.INFO


def setup_logging():
    """配置日志系统"""
    import os
    os.makedirs(Config.LOG_DIR, exist_ok=True)

    log_filename = f"{Config.LOG_DIR}/pgy_intro_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


# 小红书请求头
XHS_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'www.xiaohongshu.com',
    'Priority': 'u=0, i',
    'Referer': 'https://pgy.xiaohongshu.com/',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

def create_session_with_retry() -> requests.Session:
    """创建带重试机制的 requests session"""
    session = requests.Session()
    retry_strategy = Retry(
        total=Config.MAX_RETRIES,
        backoff_factor=Config.RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def create_payload_template(blogger_id: str = None, creator_intro: str = None) -> Dict:
    """创建标准 payload 模板"""
    blogger_data = []
    if blogger_id and creator_intro:
        blogger_data = [{"platform_user_id": blogger_id, "creator_intro": creator_intro}]

    return {
        "apis": [
            {"tb_name": "blogger_info", "tb_data": blogger_data},
            {"tb_name": "blogger_note_rate", "tb_data": []},
            {"tb_name": "blogger_data_summary", "tb_data": []},
            {"tb_name": "blogger_note_detail", "tb_data": []},
            {"tb_name": "blogger_fans_summary", "tb_data": []},
            {"tb_name": "blogger_fans_profile", "tb_data": []},
            {"tb_name": "blogger_fans_history", "tb_data": []},
        ],
        "client_id": 1
    }


def fetch_blogger_list(logger: logging.Logger, session: requests.Session) -> Optional[List[Dict]]:
    """从API获取博主列表"""
    api_url = f"{Config.BLOGGER_LIST_API}?type={Config.BLOGGER_TYPE}&page={Config.PAGE}&pageSize={Config.PAGE_SIZE}"

    logger.info(f"正在请求API获取博主列表: {api_url}")

    try:
        headers = {"Content-Type": "application/json"}
        response = session.post(api_url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()

        data = response.json()
        records = data.get('data', [])
        logger.info(f"成功获取 {len(records)} 条博主记录")
        return records

    except RequestException as e:
        logger.error(f"获取博主列表失败: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"解析API响应失败: {e}")
        return None


def extract_user_intro(html: str, blogger_id: str, logger: logging.Logger) -> Optional[str]:
    """从HTML中提取用户简介"""
    match = re.search(r'<div class="user-desc"[^>]*>(.*?)</div>', html, re.S)

    if match:
        user_desc = match.group(1).strip()
        logger.info(f"博主 {blogger_id} 简介: {user_desc}")
        return user_desc
    else:
        logger.warning(f"未找到博主 {blogger_id} 的简介")
        if logger.level == logging.DEBUG:
            logger.debug(f"HTML片段: {html[:1000]}...")
        return None


def fetch_blogger_intro(blogger_id: str, logger: logging.Logger, session: requests.Session) -> Optional[str]:
    """获取博主简介"""
    url = f"https://www.xiaohongshu.com/user/profile/{blogger_id}"
    logger.info(f"访问博主主页: {url}")

    try:
        time.sleep(Config.REQUEST_DELAY)
        response = session.get(url, headers=XHS_HEADERS, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()
        return extract_user_intro(response.text, blogger_id, logger)

    except RequestException as e:
        logger.error(f"访问博主 {blogger_id} 主页失败: {e}")
        return None


def sync_single_record_to_api(payload: Dict, logger: logging.Logger, session: requests.Session) -> bool:
    """同步单条记录到API"""
    try:
        headers = {"Content-Type": "application/json"}
        response = session.post(
            Config.SYNC_API_URL,
            json=payload,
            headers=headers,
            timeout=Config.REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            try:
                response_data = response.json()
                if response_data.get('code') == 200:
                    logger.info("数据同步成功")
                    return True
                else:
                    logger.error(f"数据同步失败，API返回错误: {response_data}")
                    return False
            except ValueError:
                logger.error(f"API返回非JSON响应: {response.text[:200]}")
                return False
        else:
            logger.error(f"数据同步失败，HTTP状态码: {response.status_code}, 响应: {response.text[:200]}")
            return False

    except RequestException as e:
        logger.error(f"数据同步请求失败: {e}")
        return False
    except Exception as e:
        logger.error(f"数据同步异常: {e}")
        logger.debug(traceback.format_exc())
        return False


def process_blogger(record: Dict, logger: logging.Logger, session: requests.Session) -> bool:
    """处理单个博主"""
    blogger_id = record.get('platform_user_id')

    if not blogger_id:
        logger.warning(f"记录缺少 platform_user_id: {record}")
        return False

    logger.info(f"开始处理博主ID: {blogger_id}")

    try:
        user_intro = fetch_blogger_intro(blogger_id, logger, session)

        if user_intro:
            payload = create_payload_template(blogger_id, user_intro)
            return sync_single_record_to_api(payload, logger, session)
        else:
            logger.warning(f"博主 {blogger_id} 未找到简介，跳过同步")
            return False

    except Exception as e:
        logger.error(f"处理博主 {blogger_id} 时发生异常: {e}")
        logger.debug(traceback.format_exc())
        return False


def main():
    """主函数"""
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("小红书博主简介抓取脚本启动")
    logger.info("=" * 60)

    session = create_session_with_retry()
    total_count = 0
    success_count = 0
    fail_count = 0

    try:
        records = fetch_blogger_list(logger, session)

        if not records:
            logger.error("未获取到博主列表，程序退出")
            return

        total_count = len(records)

        for index, record in enumerate(records, 1):
            logger.info(f"进度: {index}/{total_count}")

            if process_blogger(record, logger, session):
                success_count += 1
            else:
                fail_count += 1

        logger.info("=" * 60)
        logger.info(f"处理完成！总计: {total_count}, 成功: {success_count}, 失败: {fail_count}")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行异常: {e}")
        logger.debug(traceback.format_exc())
    finally:
        session.close()
        logger.info("程序结束")


if __name__ == '__main__':
    main()