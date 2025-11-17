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

from core.localhost_fp_project import session
from models.models import PgyUser


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
    'Cookic': 'a1=19882f48bcap04qq1r777p9ggafrdrroz4abocygo50000339517; webId=e7111fec356dc781ca1d14d236afda9a; customerClientId=016216997022661; abRequestId=e7111fec356dc781ca1d14d236afda9a; x-user-id-creator.xiaohongshu.com=634cc30badd08a00019ee4e3; gid=yjYSKK8dd0uiyjYYJi4YD4SvS0U84AAyFWWW0klUjkk0iD28FfFFll888qqj2yW8DY2KKijK; x-user-id-ad-market.xiaohongshu.com=67bbea69000000000d009ec6; access-token-ad-market.xiaohongshu.com=customer.ad_market.AT-68c517561719769394855947z3wtukxckssgow61; webBuild=4.85.1; customer-sso-sid=68c51757256798490619084805paie9ao3lces1r; x-user-id-pgy.xiaohongshu.com=634cc30badd08a00019ee4e3; solar.beaker.session.id=AT-68c517572567984906207233bejag7rlmfv3tqmi; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517572567984906207233bejag7rlmfv3tqmi; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517572567984906207233bejag7rlmfv3tqmi; xsecappid=xhs-pc-web; unread={%22ub%22:%226919658500000000070034b6%22%2C%22ue%22:%226919f1f40000000007031061%22%2C%22uc%22:31}; acw_tc=0a4a453a17633503670788439e1f7e150b4e0b232325341df72f72139eccab; loadts=1763350468449; web_session=040069b68a4f0566d40163d02f3b4b36c08690'
}


def extract_user_intro(html: str, blogger_id: str, logger: logging.Logger) -> Optional[str]:
    """从HTML中提取用户简介"""
    match = re.search(r'<div class="user-name"[^>]*>\s*([^<]+)', html)

    if match:
        user_desc = match.group(1).strip()
        logger.info(f"博主 {blogger_id} 达人名: {user_desc}")
        return user_desc
    else:
        logger.warning(f"未找到博主 {blogger_id} 的粉丝数")
        if logger.level == logging.DEBUG:
            logger.debug(f"HTML片段: {html[:1000]}...")
        return None

def create_session_with_retry() -> requests.Session:
    """创建带重试机制的 requests session"""
    sessions = requests.Session()
    retry_strategy = Retry(
        total=Config.MAX_RETRIES,
        backoff_factor=Config.RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    sessions.mount("http://", adapter)
    sessions.mount("https://", adapter)
    return sessions

def fetch_blogger_intro(blogger_id: str, logger: logging.Logger) -> Optional[str]:
    """获取博主简介"""
    url = f"https://www.xiaohongshu.com/user/profile/{blogger_id}"
    logger.info(f"访问博主主页: {url}")

    try:
        time.sleep(3)
        sessions = create_session_with_retry()
        response = sessions.get(url, headers=XHS_HEADERS, timeout=3000)
        response.raise_for_status()
        return extract_user_intro(response.text, blogger_id, logger)

    except RequestException as e:
        logger.error(f"访问博主 {blogger_id} 主页失败: {e}")
        return None

def main():
    """主函数"""
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("小红书博主简介抓取脚本启动")
    logger.info("=" * 60)

    total_count = 0
    success_count = 0
    fail_count = 0

    try:
        alls = session.query(PgyUser).all()
        for user in alls:
            intro = fetch_blogger_intro(user.userId, logger)
            user.nick_name = intro
            session.commit()

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