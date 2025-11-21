"""
小红书博主简介抓取脚本
从API获取博主列表，访问小红书主页抓取简介，并同步到数据API
支持多台机器并行抓取
"""
import re
import time
import argparse
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 配置常量
class Config:
    # API URLs
    BLOGGER_LIST_API = "https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator"
    # BLOGGER_LIST_API = "http://localhost:5666/api/admin/creatorBusiness/getNewerCreator"
    SYNC_API_URL = "https://tianji.fangpian999.com/api/admin/creatorBusiness/saveCreatorIntro"

    # 请求参数
    BLOGGER_TYPE = 4
    PAGE_SIZE = 100  # 每次抓取100条
    TOTAL_MACHINES = 4  # 总机器数
    REQUEST_TIMEOUT = 30
    REQUEST_DELAY = 8  # 请求间隔（秒）

    # 重试配置
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 0.5


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


def fetch_blogger_list(session: requests.Session, page: int) -> Optional[List[Dict]]:
    """从API获取博主列表"""
    api_url = f"{Config.BLOGGER_LIST_API}?type={Config.BLOGGER_TYPE}&page={page}&pageSize={Config.PAGE_SIZE}"
    print(f"正在请求API获取博主列表: {api_url} (第{page}页)")

    try:
        headers = {"Content-Type": "application/json"}
        response = session.post(api_url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()

        data = response.json()
        records = data.get('data', [])
        print(f"成功获取 {len(records)} 条博主记录")
        return records

    except Exception as e:
        print(f"获取博主列表失败: {e}")
        return None


def extract_user_intro(html: str, blogger_id: str) -> Optional[str]:
    """从HTML中提取用户简介"""
    match = re.search(r'<div class="user-desc"[^>]*>(.*?)</div>', html, re.S)

    if match:
        user_desc = match.group(1).strip()
        print(f"博主 {blogger_id} 简介: {user_desc}")
        return user_desc
    else:
        print(f"未找到博主 {blogger_id} 的简介")
        return None


def fetch_blogger_intro(blogger_id: str, session: requests.Session) -> Optional[str]:
    """获取博主简介"""
    url = f"https://www.xiaohongshu.com/user/profile/{blogger_id}"
    print(f"访问博主主页: {url}")

    try:
        time.sleep(Config.REQUEST_DELAY)
        response = session.get(url, headers=XHS_HEADERS, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()
        return extract_user_intro(response.text, blogger_id)

    except Exception as e:
        print(f"访问博主 {blogger_id} 主页失败: {e}")
        return None


def sync_single_record_to_api(payload: Dict, session: requests.Session) -> bool:
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
                    print("数据同步成功")
                    return True
                else:
                    print(f"数据同步失败，API返回错误: {response_data}")
                    return False
            except ValueError:
                print(f"API返回非JSON响应: {response.text[:200]}")
                return False
        else:
            print(f"数据同步失败，HTTP状态码: {response.status_code}, 响应: {response.text[:200]}")
            return False

    except Exception as e:
        print(f"数据同步请求失败: {e}")
        return False


def process_blogger(record: Dict, session: requests.Session) -> bool:
    """处理单个博主"""
    blogger_id = record.get('platform_user_id')

    if not blogger_id:
        print(f"记录缺少 platform_user_id: {record}")
        return False

    print(f"开始处理博主ID: {blogger_id}")

    try:
        user_intro = fetch_blogger_intro(blogger_id, session)

        if user_intro:
            data = {
                'platform_user_id': blogger_id,
                'creator_intro': user_intro
            }
            return sync_single_record_to_api(data, session)
        else:
            print(f"博主 {blogger_id} 未找到简介，跳过同步")
            return False

    except Exception as e:
        print(f"处理博主 {blogger_id} 时发生异常: {e}")
        return False


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='小红书博主简介抓取脚本')
    parser.add_argument('--machine-id', type=int, required=True,
                        help='机器编号 (1-10)')
    parser.add_argument('--max-rounds', type=int, default=None,
                        help='最大抓取轮次，不设置则一直抓取直到没有数据')
    args = parser.parse_args()

    machine_id = args.machine_id
    max_rounds = args.max_rounds

    # 验证机器编号
    if machine_id < 1 or machine_id > Config.TOTAL_MACHINES:
        print(f"错误：机器编号必须在 1-{Config.TOTAL_MACHINES} 之间")
        return

    print("=" * 60)
    print(f"小红书博主简介抓取脚本启动 - 机器 {machine_id}")
    print("=" * 60)

    session = create_session_with_retry()
    round_num = 0  # 当前轮次
    total_success = 0
    total_fail = 0

    try:
        while True:
            round_num += 1

            # 如果设置了最大轮次，检查是否超过
            if max_rounds and round_num > max_rounds:
                print(f"已完成 {max_rounds} 轮抓取，程序退出")
                break

            # 计算当前应该抓取的页码
            # 第1轮：机器1抓page=1, 机器2抓page=2, ..., 机器10抓page=10
            # 第2轮：机器1抓page=11, 机器2抓page=12, ..., 机器10抓page=20
            current_page = machine_id + (round_num - 1) * Config.TOTAL_MACHINES

            print("\n" + "=" * 60)
            print(f"第 {round_num} 轮 - 机器 {machine_id} - 抓取第 {current_page} 页")
            print("=" * 60)

            records = fetch_blogger_list(session, current_page)

            if not records:
                print("未获取到博主列表，程序退出")
                break

            # 如果返回的记录数为0，说明已经没有更多数据
            if len(records) == 0:
                print("没有更多数据了，程序退出")
                break

            success_count = 0
            fail_count = 0

            for index, record in enumerate(records, 1):
                print(f"进度: {index}/{len(records)}")

                if process_blogger(record, session):
                    success_count += 1
                    total_success += 1
                else:
                    fail_count += 1
                    total_fail += 1

            print(f"本轮完成！成功: {success_count}, 失败: {fail_count}")
            print(f"总计：成功: {total_success}, 失败: {total_fail}")

        print("=" * 60)
        print(f"所有任务完成！总计: 成功 {total_success}, 失败 {total_fail}")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        print(f"当前统计：成功 {total_success}, 失败 {total_fail}")
    except Exception as e:
        print(f"程序执行异常: {e}")
    finally:
        session.close()
        print("程序结束")


if __name__ == '__main__':
    main()
