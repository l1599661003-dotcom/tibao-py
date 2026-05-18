# -*- coding: utf-8 -*-
"""
抖音MCN详情数据抓取程序（简化版）
从数据库获取MCN列表，抓取每个MCN的博主数据并推送到接口
"""

import os
import sys
import time
import json
import schedule
import configparser
from datetime import datetime

from requests import RequestException

from core.localhost_fp_project import Session  # 使用Session工厂而不是全局session
from models.models import DouyinMcn

import requests
import urllib3
from loguru import logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 固定配置
BASE_URL = "https://www.xingtu.cn/gw/api/mcn/get_mcn_unsigned_author_list"
SAVE_DATA_URL = "https://tianji.fangpian999.com/api/admin/creatorSign/updateDouyinTerminationData"
# SAVE_DATA_URL = "http://localhost:5666/api/admin/creatorSign/updateDouyinTerminationData"
PLATFORM_ID = 2  # 抖音平台ID
PAGE_SIZE = 100
REQUEST_DELAY = 10  # 请求延迟(秒)


def get_resource_path(relative_path):
    """获取资源文件路径，支持exe打包"""
    try:
        # PyInstaller创建临时文件夹并将路径存储在_MEIPASS中
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        get_resource_path('douyin_waibu_config.ini'),
        'command/douyin/douyin_waibu_config.ini',
        'douyin_waibu_config.ini',
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"成功加载配置文件: {config_path}")
            break

    if not config_loaded:
        logger.error("未找到配置文件 douyin_waibu_config.ini")
        raise FileNotFoundError("配置文件不存在")

    # 解析配置
    return {
        'cookie': config.get('DOUYIN_LOGIN', 'cookie', raw=True),
        'daily_time': config.get('SCHEDULER', 'daily_time', fallback='08:30'),
        'check_interval': config.getint('SCHEDULER', 'check_interval', fallback=60),
    }

# 全局配置变量（将在main函数中初始化）
CONFIG = None


def save_creator_data(termination_data):
    """保存解约博主数据到后端接口"""
    headers = {"Content-Type": "application/json"}

    # 组装新接口需要的数据格式
    payload = {
        "termination_data": termination_data
    }

    try:
        print(payload)
        response = requests.post(SAVE_DATA_URL, headers=headers, json=payload, timeout=3000, verify=False)
        if response.status_code == 200:
            result = response.json()
            # 检查业务状态码
            if result.get('code') == 200:
                logger.info(f"数据保存成功: 共 {len(termination_data)} 条解约记录")
                logger.info(f"接口返回: total={result.get('data', {}).get('total')}, success={result.get('data', {}).get('success')}")
                return True
            else:
                logger.error(f"业务处理失败: {result.get('msg', '未知错误')}")
                return False
        else:
            logger.error(f"数据保存失败: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"保存数据时出错: {str(e)}")
        return False


def extract_creator_info(author_data):
    """提取解约博主关键信息,用于updateDouyinTerminationData接口"""
    return {
        "star_id": str(author_data.get('star_id', '')),  # 博主ID (对应author_id)
        "unsigned_time": author_data.get('unsigned_time', 0),  # 解约时间戳
    }


def fetch_mcn_authors(headers):
    """获取指定MCN的所有博主数据"""
    all_authors = []
    page = 1
    max_pages = 50
    consecutive_empty_pages = 0
    MAX_RETRY = 3  # 最大重试次数
    RETRY_DELAY = 6

    while page <= max_pages:
        try:
            url = f"{BASE_URL}?page={page}&limit={PAGE_SIZE}"
            logger.info(f"请求第 {page} 页: {url}")
            response = ''

            success = False
            for attempt in range(1, MAX_RETRY + 1):
                try:
                    response = requests.get(url, headers=headers, verify=False, timeout=30)

                    if response.status_code == 200:
                        success = True
                        break
                    else:
                        logger.warning(f"请求失败，状态码: {response.status_code}，第 {attempt}/{MAX_RETRY} 次重试中...")
                        time.sleep(RETRY_DELAY)

                except RequestException as e:
                    logger.warning(f"请求异常: {e}，第 {attempt}/{MAX_RETRY} 次重试中...")
                    time.sleep(RETRY_DELAY)

            if not success:
                logger.error(f"第 {page} 页请求失败，已重试 {MAX_RETRY} 次，跳过该页。")
                continue

            data = response.json()

            # 检查响应格式
            if 'base_resp' not in data or data['base_resp'].get('status_code') != 0:
                logger.error(f"API响应异常: {data}")
                break

            # 获取作者列表
            authors = data.get('unsigned_author_items', [])
            pagination = data.get('pagination', {})

            logger.info(f"第 {page} 页获取到 {len(authors)} 条博主数据")
            time.sleep(RETRY_DELAY)

            if len(authors) == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    logger.info(f"连续{consecutive_empty_pages}页无数据，停止抓取")
                    break
            else:
                consecutive_empty_pages = 0
                # 提取博主信息
                for author in authors:
                    creator_info = extract_creator_info(author)
                    all_authors.append(creator_info)

            # 检查是否还有更多页
            has_more = pagination.get('has_more', False)
            if not has_more and len(authors) < PAGE_SIZE:
                logger.info(f"已到最后一页，停止抓取")
                break

            page += 1
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            logger.error(f"抓取第 {page} 页时出错: {str(e)}")
            break

    logger.info(f"抓取完成，共 {len(all_authors)} 个博主")
    return all_authors


def get_mcn_list():
    """获取MCN列表 - 使用新session确保连接有效"""
    db_session = None
    try:
        # 创建新的session，确保连接是新鲜的
        db_session = Session()
        mcn_list = db_session.query(DouyinMcn).filter(DouyinMcn.status > 5).all()
        logger.info(f"获取到 {len(mcn_list)} 个MCN")
        return mcn_list
    except Exception as e:
        logger.error(f"获取MCN列表时出错: {str(e)}")
        if db_session:
            db_session.rollback()  # 出错时回滚
        return []
    finally:
        # 确保session被正确关闭
        if db_session:
            db_session.close()
            logger.debug("数据库session已关闭")

def run_spider_task():
    """执行爬虫任务"""
    try:
        start_time = datetime.now()
        logger.info("=" * 70)
        logger.info("🚀 抖音MCN博主数据抓取程序启动")
        logger.info(f"⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        # 加载配置
        global CONFIG
        if CONFIG is None:
            CONFIG = load_config()

        # 构建请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
            'Cookie': 'is_staff_user=false; tt_webid=7552842709368161835; ttcid=53fc342d42aa43518a76e246c694022428; tt_scid=zJHLD5xO4kxK.yYsfI4iT4Yn-oxddiLcqcC-Uqy0xxEznc9bSc6su38iOqjw7Cd31801; use_biz_token=true; has_biz_token=false; passport_csrf_token=c6ee59bd8dc707c5e14effb44c092311; passport_csrf_token_default=c6ee59bd8dc707c5e14effb44c092311; s_v_web_id=verify_motxgyfh_VOZVzvEp_nRbB_4E7o_AHAF_wNCqDplPiEwm; passport_auth_status=3ef84d8c822bd1785dd01aaefbc85ed5%2C744c1f6120187741c6edebb9ac2a5931; passport_auth_status_ss=3ef84d8c822bd1785dd01aaefbc85ed5%2C744c1f6120187741c6edebb9ac2a5931; uid_tt=bc4cbb70f2c08ad4e42bee3756b402fa; uid_tt_ss=bc4cbb70f2c08ad4e42bee3756b402fa; sid_tt=a45a0456888b471c99396d46ee771fed; sessionid=a45a0456888b471c99396d46ee771fed; sessionid_ss=a45a0456888b471c99396d46ee771fed; Hm_lvt_5d77c979053345c4bd8db63329f818ec=1778064101,1778068791,1778117141,1778563089; Hm_lpvt_5d77c979053345c4bd8db63329f818ec=1778563089; HMACCOUNT=A9193F3F989E70E1; sid_guard=a45a0456888b471c99396d46ee771fed%7C1778563142%7C5184000%7CSat%2C+11-Jul-2026+05%3A19%3A02+GMT; session_tlb_tag=sttt%7C17%7CpFoEVoiLRxyZOW1G7ncf7f________-xuTprnmRTfvTQcuJm4epY2JRwVcfWjVb6wo-US2ot1TQ%3D; sid_ucp_v1=1.0.0-KDBkN2RmYjUwZThhM2NlYjc4OTQ5OTkzNDNiMWI3MGZhMGIyMWY4MjUKFwisvfDbmq2EBhDG8IrQBhimDDgCQPEHGgJscSIgYTQ1YTA0NTY4ODhiNDcxYzk5Mzk2ZDQ2ZWU3NzFmZWQ; ssid_ucp_v1=1.0.0-KDBkN2RmYjUwZThhM2NlYjc4OTQ5OTkzNDNiMWI3MGZhMGIyMWY4MjUKFwisvfDbmq2EBhDG8IrQBhimDDgCQPEHGgJscSIgYTQ1YTA0NTY4ODhiNDcxYzk5Mzk2ZDQ2ZWU3NzFmZWQ; star_sessionid=a45a0456888b471c99396d46ee771fed; possess_scene_star_id=1840219488151812; star_reformat_gray=true; csrf_session_id=d52181b5e482e4542f121a598c4a82d0'
        }

        # 抓取该MCN的所有博主数据
        authors = fetch_mcn_authors(headers)

        if authors and len(authors) > 0:
            # 直接使用解约数据列表
            logger.info(f"开始推送 {len(authors)} 条解约数据到后端...")
            logger.info(f"解约数据示例: {authors[0] if authors else 'None'}")

            # 调用保存接口
            save_creator_data(authors)

        logger.info(f"\n数据收集完成:")

        # 输出统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 70)
        logger.info("✅ 所有数据处理完成!")
        logger.info(f"⏱️  执行时长: {duration:.2f} 秒")
        logger.info(f"🏁 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        return True

    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
        return False
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


def main():
    """主函数"""
    try:
        # 加载配置
        global CONFIG
        CONFIG = load_config()

        logger.info("=" * 70)
        logger.info("🚀 抖音MCN博主数据抓取程序")
        logger.info(f"📅 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        # 调度器模式
        logger.info("🔄 调度器模式")
        logger.info(f"⏰ 执行时间: 每天 {CONFIG['daily_time']}")
        logger.info(f"🔍 检查间隔: {CONFIG['check_interval']}秒")
        logger.info("=" * 70)
        run_spider_task()

        # 设置定时任务
        # schedule.every().day.at(CONFIG['daily_time']).do(run_spider_task)
        # logger.info(f"✅ 已设置定时任务: 每天 {CONFIG['daily_time']}")
        # logger.info("\n🔄 调度器运行中，按 Ctrl+C 停止...\n")

        # 运行调度器
        # while True:
        #     schedule.run_pending()
        #     time.sleep(CONFIG['check_interval'])

    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户手动中断程序")
        return True
    except Exception as e:
        logger.error(f"❌ 程序启动失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"程序异常退出: {str(e)}")
        sys.exit(1)
