"""
获取公司博主报价等信息
"""

import os
import sys
import time
import json
import re
import schedule
import configparser
from datetime import datetime

import requests
import urllib3
from loguru import logger

from service.pgy_service import get_mcn_detail

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
    获取公司博主的信息
"""


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
        get_resource_path('get_onth_kol_config.ini'),
        'command/pgy/get_onth_kol_config.ini',
        'get_onth_kol_config.ini',
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"成功加载配置文件: {config_path}")
            break

    if not config_loaded:
        logger.error("未找到配置文件 get_onth_kol_config.ini")
        raise FileNotFoundError("配置文件不存在")

    # 解析配置
    return {
        'cookie': config.get('PGY_LOGIN', 'cookie', raw=True),
        'enable_scheduler': config.getboolean('SCHEDULER', 'enable_scheduler', fallback=False),
        'daily_time': config.get('SCHEDULER', 'daily_time', fallback='08:30'),
        'check_interval': config.getint('SCHEDULER', 'check_interval', fallback=60),
    }


# 全局配置变量（将在main函数中初始化）
CONFIG = None

FETCH_NUM_RANGE = range(2, 42)
SKIPPED_FETCH_NUMS = set(range(6, 18))
MCN_FETCH_MAX_RETRIES = 3
MCN_FETCH_RETRY_DELAY_SECONDS = 5
SAVE_MAX_RETRIES = 3
SAVE_RETRY_DELAY_SECONDS = 5
SAVE_REQUEST_TIMEOUT = (10, 120)


def _load_json_response(response, context):
    """解析接口响应，遇到非 JSON 时给出可读错误。"""
    try:
        return response.json()
    except ValueError as exc:
        preview = response.text[:200].replace("\n", " ")
        raise RuntimeError(f"{context} 返回非 JSON 响应: {preview}") from exc


def _validate_api_payload(payload, context):
    """校验常见接口返回结构，避免 HTTP 200 但业务失败被误判成功。"""
    if not isinstance(payload, dict):
        raise RuntimeError(f"{context} 返回类型异常: {type(payload).__name__}")

    code = payload.get("code")
    success = payload.get("success")

    if success is False:
        raise RuntimeError(f"{context} 业务失败: code={code}, msg={payload.get('msg') or payload.get('message')}")

    if code is not None and str(code) not in {"0", "200"}:
        raise RuntimeError(f"{context} 业务失败: code={code}, msg={payload.get('msg') or payload.get('message')}")

    return payload


def fetch_mcn_creators_with_retry(mcn_id, header, max_retries=MCN_FETCH_MAX_RETRIES, retry_delay=MCN_FETCH_RETRY_DELAY_SECONDS):
    """抓取单个 MCN 的博主数据，仅对异常做有限重试。"""
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            kols = get_mcn_detail(mcn_id, header)
            if attempt > 1:
                logger.info(f"MCN {mcn_id} 在第 {attempt} 次尝试后抓取成功，返回 {len(kols)} 个博主")
            return kols
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                logger.warning(
                    f"MCN {mcn_id} 第 {attempt}/{max_retries} 次抓取失败: {str(exc)}，"
                    f"{retry_delay} 秒后重试"
                )
                time.sleep(retry_delay)
                continue

            raise RuntimeError(
                f"MCN {mcn_id} 连续 {max_retries} 次抓取失败，最后一次错误: {str(last_error)}"
            ) from last_error

def save_creator_data(data_to_save, max_retries=SAVE_MAX_RETRIES, retry_delay=SAVE_RETRY_DELAY_SECONDS):
    """保存创作者数据到后端接口"""
    save_url = "https://tianji.fangpian999.com/api/admin/creator/CreatorOut/saveData"
    headers = {"Content-Type": "application/json"}
    creator_mcn = data_to_save.get('creator_mcn')
    total_count = len(data_to_save.get('raw_data', []))

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                save_url,
                headers=headers,
                json=data_to_save,
                timeout=SAVE_REQUEST_TIMEOUT,
                verify=False
            )
            response.raise_for_status()
            payload = _load_json_response(response, f"保存 creator_mcn={creator_mcn} 数据接口")
            _validate_api_payload(payload, f"保存 creator_mcn={creator_mcn} 数据接口")
            logger.info(f"数据保存成功: creator_mcn={creator_mcn}, 共 {total_count} 条数据")
            return True
        except Exception as e:
            logger.error(f"数据保存失败: creator_mcn={creator_mcn}, 第 {attempt}/{max_retries} 次尝试, 错误: {str(e)}")
            if attempt < max_retries:
                time.sleep(retry_delay)

    return False


def cli_main():
    """CLI entry with single-run support for debugging."""
    global CONFIG
    CONFIG = load_config()

    run_once = '--once' in sys.argv
    enable_scheduler = CONFIG.get('enable_scheduler', False)

    logger.info("=" * 70)
    logger.info("program start")
    logger.info(f"start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    if run_once or not enable_scheduler:
        logger.info("single run mode")
        return run_spider_task()

    logger.info("scheduler mode")
    logger.info(f"daily time: {CONFIG['daily_time']}")
    logger.info(f"check interval: {CONFIG['check_interval']}s")
    logger.info("=" * 70)

    schedule.every().day.at(CONFIG['daily_time']).do(run_spider_task)
    logger.info(f"scheduled at {CONFIG['daily_time']}")
    logger.info("scheduler running, press Ctrl+C to stop")

    while True:
        schedule.run_pending()
        time.sleep(CONFIG['check_interval'])


def extract_creator_info(kol_data):
    """提取创作者关键信息"""
    return {
        "userId": kol_data.get("userId"),
        "name": kol_data.get("name"),
        "location": kol_data.get("location"),
        "fansCount": kol_data.get("fansCount"),
        "likeCollectCountInfo": kol_data.get("likeCollectCountInfo"),
        "picturePrice": kol_data.get("picturePrice"),
        "videoPrice": kol_data.get("videoPrice"),
        "contentTags": kol_data.get("contentTags", []),
        "headPhoto": kol_data.get("headPhoto"),
        "redId": kol_data.get("redId"),
        "personalTags": kol_data.get("personalTags", []),
        "businessNoteCount": kol_data.get("businessNoteCount"),
        "lowerPrice": kol_data.get("lowerPrice"),
        "gender": kol_data.get("gender"),
        "featureTags": kol_data.get("featureTags", []),
    }


def run_spider_task():
    """执行爬虫任务 - 单次执行版本"""
    # 加载配置
    global CONFIG
    if CONFIG is None:
        CONFIG = load_config()

    # 构建请求头
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        "cookie": CONFIG['cookie'],
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://pgy.xiaohongshu.com/',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://pgy.xiaohongshu.com',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Ch-Ua': '"Chromium";v="120", "Google Chrome";v="120", "Not_A Brand";v="99"',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Ch-Ua-Mobile': '?0',
    }

    headers = {"Content-Type": "application/json"}
    base_url = "https://tianji.fangpian999.com/api/admin/creatorOut/getSpiderMcn"

    # 确保output目录存在
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"创建输出目录: {output_dir}")

    task_success = True
    processed_fetch_nums = 0
    failed_fetch_nums = []
    total_creators_collected = 0
    total_failed_mcn_count = 0
    total_saved_batches = 0

    # 外层循环: 每个 fetch_num 单独抓取、单独保存
    for fetch_num in FETCH_NUM_RANGE:
        if fetch_num in SKIPPED_FETCH_NUMS:
            continue
        processed_fetch_nums += 1
        fetch_had_error = False
        logger.info(f"\n{'='*50}")
        logger.info(f"开始处理 is_fetch_creator={fetch_num}")
        logger.info(f"{'='*50}")

        api_url = base_url

        try:
            # 获取MCN列表
            response = requests.get(
                api_url,
                params={"is_fetch_creator": fetch_num},
                headers=headers,
                timeout=30,
                verify=False
            )
            response.raise_for_status()
            payload = _load_json_response(response, f"获取 is_fetch_creator={fetch_num} 的 MCN 列表接口")
            if isinstance(payload, dict):
                _validate_api_payload(payload, f"获取 is_fetch_creator={fetch_num} 的 MCN 列表接口")
                creator_data = payload.get('data', [])
            else:
                creator_data = payload

            if creator_data is None:
                creator_data = []
            if not isinstance(creator_data, list):
                raise RuntimeError(f"is_fetch_creator={fetch_num} 的 MCN 列表不是数组: {type(creator_data).__name__}")
            logger.info(f"获取到 {len(creator_data)} 个MCN")

            # 用于收集当前fetch_num的所有创作者数据
            all_creators = []
            total_success = 0
            failed_mcn_ids = []

            # 遍历每个MCN
            for index, item in enumerate(creator_data, start=1):
                mcn_id = None
                try:
                    if not isinstance(item, dict):
                        raise ValueError(f"第 {index} 条 MCN 数据类型异常: {type(item).__name__}")

                    mcn_id = item.get('mcn_user_id')
                    if not mcn_id:
                        raise ValueError(f"第 {index} 条 MCN 数据缺少 mcn_user_id: {item}")

                    logger.info(f"开始获取MCN {mcn_id} 的数据...")

                    # 调用get_mcn_detail获取创作者列表
                    kols = fetch_mcn_creators_with_retry(mcn_id, header)

                    if kols:
                        # 提取并添加到all_creators数组
                        for kol in kols:
                            creator_info = extract_creator_info(kol)
                            all_creators.append(creator_info)

                        success_count = len(kols)
                        total_success += success_count
                        logger.info(f"MCN {mcn_id} 返回了 {len(kols)} 个博主数据")
                    else:
                        logger.warning(f"MCN {mcn_id} 没有返回数据")

                    time.sleep(6)  # 避免请求过快

                except Exception as e:
                    fetch_had_error = True
                    task_success = False
                    total_failed_mcn_count += 1
                    failed_mcn_ids.append(mcn_id or f"unknown_index_{index}")
                    logger.error(f"获取MCN {mcn_id or 'UNKNOWN'} 数据失败: {str(e)}")
                    time.sleep(3)  # 出错后稍微等待一下再继续

            # 组装数据并保存
            data_to_save = {
                "creator_mcn": str(fetch_num),
                "platform_id": 1,
                "raw_data": all_creators
            }

            logger.info(f"\nis_fetch_creator={fetch_num} 数据收集完成:")
            logger.info(f"- 成功处理 {total_success} 个博主")
            logger.info(f"- 失败 {len(failed_mcn_ids)} 个MCN")
            logger.info(f"- 总计收集 {len(all_creators)} 条创作者数据")
            if failed_mcn_ids:
                logger.warning(f"- 失败的MCN列表: {failed_mcn_ids}")

            # 调用保存接口
            if len(all_creators) > 0:
                logger.info(f"开始保存数据到后端...")
                if save_creator_data(data_to_save):
                    total_saved_batches += 1
                else:
                    fetch_had_error = True
                    task_success = False
                    logger.error(f"is_fetch_creator={fetch_num} 保存失败，已保留本地备份文件")
            else:
                logger.warning(f"没有数据需要保存")

            # 保存到本地JSON文件作为备份
            try:
                json_filename = f"output/mcn_data_fetch{fetch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(data_to_save, f, ensure_ascii=False, indent=2)
                logger.info(f"数据已备份到JSON文件: {json_filename}")
            except Exception as json_error:
                fetch_had_error = True
                task_success = False
                logger.error(f"保存JSON文件时出错: {str(json_error)}")

            total_creators_collected += len(all_creators)
            if fetch_had_error:
                failed_fetch_nums.append(fetch_num)

            logger.info(f"is_fetch_creator={fetch_num} 处理完成!\n")
            time.sleep(3)  # 每个fetch_num处理完后休息一下

        except Exception as e:
            task_success = False
            failed_fetch_nums.append(fetch_num)
            logger.error(f"处理 is_fetch_creator={fetch_num} 时出错: {str(e)}")
            continue

    logger.info(f"\n{'='*50}")
    logger.info("所有数据处理完成!")
    logger.info(f"共处理 {processed_fetch_nums} 个 fetch_num, 成功保存 {total_saved_batches} 批, 收集 {total_creators_collected} 条创作者数据, 失败 {total_failed_mcn_count} 个MCN")
    if failed_fetch_nums:
        logger.warning(f"存在异常的 fetch_num: {failed_fetch_nums}")
    logger.info(f"{'='*50}")
    return task_success


def main():
    """主函数"""
    try:
        # 加载配置
        global CONFIG
        CONFIG = load_config()

        logger.info("=" * 70)
        logger.info("🚀 获取公司博主信息程序")
        logger.info(f"📅 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        # 调度器模式
        logger.info("🔄 调度器模式")
        logger.info(f"⏰ 执行时间: 每天 {CONFIG['daily_time']}")
        logger.info(f"🔍 检查间隔: {CONFIG['check_interval']}秒")
        logger.info("=" * 70)

        # 设置定时任务
        schedule.every().day.at(CONFIG['daily_time']).do(run_spider_task)
        logger.info(f"✅ 已设置定时任务: 每天 {CONFIG['daily_time']}")
        logger.info("🔄 调度器运行中，按 Ctrl+C 停止...")

        # 运行调度器
        while True:
            schedule.run_pending()
            time.sleep(CONFIG['check_interval'])

    except KeyboardInterrupt:
        logger.warning("⚠️ 用户手动中断程序")
        return True
    except Exception as e:
        logger.error(f"❌ 程序启动失败: {str(e)}")
        return False


if __name__ == "__main__":
    try:
        success = cli_main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"程序异常退出: {str(e)}")
        sys.exit(1)
