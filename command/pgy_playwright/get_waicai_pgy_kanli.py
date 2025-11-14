import json
import os
import sys
import configparser
import time
from datetime import datetime

import requests
from loguru import logger
import traceback

from service.pgy_service import get_blogger_info, get_fans_profile, get_data_summary, get_notes_rate, get_notes_detail
from unitl.common import Common
from requests.exceptions import RequestException

"""
    更新外采博主账号信息, 博主变现，粉丝情况，从蒲公英抓取数据
    重构版本：基于Playwright模拟浏览器操作，无需token
"""


def get_resource_path(relative_path):
    """获取资源文件路径，支持exe打包"""
    try:
        # PyInstaller创建临时文件夹并将路径存储在_MEIPASS中
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath("../../WeekAccountUpdate")
    return os.path.join(base_path, relative_path)


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        get_resource_path('WeekAccountUpdate/config.ini'),
        get_resource_path('config.ini'),
        'WeekAccountUpdate/config.ini',
        'command/pgy_playwright/config.ini',
        'config.ini'
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"成功加载配置文件: {config_path}")
            break

    if not config_loaded:
        logger.error("未找到配置文件")
        raise FileNotFoundError("配置文件不存在")

    # 解析配置
    return {
        'PGY_LOGIN_CONFIG': {
            'page': config.get('PGY_LOGIN', 'page', fallback='1'),
            'pageSize': config.get('PGY_LOGIN', 'pageSize', fallback='10'),
            'cookic': config.get('PGY_LOGIN', 'cookic')
        }
    }


class WaicaiPGYSpider:
    """外采博主数据采集类"""

    def __init__(self):
        """初始化爬虫"""
        # 先设置日志
        self.setup_logger()

        # 加载配置
        self.config = load_config()

        # 设置数据目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，使用exe文件所在目录（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            self.data_dir = os.path.join(exe_dir, 'data')
        else:
            # 开发环境下，使用当前文件同级目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_dir = os.path.join(current_dir, 'data')

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        self.common = Common()

        # 初始化payload结构
        self.payload = {
            "apis": [
                {"tb_name": "blogger_info", "tb_data": []},
                {"tb_name": "blogger_note_rate", "tb_data": []},
                {"tb_name": "blogger_data_summary", "tb_data": []},
                {"tb_name": "blogger_note_detail", "tb_data": []},
                {"tb_name": "blogger_fans_summary", "tb_data": []},
                {"tb_name": "blogger_fans_profile", "tb_data": []},
                {"tb_name": "blogger_fans_history", "tb_data": []},
            ],
            "client_id": 1
        }

        logger.info("外采博主数据采集器初始化完成")

    def setup_logger(self):
        """配置日志系统"""
        # 设置日志目录
        if hasattr(sys, '_MEIPASS'):
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            log_dir = os.path.join(exe_dir, 'logs')
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(current_dir, 'logs')

        os.makedirs(log_dir, exist_ok=True)

        # 配置loguru
        logger.remove()  # 移除默认处理器

        # 添加控制台输出
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            level="INFO"
        )

        # 添加文件输出
        log_file = os.path.join(log_dir, f"waicai_pgy_{datetime.now().strftime('%Y-%m-%d')}.log")
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="DEBUG",
            rotation="00:00",  # 每天午夜切换日志文件
            retention="30 days",  # 保留30天的日志
            encoding="utf-8"
        )

        logger.info("日志系统配置完成")

    def scrape_user_notes(self):
        """抓取博主信息 - 重构版本，匹配PHP逻辑"""
        try:
            # 查询需要更新的博主数据 - 匹配PHP查询逻辑
            api_url = f"https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator?type=3&page={self.config['PGY_LOGIN_CONFIG']['page']}&pageSize={self.config['PGY_LOGIN_CONFIG']['pageSize']}"

            headers = {"Content-Type": "application/json"}
            logger.info(f"正在请求API: {api_url}")

            response = requests.post(api_url, headers=headers, timeout=30)
            response_data = response.json()['data']

            logger.info(f"获取到 {len(response_data) if isinstance(response_data, list) else 0} 个博主数据")

            # 检查响应数据格式
            if not isinstance(response_data, list):
                logger.error(f"API返回数据格式错误: {response_data}")
                return

            # 如果需要调试，可以在这里查看返回的数据
            if len(response_data) > 0:
                logger.debug(f"第一个博主数据示例: {response_data[0]}")

            for idx, url in enumerate(response_data, 1):
                # 清空payload数据，准备处理下一个博主
                self.payload = {
                    "apis": [
                        {"tb_name": "blogger_info", "tb_data": []},
                        {"tb_name": "blogger_note_rate", "tb_data": []},
                        {"tb_name": "blogger_data_summary", "tb_data": []},
                        {"tb_name": "blogger_note_detail", "tb_data": []},
                        {"tb_name": "blogger_fans_summary", "tb_data": []},
                        {"tb_name": "blogger_fans_profile", "tb_data": []},
                        {"tb_name": "blogger_fans_history", "tb_data": []},
                    ],
                    "client_id": 1
                }

                logger.info(f"[{idx}/{len(response_data)}] 正在处理博主: {url.get('creator_nickname', 'Unknown')}")

                try:
                    self._process_blogger(url)

                    # 调用同步接口
                    sync_result = self.sync_single_record_to_api(self.payload)
                    if sync_result:
                        logger.info(f"✓ 成功同步博主 {url.get('creator_nickname', 'Unknown')} 的数据到API")
                    else:
                        logger.warning(f"✗ 同步博主 {url.get('creator_nickname', 'Unknown')} 的数据到API失败")
                    time.sleep(6)

                except Exception as blogger_error:
                    logger.error(f"处理博主 {url.get('creator_nickname', 'Unknown')} 时出错: {str(blogger_error)}")
                    logger.error(f"错误详情: {traceback.format_exc()}")
                    # 继续处理下一个博主，不中断整个流程
                    continue

            logger.info("本轮数据处理完成")

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise  # 重新抛出异常，让上层处理重启逻辑

    def _process_blogger(self, url):
        """处理博主基本信息"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                "cookie": self.config['PGY_LOGIN_CONFIG']['cookic'],
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Encoding': 'gzip, deflate, br',
            }
            data = get_blogger_info(url['platform_user_id'], headers)
            logger.info(f"基本信息: {data}")
            # 将数据添加到payload中
            blogger_info_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_info"), None)
            if blogger_info_index is not None:
                # 克隆数据并添加博主ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
                self.payload["apis"][blogger_info_index]["tb_data"] = [payload_data]
        except Exception as e:
            logger.error(f"获取博主信息失败: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def sync_single_record_to_api(self, payload):
        """同步单条记录到API"""
        try:
            url = "http://47.104.76.46:19000/api/v1/sync/spider/data"
            headers = {"Content-Type": "application/json"}

            try:
                # 发送payload数据
                response = requests.post(url, json=payload, headers=headers, timeout=30)
            except RequestException as sync_error:
                logger.error(f"单条数据同步请求失败: {str(sync_error)}")
                return False

            if response.status_code == 200:
                try:
                    response_data = response.json()  # 尝试解析 JSON 内容
                    if response_data.get('status') == 'success':  # 假设接口返回的 JSON 有 status 字段
                        logger.debug(f"同步成功: {response_data}")
                        return True
                    else:
                        logger.warning(f"同步失败，API返回错误: {response_data}")
                        return False
                except ValueError:
                    logger.error(f"同步请求返回非JSON响应，无法解析: {response.text}")
                    return False
            else:
                logger.warning(f"同步请求失败，HTTP 状态码: {response.status_code}, 响应: {response.text}")
                return False
        except Exception as e:
            logger.warning(f"单条数据同步异常: {str(e)}")
            logger.warning(f"错误详情: {traceback.format_exc()}")
            return False

    def run(self):
        """运行爬虫主程序"""
        try:
            logger.info("=" * 50)
            logger.info("外采博主数据采集程序启动")
            logger.info("=" * 50)

            self.scrape_user_notes()

            logger.info("=" * 50)
            logger.info("程序执行完成")
            logger.info("=" * 50)
        except KeyboardInterrupt:
            logger.info("用户中断程序")
        except Exception as e:
            logger.error(f"程序异常: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise


def main():
    """主函数"""
    try:
        spider = WaicaiPGYSpider()
        spider.run()
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
