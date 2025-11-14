import json
import os
import sys
import configparser
import time
from datetime import datetime
import cv2
import requests
import schedule
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback

from core.database_text_tibao_2 import session
from models.models_tibao import KolMediaAccountsConfig
from unitl.common import Common

"""
    更新外采博主账号信息,博主变现，粉丝情况,从蒲公英抓取数据
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
        'config.ini'
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            break

    if not config_loaded:
        logger.error("未找到配置文件")
        raise FileNotFoundError("配置文件不存在")

    # 解析配置
    return {
        'PGY_LOGIN_CONFIG': {
            'id': config.get('PGY_LOGIN', 'id')
        },
        'API_TARGETS': config.get('API_TARGETS', 'targets').split(','),
        'SCHEDULER_CONFIG': {
            'enable_scheduler': config.getboolean('SCHEDULER', 'enable_scheduler'),
            'daily_time': config.get('SCHEDULER', 'daily_time'),
            'run_once': config.getboolean('SCHEDULER', 'run_once'),
            'check_interval': config.getint('SCHEDULER', 'check_interval')
        }
    }


class PGYSpider:
    def __init__(self):
        # 加载配置
        self.config = load_config()
        self.setup_logger()

        # 设置cookie和数据目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，使用exe文件所在目录（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            self.cookie_file = os.path.join(exe_dir, 'cookies.json')
            self.data_dir = os.path.join(exe_dir, 'data')
        else:
            # 开发环境下，使用当前文件同级目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.cookie_file = os.path.join(current_dir, 'cookies.json')
            self.data_dir = os.path.join(current_dir, 'data')

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = 'https://pgy.xiaohongshu.com'
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.progress_file = os.path.join(self.data_dir, 'scraping_progress.json')

        self.common = Common()

        self.setup_browser()
        self.notes = []
        self.stopScroll = False
        self.monitor_data = {
            'fail_count': 0,
            'total_count': 0,
            'completed_count': 0,
            'process': 0
        }
        # 存储比对结果
        self.comparison_results = []
        # 获取配置信息并立即提取到普通变量中，避免数据库会话问题
        try:
            config = session.query(KolMediaAccountsConfig).filter(
                KolMediaAccountsConfig.id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).first()

            if config:
                # 立即提取所有需要的配置信息到普通变量中
                self.config_email = config.email
                self.config_password = config.password
                self.config_name = config.name
                self.config_client_id = config.client_id
            else:
                logger.error(f"未找到ID为{self.config['PGY_LOGIN_CONFIG']['id']}的配置信息")
                raise ValueError(f"配置信息不存在: ID={self.config['PGY_LOGIN_CONFIG']['id']}")

        except Exception as e:
            logger.error(f"加载配置信息失败: {str(e)}")
            raise
        # 初始化空的payload结构
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
            "client_id": self.config_client_id
        }

    def setup_logger(self):
        """设置日志配置，支持exe打包"""
        # 设置日志目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，在exe文件所在目录创建logs文件夹（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            log_path = os.path.join(exe_dir, 'logs')
        else:
            # 开发环境下，在WeekAccountUpdate同级目录创建logs文件夹
            current_dir = os.path.dirname(os.path.abspath(__file__))
            log_path = os.path.join(current_dir, 'logs')

        # 确保logs目录存在
        os.makedirs(log_path, exist_ok=True)

        # 移除默认处理器，避免重复输出
        logger.remove()

        # 添加控制台输出
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="INFO"
        )

        # 添加文件输出
        logger.add(
            os.path.join(log_path, "pgy_{time:YYYY-MM-DD}.log"),
            rotation="1 day",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            encoding="utf-8"
        )

    def setup_browser(self):
        """初始化浏览器"""
        # 设置playwright浏览器路径，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，使用exe文件所在目录的ms-playwright（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            playwright_browsers_path = os.path.join(exe_dir, 'ms-playwright')
        else:
            # 开发环境下，使用当前目录同级的ms-playwright
            current_dir = os.path.dirname(os.path.abspath(__file__))
            playwright_browsers_path = os.path.join(current_dir, 'ms-playwright')

        # 设置环境变量
        if os.path.exists(playwright_browsers_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = playwright_browsers_path
            logger.info(f"使用自定义浏览器路径: {playwright_browsers_path}")
        else:
            logger.warning(f"未找到自定义浏览器路径: {playwright_browsers_path}")

        self.playwright = sync_playwright().start()
        # 配置浏览器选项
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )
        # 创建上下文
        self.context = self.browser.new_context(
            viewport={
                'width': 1512,
                'height': 768
            },
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )

        # 尝试加载已保存的Cookie
        if self._load_cookies():
            # 验证Cookie是否有效
            self.page = self.context.new_page()
            try:
                self.page.goto(self.base_url)
                self.common.random_sleep(2, 3)

                # 检查是否存在用户头像元素
                userSide = self.page.locator(".home_head_user_info").all()
                logger.info(f"找到用户头像元素数量: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 0 or self.page.locator(".home_head_user_info").is_visible(timeout=5000):
                    self.is_logged_in = True
                    logger.info("Cookie有效，已自动登录")
                else:
                    logger.info("Cookie已失效，需要重新登录")
                    self.is_logged_in = False
            except Exception as e:
                logger.warning(f"Cookie验证失败: {str(e)}")
                logger.info("将进行重新登录")
                self.is_logged_in = False
        else:
            logger.info("未找到Cookie文件，需要登录")
            self.page = self.context.new_page()
            self.is_logged_in = False

        # 设置页面超时时间
        self.page.set_default_timeout(20000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

    def login(self):
        """
        使用账号密码登录蒲公英
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

            logger.info("开始登录流程...")
            # try:
            #     # 访问首页
            #     self.page.goto(self.base_url)
            #     self.common.random_sleep()
            #
            #     # 等待并点击第一个登录按钮
            #     logger.info("等待第一个登录按钮出现...")
            #     first_login_button = self.page.wait_for_selector("text=账号登录", timeout=10000)
            #     if not first_login_button:
            #         logger.error("未找到第一个登录按钮")
            #         return False
            #     first_login_button.click()
            #     self.common.random_sleep(1, 2)
            #
            #     # 等待并点击弹窗中的账号登录按钮
            #     logger.info("等待弹窗中的账号登录按钮...")
            #     second_login_button = self.page.wait_for_selector("text=账号登录 >> nth=1", timeout=10000)
            #     if not second_login_button:
            #         logger.error("未找到弹窗中的账号登录按钮")
            #         return False
            #     second_login_button.click()
            #     self.common.random_sleep(1, 2)
            #
            #     # 等待邮箱输入框出现并输入邮箱
            #     logger.info("正在输入账号密码...")
            #     email_input = self.page.wait_for_selector("input.css-1dbyz17.css-xno39g.dyn", timeout=5000)
            #     email_input.fill(self.config_email)
            #     self.common.random_sleep(1, 2)  # 模拟人工输入间隔
            #
            #     # 等待5分钟，每10秒检查一次登录状态
            #     max_wait_time = 300  # 5分钟 = 300秒
            #     check_interval = 10  # 每10秒检查一次
            #     elapsed_time = 0
            #
            #     while elapsed_time < max_wait_time:
            #         try:
            #             # 检查是否存在用户头像元素（登录成功的标志）
            #             user_avatar = self.page.locator(".home_head_user_info").first
            #             if user_avatar and user_avatar.is_visible():
            #                 logger.info("检测到登录成功！")
            #                 self.is_logged_in = True
            #
            #                 # 登录成功后保存Cookie
            #                 self._save_cookies()
            #
            #                 return True
            #
            #             time.sleep(check_interval)
            #             elapsed_time += check_interval
            #
            #         except Exception as e:
            #             logger.warning(f"检查登录状态时出错: {str(e)}")
            #             time.sleep(check_interval)
            #             elapsed_time += check_interval
            #
            #     # 5分钟超时，仍未登录成功
            #     logger.error("等待登录超时（5分钟），程序退出")
            #     return False
            #
            # except Exception as e:
            #     logger.error(f"等待登录过程中出现异常: {str(e)}")
            #     return False
            logger.info("开始等待用户手动登录,请在5分钟内完成登录操作，程序将自动检测登录状态")

            try:
                # 访问首页
                self.page.goto(self.base_url)
                self.common.random_sleep(2, 3)

                # 等待5分钟，每10秒检查一次登录状态
                max_wait_time = 300  # 5分钟 = 300秒
                check_interval = 10  # 每10秒检查一次
                elapsed_time = 0

                while elapsed_time < max_wait_time:
                    try:
                        # 检查是否存在用户头像元素（登录成功的标志）
                        user_avatar = self.page.locator(".home_head_user_info").first
                        if user_avatar and user_avatar.is_visible():
                            logger.info("检测到登录成功！")
                            self.is_logged_in = True

                            # 登录成功后保存Cookie
                            self._save_cookies()

                            return True

                        time.sleep(check_interval)
                        elapsed_time += check_interval

                    except Exception as e:
                        logger.warning(f"检查登录状态时出错: {str(e)}")
                        time.sleep(check_interval)
                        elapsed_time += check_interval

                # 5分钟超时，仍未登录成功
                logger.error("等待登录超时（5分钟），程序退出")
                return False

            except Exception as e:
                logger.error(f"等待登录过程中出现异常: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def scrape_user_notes(self):
        """抓取博主信息 - 重构版本，匹配PHP逻辑"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return None

            # 检查页面状态
            if not self.page or self.page.is_closed():
                logger.error("页面已关闭，无法抓取数据")
                # 尝试重新创建页面
                try:
                    self.page = self.context.new_page()
                    logger.info("已重新创建页面")
                except Exception as recreate_error:
                    logger.error(f"重新创建页面失败: {str(recreate_error)}")
                    return None
            config = session.query(KolMediaAccountsConfig).filter(
                KolMediaAccountsConfig.id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).first()

            if not config:
                logger.error(f"未找到ID为{self.config['PGY_LOGIN_CONFIG']['id']}的配置信息")
                raise ValueError(f"配置信息不存在: ID={self.config['PGY_LOGIN_CONFIG']['id']}")

            api_url = f"https://tianji.fangpian999.com/api/admin/creatorBusiness/getRandomCreator"
            headers = {"Content-Type": "application/json"}

            response = requests.post(api_url, headers=headers, timeout=30)
            response_data = response.json()

            # 处理不同的返回格式
            if isinstance(response_data, dict):
                # 如果返回的是字典，尝试从data字段或其他常见字段获取列表
                urls = response_data.get('data', response_data.get('list', response_data.get('result', [])))
            elif isinstance(response_data, list):
                # 如果直接返回列表
                urls = response_data
            else:
                logger.error(f"未知的返回数据格式: {type(response_data)}")
                return

            logger.info(f"找到 {len(urls)} 个博主数据")
            if len(urls) <= 0:
                logger.info("没有需要处理的数据")
                return

            # 清空之前的比对结果
            self.comparison_results = []

            for url in urls:
                if not url.get('platform_user_id') or "aaaaaaaaa" in url.get('platform_user_id', ''):
                    continue

                try:
                    # 检查页面状态
                    if not self.page or self.page.is_closed():
                        logger.error("页面已关闭，无法继续处理博主数据")
                        # 尝试重新创建页面
                        try:
                            self.page = self.context.new_page()
                            logger.info("已重新创建页面")
                        except Exception as recreate_error:
                            logger.error(f"重新创建页面失败: {str(recreate_error)}")
                            break

                    # 清空之前的数据
                    self.api_data.clear()

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
                        "client_id": self.config_client_id
                    }

                    logger.info(f"正在处理博主: {url.get('creator_nickname', '未知')}")

                    # 访问页面
                    page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{url.get('platform_user_id')}"
                    logger.info(f"开始访问页面: {page_url}")

                    try:
                        # 检查页面状态
                        if not self.page or self.page.is_closed():
                            logger.error("页面已关闭，无法继续处理")
                            continue

                        self.page.goto(page_url)

                        # 等待页面加载完成
                        try:
                            self.page.wait_for_load_state('networkidle', timeout=5000)
                        except Exception as e:
                            logger.warning(f"等待页面加载完成时出错: {str(e)}")
                            # 尝试等待DOM加载完成
                            try:
                                self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                            except Exception as dom_error:
                                logger.warning(f"等待DOM加载完成也失败: {str(dom_error)}")

                        # 检查页面是否仍然有效
                        if self.page.is_closed():
                            logger.error("页面在加载过程中被关闭")
                            continue

                        self._click_ignore_button()
                        self.common.random_sleep(30, 40)

                    except Exception as e:
                        logger.error(f"访问页面失败: {str(e)}")
                        # 检查是否是页面关闭错误
                        if "Target page, context or browser has been closed" in str(e):
                            logger.error("页面已关闭，跳过当前博主")
                            continue
                        continue

                    if not self.api_data:
                        logger.info(f"未找到API数据，跳过处理博主 {url.get('creator_nickname', '未知')}")
                        continue
                    # 创建api_data的副本进行遍历，参考原版
                    api_data_copy = dict(self.api_data)
                    for api_url, response_data in api_data_copy.items():
                        if 'data' not in response_data:
                            continue

                        api_data = response_data['data']
                        # 根据不同的API进行不同的处理
                        if 'fans_profile' in api_url:
                            self._process_fans_profile(api_data, url)
                        elif 'data_summary' in api_url:
                            self._process_data_summary(api_data, url)
                        elif 'fans_overall_new_history' in api_url:
                            self._process_fans_history(api_data, url)
                        elif 'fans_summary' in api_url:
                            self._process_fans_summary(api_data, url)
                        elif 'notes_rate' in api_url:
                            self._process_notes_rate(api_data, url, 0, 3, 1, 1)
                        elif 'notes_detail' in api_url:
                            self._process_notes_detail(api_data, url)
                        elif 'blogger' in api_url:
                            self._process_blogger(api_data, url)

                    # 处理不同类型的笔记数据
                    note_types_config = [
                        {
                            'name': '合作笔记',
                            'action': 'click_button',
                            'selector': 'button:has-text("合作笔记")',
                            'advertise_switch': 1
                        },
                        {
                            'name': '自然流量选项',
                            'action': 'select_dropdown',
                            'selector': "div.d-select-wrapper.d-inline-block div.d-select.--color-text-title.--color-bg-fill div.d-grid.d-select-main.d-select-main-indicator.--color-text-title div.d-select-content span:has-text('全流量')",
                            'option_text': '仅自然流量',
                            'advertise_switch': 0
                        }
                    ]

                    for config in note_types_config:
                        try:
                            self._process_notes_type_data(
                                action_type=config['action'],
                                selector=config['selector'],
                                option_text=config.get('option_text'),
                                advertise_switch=config.get('advertise_switch'),
                                url=url
                            )
                        except Exception as e:
                            logger.error(f"处理{config['name']}时出错: {str(e)}")
                            continue

                    # 调用同步接口
                    sync_result = self.sync_single_record_to_api(self.payload)
                    if sync_result:
                        logger.info(f"成功同步博主 {url.get('creator_nickname', '未知')} 的数据到API")
                    else:
                        logger.warning(f"同步博主 {url.get('creator_nickname', '未知')} 的数据到API失败")

                    # 数据比对逻辑
                    try:
                        # 提取爬取的数据
                        scraped_data = {
                            'blogger': {},
                            'fans_profile': {},
                            'notes_rate': []
                        }

                        # 从payload中提取数据
                        for api_item in self.payload["apis"]:
                            if api_item["tb_name"] == "blogger_info" and api_item["tb_data"]:
                                scraped_data['blogger'] = api_item["tb_data"][0]
                            elif api_item["tb_name"] == "blogger_fans_profile" and api_item["tb_data"]:
                                scraped_data['fans_profile'] = api_item["tb_data"][0]
                            elif api_item["tb_name"] == "blogger_note_rate" and api_item["tb_data"]:
                                scraped_data['notes_rate'] = api_item["tb_data"]

                        # 进行数据比对
                        check_result = self._compare_data(url, scraped_data)

                        # 记录比对结果
                        self.comparison_results.append({
                            'creator_name': url.get('creator_nickname', '未知'),
                            'platform_user_id': url.get('platform_user_id', ''),
                            'check_result': check_result
                        })

                        # 日志输出
                        if not check_result['price_accurate'] or not check_result['level_accurate'] or not check_result['data_accurate']:
                            issues = []
                            if not check_result['price_accurate']:
                                issues.append("价格")
                            if not check_result['level_accurate']:
                                issues.append("等级")
                            if not check_result['data_accurate']:
                                issues.append("数据")
                            logger.info(f"博主 {url.get('creator_nickname', '未知')} 发现异常: {','.join(issues)}")
                        else:
                            logger.info(f"博主 {url.get('creator_nickname', '未知')} 数据一致")

                    except Exception as compare_error:
                        logger.error(f"比对博主 {url.get('creator_nickname', '未知')} 数据时出错: {str(compare_error)}")

                except Exception as e:
                    logger.error(f"处理博主 {url.get('creator_nickname', '未知')} 数据时出错: {str(e)}")
                    continue

            # 保存进度和Cookie
            self._save_cookies()

            # 发送企微通知
            if self.comparison_results:
                logger.info(f"准备发送企微通知，共 {len(self.comparison_results)} 条比对结果")
                self._send_wechat_notification(self.comparison_results)
            else:
                logger.info("没有比对结果，跳过企微通知")

            logger.info("本轮数据处理完成")

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            self.update_monitor_status(
                status="出错",
                fail_count=self.monitor_data.get('fail_count', 0) + 1
            )
            raise  # 重新抛出异常，让上层处理重启逻辑

    def _process_blogger(self, api_data, url):
        if api_data['code'] == 0:
            data = api_data['data']
            logger.info(f"基本信息:{data}")
            logger.info(f"基本信息字段列表: {list(data.keys())}")
            # 将数据添加到payload中
            blogger_info_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_info"), None)
            if blogger_info_index is not None:
                # 克隆数据并添加博主ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url.get('platform_user_id')
                self.payload["apis"][blogger_info_index]["tb_data"] = [payload_data]

    def _process_fans_profile(self, api_data, url):
        """处理粉丝画像数据"""
        if api_data['code'] == 0:
            data = api_data['data']
            logger.info(f"粉丝画像:{data}")
            logger.info(f"粉丝画像字段列表: {list(data.keys())}")
            # 将数据添加到payload中
            fans_profile_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_profile"), None)
            if fans_profile_index is not None:
                # 克隆数据并添加平台用户ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url.get('platform_user_id')
                self.payload["apis"][fans_profile_index]["tb_data"] = [payload_data]

    def _compare_data(self, random_creator_data, scraped_data):
        """
        比对getRandomCreator数据和爬取的数据
        :param random_creator_data: getRandomCreator接口返回的单条数据
        :param scraped_data: 爬取到的数据（包含blogger、fans_profile、notes_rate）
        :return: 差异分类字典 {'price': bool, 'level': bool, 'data': bool}
        """
        result = {
            'price_accurate': True,  # 价格准确
            'level_accurate': True,  # 等级准确
            'data_accurate': True,   # 数据准确（20%内波动视为正常）
        }
        creator_name = random_creator_data.get('creator_nickname', '未知博主')

        try:
            # 1. 比对报价信息（从blogger接口）
            blogger_data = scraped_data.get('blogger', {})
            if blogger_data:
                # 比对图文报价
                random_picture_price = float(random_creator_data.get('picture_price', 0))
                scraped_picture_price = float(blogger_data.get('picturePrice', 0))
                if abs(random_picture_price - scraped_picture_price) > 0.01:
                    result['price_accurate'] = False
                    logger.info(f"{creator_name} 图文报价有误,刊例库为{random_picture_price},实际为{scraped_picture_price}")

                # 比对视频报价
                random_video_price = float(random_creator_data.get('video_price', 0))
                scraped_video_price = float(blogger_data.get('videoPrice', 0))
                if abs(random_video_price - scraped_video_price) > 0.01:
                    result['price_accurate'] = False
                    logger.info(f"{creator_name} 视频报价有误,刊例库为{random_video_price},实际为{scraped_video_price}")

                # 比对账号等级
                random_account_level = random_creator_data.get('account_level')
                scraped_account_level = blogger_data.get('currentLevel')
                if random_account_level != scraped_account_level:
                    result['level_accurate'] = False
                    logger.info(f"{creator_name} 账号等级有误,刊例库为{random_account_level},实际为{scraped_account_level}")

            # 2. 比对粉丝性别（从fans_profile接口）- 20%波动视为正常
            fans_profile_data = scraped_data.get('fans_profile', {})
            if fans_profile_data and random_creator_data.get('fans_gender'):
                try:
                    random_fans_gender = json.loads(random_creator_data.get('fans_gender', '{}'))
                    scraped_fans_gender = fans_profile_data.get('gender', {})

                    # 比对男性比例（20%波动容忍度）
                    random_male = float(random_fans_gender.get('male', 0))
                    scraped_male = float(scraped_fans_gender.get('male', 0))
                    if random_male > 0:
                        deviation_male = abs(random_male - scraped_male) / random_male
                        if deviation_male > 0.20:  # 超过20%波动
                            result['data_accurate'] = False
                            logger.info(f"{creator_name} 粉丝男性比例差异过大,刊例库为{random_male:.4f},实际为{scraped_male:.4f},偏差{deviation_male*100:.2f}%")

                    # 比对女性比例（20%波动容忍度）
                    random_female = float(random_fans_gender.get('female', 0))
                    scraped_female = float(scraped_fans_gender.get('female', 0))
                    if random_female > 0:
                        deviation_female = abs(random_female - scraped_female) / random_female
                        if deviation_female > 0.20:  # 超过20%波动
                            result['data_accurate'] = False
                            logger.info(f"{creator_name} 粉丝女性比例差异过大,刊例库为{random_female:.4f},实际为{scraped_female:.4f},偏差{deviation_female*100:.2f}%")
                except Exception as e:
                    logger.warning(f"比对粉丝性别数据时出错: {str(e)}")

            # 3. 比对笔记数据（从notes_rate接口）- 20%波动视为正常
            notes_rate_data_list = scraped_data.get('notes_rate', [])
            if notes_rate_data_list and random_creator_data.get('note_rates'):
                try:
                    random_note_rates = json.loads(random_creator_data.get('note_rates', '[]'))
                    if isinstance(random_note_rates, list) and len(random_note_rates) > 0:
                        # 只比对第一条笔记数据
                        random_note = random_note_rates[0]
                        # 找到匹配的爬取数据（business=0, note_type=3, date_type=1, advertise_switch=1）
                        matching_note = next(
                            (note for note in notes_rate_data_list
                             if note.get('business') == random_note.get('business', 0)
                             and note.get('note_type') == random_note.get('note_type', 3)
                             and note.get('date_type') == random_note.get('date_type', 1)
                             and note.get('advertise_switch') == random_note.get('advertise_switch', 1)),
                            None
                        )

                        if matching_note:
                            # 比对曝光中位数（20%波动容忍度）
                            random_imp = random_note.get('impMedian', 0)
                            scraped_imp = matching_note.get('imp_median', 0)
                            if random_imp > 0:
                                deviation_imp = abs(random_imp - scraped_imp) / random_imp
                                if deviation_imp > 0.20:
                                    result['data_accurate'] = False
                                    logger.info(f"{creator_name} 笔记曝光中位数差异过大,刊例库为{random_imp},实际为{scraped_imp},偏差{deviation_imp*100:.2f}%")

                            # 比对阅读中位数（20%波动容忍度）
                            random_read = random_note.get('readMedian', 0)
                            scraped_read = matching_note.get('read_median', 0)
                            if random_read > 0:
                                deviation_read = abs(random_read - scraped_read) / random_read
                                if deviation_read > 0.20:
                                    result['data_accurate'] = False
                                    logger.info(f"{creator_name} 笔记阅读中位数差异过大,刊例库为{random_read},实际为{scraped_read},偏差{deviation_read*100:.2f}%")

                            # 比对互动中位数（20%波动容忍度）
                            random_interaction = random_note.get('interaction_median', 0)
                            scraped_interaction = matching_note.get('mEngagementNum', 0)
                            if random_interaction > 0:
                                deviation_interaction = abs(random_interaction - scraped_interaction) / random_interaction
                                if deviation_interaction > 0.20:
                                    result['data_accurate'] = False
                                    logger.info(f"{creator_name} 笔记互动中位数差异过大,刊例库为{random_interaction},实际为{scraped_interaction},偏差{deviation_interaction*100:.2f}%")
                except Exception as e:
                    logger.warning(f"比对笔记数据时出错: {str(e)}")

        except Exception as e:
            logger.error(f"数据比对过程出错: {str(e)}")

        return result

    def _send_wechat_notification(self, comparison_results):
        """
        发送企业微信通知
        :param comparison_results: 比对结果列表
        """
        try:
            webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6de13e37-f50e-4384-967b-496845eb6d57"

            # 构建消息内容
            if not comparison_results:
                message = "今日抽检刊例库0位博主。"
            else:
                total_count = len(comparison_results)
                price_accurate_count = 0
                level_accurate_count = 0
                data_accurate_count = 0

                price_abnormal_list = []
                level_abnormal_list = []
                data_abnormal_list = []

                # 收集所有博主名字
                all_blogger_names = []

                # 统计各类准确数量和异常博主
                for result in comparison_results:
                    check_result = result.get('check_result', {})
                    creator_name = result['creator_name']

                    # 收集博主名字
                    all_blogger_names.append(f"{creator_name}, ")

                    if check_result.get('price_accurate', True):
                        price_accurate_count += 1
                    else:
                        price_abnormal_list.append(creator_name)

                    if check_result.get('level_accurate', True):
                        level_accurate_count += 1
                    else:
                        level_abnormal_list.append(creator_name)

                    if check_result.get('data_accurate', True):
                        data_accurate_count += 1
                    else:
                        data_abnormal_list.append(creator_name)

                # 构建播报消息
                # 第一行：今日抽检刊例库20位博主[博主1][博主2]...
                message = f"今日抽检刊例库: {total_count}位博主:[{' '.join(all_blogger_names)}]，"
                message += f"价格准确 {price_accurate_count} 位，"
                message += f"健康等级准确 {level_accurate_count} 位，"
                message += f"其他数据符合 {data_accurate_count} 位，"

                # 添加异常博主列表
                if price_abnormal_list:
                    message += f"其中价格异常为:[{','.join(price_abnormal_list)}]，"
                else:
                    message += "其中价格异常为，无，"

                if level_abnormal_list:
                    message += f"健康等级异常博主为:[{','.join(level_abnormal_list)}]，"
                else:
                    message += "健康等级异常博主为，无，"

                if data_abnormal_list:
                    message += f"其他数据异常博主为:[{','.join(data_abnormal_list)}]"
                else:
                    message += "其他数据异常博主为，无"

            # 发送消息
            headers = {"Content-Type": "application/json"}
            data = {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }

            response = requests.post(webhook_url, headers=headers, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), timeout=10)

            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info("企业微信通知发送成功")
                    return True
                else:
                    logger.warning(f"企业微信通知发送失败: {result.get('errmsg')}")
                    return False
            else:
                logger.warning(f"企业微信通知发送失败，状态码: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"发送企业微信通知时出错: {str(e)}")
            return False

    def _process_data_summary(self, api_data, url):
        """处理数据摘要"""
        if api_data['code'] == 0:
            data = api_data['data']
            logger.info(f"数据摘要:{data}")
            # 将数据添加到payload中
            data_summary_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_data_summary"), None)
            if data_summary_index is not None:
                # 克隆数据并添加平台用户ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url.get('platform_user_id')
                self.payload["apis"][data_summary_index]["tb_data"] = [payload_data]

    def _process_fans_history(self, api_data, url):
        """处理粉丝历史数据"""
        if api_data['code'] == 0:
            data = api_data['data']
            logger.info(f"粉丝历史:{data}")
            # 将数据添加到payload中
            fans_history_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_history"), None)
            if fans_history_index is not None:
                # 克隆数据并添加平台用户ID
                for item in data['list']:
                    item['platform_user_id'] = url.get('platform_user_id')
                self.payload["apis"][fans_history_index]["tb_data"] = data['list']

    def _process_fans_summary(self, api_data, url):
        """处理粉丝概要数据"""
        if api_data['code'] == 0:
            data = api_data['data']
            logger.info(f"粉丝概要:{data}")
            # 将数据添加到payload中
            fans_summary_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_summary"), None)
            if fans_summary_index is not None:
                # 克隆数据并添加平台用户ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url.get('platform_user_id')
                self.payload["apis"][fans_summary_index]["tb_data"] = [payload_data]

    def _process_notes_rate(self, api_data, url, business, note_type, date_type, advertise_switch):
        """处理数据摘要"""
        try:
            if api_data['code'] == 0:
                data = api_data['data']
                logger.info(f"数据摘要:{data}")
                # 将数据添加到payload中
                note_rate_index = next(
                    (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_note_rate"), None)
                if note_rate_index is not None:
                    # 克隆数据并添加平台用户ID和额外信息
                    payload_data = dict(data)
                    payload_data['platform_user_id'] = url.get('platform_user_id')
                    payload_data['business'] = business
                    payload_data['note_type'] = note_type
                    payload_data['date_type'] = date_type
                    payload_data['advertise_switch'] = advertise_switch

                    # 检查是否已存在相同的数据，避免重复添加
                    existing_data = self.payload["apis"][note_rate_index]["tb_data"]
                    is_duplicate = any(
                        item.get('platform_user_id') == payload_data['platform_user_id'] and
                        item.get('business') == payload_data['business'] and
                        item.get('note_type') == payload_data['note_type'] and
                        item.get('date_type') == payload_data['date_type'] and
                        item.get('advertise_switch') == payload_data['advertise_switch']
                        for item in existing_data
                    )

                    if not is_duplicate:
                        # 只有不重复的数据才追加
                        self.payload["apis"][note_rate_index]["tb_data"].append(payload_data)
                        logger.debug(
                            f"添加笔记率数据: business={business}, note_type={note_type}, date_type={date_type}, advertise_switch={advertise_switch}")
                    else:
                        logger.debug(
                            f"跳过重复的笔记率数据: business={business}, note_type={note_type}, date_type={date_type}, advertise_switch={advertise_switch}")

        except Exception as e:
            logger.error(f"处理笔记率数据时出错: {str(e)}")

    def _process_notes_detail(self, api_data, url):
        """处理笔记详情数据"""
        try:
            if api_data['code'] == 0:
                data = api_data['data']
                logger.info(f"笔记详情:{data}")
                # 将数据添加到payload中
                note_detail_index = next(
                    (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_note_detail"),
                    None)
                if note_detail_index is not None:
                    # 克隆数据并添加平台用户ID和额外信息
                    for item in data['list']:
                        item['platform_user_id'] = url.get('platform_user_id')
                    self.payload["apis"][note_detail_index]["tb_data"] = data['list']

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")

    def _click_ignore_button(self):
        """
        点击"已读"按钮
        """
        try:
            # 检查页面状态
            if not self.page or self.page.is_closed():
                logger.debug("页面已关闭，无法点击'已读'按钮")
                return False

            # 检查是否存在'已读'按钮
            ignore_button_elements = self.page.locator("button:has-text('已读')").all()

            if len(ignore_button_elements) > 0:
                # 找到第一个'已读'按钮
                ignore_button = ignore_button_elements[0]

                # 再次检查页面状态
                if self.page.is_closed():
                    logger.debug("页面在查找按钮过程中被关闭")
                    return False

                # 检查按钮是否可见
                try:
                    if ignore_button.is_visible(timeout=3000):
                        ignore_button.click()
                        logger.info("成功点击'已读'按钮")
                        return True
                    else:
                        logger.debug("'已读'按钮不可见")
                except Exception as visibility_error:
                    if "Target page, context or browser has been closed" in str(visibility_error):
                        logger.debug("页面已关闭，无法检查按钮可见性")
                        return False
                    else:
                        logger.debug(f"检查按钮可见性时出错: {str(visibility_error)}")
            else:
                logger.debug("未找到'已读'按钮")

        except Exception as e:
            if "Target page, context or browser has been closed" in str(e):
                logger.debug("页面已关闭，无法查找'已读'按钮")
                return False
            else:
                logger.debug(f"查找'已读'按钮时出错: {str(e)}")
            return False

    def _process_notes_type_data(self, action_type, selector, option_text=None, advertise_switch=None, url=None):
        try:
            # 检查页面状态
            if not self.page or self.page.is_closed():
                logger.warning("页面已关闭，无法处理笔记类型数据")
                return False

            # 清空API数据
            self.api_data.clear()
            self.common.random_sleep(10, 20)

            # 根据action_type执行不同的操作
            if action_type == 'click_button':
                # 点击按钮操作
                try:
                    dropdown_container = self.page.locator('.d-spinner-nested-loading')
                    switch_button = dropdown_container.locator(selector).first

                    # 检查页面状态
                    if self.page.is_closed():
                        logger.warning("页面在查找按钮过程中被关闭")
                        return False

                    if not switch_button.is_visible(timeout=5000):
                        logger.warning(f"按钮不可见: {selector}")
                        return False
                    switch_button.click()
                except Exception as button_error:
                    if "Target page, context or browser has been closed" in str(button_error):
                        logger.warning("页面已关闭，无法点击按钮")
                        return False
                    else:
                        logger.warning(f"点击按钮时出错: {str(button_error)}")
                        return False

            elif action_type == 'select_dropdown':
                # 下拉框选择操作
                try:
                    dropdown_element = self.page.locator(selector)

                    # 检查页面状态
                    if self.page.is_closed():
                        logger.warning("页面在查找下拉框过程中被关闭")
                        return False

                    if not dropdown_element.is_visible(timeout=5000):
                        logger.warning(f"下拉框不可见: {selector}")
                        return False
                    dropdown_element.click()

                    # 等待下拉选项出现
                    self.common.random_sleep(1, 2)

                    # 选择指定选项
                    if option_text:
                        option_element = self.page.locator(f"text=/^{option_text}$/").first

                        # 再次检查页面状态
                        if self.page.is_closed():
                            logger.warning("页面在选择选项过程中被关闭")
                            return False

                        if not option_element.is_visible(timeout=3000):
                            logger.warning(f"选项不可见: {option_text}")
                            return False
                        option_element.click()
                except Exception as dropdown_error:
                    if "Target page, context or browser has been closed" in str(dropdown_error):
                        logger.warning("页面已关闭，无法操作下拉框")
                        return False
                    else:
                        logger.warning(f"操作下拉框时出错: {str(dropdown_error)}")
                        return False
            else:
                logger.warning(f"不支持的操作类型: {action_type}")
                return False

            # 等待页面加载完成
            try:
                if not self.page.is_closed():
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                else:
                    logger.warning("页面已关闭，无法等待加载完成")
                    return False
            except Exception as e:
                if "Target page, context or browser has been closed" in str(e):
                    logger.warning("页面已关闭，无法等待加载完成")
                    return False
                else:
                    logger.warning(f"等待页面加载完成时出错: {str(e)}")

            # 处理API数据
            notes_rate_copy = dict(self.api_data)
            for api_url, response_data in notes_rate_copy.items():
                try:
                    if not response_data or not isinstance(response_data, dict):
                        continue

                    if 'notes_rate' in api_url and 'data' in response_data:
                        api_data = response_data.get('data', {})
                        if api_data and isinstance(api_data, dict):
                            self._process_notes_rate(api_data, url, 1, 3, 1, advertise_switch)
                            return True
                except Exception as e:
                    logger.warning(f"处理API时出错: {str(e)}")
                    continue

            logger.warning(f"未找到的API数据")
            return False

        except Exception as e:
            if "Target page, context or browser has been closed" in str(e):
                logger.warning("页面已关闭，无法处理笔记类型数据")
                return False
            else:
                logger.error(f"处理时出错: {str(e)}")
                return False

    def update_monitor_status(self, **kwargs):
        """更新监控状态"""
        logger.info(kwargs)
        if kwargs.get('completed_count'):
            self.monitor_data['completed_count'] = kwargs.get('completed_count')
        if kwargs.get('fail_count'):
            self.monitor_data['fail_count'] = kwargs.get('fail_count')

    def close(self):
        """
        关闭浏览器和playwright
        """
        try:
            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 关闭所有OpenCV窗口
            cv2.destroyAllWindows()

            # 安全关闭页面
            if hasattr(self, 'page') and self.page:
                try:
                    if not self.page.is_closed():
                        self.page.close()
                except Exception as page_error:
                    logger.warning(f"关闭页面时出错: {str(page_error)}")
                finally:
                    self.page = None

            # 安全关闭上下文
            if hasattr(self, 'context') and self.context:
                try:
                    self.context.close()
                except Exception as context_error:
                    logger.warning(f"关闭上下文时出错: {str(context_error)}")
                finally:
                    self.context = None

            # 安全关闭浏览器
            if hasattr(self, 'browser') and self.browser:
                try:
                    self.browser.close()
                except Exception as browser_error:
                    logger.warning(f"关闭浏览器时出错: {str(browser_error)}")
                finally:
                    self.browser = None

            # 安全停止playwright
            if hasattr(self, 'playwright') and self.playwright:
                try:
                    self.playwright.stop()
                except Exception as playwright_error:
                    logger.warning(f"停止playwright时出错: {str(playwright_error)}")
                finally:
                    self.playwright = None

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

    def sync_single_record_to_api(self, payload):
        try:
            url = "http://47.104.76.46:19000/api/v1/sync/spider/data"
            headers = {"Content-Type": "application/json"}

            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload, ensure_ascii=False),
                timeout=30
            )

            if response.status_code == 200:
                return True
            else:
                logger.warning(f"单条数据同步失败，状态码: {response.status_code}")
                return False

        except Exception as e:
            logger.warning(f"单条数据同步异常: {str(e)}")
            return False

    def _handle_api_response(self, response):
        """处理API响应，只捕获指定的API请求"""
        try:
            # 检查页面状态
            if not hasattr(self, 'page') or not self.page or self.page.is_closed():
                return

            url = response.url
            # 从配置获取需要捕获的API路径
            target_apis = self.config['API_TARGETS']

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                try:
                    # 再次检查页面状态
                    if self.page.is_closed():
                        return

                    # 检查响应状态
                    if response.status != 200:
                        logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                        return

                    data = response.json()

                    # 找到匹配的API类型
                    matched_api = None
                    for api in target_apis:
                        if api in url:
                            matched_api = api
                            break

                    # 存储API数据
                    self.api_data[url] = {
                        'url': url,
                        'data': data,
                        'api_type': matched_api,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'status': response.status
                    }

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def _save_cookies(self):
        """
        保存当前会话的Cookie到同级目录
        """
        try:
            cookies = self.context.cookies()
            # 确保cookie文件的目录存在
            cookie_dir = os.path.dirname(self.cookie_file)
            if cookie_dir and not os.path.exists(cookie_dir):
                os.makedirs(cookie_dir, exist_ok=True)

            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logger.info(f"Cookie已保存到: {self.cookie_file}")
        except Exception as e:
            logger.error(f"保存Cookie时出错: {str(e)}")

    def _load_cookies(self):
        """
        从同级目录加载保存的Cookie
        :return: 是否成功加载Cookie
        """
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)

                if cookies:
                    self.context.add_cookies(cookies)
                    logger.info(f"已成功加载 {len(cookies)} 个Cookie")
                    return True
                else:
                    logger.warning("Cookie文件为空")
                    return False
            else:
                logger.info(f"Cookie文件不存在: {self.cookie_file}")
                return False
        except Exception as e:
            logger.error(f"加载Cookie时出错: {str(e)}")
            # 如果cookie文件损坏，删除它
            try:
                if os.path.exists(self.cookie_file):
                    os.remove(self.cookie_file)
                    logger.info("已删除损坏的Cookie文件")
            except:
                pass
            return False


def run_spider_task():
    """
    执行爬虫任务 - 只在异常时重启版本
    """
    spider = None
    try:
        # 1. 初始化爬虫实例
        spider = PGYSpider()
        logger.info("爬虫实例初始化成功")

        # 2. 执行登录
        logger.info("开始登录流程...")
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        logger.info("登录成功，开始抓取数据...")

        # 3. 执行抓取和数据更新
        spider.scrape_user_notes()

        logger.info("数据抓取任务完成，程序正常结束")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        logger.info("程序异常停止，将在1小时后重启...")
        return False
    finally:
        # 确保资源被正确释放
        if spider:
            try:
                spider.close()
                logger.info("资源清理完成")
            except Exception as e:
                logger.error(f"清理资源时出错: {str(e)}")


def main():
    """
    主函数 - 只在异常时重启版本
    """
    try:
        # 加载配置
        config = load_config()
        scheduler_config = config['SCHEDULER_CONFIG']

        logger.info("=== 蒲公英数据抓取程序启动 ===")
        logger.info(f"执行时间: 每天 {scheduler_config['daily_time']}")

        if scheduler_config['run_once']:
            success = run_spider_task()
            if not success:
                logger.info("程序异常停止，将在1小时后重启...")
                time.sleep(3600)
                return main()  # 递归重启
            return success

        elif scheduler_config['enable_scheduler']:
            # 注册定时任务
            schedule.every().day.at(scheduler_config['daily_time']).do(run_spider_task)

            # 运行调度器
            logger.info("调度器开始运行...")
            while True:
                try:
                    schedule.run_pending()
                    time.sleep(scheduler_config['check_interval'])
                except Exception as e:
                    logger.error(f"调度器运行出错: {str(e)}")
                    logger.info("调度器异常停止，将在1小时后重启...")
                    time.sleep(3600)
                    return main()  # 重启整个程序

        else:
            # 调度器未启用，直接执行一次
            success = run_spider_task()
            if not success:
                logger.info("程序异常停止，将在1小时后重启...")
                time.sleep(3600)
                return main()  # 递归重启
            return success

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        return True
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        logger.info("程序启动异常，将在1小时后重启...")
        time.sleep(3600)
        return main()  # 递归重启


if __name__ == "__main__":
    try:
        success = main()
        if success:
            logger.info("程序执行成功")
            sys.exit(0)
        else:
            logger.error("程序执行失败")
            sys.exit(1)
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        sys.exit(1)