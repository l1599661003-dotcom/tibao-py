import json
import os
import sys
import configparser
import time
from datetime import datetime, timedelta
import cv2
import requests
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback

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
            'email': config.get('PGY_LOGIN', 'email'),
            'password': config.get('PGY_LOGIN', 'password'),
            'base_url': config.get('PGY_LOGIN', 'base_url'),
            'client_id': config.get('PGY_LOGIN', 'client_id')
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

        self.base_url = self.config['PGY_LOGIN_CONFIG']['base_url']
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
            "client_id": self.config['PGY_LOGIN_CONFIG']['client_id']
        }
        self.payload_creator_data = {
            "creator_data": {
                "blogger_info": [],
                "blogger_note_rate": [],
                "blogger_data_summary": [],
                "blogger_notes_detail": [],
                "blogger_fans_summary": [],
                "blogger_fans_profile": [],
                "blogger_fans_history": [],
            }
        }
        self.current_user_data = {}  # 当前用户数据

        # 添加导航错误计数器
        self.navigation_error_count = 0
        self.max_navigation_errors = 3

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
        logger.info("开始无限循环查询新签约博主数据...")

        while True:
            try:
                if not self.is_logged_in:
                    logger.error("未登录状态，无法抓取数据")
                    logger.info("等待5分钟后重试...")
                    time.sleep(300)  # 5分钟 = 300秒
                    continue

                api_url = "https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator"
                headers = {"Content-Type": "application/json"}

                logger.info("正在查询新签约博主接口...")
                response = requests.post(api_url, headers=headers, timeout=30)
                creator_data = response.json()['data']

                if len(creator_data) > 0:
                    logger.info(f"找到 {len(creator_data)} 个博主数据，开始处理...")

                    for creator in creator_data:
                        if not creator.get('platform_user_id') or "aaaaaaaaa" in creator.get('platform_user_id', ''):
                            continue

                        # 检查是否超过最大导航错误次数
                        if self.navigation_error_count >= self.max_navigation_errors:
                            logger.warning(f"导航错误次数已达到上限({self.max_navigation_errors})，重置计数器并继续")
                            self.navigation_error_count = 0

                        try:
                            # 清空之前的数据
                            self.api_data.clear()
                            self.current_user_data.clear()

                            logger.info(f"正在处理博主: {creator.get('creator_nickname')}")

                            # 访问页面
                            page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{creator.get('platform_user_id')}"
                            logger.info(f"开始访问页面: {page_url}")

                            try:
                                self.page.goto(page_url)
                            except Exception as e:
                                logger.error(f"访问页面失败: {str(e)}")
                                continue

                            # 等待页面加载完成
                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(30, 40)

                            if self.api_data:
                                # 创建api_data的副本进行遍历，参考原版
                                api_data_copy = dict(self.api_data)
                                for api_url, response_data in api_data_copy.items():
                                    if 'data' not in response_data:
                                        continue

                                    api_data = response_data['data']
                                    # 根据不同的API进行不同的处理
                                    if 'fans_profile' in api_url:
                                        self._process_fans_profile(api_data, creator)
                                    elif 'data_summary' in api_url:
                                        self._process_data_summary(api_data, creator)
                                    elif 'fans_overall_new_history' in api_url:
                                        self._process_fans_history(api_data, creator)
                                    elif 'fans_summary' in api_url:
                                        self._process_fans_summary(api_data, creator)
                                    elif 'notes_rate' in api_url:
                                        # 确保价格数据已获取
                                        graphic_price = self.current_user_data.get('graphic_price', 0)
                                        video_price = self.current_user_data.get('video_price', 0)
                                        self._process_notes_rate(api_data, graphic_price, video_price, 'daily_pic_video',
                                                                 'daily_pic_video_text', creator, 0, 3, 1, 1)
                                    elif 'notes_detail' in api_url:
                                        self._process_notes_detail(api_data, creator)

                                    elif 'blogger' in api_url:
                                        self._process_blogger(api_data, creator)
                                # 所有 API 数据处理完毕后，点击笔记按钮
                                try:
                                    self.api_data.clear()
                                    self.common.random_sleep(10, 15)
                                    # 处理第一页笔记详情数据
                                    notes_data_copy = dict(self.api_data)
                                    should_continue_pagination = True

                                    # 处理第一页数据
                                    for api_url, response_data in notes_data_copy.items():
                                        if 'notes_detail' in api_url and 'data' in response_data:
                                            api_data = response_data['data']
                                            # 检查是否应该继续分页
                                            should_continue_pagination = self._process_notes_detail(api_data, creator)
                                            break

                                    # 如果应该继续分页，继续处理后续页面
                                    if should_continue_pagination:
                                        logger.info("开始处理分页数据...")
                                        page_count = 1

                                        while self._click_next_page():
                                            page_count += 1
                                            logger.info(f"正在处理第 {page_count} 页数据...")

                                            # 等待API数据加载
                                            self.common.random_sleep(10, 15)

                                            # 处理当前页数据
                                            for api_url, response_data in self.api_data.items():
                                                if 'notes_detail' in api_url and 'data' in response_data:
                                                    api_data = response_data['data']
                                                    should_continue_pagination = self._process_notes_detail(api_data, creator)
                                                    break

                                            # 如果不应该继续分页，退出循环
                                            if not should_continue_pagination:
                                                logger.info(f"第 {page_count} 页数据距离今天超过90天，停止分页")
                                                break

                                            # 清空当前页的API数据，准备处理下一页
                                            self.api_data.clear()

                                        logger.info(f"分页处理完成，共处理了 {page_count} 页数据")
                                    else:
                                        logger.info("第一页数据距离今天超过90天，无需分页")

                                except Exception as db_error:
                                    logger.error(f"更新数据库时出错: {str(db_error)}")
                                    # 继续处理下一个博主，不退出程序
                                # 处理不同类型的笔记数据
                                note_types_config = [
                                    {
                                        'name': '合作笔记',
                                        'action': 'click_button',
                                        'selector': 'button:has-text("合作笔记")',
                                        'note_type': 'cooperation_pic_video',
                                        'note_type1': 'cooperation_pic_video_text',
                                        'wait_time': (10, 20)
                                    },
                                    {
                                        'name': '自然流量选项',
                                        'action': 'select_dropdown',
                                        'selector': "div.d-select-wrapper.d-inline-block div.d-select.--color-text-title.--color-bg-fill div.d-grid.d-select-main.d-select-main-indicator.--color-text-title div.d-select-content span:has-text('全流量')",
                                        'option_text': '仅自然流量',
                                        'note_type': 'cooperation_pic_video_zr',
                                        'note_type1': 'cooperation_pic_video_zr',
                                        'wait_time': (10, 20)
                                    },
                                    # {
                                    #     'name': '图文选项',
                                    #     'action': 'select_dropdown',
                                    #     'selector': "div.d-select-wrapper.d-inline-block div.d-select.--color-text-title.--color-bg-fill div.d-grid.d-select-main.d-select-main-indicator.--color-text-title div.d-select-content div:has-text('图文+视频')",
                                    #     'option_text': '图文',
                                    #     'note_type': 'coop_pic_text',
                                    #     'note_type1': 'coop_pic_text',
                                    #     'wait_time': (10, 15)
                                    # },
                                    # {
                                    #     'name': '日常笔记',
                                    #     'action': 'click_button',
                                    #     'selector': 'button:has-text("日常笔记")',
                                    #     'note_type': 'daily_pic_text',
                                    #     'note_type1': 'daily_pic_text',
                                    #     'wait_time': (5, 10)
                                    # },
                                    # {
                                    #     'name': '视频选项',
                                    #     'action': 'select_dropdown',
                                    #     'selector': "div.d-select-wrapper.d-inline-block div.d-select.--color-text-title.--color-bg-fill div.d-grid.d-select-main.d-select-main-indicator.--color-text-title div.d-select-content div:has-text('图文')",
                                    #     'option_text': '视频',
                                    #     'note_type': 'daily_video',
                                    #     'note_type1': 'daily_video',
                                    #     'wait_time': (10, 15)
                                    # },
                                    # {
                                    #     'name': '合作笔记',
                                    #     'action': 'click_button',
                                    #     'selector': 'button:has-text("合作笔记")',
                                    #     'note_type': 'coop_video',
                                    #     'note_type1': 'coop_video',
                                    #     'wait_time': (5, 10)
                                    # },
                                ]

                                for config in note_types_config:
                                    try:
                                        success = self._process_notes_type_data(
                                            action_type=config['action'],
                                            selector=config['selector'],
                                            note_type=config['note_type'],
                                            note_type1=config['note_type1'],
                                            option_text=config.get('option_text'),
                                            wait_time=config['wait_time'],
                                            url=creator
                                        )

                                        if success:
                                            logger.info(f"成功处理{config['name']}数据")
                                        else:
                                            logger.warning(f"处理{config['name']}数据失败")

                                    except Exception as e:
                                        logger.error(f"处理{config['name']}时出错: {str(e)}")
                                        continue

                                logger.info(self.current_user_data)

                                # 所有 API 数据处理完毕后，统一更新数据库
                                if self.current_user_data:
                                    try:
                                        # 添加额外的字段
                                        self.current_user_data.update({
                                            'last_update_time': datetime.now(),
                                            'is_update': 2,
                                        })

                                        # 调用接口同步数据
                                        try:
                                            # 确保所有API数据中都有平台用户ID
                                            for api_item in self.payload["apis"]:
                                                for data_item in api_item["tb_data"]:
                                                    if isinstance(data_item, dict) and "platform_user_id" not in data_item:
                                                        data_item["platform_user_id"] = creator.get('platform_user_id')

                                            # 调用同步接口
                                            sync_result = self.sync_single_record_to_api(self.payload)
                                            if sync_result:
                                                logger.info(f"成功同步博主 {creator.get('creator_nickname')} 的数据到API")
                                            else:
                                                logger.warning(f"同步博主 {creator.get('creator_nickname')} 的数据到API失败")

                                            # 调用创作者数据记录接口
                                            creator_sync_result = self.sync_record_creator_data(self.payload_creator_data)
                                            if creator_sync_result:
                                                logger.info(f"成功同步博主 {creator.get('creator_nickname')} 的创作者数据到API")
                                            else:
                                                logger.warning(
                                                    f"同步博主 {creator.get('creator_nickname')} 的创作者数据到API失败")
                                        except Exception as api_error:
                                            logger.error(f"同步数据到API时出错: {str(api_error)}")

                                        logger.info(f"成功处理博主 {creator.get('creator_nickname')} 的所有数据")

                                    except Exception as db_error:
                                        logger.error(f"处理数据时出错: {str(db_error)}")
                                        continue
                                else:
                                    logger.info(f"未捕获到博主 {creator.get('creator_nickname')} 的API请求")

                            else:
                                logger.info(f"未捕获到博主 {creator.get('creator_nickname')} 的API请求")

                        except Exception as e:
                            logger.error(f"处理博主 {creator.get('creator_nickname')} 数据时出错: {str(e)}")
                            continue
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
                            "client_id": self.config['PGY_LOGIN_CONFIG']['client_id']
                        }
                        self.payload_creator_data = {
                            "creator_data": {
                                "blogger_info": {},
                                "blogger_note_rate": {},
                                "blogger_data_summary": {},
                                "blogger_notes_detail": {},
                                "blogger_fans_summary": {},
                                "blogger_fans_profile": {},
                                "blogger_fans_history": {},
                            }
                        }
                else:
                    logger.info("没有找到新签约博主数据")

                # 保存进度和Cookie
                self._save_cookies()

                logger.info("本轮数据处理完成，等待5分钟后继续查询...")
                time.sleep(300)  # 5分钟 = 300秒

            except Exception as e:
                logger.error(f"抓取用户笔记时出错: {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                self.update_monitor_status(
                    status="出错",
                    fail_count=self.monitor_data.get('fail_count', 0) + 1
                )
                logger.info("发生错误，等待5分钟后重试...")
                time.sleep(300)  # 5分钟 = 300秒
                continue  # 继续下一次循环

    def _process_blogger(self, api_data, url):
        if api_data['code'] == 0:
            data = api_data['data']

            # 将数据添加到payload中
            blogger_info_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_info"), None)
            if blogger_info_index is not None:
                # 克隆数据并添加博主ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
                self.payload["apis"][blogger_info_index]["tb_data"] = [payload_data]

            # 将数据添加到payload_creator_data中
            payload_creator_data = dict(data)
            payload_creator_data['platform_user_id'] = url['platform_user_id']
            self.payload_creator_data["creator_data"]["blogger_info"] = [payload_creator_data]

            notesign = ''
            # 处理签约信息
            if data.get('noteSign'):
                notesign = data['noteSign']['name']

            # 处理价格信息
            picture_price = 0
            video_price = 0
            if data.get('videoPrice'):
                video_price = data.get('videoPrice', 0)
            if data.get('picturePrice'):
                picture_price = data.get('picturePrice', 0)

            # 更新用户数据
            self.current_user_data.update({
                'gender': data.get('gender', ''),
                'nickname': data.get('name', ''),  # 昵称
                'notesign': notesign,  # 博主机构
                'talent_id': data.get('redId', ''),  # 小红书ID
                'shipping_address': data.get('location', ''),  # 所在地区
                'followers': data.get('fansCount', 0),  # 粉丝数量
                'like_count': data.get('likeCollectCountInfo', ''),  # 获赞与收藏
                'graphic_price': picture_price,  # 图文一口价
                'video_price': video_price,  # 视频一口价
                'currentLevel': data.get('currentLevel', ''),  # 账号状态
                'xhs_url': f"https://www.xiaohongshu.com/user/profile/{url['platform_user_id']}"
            })

    def _process_fans_profile(self, api_data, url):
        """处理粉丝画像数据"""
        if api_data['code'] == 0:
            data = api_data['data']

            # 将数据添加到payload中
            fans_profile_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_profile"), None)
            if fans_profile_index is not None:
                # 克隆数据并添加平台用户ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
                self.payload["apis"][fans_profile_index]["tb_data"] = [payload_data]

            # 将数据添加到payload_creator_data中
            payload_creator_data = dict(data)
            payload_creator_data['platform_user_id'] = url['platform_user_id']
            self.payload_creator_data["creator_data"]["blogger_fans_profile"] = [payload_creator_data]

            try:
                # 安全获取数组元素的辅助函数
                def safe_get_array_item(arr, index, default_name="未知", default_percent=0.0):
                    """安全获取数组元素，避免索引越界"""
                    if arr and len(arr) > index:
                        name = arr[index].get('name', default_name)
                        percent = arr[index].get('percent', default_percent)
                        return name, float(percent)
                    return default_name, default_percent

                # 处理年龄分布（确保有5个年龄段）
                ages = data.get('ages', [])
                if len(ages) >= 5:
                    age_data = {
                        'age_less_than_18': f"{float(ages[0]['percent']):.2f}",  # 年龄<18
                        'age_18_to_24': f"{float(ages[1]['percent']):.2f}",  # 年龄18_24
                        'age_25_to_34': f"{float(ages[2]['percent']):.2f}",  # 年龄25_34
                        'age_35_to_44': f"{float(ages[3]['percent']):.2f}",  # 年龄35_44
                        'age_greater_than_44': f"{float(ages[4]['percent']):.2f}",  # 年龄>44
                    }
                else:
                    # 如果年龄数据不足，设置默认值
                    age_data = {
                        'age_less_than_18': '0.00',
                        'age_18_to_24': '0.00',
                        'age_25_to_34': '0.00',
                        'age_35_to_44': '0.00',
                        'age_greater_than_44': '0.00',
                    }

                # 处理性别分布
                gender = data.get('gender', {})
                gender_data = {
                    'male_fan_percentage': f"{float(gender.get('male', 0)):.2f}",  # 男粉丝占比
                    'female_fan_percentage': f"{float(gender.get('female', 0)):.2f}",  # 女粉丝占比
                }

                # 处理兴趣 top5
                interests = data.get('interests', [])
                interest_data = {}
                for i in range(1, 6):  # top1 到 top5
                    name, percent = safe_get_array_item(interests, i - 1)
                    interest_data[f'interest_top{i}'] = f"{name}({percent * 100:.2f}%)"

                # 处理省份 top3
                provinces = data.get('provinces', [])
                province_data = {}
                for i in range(1, 4):  # top1 到 top3
                    name, percent = safe_get_array_item(provinces, i - 1)
                    province_data[f'province_top{i}'] = f"{name}({percent * 100:.2f}%)"

                # 处理城市 top3
                cities = data.get('cities', [])
                city_data = {}
                for i in range(1, 4):  # top1 到 top3
                    name, percent = safe_get_array_item(cities, i - 1)
                    city_data[f'city_top{i}'] = f"{name}({percent * 100:.2f}%)"

                # 处理设备 top3
                devices = data.get('devices', [])
                device_data = {}
                for i in range(1, 4):  # top1 到 top3
                    name, percent = safe_get_array_item(devices, i - 1)
                    # 使用desc字段作为显示名称，如果没有则使用name
                    display_name = devices[i - 1].get('desc', name) if devices and len(devices) > i - 1 else name
                    device_data[f'device_top{i}'] = f"{display_name}({percent * 100:.2f}%)"

                # 合并所有数据
                self.current_user_data.update({
                    **age_data,
                    **gender_data,
                    **interest_data,
                    **province_data,
                    **city_data,
                    **device_data
                })

            except Exception as e:
                logger.error(f"处理粉丝画像数据时出错: {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                # 设置默认值
                self.current_user_data.update({
                    'age_less_than_18': '0.00',
                    'age_18_to_24': '0.00',
                    'age_25_to_34': '0.00',
                    'age_35_to_44': '0.00',
                    'age_greater_than_44': '0.00',
                    'male_fan_percentage': '0.00',
                    'female_fan_percentage': '0.00',
                    'interest_top1': '未知(0.00%)',
                    'interest_top2': '未知(0.00%)',
                    'interest_top3': '未知(0.00%)',
                    'interest_top4': '未知(0.00%)',
                    'interest_top5': '未知(0.00%)',
                    'province_top1': '未知(0.00%)',
                    'province_top2': '未知(0.00%)',
                    'province_top3': '未知(0.00%)',
                    'city_top1': '未知(0.00%)',
                    'city_top2': '未知(0.00%)',
                    'city_top3': '未知(0.00%)',
                    'device_top1': '未知(0.00%)',
                    'device_top2': '未知(0.00%)',
                    'device_top3': '未知(0.00%)',
                })

    def _process_data_summary(self, api_data, url):
        """处理数据摘要"""
        if api_data['code'] == 0:
            data = api_data['data']

            # 将数据添加到payload中
            data_summary_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_data_summary"), None)
            if data_summary_index is not None:
                # 克隆数据并添加平台用户ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
                self.payload["apis"][data_summary_index]["tb_data"] = [payload_data]

            # 将数据添加到payload_creator_data中
            payload_creator_data = dict(data)
            payload_creator_data['platform_user_id'] = url['platform_user_id']
            self.payload_creator_data["creator_data"]["blogger_data_summary"] = [payload_creator_data]

            try:
                self.current_user_data.update({
                    'notes_published': data.get('noteNumber', 0),  # 发布笔记数量
                    'read_median': data.get('mValidRawReadFeedNum', 0),
                    'interaction_median': data.get('mEngagementNum', 0),
                    'video_read_cost': data.get('videoReadCostV2', 0),
                    'picture_read_cost': data.get('pictureReadCost', 0),
                    'content_categories': ','.join([item.get('contentTag', '') for item in data.get('noteType', [])]),
                    # 内容类目及占比
                    'cooperated_industries': ','.join(data.get('tradeNames', [])),  # 合作行业
                })
            except Exception as e:
                logger.error(f"处理数据摘要时出错: {str(e)}")

    def _process_fans_history(self, api_data, url):
        """处理粉丝历史数据"""
        if api_data['code'] == 0:
            data = api_data['data']

            # 将数据添加到payload中
            fans_history_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_history"), None)
            if fans_history_index is not None:
                # 克隆数据并添加平台用户ID
                for item in data['list']:
                    item['platform_user_id'] = url['platform_user_id']
                self.payload["apis"][fans_history_index]["tb_data"] = data['list']

            # 将数据添加到payload_creator_data中
            payload_creator_data = dict(data)
            payload_creator_data['platform_user_id'] = url['platform_user_id']
            self.payload_creator_data["creator_data"]["blogger_fans_history"] = [payload_creator_data]

            # 更新用户数据
            self.current_user_data.update({
                'followers_increase': data.get('fansNumInc', 0),
                'followers_change_rate': float(f"{float(data.get('fansNumIncRate', 0)):.2f}")
            })

    def _process_fans_summary(self, api_data, url):
        """处理粉丝概要数据"""
        if api_data['code'] == 0:
            data = api_data['data']

            # 将数据添加到payload中
            fans_summary_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_summary"), None)
            if fans_summary_index is not None:
                # 克隆数据并添加平台用户ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
                self.payload["apis"][fans_summary_index]["tb_data"] = [payload_data]

            # 将数据添加到payload_creator_data中
            payload_creator_data = dict(data)
            payload_creator_data['platform_user_id'] = url['platform_user_id']
            self.payload_creator_data["creator_data"]["blogger_fans_summary"] = [payload_creator_data]

            # 更新用户数据
            self.current_user_data.update({
                'active_followers_percentage': float(data.get('activeFansRate', 0)) / 100,
                'active_followers_benchmark_exceed': float(data.get('activeFansBeyondRate', 0)),
                'engaged_followers_percentage': float(data.get('engageFansRate', 0)),
                'engaged_followers_benchmark_exceed': float(data.get('engageFansBeyondRate', 0)),
                'reading_followers_percentage': float(data.get('readFansRate', 0)),
                'reading_followers_benchmark_exceed': float(data.get('readFansBeyondRate', 0)),
            })

    def _process_notes_rate(self, api_data, graphic_price, video_price, type, type1, url, business, note_type,
                            date_type, advertise_switch):
        """处理数据摘要"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning(f"{type}笔记率API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning(f"{type}笔记率数据为空或格式错误")
                return

            # 将数据添加到payload中
            note_rate_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_note_rate"), None)
            if note_rate_index is not None:
                # 克隆数据并添加平台用户ID和额外信息
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
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

            # 将数据添加到payload_creator_data中
            payload_creator_data = dict(data)
            payload_creator_data['platform_user_id'] = url['platform_user_id']
            payload_creator_data['business'] = business
            payload_creator_data['note_type'] = note_type
            payload_creator_data['date_type'] = date_type
            payload_creator_data['advertise_switch'] = advertise_switch
            self.payload_creator_data["creator_data"]["blogger_note_rate"] = [payload_creator_data]

            # 安全获取数值，避免除零错误
            def safe_get_numeric(key, default=0):
                """安全获取数值"""
                try:
                    value = data.get(key, default)
                    if value is None:
                        return default
                    return float(value) if value != '' else default
                except (ValueError, TypeError):
                    return default

            imp_median = safe_get_numeric('impMedian', 0)
            read_median = safe_get_numeric('readMedian', 0)
            engage_median = safe_get_numeric('mEngagementNum', 0)
            hundred_like = safe_get_numeric('hundredLikePercent', 0)
            thousand_like = safe_get_numeric('thousandLikePercent', 0)
            picture3sViewRate = safe_get_numeric('picture3sViewRate', 0)
            videoFullViewRate = safe_get_numeric('videoFullViewRate', 0)
            interactionRate = safe_get_numeric('interactionRate', 0)

            # 确保价格不为零，避免除零错误
            safe_graphic_price = graphic_price if graphic_price and graphic_price > 0 else 1
            safe_video_price = video_price if video_price and video_price > 0 else 1

            # 计算CPM、CPC、CPE，添加安全检查
            def safe_division(numerator, denominator, multiplier=1):
                """安全除法，避免除零错误，保留两位小数"""
                try:
                    if denominator and denominator > 0:
                        return round((numerator / denominator) * multiplier, 2)
                    return 0
                except (ZeroDivisionError, TypeError):
                    return 0

            # 计算各项指标
            cpm_value = min(
                safe_division(safe_graphic_price, imp_median, 1000),
                safe_division(safe_video_price, imp_median, 1000)
            )

            cpc_value = min(
                safe_division(safe_graphic_price, read_median),
                safe_division(safe_video_price, read_median)
            )

            cpe_value = min(
                safe_division(safe_graphic_price, engage_median),
                safe_division(safe_video_price, engage_median)
            )

            cpr_value = safe_division(read_median, imp_median)

            # 更新用户数据
            update_data = {
                type + '_exposure_median': imp_median,
                type + '_reading_median': read_median,
                type + '_interaction_median': engage_median,
                type + '_interaction_rate': float(interactionRate) / 100,
                type + '_likes_note_ratio': float(hundred_like) / 100,
                type + '_hundred_likes_note_ratio': float(thousand_like) / 100,
                type + '_completion_rate': float(videoFullViewRate) / 100,
                type + '_three_sec_reading_rate': float(picture3sViewRate) / 100,
                type1 + '_cpm': cpm_value,
                type1 + '_cpc': cpc_value,
                type1 + '_cpe': cpe_value,
                type1 + '_cpr': cpr_value,
            }
            self.current_user_data.update(update_data)

        except Exception as e:
            logger.error(f"处理{type}笔记率数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_notes_detail(self, api_data, creator):
        """处理笔记详情数据"""
        if api_data['code'] != 0:
            return False  # 返回False表示不应该继续分页

        try:
            data = api_data['data']
            # 将数据添加到payload中
            note_detail_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_note_detail"), None)
            if note_detail_index is not None:
                # 克隆数据并添加平台用户ID和额外信息
                for item in data['list']:
                    item['platform_user_id'] = creator['platform_user_id']
                self.payload["apis"][note_detail_index]["tb_data"] = data['list']

            # 将数据添加到payload_creator_data中 - 支持分页数据累积
            if 'blogger_notes_detail' not in self.payload_creator_data["creator_data"]:
                self.payload_creator_data["creator_data"]["blogger_notes_detail"] = []
            
            # 确保blogger_notes_detail是列表类型
            if not isinstance(self.payload_creator_data["creator_data"]["blogger_notes_detail"], list):
                logger.warning(f"blogger_notes_detail不是列表类型，当前类型: {type(self.payload_creator_data['creator_data']['blogger_notes_detail'])}")
                self.payload_creator_data["creator_data"]["blogger_notes_detail"] = []
            
            # 将当前页的每个笔记数据添加到列表中
            if data.get('list') and isinstance(data['list'], list):
                for item in data['list']:
                    # 克隆每个笔记项目并添加平台用户ID
                    item_data = dict(item)
                    item_data['platform_user_id'] = creator.get('platform_user_id', '')
                    # 将每个笔记数据添加到列表中
                    self.payload_creator_data["creator_data"]["blogger_notes_detail"].append(item_data)

            # 初始化计数器
            day30 = 0
            day90 = 0
            all_orders = 0
            video_order = 0
            graphic_order = 0

            # 获取当前时间
            current_date = datetime.now()
            # 计算30天前和90天前的日期
            thirty_days_ago = current_date - timedelta(days=30)
            days_ago_90 = current_date - timedelta(days=90)

            # 检查最后一条数据的时间，判断是否应该继续分页
            should_continue = True
            if data.get('list') and len(data['list']) > 0:
                try:
                    # 获取最后一条数据的日期
                    last_item = data['list'][-1]
                    last_date_str = last_item.get('date')
                    if last_date_str:
                        last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
                        # 如果最后一条数据距离今天超过90天，停止分页
                        if (current_date - last_date).days > 90:
                            should_continue = False
                            logger.info(f"最后一条数据日期为 {last_date_str}，距离今天超过90天，停止分页")
                except Exception as e:
                    logger.warning(f"解析最后一条数据日期时出错: {str(e)}")
                    # 如果解析失败，默认继续分页
                    should_continue = True

            # 遍历列表计算订单数量
            for item in data.get('list', []):
                try:
                    isVideo = item['isVideo']
                    if isVideo:
                        video_order += 1
                    else:
                        graphic_order += 1
                    # 将字符串日期转换为datetime对象
                    item_date = datetime.strptime(item['date'], '%Y-%m-%d')

                    # 检查是否在最近30天内
                    if thirty_days_ago <= item_date <= current_date:
                        day30 += 1

                    # 检查是否在最近90天内
                    if days_ago_90 <= item_date <= current_date:
                        day90 += 1

                    all_orders += 1
                except (ValueError, KeyError):
                    continue

            # 获取所有品牌名称
            brand_names = [item.get('brandName', '') for item in data.get('list', []) if item.get('brandName')]

            # 更新用户数据
            self.current_user_data.update({
                'brand_name': ','.join(brand_names),  # 品牌名
                'pgy_total_orders': all_orders,
                'pgy_orders_30_days': day30,
                'pgy_orders_90_days': day90,
                'half_year_graphic_orders': graphic_order,
                'half_year_video_orders': video_order,
            })

            # 返回是否应该继续分页
            return should_continue

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")
            # 设置默认值
            self.current_user_data.update({
                'brand_name': '',
                'pgy_total_orders': 0,
                'pgy_orders_30_days': 0,
                'pgy_orders_90_days': 0,
                'half_year_graphic_orders': 0,
                'half_year_video_orders': 0,
            })
            # 出错时默认继续分页
            return True

    def _click_next_page(self):
        """
        点击下一页按钮

        Returns:
            bool: 是否成功点击下一页（True表示有下一页，False表示已经是最后一页）
        """
        try:

            # 记录点击前的API数据数量
            api_count_before = len(self.api_data)

            # 查找下一页按钮（精确定位到分页按钮元素）
            next_page_button = self.page.locator(
                "div.d-pagination div.d-pagination-page:has(span svg path[d='M19 12L31 24L19 36'])").first

            # 检查按钮是否存在
            if not next_page_button.is_visible(timeout=3000):
                logger.info("未找到下一页按钮，可能已到最后一页")
                return False

            # 调试：输出找到的按钮信息
            try:
                button_class = next_page_button.get_attribute("class") or ""
            except Exception as e:
                logger.warning(f"获取按钮class时出错: {str(e)}")

            # 检查按钮是否被禁用
            button_class = next_page_button.get_attribute("class") or ""
            if "disabled" in button_class:
                logger.info("下一页按钮被禁用，确认已到最后一页")
                return False

            # 点击下一页按钮
            logger.info("点击下一页按钮")
            next_page_button.click()

            # 等待页面网络空闲
            try:
                self.page.wait_for_load_state('networkidle', timeout=1000)
            except Exception as e:
                logger.warning(f"等待网络空闲时出错: {str(e)}")

            # 检查是否有新的API响应
            api_count_after = len(self.api_data)

            # 输出详细的API数据信息用于调试
            logger.info(f"点击前API数量: {api_count_before}, 点击后API数量: {api_count_after}")

            if api_count_after > api_count_before:
                logger.info(f"成功获取到新的API数据，数量: {api_count_after - api_count_before}")
                return True
            else:
                logger.info("点击后没有新的API响应，可能已到最后一页")
                return False

        except Exception as e:
            logger.error(f"点击下一页时出错: {str(e)}")
            return False

    def _process_notes_type_data(self, action_type, selector, note_type, note_type1, option_text=None,
                                 wait_time=(5, 10), url=None):
        try:
            # 清空API数据
            self.api_data.clear()
            self.common.random_sleep(10, 15)

            # 根据action_type执行不同的操作
            if action_type == 'click_button':
                # 点击按钮操作
                dropdown_container = self.page.locator('.d-spinner-nested-loading')
                switch_button = dropdown_container.locator(selector).first
                if not switch_button.is_visible(timeout=5000):
                    logger.warning(f"按钮不可见: {selector}")
                    return False
                switch_button.click()
                logger.info(f"成功点击按钮: {selector}")

            elif action_type == 'select_dropdown':
                # 下拉框选择操作
                dropdown_element = self.page.locator(selector)
                if not dropdown_element.is_visible(timeout=5000):
                    logger.warning(f"下拉框不可见: {selector}")
                    return False
                dropdown_element.click()

                # 等待下拉选项出现
                self.common.random_sleep(1, 2)

                # 选择指定选项
                if option_text:
                    option_element = self.page.locator(f"text=/^{option_text}$/").first
                    if not option_element.is_visible(timeout=3000):
                        logger.warning(f"选项不可见: {option_text}")
                        return False
                    option_element.click()
                    logger.info(f"成功选择选项: {option_text}")
            else:
                logger.warning(f"不支持的操作类型: {action_type}")
                return False

            # 等待页面加载完成
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.warning(f"等待页面加载完成时出错: {str(e)}")

            # 等待一段时间让API数据加载
            if action_type == 'select_dropdown':
                self.common.random_sleep(*wait_time)

            # 处理API数据
            notes_rate_copy = dict(self.api_data)
            for api_url, response_data in notes_rate_copy.items():
                try:
                    if not response_data or not isinstance(response_data, dict):
                        continue

                    if 'notes_rate' in api_url and 'data' in response_data:
                        api_data = response_data.get('data', {})
                        if api_data and isinstance(api_data, dict):
                            # 确保价格数据已获取
                            graphic_price = self.current_user_data.get('graphic_price', 0)
                            video_price = self.current_user_data.get('video_price', 0)
                            self._process_notes_rate(api_data, graphic_price, video_price, note_type, note_type1, url,
                                                     1, 3, 1, 0)
                            logger.info(f"成功处理{note_type}数据")
                            return True
                except Exception as e:
                    logger.warning(f"处理{note_type}API时出错: {str(e)}")
                    continue

            logger.warning(f"未找到{note_type}的API数据")
            return False

        except Exception as e:
            logger.error(f"处理{note_type}时出错: {str(e)}")
            return False

    def _validate_api_data(self, api_data, required_fields=None):
        """
        验证API数据的有效性

        Args:
            api_data: API响应数据
            required_fields: 必需字段列表

        Returns:
            bool: 数据是否有效
        """
        try:
            if not isinstance(api_data, dict):
                logger.warning("API数据不是字典格式")
                return False

            if api_data.get('code') != 0:
                logger.warning(f"API响应码异常: {api_data.get('code')}")
                return False

            if 'data' not in api_data:
                logger.warning("API数据缺少data字段")
                return False

            if required_fields:
                data = api_data.get('data', {})
                missing_fields = [field for field in required_fields if field not in data]
                if missing_fields:
                    logger.warning(f"API数据缺少必需字段: {missing_fields}")
                    return False

            return True

        except Exception as e:
            logger.error(f"验证API数据时出错: {str(e)}")
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

            if hasattr(self, 'page'):
                self.page.close()
            if hasattr(self, 'context'):
                self.context.close()
            if hasattr(self, 'browser'):
                self.browser.close()
            if hasattr(self, 'playwright'):
                self.playwright.stop()

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

    def sync_record_creator_data(self, payload):
        try:
            print(payload)
            url = "https://tianji.fangpian999.com/api/admin/creatorSign/recordCreatorData"
            headers = {"Content-Type": "application/json"}

            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload, ensure_ascii=False),
                timeout=30
            )

            if response.status_code == 200:
                logger.info(response.json())
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
            url = response.url
            # 从配置获取需要捕获的API路径
            target_apis = self.config['API_TARGETS']

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                try:
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