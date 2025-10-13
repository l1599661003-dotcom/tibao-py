import json
import os
import time
from datetime import datetime
import sys
from pathlib import Path
import random
import functools
from tenacity import retry, stop_after_attempt, wait_exponential

import keyboard
import pandas as pd
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from core.database_text_fangpian import session
from unitl.common import Common
from models.models_tibao import Base, DouYinAuthorInfo, DouYinVideoInfo

"""
    获取抖音博主的信息和视频数据
"""


def retry_on_exception(max_attempts=3, initial_wait=1):
    """重试装饰器，处理网络请求等可能失败的操作"""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=initial_wait, min=1, max=10),
        reraise=True
    )


class DouYinSpider:
    def __init__(self):
        self.logger = logger.bind(class_name=self.__class__.__name__)
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://trendinsight.oceanengine.com/arithmetic-index/daren/detail"
        self.is_logged_in = False
        self.found_match = False  # 添加标志位作为类属性
        self.api_data = {}  # 存储API数据
        self.cookie_file = os.path.join(self.data_dir, 'cookies.json')
        self.progress_file = os.path.join(self.data_dir, 'scraping_progress.json')
        self.common = Common()
        self.api_data_buffer = []  # 用于缓存API数据
        self.current_kol = None  # 当前正在处理的KOL信息
        self.processed_api_responses = set()  # 用于追踪已处理的API响应
        self.monthly_count = 0  # 当月计数器
        self.marketing_info = {}  # 存储营销信息
        self.current_record_id = None  # 存储当前处理的记录ID
        self.current_profile_url = None  # 存储当前博主的主页URL
        self.session = session  # 直接使用导入的 session 对象

        # 设置Playwright
        os.environ['PLAYWRIGHT_BROWSERS_PATH'] = self.get_playwright_driver_path()
        driver_path = self.get_playwright_driver_path()
        if not os.path.exists(driver_path):
            os.makedirs(driver_path, exist_ok=True)

        self.setup_browser()
        self.notes = []
        self.stopScroll = False
        self.monitor_data = {
            'fail_count': 0,
            'total_count': 0,
            'completed_count': 0,
            'process': 0
        }

    def get_executable_path(self):
        """获取可执行文件路径"""
        if getattr(sys, 'frozen', False):
            # 如果是打包后的exe
            return os.path.dirname(sys.executable)
        else:
            # 如果是开发环境
            return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def get_playwright_driver_path(self):
        """获取Playwright驱动路径"""
        if getattr(sys, 'frozen', False):
            # 如果是打包后的exe
            return os.path.join(self.get_executable_path(), '.cache', 'ms-playwright')
        else:
            # 如果是开发环境
            return os.path.join(Path.home(), '.cache', 'ms-playwright')

    def setup_browser(self):
        """初始化浏览器"""
        try:
            # 检查并安装浏览器
            import subprocess
            import sys

            try:
                # 使用 python -m playwright install chromium 安装浏览器
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                               check=True,
                               capture_output=True)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"安装浏览器时出错: {e.stderr.decode()}")
                raise Exception("浏览器安装失败")

            self.playwright = sync_playwright().start()

            # 配置浏览器选项
            browser_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-extensions',
                '--disable-popup-blocking',
                '--disable-notifications',
                '--disable-infobars'
            ]

            browser_options = {
                'headless': False,
                'args': browser_args,
                'ignore_default_args': ['--enable-automation']
            }

            self.browser = self.playwright.chromium.launch(**browser_options)

            # 创建上下文
            context_options = {
                'viewport': {'width': 1280, 'height': 800},
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'bypass_csp': True,  # 绕过内容安全策略
                'ignore_https_errors': True,  # 忽略HTTPS错误
                'java_script_enabled': True,  # 启用JavaScript
                'has_touch': False,  # 禁用触摸
                'is_mobile': False  # 非移动设备
            }

            self.context = self.browser.new_context(**context_options)

            # 设置请求拦截和超时
            self.context.set_default_timeout(30000)  # 设置默认超时时间为30秒
            self.context.route("**/*", lambda route: route.continue_())

            # 尝试加载已保存的Cookie
            if self._load_cookies():
                try:
                    self.page = self.context.new_page()
                    self.page.goto(self.base_url)
                    self.common.random_sleep(2, 3)

                    # 检查是否存在用户头像元素
                    userSide = self.page.locator(".avatarContainer-fanphD").all()
                    user_len = len(userSide)
                    self.logger.info(f"检测到用户头像元素数量: {user_len}")

                    if user_len > 0 and self.page.locator(".avatarContainer-fanphD").is_visible(
                            timeout=5000):
                        self.is_logged_in = True
                        self.logger.info("Cookie验证成功，已自动登录")
                    else:
                        self.logger.info("Cookie已失效，需要重新登录")
                except Exception as e:
                    self.logger.error(f"验证Cookie时出错: {str(e)}")
                    self.logger.info("Cookie验证失败，需要重新登录")
            else:
                self.page = self.context.new_page()
                self.logger.info("创建新页面成功")

            # 设置页面事件监听
            self.page.on("response", self._handle_api_response)
            self.page.on("pageerror", lambda err: self.logger.error(f"页面错误: {err}"))

            return True

        except Exception as e:
            self.logger.error(f"初始化浏览器时出错: {str(e)}")
            self._cleanup_browser_resources()
            raise Exception("浏览器初始化失败")

    def _cleanup_browser_resources(self):
        """清理浏览器资源"""
        try:
            if hasattr(self, 'page') and self.page:
                self.page.close()
            if hasattr(self, 'context') and self.context:
                self.context.close()
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()
        except Exception as e:
            self.logger.error(f"清理浏览器资源时出错: {str(e)}")

    def login(self):
        """
        使用扫码方式登录小红书
        :return: 是否登录成功
        """
        try:
            if self.is_logged_in:
                self.logger.info("已处于登录状态")
                return True

            self.logger.info("开始登录流程...")
            # 访问首页前等待
            self.common.random_sleep(2, 3)

            self.common.random_sleep(2, 3)
            # 输入账号密码
            self.page.fill("input[placeholder='请输入手机号码']", '18848131546')
            self.common.random_sleep(2, 3)
            get_button = self.page.wait_for_selector("text=发送验证码", timeout=10000)
            if not get_button:
                self.logger.error("未找到验证码按钮")
                return False
            # 点击登录按钮前等待
            self.common.random_sleep(1, 2)
            get_button.click()
            time.sleep(20)
            self.common.random_sleep(1, 2)
            # 勾选协议
            self.page.click('.account-center-agreement-check')
            time.sleep(1)

            # 点击登录按钮
            denglu = self.page.wait_for_selector(".account-center-submit", timeout=10000)
            denglu.click()

            try:
                # 等待个人头像出现，表示登录成功
                avatar = self.page.wait_for_selector(".avatarContainer-fanphD", timeout=60000)  # 给用户60秒扫码时间
                if avatar:
                    self.logger.info("登录成功！")
                    self.is_logged_in = True

                    # 登录成功后等待一段时间再保存Cookie
                    self.common.random_sleep(3, 5)
                    # 保存Cookie
                    self._save_cookies()

                    return True
                else:
                    self.logger.error("登录失败，未检测到登录成功状态")
                    return False
            except PlaywrightTimeoutError:
                self.logger.error("登录超时，请重试")
                return False

        except Exception as e:
            self.logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def scrape_user_notes(self, kol_id):
        """抓取指定KOL的笔记信息并匹配更新数据库"""
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return False

            # 重置标志位
            self.found_match = False
            self.api_data_buffer = []  # 清空之前的数据缓存
            self.processed_api_responses.clear()
            self.monthly_count = 0
            self.marketing_info = {}
            
            self.common.random_sleep(1, 2)
            return self.search_and_get_author_data(kol_id)

        except Exception as e:
            self.logger.error(f"抓取KOL {kol_id} 笔记时出错: {str(e)}")
            return False

    def update_monitor_status(self, **kwargs):
        """更新监控状态"""
        logger.info(kwargs)
        if kwargs.get('completed_count'):
            self.monitor_data['completed_count'] = kwargs.get('completed_count')
        if kwargs.get('fail_count'):
            self.monitor_data['fail_count'] = kwargs.get('fail_count')

    def save_data(self, user_id, data):
        """
        保存抓取的数据到CSV文件
        """
        try:
            filename = os.path.join(self.data_dir, f'user_{user_id}_{datetime.now().strftime("%Y%m%d")}.csv')
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"数据已保存到 {filename}")
        except Exception as e:
            logger.error(f"保存数据时出错: {str(e)}")

    def close(self):
        """
        关闭浏览器和playwright
        """
        try:
            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 关闭浏览器资源
            if hasattr(self, 'page'):
                try:
                    self.page.close()
                except Exception as e:
                    self.logger.error(f"关闭页面时出错: {str(e)}")

            if hasattr(self, 'context'):
                try:
                    self.context.close()
                except Exception as e:
                    self.logger.error(f"关闭上下文时出错: {str(e)}")

            if hasattr(self, 'browser'):
                try:
                    self.browser.close()
                except Exception as e:
                    self.logger.error(f"关闭浏览器时出错: {str(e)}")

            if hasattr(self, 'playwright'):
                try:
                    self.playwright.stop()
                except Exception as e:
                    self.logger.error(f"停止playwright时出错: {str(e)}")

            self.session.close()
            self.logger.info("所有资源已正确关闭")
        except Exception as e:
            self.logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if 'api/v2/daren/get_author_info' in url:
                try:
                    data = response.json()
                    if data and isinstance(data, dict):
                        if self._save_author_info(data['data']):
                            # 如果成功保存作者信息，设置标志位
                            self.found_match = True
                except Exception as e:
                    self.logger.error(f"处理作者信息时出错: {str(e)}")

            # 暂时注释掉视频信息的获取
            # elif 'api/v2/daren/get_great_user_top_video' in url:
            #     try:
            #         data = response.json()
            #         if data and isinstance(data, dict):
            #             self._save_video_info(data['data'])
            #     except Exception as e:
            #         self.logger.error(f"处理视频信息时出错: {str(e)}")

        except Exception as e:
            self.logger.error(f"处理API响应时出错: {str(e)}")

    def _save_author_info(self, data):
        """保存作者信息到数据库"""
        try:
            # 首先查询是否存在该记录
            existing_author = self.session.query(DouYinAuthorInfo).filter_by(
                user_id=data.get('aweme_id')
            ).first()

            # 准备要更新的数据
            author_data = {
                'user_name': data.get('user_name'),
                'user_head_logo': data.get('user_head_logo'),
                'user_introduction': data.get('user_introduction'),
                'fans_count': data.get('fans_count'),
                'like_count': data.get('like_count'),
                'item_count': data.get('item_count'),
                'first_tag_name': data.get('first_tag_name'),
                'second_tag_name': data.get('second_tag_name'),
                'user_aweme_url': data.get('user_aweme_url'),
                'profile_url': self.current_profile_url,
                'update_time': datetime.now()
            }

            if existing_author:
                # 如果记录存在，更新数据
                for key, value in author_data.items():
                    setattr(existing_author, key, value)
                self.logger.info(f"更新作者信息: {data.get('user_name')}")
            else:
                # 如果记录不存在，创建新记录
                author_data['user_id'] = data.get('aweme_id')
                author_data['create_time'] = datetime.now()
                new_author = DouYinAuthorInfo(**author_data)
                self.session.add(new_author)
                self.logger.info(f"创建新作者信息: {data.get('user_name')}")

            self.session.commit()
            return True

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"保存作者信息时出错: {str(e)}")
            if hasattr(e, '__cause__'):
                self.logger.error(f"具体错误: {str(e.__cause__)}")
            return False

    def _save_video_info(self, data):
        """保存视频信息到数据库"""
        try:
            if isinstance(data, list):
                for video_data in data:
                    video = DouYinVideoInfo(
                        user_id=video_data.get('user_id'),
                        picture=video_data.get('picture'),
                        video_text=video_data.get('video_text'),
                        like_cnt=video_data.get('like_cnt'),
                        coment_cnt=video_data.get('coment_cnt'),
                        share_cnt=video_data.get('share_cnt'),
                        follow_cnt=video_data.get('follow_cnt'),
                        video_url=video_data.get('video_url'),
                        rank=video_data.get('rank'),
                        item_id=video_data.get('item_id'),
                        create_time=datetime.fromtimestamp(int(video_data.get('create_time', 0)))
                    )
                    self.session.merge(video)
                
                self.session.commit()
                self.logger.info(f"成功保存 {len(data)} 条视频信息")

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"保存视频信息时出错: {str(e)}")

    def _save_cookies(self):
        """保存当前会话的Cookie"""
        try:
            if not hasattr(self, 'context') or not self.context:
                self.logger.error("浏览器上下文不存在，无法保存Cookie")
                return False

            cookies = self.context.cookies()
            if not cookies:
                self.logger.warning("没有Cookie数据可保存")
                return False

            # 确保目录存在
            os.makedirs(os.path.dirname(self.cookie_file), exist_ok=True)

            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            self.logger.info(f"成功保存 {len(cookies)} 个Cookie到文件")
            return True
        except Exception as e:
            self.logger.error(f"保存Cookie时出错: {str(e)}")
            return False

    def _load_cookies(self):
        """加载保存的Cookie
        :return: 是否成功加载Cookie
        """
        try:
            if not os.path.exists(self.cookie_file):
                self.logger.info("Cookie文件不存在")
                return False

            with open(self.cookie_file, 'r', encoding='utf-8') as f:
                cookies = json.load(f)

            if not cookies:
                self.logger.warning("Cookie文件为空")
                return False

            # 验证Cookie格式
            required_fields = {'name', 'value', 'domain'}
            for cookie in cookies:
                if not all(field in cookie for field in required_fields):
                    self.logger.error("Cookie数据格式不正确")
                    return False

            self.context.add_cookies(cookies)
            return True
        except json.JSONDecodeError:
            self.logger.error("Cookie文件格式不正确")
            return False
        except Exception as e:
            self.logger.error(f"加载Cookie时出错: {str(e)}")
            return False

    def _save_progress(self, user_id, processed_notes):
        """保存抓取进度
        :param user_id: 用户ID
        :param processed_notes: 已处理的笔记列表
        """
        try:
            if not user_id or not isinstance(processed_notes, list):
                self.logger.error("保存进度参数无效")
                return False

            progress_data = {
                'user_id': user_id,
                'processed_notes': processed_notes,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': len(processed_notes),
                'last_note_id': processed_notes[-1] if processed_notes else None
            }

            # 确保目录存在
            os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)

            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"进度已保存: 用户{user_id}, 已处理{len(processed_notes)}条笔记")
            return True
        except Exception as e:
            self.logger.error(f"保存进度时出错: {str(e)}")
            return False

    def _load_progress(self):
        """加载抓取进度
        :return: 进度数据字典或空字典
        """
        try:
            if not os.path.exists(self.progress_file):
                self.logger.info("进度文件不存在")
                return {}

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)

            # 验证进度数据格式
            required_fields = {'user_id', 'processed_notes', 'timestamp'}
            if not all(field in progress_data for field in required_fields):
                self.logger.error("进度文件格式不正确")
                return {}

            self.logger.info(f"成功加载进度数据: 用户{progress_data['user_id']}, "
                             f"已处理{len(progress_data['processed_notes'])}条笔记, "
                             f"最后更新时间: {progress_data['timestamp']}")
            return progress_data
        except json.JSONDecodeError:
            self.logger.error("进度文件格式不正确")
            return {}
        except Exception as e:
            self.logger.error(f"加载进度时出错: {str(e)}")
            return {}

    def search_and_get_author_data(self, author_name):
        """搜索博主并获取数据"""
        try:
            if not self.is_logged_in:
                if not self.login():
                    self.logger.error("登录失败")
                    return False

            # 等待搜索框出现
            search_box = self.page.wait_for_selector('.byted-select-input-content-wrapper', timeout=10000)
            if not search_box:
                self.logger.error("未找到搜索框")
                return False

            # 点击搜索框
            search_box.click()
            self.common.random_sleep(1, 2)

            # 清空搜索框并输入搜索内容
            search_input = self.page.locator('.byted-select-input-content-wrapper input')
            search_input.fill('')  # 清空搜索框
            self.common.random_sleep(0.5, 1)
            search_input.fill(author_name)  # 输入搜索内容
            
            self.common.random_sleep(2, 3)

            # 等待搜索结果出现并点击第一个结果
            # first_result = self.page.locator('.ReactVirtualized__Grid__innerScrollContainer div').all()
            first_result = self.page.wait_for_selector('.byted-list-item-container.byted-list-item-container-size-md.byted-list-item-container-level-1.byted-auto-complete-option-container')
            if not first_result:
                self.logger.error("未找到搜索结果")
                return False
            self.common.random_sleep(1, 1)
            first_result.click()
            self.common.random_sleep(2, 3)

            # 保存当前页面URL
            self.current_profile_url = self.page.url
            self.logger.info(f"已进入博主主页: {self.current_profile_url}")

            # 等待API数据加载和处理
            wait_time = 0
            max_wait_time = 30
            while not self.found_match and wait_time < max_wait_time:
                self.common.random_sleep(2, 3)
                wait_time += 3
                if self.found_match:
                    break

            return self.found_match

        except Exception as e:
            self.logger.error(f"搜索博主时出错: {str(e)}")
            return False

    def _get_month_range(self, date_str):
        """获取给定日期所在月份的起止时间
        :param date_str: 格式为 'YYYY-MM-DD' 的日期字符串
        :return: (月初时间戳, 月末时间戳)
        """
        try:
            from datetime import datetime, time
            import calendar

            # 解析日期
            date = datetime.strptime(date_str, '%Y-%m-%d')

            # 获取当月第一天
            first_day = date.replace(day=1)
            first_day = datetime.combine(first_day, time.min)  # 设置为0点

            # 获取当月最后一天
            last_day = date.replace(day=calendar.monthrange(date.year, date.month)[1])
            last_day = datetime.combine(last_day, time.max)  # 设置为23:59:59

            return first_day.timestamp(), last_day.timestamp()
        except Exception as e:
            self.logger.error(f"计算月份范围时出错: {str(e)}")
            return None, None
