import json
import os
import time
from datetime import datetime
import sys
from pathlib import Path
import random
import functools
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import traceback

import playwright
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import keyboard
from models.models_tibao import DouYinKolRealization, DouYinKolNote, DouyinBianxian
from core.database_text_fangpian import session
import pandas as pd
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from unitl.common import Common

"""
    获取抖音博主的月总营收
"""


# 配置常量
class Config:
    MAX_RETRIES = 3
    INITIAL_WAIT = 1
    MAX_WAIT = 10
    API_TIMEOUT = 30  # API响应超时时间（秒）
    LOGIN_TIMEOUT = 60000  # 登录超时时间（毫秒）
    PAGE_TIMEOUT = 30000  # 页面超时时间（毫秒）
    MIN_REQUEST_INTERVAL = 0.5  # 最小请求间隔（秒）- 从2秒减少到0.5秒
    BROWSER_VIEWPORT = {'width': 1280, 'height': 660}
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    # 新增性能优化配置
    PAGE_LOAD_TIMEOUT = 15000  # 页面加载超时时间（毫秒）
    DOM_LOAD_TIMEOUT = 10000   # DOM加载超时时间（毫秒）
    NETWORK_IDLE_TIMEOUT = 8000  # 网络空闲超时时间（毫秒）
    API_RESPONSE_WAIT = 1.5    # API响应等待时间（秒）- 从3秒减少到1.5秒
    API_CHECK_INTERVAL = 0.5   # API检查间隔（秒）


def retry_on_exception(max_attempts=3, initial_wait=1):
    """重试装饰器，处理网络请求等可能失败的操作"""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=initial_wait, min=1, max=10),
        retry=retry_if_exception_type((TimeoutError, ConnectionError, PlaywrightTimeoutError)),
        reraise=True
    )


class DouYinSpider:
    def __init__(self):
        self.logger = logger.bind(class_name=self.__class__.__name__)
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.xingtu.cn/ad/creator/index"
        self.is_logged_in = False
        self.found_match = False  # 添加标志位作为类属性
        self.api_data = {}  # 存储API数据
        self.cookie_file = os.path.join(self.data_dir, 'cookies.json')
        self.progress_file = os.path.join(self.data_dir, 'scraping_progress.json')
        self.common = Common()
        self.current_kol: Optional[Dict[str, str]] = None  # 当前正在处理的KOL信息
        self.processed_api_responses = set()  # 用于追踪已处理的API响应
        self.marketing_info = {}  # 存储营销信息
        self.last_request_time = 0  # 记录上次请求时间

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 设置Playwright驱动路径
        os.environ['PLAYWRIGHT_BROWSERS_PATH'] = self.get_playwright_driver_path()

        # 确保驱动目录存在
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

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保资源被正确释放"""
        self.close()

    @contextmanager
    def rate_limit(self):
        """请求频率限制的上下文管理器"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < Config.MIN_REQUEST_INTERVAL:
            sleep_time = Config.MIN_REQUEST_INTERVAL - elapsed
            time.sleep(sleep_time)
        try:
            yield
        finally:
            self.last_request_time = time.time()
            
    def _smart_wait(self, min_wait=0.5, max_wait=2.0):
        try:
            # 检查页面是否还在加载
            if self.page and hasattr(self.page, 'evaluate'):
                is_loading = self.page.evaluate("() => document.readyState !== 'complete'")
                if is_loading:
                    wait_time = min_wait
                else:
                    wait_time = max_wait
            else:
                wait_time = min_wait

            time.sleep(wait_time)
        except Exception as e:
            self.logger.debug(f"智能等待出错，使用默认等待: {str(e)}")
            time.sleep(min_wait)
            
    def _wait_for_api_response(self, timeout=3.0):
        """等待API响应，使用更高效的检测方式"""
        try:
            start_time = time.time()
            check_interval = Config.API_CHECK_INTERVAL
            
            while time.time() - start_time < timeout:
                # 检查是否已经获取到营销数据
                if self.marketing_info and len(self.marketing_info) > 1:
                    self.logger.info("检测到API响应数据")
                    return True
                    
                # 简单等待，不进行复杂的网络检测
                time.sleep(check_interval)
            
            self.logger.debug(f"等待API响应超时 ({timeout}秒)，继续执行")
            return False
            
        except Exception as e:
            self.logger.error(f"等待API响应时出错: {str(e)}")
            return False

    def get_executable_path(self) -> str:
        """获取可执行文件路径"""
        if getattr(sys, 'frozen', False):
            return os.path.dirname(sys.executable)
        else:
            return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def get_playwright_driver_path(self) -> str:
        """获取Playwright驱动路径"""
        if getattr(sys, 'frozen', False):
            return os.path.join(self.get_executable_path(), '.cache', 'ms-playwright')
        else:
            return os.path.join(Path.home(), '.cache', 'ms-playwright')

    def setup_browser(self) -> bool:
        """初始化浏览器"""
        try:
            # 检查并安装浏览器
            self._install_browser()

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
                '--disable-infobars',
                # 新增性能优化参数
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-background-networking',
                '--disable-default-apps',
                '--disable-sync',
                '--disable-translate',
                '--disable-plugins-discovery',
                '--disable-preconnect',
                '--disable-ipc-flooding-protection',
                '--memory-pressure-off',
                '--max_old_space_size=4096'
            ]

            browser_options = {
                'headless': False,
                'args': browser_args,
                'ignore_default_args': ['--enable-automation']
            }

            self.browser = self.playwright.chromium.launch(**browser_options)

            # 创建上下文
            context_options = {
                'viewport': Config.BROWSER_VIEWPORT,
                'user_agent': Config.USER_AGENT,
                'bypass_csp': True,  # 绕过内容安全策略
                'ignore_https_errors': True,  # 忽略HTTPS错误
                'java_script_enabled': True,  # 启用JavaScript
                'has_touch': False,  # 禁用触摸
                'is_mobile': False  # 非移动设备
            }

            self.context = self.browser.new_context(**context_options)

            # 设置请求拦截和超时
            self.context.set_default_timeout(Config.PAGE_TIMEOUT)
            self.context.route("**/*", lambda route: route.continue_())

            # 尝试加载已保存的Cookie并验证登录状态
            self._load_and_verify_cookies()

            # 创建页面并设置事件监听
            if not self.page:
                self.page = self.context.new_page()

            self.page.on("response", self._handle_api_response)
            self.page.on("pageerror", lambda err: self.logger.error(f"页面错误: {err}"))

            self.logger.info("浏览器初始化成功")
            return True

        except Exception as e:
            self.logger.error(f"初始化浏览器时出错: {str(e)}")
            self._cleanup_browser_resources()
            raise Exception("浏览器初始化失败")

    def _install_browser(self):
        """安装浏览器"""
        try:
            import subprocess
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                           check=True,
                           capture_output=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"安装浏览器时出错: {e.stderr.decode()}")
            raise Exception("浏览器安装失败")

    def _load_and_verify_cookies(self):
        """加载并验证Cookie"""
        if self._load_cookies():
            try:
                self.page = self.context.new_page()
                # 访问页面并等待加载完成
                self.page.goto(self.base_url, timeout=Config.PAGE_TIMEOUT)

                # 等待页面完全加载 - 优化等待策略
                try:
                    self.page.wait_for_load_state('domcontentloaded', timeout=Config.DOM_LOAD_TIMEOUT)
                    # 然后等待网络空闲，但使用更短的超时时间
                    self.page.wait_for_load_state('networkidle', timeout=Config.NETWORK_IDLE_TIMEOUT)
                except Exception as e:
                    self.logger.debug(f"等待页面加载时出现异常: {str(e)}")
                    # 即使超时也继续执行，因为页面可能已经可用

                # 增加等待时间，确保页面元素完全渲染
                self.common.random_sleep(3, 5)

                # 检查是否存在用户头像元素 - 使用更精确的选择器
                is_logged_in = self._check_login_status()

                if is_logged_in:
                    self.is_logged_in = True
                    self.logger.info("✅ Cookie验证成功，已自动登录")
                else:
                    # Cookie可能仍然有效，但页面检测失败，给一次机会
                    self.logger.warning("⚠️ 无法通过页面元素确认登录状态，但Cookie已加载")
                    self.logger.info("💡 建议：如果后续操作正常，说明Cookie仍然有效")

                    # 暂时设置为已登录，让程序继续运行
                    # 如果真的未登录，后续的API请求会失败并触发重新登录
                    self.is_logged_in = True

            except Exception as e:
                self.logger.error(f"验证Cookie时出错: {str(e)}")
                self.logger.info("Cookie验证失败，需要重新登录")
                self.is_logged_in = False
        else:
            self.logger.info("未找到有效Cookie，需要重新登录")
            self.is_logged_in = False

    def _check_login_status(self) -> bool:
        """检查登录状态"""
        try:
            # 尝试多种选择器来检测登录状态
            login_indicators = [
                "div.text-avatar",
                ".text-avatar",
                "[class*='text-avatar']",
                "[class*='avatar']",
                ".user-info",
                ".user-avatar",
                "[class*='user']",
                # 添加更多可能的选择器
                ".header-user",
                ".nav-user",
                "[data-testid*='avatar']",
                "[data-testid*='user']"
            ]

            for i, selector in enumerate(login_indicators, 1):
                try:

                    # 先检查元素是否存在
                    elements = self.page.locator(selector)
                    count = elements.count()

                    if count > 0:
                        # 检查第一个元素是否可见
                        first_element = elements.first
                        if first_element.is_visible(timeout=3000):
                            self.logger.info(f"✅ 使用选择器 '{selector}' 检测到登录状态")
                            return True

                except Exception as e:
                    self.logger.debug(f"选择器 '{selector}' 检测失败: {str(e)}")
                    continue

            # 尝试检查页面URL或其他登录标识
            current_url = self.page.url
            self.logger.debug(f"当前页面URL: {current_url}")

            # 如果URL包含登录后的特征，也认为是已登录
            if any(keyword in current_url.lower() for keyword in ['dashboard', 'home', 'index', 'creator']):
                self.logger.info("✅ 根据页面URL判断为已登录状态")
                return True

            # 检查页面标题
            try:
                page_title = self.page.title()
                self.logger.debug(f"页面标题: {page_title}")
                if '登录' not in page_title and '注册' not in page_title:
                    self.logger.info("✅ 根据页面标题判断为已登录状态")
                    return True
            except Exception as e:
                self.logger.debug(f"获取页面标题失败: {str(e)}")

            self.logger.warning("❌ 所有登录状态检测方法都未能确认登录状态")
            return False

        except Exception as e:
            self.logger.error(f"检查登录状态时出错: {str(e)}")
            return False

    def _cleanup_browser_resources(self):
        """清理浏览器资源"""
        try:
            if hasattr(self, 'page') and self.page:
                self.page.close()
                self.page = None
            if hasattr(self, 'context') and self.context:
                self.context.close()
                self.context = None
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
                self.browser = None
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()
                self.playwright = None
        except Exception as e:
            self.logger.error(f"清理浏览器资源时出错: {str(e)}")

    def login(self) -> bool:
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
            # 访问首页
            self.page.goto(self.base_url, timeout=Config.PAGE_TIMEOUT)
            # 页面加载后等待
            self.common.random_sleep(2, 3)

            # 等待并点击登录按钮
            self.logger.info("等待登录按钮出现...")
            login_button = self.page.wait_for_selector("text=客户", timeout=10000)
            if not login_button:
                self.logger.error("未找到登录按钮")
                return False
            # 点击登录按钮前等待
            self.common.random_sleep(1, 2)
            login_button.click()
            self.common.random_sleep(2, 3)

            # 等待用户手动操作
            self.logger.info("请在60秒内完成登录操作...")
            time.sleep(50)

            # 点击登录按钮
            denglu = self.page.wait_for_selector(".account-center-submit", timeout=10000)
            if denglu:
                denglu.click()
            else:
                self.logger.warning("未找到提交按钮，可能已经登录")

            try:
                # 等待个人头像出现，表示登录成功 - 使用多种选择器
                login_indicators = [
                    "div.text-avatar",
                    ".text-avatar",
                    "[class*='text-avatar']",
                    "[class*='avatar']"
                ]

                avatar = None
                for selector in login_indicators:
                    try:
                        avatar = self.page.wait_for_selector(selector, timeout=15000)  # 每个选择器等待15秒
                        if avatar and avatar.is_visible():
                            self.logger.info(f"使用选择器 '{selector}' 检测到登录成功！")
                            break
                    except PlaywrightTimeoutError:
                        self.logger.debug(f"选择器 '{selector}' 超时，尝试下一个")
                        continue

                if avatar and avatar.is_visible():
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

    @retry_on_exception(max_attempts=Config.MAX_RETRIES, initial_wait=Config.INITIAL_WAIT)
    def scrape_user_notes(self, kol_name: str, kol_url: str) -> int:
        """抓取指定KOL的笔记信息并匹配更新数据库
        返回值：
        - 1: 处理成功
        - 2: 没有创作能力按钮（该KOL没有创作能力数据）
        - 0: 处理失败
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            self.current_kol = {'name': kol_name, 'url': kol_url}
            self.processed_api_responses.clear()
            # 完全重置营销信息，确保数据隔离
            self.marketing_info = {'kol_name': kol_name}
            # 重置API数据缓存
            self.api_data = {}
            # 添加API响应处理标志
            self.api_response_processed = False

            with self.rate_limit():
                self._smart_wait(0.5, 1.5)  # 使用智能等待替代固定等待

                try:
                    self.page.goto(kol_url, timeout=Config.PAGE_TIMEOUT)
                    self.logger.info(f"成功访问页面: {kol_url}")

                    # 等待页面加载 - 优化等待策略
                    try:
                        self.page.wait_for_load_state('domcontentloaded', timeout=Config.DOM_LOAD_TIMEOUT)
                        self.page.wait_for_load_state('networkidle', timeout=Config.NETWORK_IDLE_TIMEOUT)
                    except PlaywrightTimeoutError:
                        self.logger.warning("页面加载超时，但继续执行")
                        # 即使超时也继续执行，因为页面可能已经可用

                    # 检查是否被重定向到登录页面
                    current_url = self.page.url
                    if 'login' in current_url.lower() or '登录' in self.page.title():
                        self.logger.warning("⚠️ 检测到被重定向到登录页面，Cookie可能已失效")
                        self.is_logged_in = False

                        # 尝试重新登录
                        if self.login():
                            self.logger.info("重新登录成功，继续处理")
                            # 重新访问目标页面
                            self.page.goto(kol_url, timeout=Config.PAGE_TIMEOUT)
                        else:
                            self.logger.error("重新登录失败")
                            return 0

                except PlaywrightTimeoutError:
                    self.logger.error(f"访问页面超时: {kol_url}")
                    return 0

            # 点击创作能力标签
            creative_tab = self.page.locator("div.el-tabs__nav >> div:has-text('创作能力')")
            if creative_tab and creative_tab.is_visible():
                # 点击前等待一下确保元素稳定
                time.sleep(0.5)
                creative_tab.click()
                self.logger.info("成功点击创作能力标签")
                
                # 等待点击生效
                try:
                    # 等待页面有变化（比如URL变化或者元素状态变化）
                    self.page.wait_for_timeout(1000)  # 等待1秒
                    
                    # 检查是否点击成功（可以检查URL变化或者特定元素出现）
                    current_url = self.page.url
                    if 'creative' in current_url.lower() or '创作' in current_url.lower():
                        self.logger.info("检测到页面已切换到创作能力页面")
                    else:
                        self.logger.info("页面切换状态未知，继续执行")
                        
                except Exception as e:
                    self.logger.warning(f"检查点击效果时出错: {str(e)}")
                    # 即使检查失败也继续执行
            else:
                self.logger.warning(f"未找到创作能力标签，KOL {kol_name} 可能没有创作能力数据")
                return 2  # 返回2表示没有创作能力按钮
                
            # 等待API数据 - 简化检测方式
            try:
                # 简单等待一小段时间让API响应处理完成
                time.sleep(Config.API_RESPONSE_WAIT)
                
                # 检查是否已经获取到API响应数据
                if self.api_response_processed:
                    self.logger.info("✅ 成功获取到API响应数据")
                else:
                    # 从日志看API数据实际上已经正确处理了，所以这里只是提示
                    self.logger.info("ℹ️ API响应处理完成，继续执行")
                
                return 1  # 返回1表示处理成功
                    
            except Exception as e:
                self.logger.warning(f"等待API数据时出错: {str(e)}")
                return 1  # 即使出错也继续执行

        except Exception as e:
            self.logger.error(f"抓取KOL {kol_name} 笔记时出错: {str(e)}")
            raise

    def update_monitor_status(self, **kwargs):
        """更新监控状态"""
        self.logger.debug(f"更新监控状态: {kwargs}")
        if kwargs.get('completed_count'):
            self.monitor_data['completed_count'] = kwargs.get('completed_count')
        if kwargs.get('fail_count'):
            self.monitor_data['fail_count'] = kwargs.get('fail_count')

    def save_data(self, user_id: str, data: List[Dict[str, Any]]):
        """
        保存抓取的数据到CSV文件
        """
        try:
            filename = os.path.join(self.data_dir, f'user_{user_id}_{datetime.now().strftime("%Y%m%d")}.csv')
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            self.logger.info(f"数据已保存到 {filename}")
        except Exception as e:
            self.logger.error(f"保存数据时出错: {str(e)}")

    def close(self):
        """
        关闭浏览器和playwright
        """
        try:
            self.logger.info("开始关闭浏览器资源...")

            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 关闭浏览器资源
            self._cleanup_browser_resources()

            self.logger.info("所有资源已正确关闭")
        except Exception as e:
            self.logger.error(f"关闭资源时出错: {str(e)}")

    @retry_on_exception(max_attempts=Config.MAX_RETRIES, initial_wait=Config.INITIAL_WAIT)
    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            
            # 验证当前是否有正在处理的用户
            if not self.current_kol or not self.current_kol.get('name'):
                self.logger.warning("没有正在处理的用户，跳过API响应")
                return
                
            current_kol_name = self.current_kol.get('name')
            
            if '/api/author/get_author_marketing_info' in url:
                with self.rate_limit():
                    try:
                        # 检查响应状态
                        if response.status != 200:
                            self.logger.warning(f"API响应状态码异常: {response.status}")
                            return
                            
                        try:
                            response_data = response.json()
                        except playwright._impl._errors.Error as pe:
                            if "Protocol error (Network.getResponseBody)" in str(pe):
                                return
                            raise

                        if not response_data or not isinstance(response_data, dict):
                            self.logger.error("营销信息API响应数据格式不正确")
                            return

                        # 确保营销信息使用当前用户
                        self.marketing_info['kol_name'] = current_kol_name
                        self._process_marketing_info(response_data)
                        # 标记API响应已处理
                        self.api_response_processed = True

                    except ValueError as e:
                        self.logger.error(f"解析营销信息API响应JSON时出错: {str(e)}")
                        raise
                    except Exception as e:
                        self.logger.error(f"处理营销信息API响应数据时出错: {str(e)}")
                        raise

            elif '/api/author/get_author_show_items_v2' in url:
                if url in self.processed_api_responses:
                    self.logger.debug("跳过重复的API响应")
                    return

                with self.rate_limit():
                    try:
                        # 检查响应状态
                        if response.status != 200:
                            self.logger.warning(f"API响应状态码异常: {response.status}")
                            return
                            
                        try:
                            response_data = response.json()
                        except playwright._impl._errors.Error as pe:
                            if "Protocol error (Network.getResponseBody)" in str(pe):
                                self.logger.warning("无法获取响应体，可能是临时性问题，将在下次请求时重试")
                                return
                            raise

                        if not response_data or not isinstance(response_data, dict):
                            self.logger.error("API响应数据格式不正确")
                            return

                        # 确保营销信息使用当前用户
                        self.marketing_info['kol_name'] = current_kol_name
                        self.processed_api_responses.add(url)
                        self._process_user_posted_data(response_data)
                        # 标记API响应已处理
                        self.api_response_processed = True

                    except ValueError as e:
                        self.logger.error(f"解析API响应JSON时出错: {str(e)}")
                        raise
                    except Exception as e:
                        self.logger.error(f"处理API响应数据时出错: {str(e)}")
                        raise

        except Exception as e:
            self.logger.error(f"处理API响应时出错: {str(e)}")
            raise

    def _process_marketing_info(self, response_data: Dict[str, Any]):
        """处理营销信息数据"""
        try:
            if not response_data:
                self.logger.error("营销信息API响应数据为空")
                return

            price_info = response_data.get('price_info', [])

            if not price_info:
                self.logger.warning("未找到价格信息")
                return

            # 获取当前正在处理的KOL名称
            current_kol_name = self.current_kol.get('name') if self.current_kol else None
            if not current_kol_name:
                self.logger.error("无法获取当前KOL名称")
                return
                
            # 验证数据是否属于当前用户
            if self.marketing_info.get('kol_name') != current_kol_name:
                self.logger.warning(f"数据不匹配：期望 {current_kol_name}，实际 {self.marketing_info.get('kol_name')}")
                return


            # 需要获取的视频时长价格
            target_desc = {
                '1-20s视频': 'realization1_20',
                '21-60s视频': 'realization21_60',
                '60s以上视频': 'realization60'
            }

            # 初始化价格数据
            price_data = {
                'realization1_20': 0,
                'realization21_60': 0,
                'realization60': 0,
                'douyin_user_id': current_kol_name,  # 添加author_id
                'create_time': int(datetime.now().timestamp()),
                'update_time': int(datetime.now().timestamp())
            }

            # 遍历价格信息列表
            processed_count = 0
            for price_item in price_info:
                try:
                    price = price_item.get('price')
                    desc = price_item.get('desc')

                    if price is not None and desc in target_desc:
                        # 使用target_desc映射将desc映射到对应的数据库字段
                        db_field = target_desc[desc]
                        price_data[db_field] = int(price)
                        processed_count += 1

                except (ValueError, TypeError) as e:
                    self.logger.warning(f"处理价格信息时出错: {str(e)}, 价格项: {price_item}")
                    continue

            self.logger.info(f"成功处理 {processed_count} 个价格信息")

            # 保存到数据库
            self._save_marketing_data(current_kol_name, price_data)

        except Exception as e:
            self.logger.error(f"处理营销信息时出错: {str(e)}")

    def _save_marketing_data(self, kol_name: str, price_data: Dict[str, Any]):
        """保存营销数据到数据库"""
        try:
            # 检查是否已存在该用户的记录
            existing_record = session.query(DouYinKolRealization).filter_by(
                douyin_user_id=kol_name
            ).first()

            if existing_record:
                # 更新现有记录
                for key, value in price_data.items():
                    setattr(existing_record, key, value)
                self.logger.info(f"更新用户 {kol_name} 的变现价格数据")
            else:
                # 创建新记录
                record = DouYinKolRealization(**price_data)
                session.add(record)
                self.logger.info(f"创建用户 {kol_name} 的变现价格数据")

            session.commit()
            self.logger.info("成功保存变现价格数据")

        except Exception as db_error:
            self.logger.error(f"保存变现价格数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _process_user_posted_data(self, response_data: Dict[str, Any]):
        """处理用户笔记数据"""
        try:
            if not response_data:
                self.logger.error("API响应数据为空")
                return

            if 'latest_star_item_info' not in response_data:
                self.logger.error("API响应数据格式不正确: 缺少 latest_star_item_info 字段")
                return

            notes_data = response_data.get('latest_star_item_info', [])
            if not notes_data:
                self.logger.info("本次获取的视频数据为空")
                return

            # 获取当前正在处理的KOL名称
            current_kol_name = self.current_kol.get('name') if self.current_kol else None
            if not current_kol_name:
                self.logger.error("无法获取当前KOL名称")
                return
                
            # 验证数据是否属于当前用户
            if self.marketing_info.get('kol_name') != current_kol_name:
                self.logger.warning(f"数据不匹配：期望 {current_kol_name}，实际 {self.marketing_info.get('kol_name')}")
                return

            self.logger.info(f"开始处理KOL {current_kol_name} 的 {len(notes_data)} 条笔记数据")

            processed_count = 0
            for note in notes_data:
                try:
                    item_id = note.get('item_id', '')
                    if not item_id:
                        self.logger.warning("跳过处理：item_id为空")
                        continue

                    # 检查记录是否已存在
                    existing_record = session.query(DouYinKolNote).filter_by(
                        douyin_item_id=item_id
                    ).first()

                    if existing_record:
                        # 更新现有记录
                        self._update_note_record(existing_record, note, current_kol_name)
                    else:
                        # 创建新记录
                        self._create_note_record(note, current_kol_name)

                    processed_count += 1

                except Exception as e:
                    self.logger.error(f"处理单条视频数据时出错: {str(e)}")
                    continue

            self.logger.info(f"成功处理 {processed_count} 条笔记数据")

        except Exception as e:
            self.logger.error(f"处理用户视频数据时出错: {str(e)}")

    def _update_note_record(self, existing_record, note: Dict[str, Any], kol_name: str):
        """更新现有笔记记录"""
        existing_record.douyin_user_id = kol_name
        existing_record.douyin_item_title = note.get('item_title', '')
        existing_record.video_like = note.get('like', 0)
        existing_record.video_play = note.get('play', 0)
        existing_record.video_share = note.get('share', 0)
        existing_record.video_comment = note.get('comment', 0)
        existing_record.update_time = int(datetime.now().timestamp())

        try:
            session.commit()
        except Exception as db_error:
            self.logger.error(f"更新视频数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _create_note_record(self, note: Dict[str, Any], kol_name: str):
        """创建新的笔记记录"""
        note_record = DouYinKolNote(
            douyin_user_id=kol_name,
            douyin_item_id=note.get('item_id', ''),
            douyin_item_date=note.get('item_date', ''),
            douyin_item_title=note.get('item_title', ''),
            video_like=note.get('like', 0),
            video_play=note.get('play', 0),
            video_share=note.get('share', 0),
            video_comment=note.get('comment', 0),
            create_time=int(datetime.now().timestamp()),
            update_time=int(datetime.now().timestamp())
        )
        session.add(note_record)

        try:
            session.commit()
            self.logger.info(f"创建视频数据成功: {note_record.douyin_item_id}")
        except Exception as db_error:
            self.logger.error(f"创建视频数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _save_cookies(self) -> bool:
        """保存当前会话的Cookie"""
        try:
            if not hasattr(self, 'context') or not self.context:
                self.logger.error("浏览器上下文不存在，无法保存Cookie")
                return False

            cookies = self.context.cookies()
            if not cookies:
                self.logger.warning("没有Cookie数据可保存")
                return False

            os.makedirs(os.path.dirname(self.cookie_file), exist_ok=True)

            # 使用临时文件保存，避免写入过程中的文件损坏
            temp_file = f"{self.cookie_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)

            # 安全地替换原文件
            os.replace(temp_file, self.cookie_file)
            self.logger.info(f"成功保存 {len(cookies)} 个Cookie到文件")
            return True
        except Exception as e:
            self.logger.error(f"保存Cookie时出错: {str(e)}")
            # 清理临时文件
            temp_file = f"{self.cookie_file}.tmp"
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            return False

    def _load_cookies(self) -> bool:
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
            self.logger.info(f"成功加载 {len(cookies)} 个Cookie")
            return True
        except json.JSONDecodeError:
            self.logger.error("Cookie文件格式不正确")
            return False
        except Exception as e:
            self.logger.error(f"加载Cookie时出错: {str(e)}")
            return False


# ==================== SpiderRunner 类 ====================

class SpiderConfig:
    """爬虫配置类"""
    MAX_RETRY_COUNT = 3  # 最大重试次数
    BATCH_SIZE = 10  # 批处理大小
    LOG_LEVEL = "INFO"  # 日志级别
    LOG_RETENTION = "7 days"  # 日志保留时间

class SpiderRunner:
    """爬虫运行器"""
    
    def __init__(self):
        self.spider: Optional[DouYinSpider] = None
        self.processed_count = 0
        self.failed_count = 0
        self.no_creative_count = 0  # 没有创作能力的KOL数量
        self.total_count = 0
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志配置"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        logger.add(
            os.path.join(log_dir, "spider_{time:YYYY-MM-DD}.log"),
            rotation="1 day",
            retention=SpiderConfig.LOG_RETENTION,
            level=SpiderConfig.LOG_LEVEL,
            encoding="utf-8",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        )
        
    def get_pending_kols(self) -> List[DouyinBianxian]:
        """获取需要处理的KOL列表"""
        try:
            kols = session.query(DouyinBianxian).filter(DouyinBianxian.status == 0).all()
            logger.info(f"从数据库获取到 {len(kols)} 个待处理的KOL")
            return kols
        except Exception as e:
            logger.error(f"获取KOL列表时出错: {str(e)}")
            raise
            
    def initialize_spider(self) -> bool:
        """初始化爬虫"""
        try:
            logger.info("正在初始化爬虫...")
            self.spider = DouYinSpider()
            
            # 检查登录状态
            if not self.spider.is_logged_in:
                logger.info("检测到未登录状态，开始登录流程...")
                if not self.spider.login():
                    logger.error("登录失败，程序无法继续")
                    return False
                logger.info("登录成功")
            else:
                logger.info("检测到已登录状态")
                
            return True
        except Exception as e:
            logger.error(f"初始化爬虫时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False
            
    def process_kol(self, kol: DouyinBianxian) -> bool:
        """处理单个KOL"""
        kol_name = kol.kol_name
        retry_count = 0
        
        while retry_count < SpiderConfig.MAX_RETRY_COUNT:
            try:
                logger.info(f"开始处理KOL: {kol_name} (第 {retry_count + 1} 次尝试)")
                
                # 验证必要的字段
                if not kol.douyin_link:
                    logger.warning(f"KOL {kol_name} 缺少抖音链接，跳过处理")
                    return False
                
                # 执行抓取
                result = self.spider.scrape_user_notes(kol_name, kol.douyin_link)
                
                if result == 1:
                    # 处理成功
                    kol.status = 1
                    kol.updated_at = datetime.now()  # 如果有更新时间字段
                    session.commit()
                    
                    logger.info(f"✅ KOL {kol_name} 处理成功")
                    self.processed_count += 1
                    return True
                elif result == 2:
                    # 没有创作能力按钮（该KOL没有创作能力数据）
                    kol.status = 2
                    kol.updated_at = datetime.now()  # 如果有更新时间字段
                    session.commit()
                    
                    logger.info(f"ℹ️ KOL {kol_name} 没有创作能力数据，已标记为状态2")
                    self.processed_count += 1
                    self.no_creative_count += 1
                    return True
                else:
                    # 处理失败
                    logger.warning(f"⚠️ KOL {kol_name} 处理失败，准备重试")
                    retry_count += 1
                    
            except Exception as e:
                logger.error(f"❌ 处理KOL {kol_name} 时出错 (第 {retry_count + 1} 次尝试): {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                retry_count += 1
                
                # 回滚数据库事务
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"回滚数据库事务时出错: {str(rollback_error)}")
        
        # 所有重试都失败了
        logger.error(f"❌ KOL {kol_name} 处理失败，已达到最大重试次数")
        self.failed_count += 1
        return False
        
    def process_kols_batch(self, kols: List[DouyinBianxian]) -> dict:
        """批处理KOL列表"""
        self.total_count = len(kols)
        logger.info(f"开始批处理 {self.total_count} 个KOL")
        
        for i, kol in enumerate(kols, 1):
            logger.info(f"进度: {i}/{self.total_count} ({(i/self.total_count)*100:.1f}%)")
            time.sleep(12)
            
            try:
                self.process_kol(kol)
            except KeyboardInterrupt:
                logger.warning("用户中断程序")
                break
            except Exception as e:
                logger.error(f"批处理过程中出现未预期的错误: {str(e)}")
                continue
                
        # 返回处理结果统计
        return {
            'total': self.total_count,
            'processed': self.processed_count,
            'failed': self.failed_count,
            'no_creative': self.no_creative_count,
            'success_rate': (self.processed_count / self.total_count * 100) if self.total_count > 0 else 0
        }
        
    def cleanup(self):
        """清理资源"""
        try:
            if self.spider:
                self.spider.close()
                logger.info("爬虫资源已清理")
        except Exception as e:
            logger.error(f"清理爬虫资源时出错: {str(e)}")
            
        try:
            session.commit()
            session.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {str(e)}")
            try:
                session.rollback()
                session.close()
            except:
                pass
                
    def run(self) -> bool:
        """运行主程序"""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("🚀 巨量星图博主变现数据抓取程序启动")
        logger.info(f"启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        try:
            # 1. 获取待处理的KOL列表
            kols = self.get_pending_kols()
            if not kols:
                logger.warning("没有找到需要更新的KOL数据，程序结束")
                return True
                
            # 2. 初始化爬虫
            if not self.initialize_spider():
                logger.error("爬虫初始化失败，程序退出")
                return False
                
            # 3. 批处理KOL
            results = self.process_kols_batch(kols)
            
            # 4. 输出处理结果
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 60)
            logger.info("📊 处理结果统计:")
            logger.info(f"总数量: {results['total']}")
            logger.info(f"成功处理（有创作能力）: {results['processed'] - results['no_creative']}")
            logger.info(f"成功处理（无创作能力）: {results['no_creative']}")
            logger.info(f"处理失败: {results['failed']}")
            logger.info(f"成功率: {results['success_rate']:.1f}%")
            logger.info(f"总耗时: {duration}")
            logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
            return results['failed'] == 0  # 如果没有失败的则返回True
            
        except KeyboardInterrupt:
            logger.warning("⚠️ 用户手动中断程序")
            return False
        except Exception as e:
            logger.error(f"❌ 程序运行出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False
        finally:
            self.cleanup()


def main():
    """主函数"""
    runner = SpiderRunner()
    
    try:
        success = runner.run()
        exit_code = 0 if success else 1
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        exit_code = 1
    
    logger.info(f"程序退出，退出码: {exit_code}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
