import json
import os
import time
from datetime import datetime
import sys
from typing import Dict, Any, List
import traceback

from models.models_tibao import DouyinSearchList
from core.localhost_fp_project import session
import pandas as pd
from loguru import logger
from playwright.sync_api import sync_playwright
from unitl.common import Common

"""
    获取抖音博主的月总营收
"""


def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(
            os.path.abspath(__file__))
    except Exception:
        return os.path.abspath(".")


class DouYinSpider:
    def __init__(self):
        self.setup_logger()
        # 设置logger属性
        self.logger = logger

        # 设置cookie和数据目录，支持exe打包
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')
        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = 'https://www.xingtu.cn/ad/creator/index'
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.common = Common()
        self.current_kol = None  # 当前正在处理的KOL信息
        self.api_response_processed = False  # 标记API响应是否已处理
        self.button_clicked = False  # 标记是否已点击确定按钮
        self.current_page = 1  # 当前页码

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def scrape_user_notes(self, kol_name="搜索达人"):
        """抓取指定KOL的笔记信息并匹配更新数据库
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            # 设置当前KOL信息 - 这是关键
            self.current_kol = {'name': kol_name, 'url': self.base_url}
            # 重置API响应处理标志
            self.api_response_processed = False
            # 重置API数据缓存
            self.api_data = {}
            # 重置按钮点击状态
            self.button_clicked = False
            # 重置当前页码
            self.current_page = 1

            try:
                # 访问页面
                page_url = f"https://www.xingtu.cn/ad/creator/market"
                logger.info(f"开始访问页面: {page_url}")

                try:
                    self.page.goto(page_url)
                except Exception as e:
                    logger.error(f"访问页面失败: {str(e)}")

                # 等待页面加载完成
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception as e:
                    logger.warning(f"等待页面加载完成时出错: {str(e)}")

                # 点击达人报价按钮
                self._click_quotation_range_button()

                # 设置价格区间为24000-30000
                self._set_quotation_range(24000, 30000)

                # 等待用户完成价格区间设置，API数据将在点击第二个确定按钮后处理
                self.logger.info("等待用户完成价格区间设置...")

                # 等待API数据 - 等待更长时间
                try:
                    # 等待API响应处理完成
                    wait_time = 10  # 增加等待时间到10秒
                    self.logger.info(f"等待API数据处理，最多等待{wait_time}秒...")
                    time.sleep(wait_time)

                    # 检查是否已经获取到API响应数据
                    if self.api_response_processed:
                        self.logger.info("✅ 成功获取到API响应数据")
                    elif len(self.api_data) > 0:
                        self.logger.info(f"✅ 检测到 {len(self.api_data)} 个API响应")
                else:
                        self.logger.warning("⚠️ 未检测到API响应数据")

                    # 确保分页处理已完成
                    if not self._process_all_pages():
                        self.logger.warning("⚠️ 分页处理可能未完成")

                    return 1  # 返回1表示处理成功

        except Exception as e:
                    self.logger.warning(f"等待API数据时出错: {str(e)}")
                    return 1  # 即使出错也继续执行

            except Exception as e:
                logger.warning(f"处理价格区间设置时出错: {str(e)}")
                return 0  # 返回失败

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            return 0  # 返回失败

    def _click_quotation_range_button(self):
        """点击达人报价按钮，然后点击报价区间下拉框"""
        try:
            self.logger.info("正在点击达人报价按钮...")

            try:
                button_element = self.page.locator("span:has-text('达人报价')").first
                if button_element and button_element.is_visible(timeout=3000):
                    button_element.click()
            except Exception as e:
                self.logger.error(f"选择达人报价失败: {str(e)}")

            # 第二步：点击报价区间下面的下拉框
            self.logger.info("正在点击报价区间下拉框...")

            # 根据抓博主报价.py中的方法，使用更精确的定位
            dropdown_clicked = False

            # 确保页面完全加载
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
                self.logger.info("页面网络加载完成")
            except Exception as e:
                self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

            try:
                # 查找包含"报价区间"文本的price-group-item
                price_item = self.page.locator("div.price-group-item:has(div.label:has-text('报价区间'))").first

                if price_item:
                    self.logger.info("找到包含报价区间的price-group-item")

                    # 查找所有可能的下拉框选择器
                    dropdown_selectors = [
                        "div.xt-dropdown.star-select.new.use-design-element.select.el-dropdown",
                        "div.xt-dropdown",
                        "div.el-dropdown",
                        "div.dropdown",
                        "span[role='button']",
                        "span.refer-label",
                        ".xt-dropdown",
                        ".el-dropdown"
                    ]

                    dropdown_found = False
                    for selector in dropdown_selectors:
                        dropdown = price_item.locator("div.xt-dropdown").first
                        if dropdown and dropdown.is_visible(timeout=2000):
                            self.logger.info(f"找到可见的下拉框: ")
                            dropdown.click()
                            self.common.random_sleep(2, 3)
                            dropdown_clicked = True
                            dropdown_found = True
                            break
                        else:
                            self.logger.debug(f"选择器 {selector} 未找到可见元素")

                    if not dropdown_found:
                        self.logger.warning("在price-group-item中未找到任何可见的下拉框")
                else:
                    self.logger.warning("未找到包含报价区间的price-group-item")

                    # 尝试查找所有包含"报价区间"文本的元素
                    price_labels = self.page.locator(":has-text('报价区间')").all()
                    self.logger.info(f"页面上找到 {len(price_labels)} 个包含'报价区间'文本的元素")

            except Exception as e:
                self.logger.debug(f"通过filters-item定位失败: {str(e)}")

            if not dropdown_clicked:
                self.logger.warning("未能点击报价区间下拉框，但继续执行")
            else:
                # 等待下拉菜单完全展开
                self.common.random_sleep(2, 3)

            return True

        except Exception as e:
            self.logger.error(f"点击达人报价按钮或下拉框时出错: {str(e)}")
            return False

    def _set_quotation_range(self, min_value: int, max_value: int):
        """设置价格区间"""
        try:
            self.logger.info(f"正在设置价格区间: {min_value}-{max_value}")

            # 等待下拉菜单完全展开
            self.common.random_sleep(1, 2)

            # 根据截图，优先使用input-wrapper定位两个输入框
            inputs = []

            try:
                # 方法1: 直接通过input-wrapper.el-input定位
                input_wrappers = self.page.locator("div.input-wrapper.el-input")
                wrapper_count = input_wrappers.count()
                self.logger.info(f"找到 {wrapper_count} 个input-wrapper容器")

                if wrapper_count >= 2:
                    # 在第一个wrapper中查找输入框
                    first_input = input_wrappers.nth(0).locator("input[type='number']").first
                    # 在第二个wrapper中查找输入框
                    second_input = input_wrappers.nth(1).locator("input[type='number']").first

                    if first_input and second_input:
                        inputs = [first_input, second_input]
                        self.logger.info("成功通过input-wrapper定位到两个输入框")
                else:
                        self.logger.warning("在input-wrapper中未找到输入框")

            except Exception as e:
                self.logger.warning(f"通过input-wrapper定位失败: {str(e)}")

            # 方法2: 如果方法1失败，使用备用方案
            if len(inputs) < 2:
                self.logger.info("尝试备用方案：直接查找number输入框")
                input_selectors = [
                    "input[type='number']",
                    ".el-input__inner",
                    "input[placeholder='0']",
                    "input[min='0'][max='1000000']"
                ]

                for selector in input_selectors:
                    try:
                    elements = self.page.locator(selector)
                    count = elements.count()
                    if count > 0:
                            self.logger.info(f"找到 {count} 个输入框 (选择器: {selector})")
                            inputs = []
                            for i in range(count):
                                inputs.append(elements.nth(i))
                            if len(inputs) >= 2:
                                break
                except Exception as e:
                        self.logger.debug(f"查找输入框 {selector} 失败: {str(e)}")
                    continue

            if len(inputs) < 2:
                self.logger.error(f"需要至少2个输入框，但只找到 {len(inputs)} 个")
                return False

            # 清空并输入最小值到第一个输入框
            try:
                first_input = inputs[0]
                first_input.clear()
                first_input.fill(str(min_value))
                self.logger.info(f"已输入最小值: {min_value}")
                self.common.random_sleep(0.5, 1)
            except Exception as e:
                self.logger.error(f"输入最小值时出错: {str(e)}")
            return False

            # 清空并输入最大值到第二个输入框
            try:
                second_input = inputs[1]
                second_input.clear()
                second_input.fill(str(max_value))
                self.logger.info(f"已输入最大值: {max_value}")
                self.common.random_sleep(0.5, 1)
        except Exception as e:
                self.logger.error(f"输入最大值时出错: {str(e)}")
            return False

            # 点击第一个确定按钮 - 使用更精确的选择器
            try:
                # 优先选择 custom-actions 容器下的确定按钮
                confirm_button = self.page.locator("div.custom-actions button.submit-btn:has-text('确定')").first
                if confirm_button and confirm_button.is_visible(timeout=3000):
                    confirm_button.click()
                    self.logger.info("成功点击 custom-actions 下的确定按钮")
                else:
                    # 备用方案：使用通用选择器
                    confirm_button = self.page.locator("button:has-text('确定')").first
                    if confirm_button and confirm_button.is_visible(timeout=3000):
                        confirm_button.click()
                        self.logger.info("成功点击通用确定按钮")
                    else:
                        self.logger.warning("未找到第一个确定按钮")
        except Exception as e:
                self.logger.warning(f"点击第一个确定按钮失败: {str(e)}")

            self.common.random_sleep(2, 3)

            # 清空API数据缓存
            self.api_data = {}
            # 重置按钮点击状态
            self.button_clicked = False

            # 等待第二个确定按钮出现
            self.common.random_sleep(1, 2)

            try:
                confirm_button1 = self.page.locator("footer button.el-button--primary:has-text('确定')").first
                if confirm_button1 and confirm_button1.is_visible(timeout=2000):
                    # 点击前重置当前页码
                    self.current_page = 1

                    # 点击第二个确定按钮
                    confirm_button1.click()
                    self.logger.info(f"成功点击第二个确定按钮")

                    # 设置按钮点击标志
                    self.button_clicked = True
                    self.logger.info("已设置按钮点击标志，后续API响应将被处理")

                    try:
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception as e:
                        logger.warning(f"等待页面加载完成时出错: {str(e)}")

                    # 等待API数据加载
                    self.common.random_sleep(3, 5)

                    # 开始自动翻页处理
                    self._process_all_pages()
            except Exception as e:
                self.logger.debug(f"失败: {str(e)}")

            self.common.random_sleep(2, 3)
            self.logger.info(f"价格区间设置完成: {min_value}-{max_value}")
            return True

        except Exception as e:
            self.logger.error(f"设置价格区间时出错: {str(e)}")
                return False

    def _process_all_pages(self):
        """处理所有页面数据，自动翻页直到disabled
        返回值: 是否成功处理所有页面
        """
        try:
            # 使用类属性跟踪当前页码
            if not hasattr(self, 'current_page') or self.current_page < 1:
                self.current_page = 1

            self.logger.info(f"开始处理第 {self.current_page} 页数据...")

            # 处理当前页数据
            self._process_current_page_data(self.current_page)

            # 记录总处理页数
            total_processed_pages = 1
            consecutive_failures = 0  # 连续失败次数
            max_consecutive_failures = 3  # 最大连续失败次数

            # 自动翻页处理后续页面
            while self._click_next_page():
                self.current_page += 1
                total_processed_pages += 1
                self.logger.info(f"正在处理第 {self.current_page} 页数据...")

                # 等待API数据加载
                self.common.random_sleep(10, 15)

                # 等待页面网络空闲
                try:
                    self.page.wait_for_load_state('networkidle', timeout=10000)
                except Exception as e:
                    self.logger.warning(f"等待网络空闲时出错: {str(e)}")

                # 处理当前页数据
                self._process_current_page_data(self.current_page)

                # 如果不是最后一页，等待一下再处理下一页
                self.common.random_sleep(2, 3)

            self.logger.info(f"所有页面处理完成，共处理 {total_processed_pages} 页")
                    return True

        except Exception as e:
            self.logger.error(f"处理所有页面时出错: {str(e)}")
            return False

    def _process_current_page_data(self, page_num):
        """处理当前页面的数据
        返回值: 处理的记录数
        """
        try:
            if not self.api_data:
                self.logger.warning(f"第 {page_num} 页没有API数据")
                return 0

            self.logger.info(f"处理第 {page_num} 页的 {len(self.api_data)} 个API响应")

            # 处理API数据并保存到数据库
            api_data_copy = dict(self.api_data)
            total_authors_added = 0
            total_authors_skipped = 0

            for api_url, response_data in api_data_copy.items():
                if 'data' not in response_data:
                    continue

                api_data = response_data['data']
                if 'search_for_author_square' in api_url:
                    authors_added = 0
                    authors_skipped = 0

                    # 确保authors字段存在
                    if 'authors' not in api_data:
                        self.logger.warning(
                            f"API数据缺少authors字段: {list(api_data.keys()) if isinstance(api_data, dict) else 'Not a dict'}")
                        continue

                    for author_data in api_data['authors']:
                        # 检查是否已存在
                        existing_data = session.query(DouyinSearchList).filter(
                            DouyinSearchList.star_id == author_data['star_id']).first()
                        if existing_data:
                            authors_skipped += 1
                            continue

                        # 创建新记录
                        detail = DouyinSearchList(
                            attribute_datas=self._safe_json_dumps(author_data.get('attribute_datas', {})),
                            extra_data=self._safe_json_dumps(author_data.get('extra_data', {})),
                            items=self._safe_json_dumps(author_data.get('items', [])),
                            star_id=str(author_data.get('star_id', '')),
                            task_infos=self._safe_json_dumps(author_data.get('task_infos', {})),
                        )
                        session.add(detail)
                        authors_added += 1

                    # 累计总数
                    total_authors_added += authors_added
                    total_authors_skipped += authors_skipped

                    # 每个API响应单独提交事务，避免一个错误影响全部
                    try:
                        session.commit()
                        self.logger.info(f"API响应处理完成: 新增 {authors_added} 条，跳过 {authors_skipped} 条重复数据")
                    except Exception as commit_error:
                        self.logger.error(f"提交事务时出错: {str(commit_error)}")
                        session.rollback()

            # 清空已处理的API数据，避免重复处理
            for url in api_data_copy.keys():
                if url in self.api_data:
                    del self.api_data[url]

            self.logger.info(
                f"第 {page_num} 页数据处理完成: 总计新增 {total_authors_added} 条，跳过 {total_authors_skipped} 条重复数据")
            return total_authors_added

            except Exception as e:
            self.logger.error(f"处理第 {page_num} 页数据时出错: {str(e)}")
            session.rollback()  # 出错时回滚事务
            return 0

    def _click_next_page(self):
        """点击下一页按钮，返回是否成功点击"""
        try:
            # 查找下一页按钮（根据您之前提供的HTML结构）
            next_page_button = self.page.locator("button.btn-next").first

            # 检查按钮是否存在
            if not next_page_button.is_visible(timeout=3000):
                self.logger.info("未找到下一页按钮，可能已到最后一页")
                return False

            # 检查按钮是否被禁用
            button_disabled = next_page_button.get_attribute("disabled")
            if button_disabled is not None:
                self.logger.info("下一页按钮被禁用，确认已到最后一页")
                return False

            # 点击下一页按钮
            self.logger.info("点击下一页按钮")
            next_page_button.click()

            # 等待页面响应
            self.common.random_sleep(2, 3)

            # 等待网络空闲
            try:
                self.page.wait_for_load_state('networkidle', timeout=15000)
                except Exception as e:
                self.logger.warning(f"等待网络空闲时出错: {str(e)}")

            return True

            except Exception as e:
            self.logger.error(f"点击下一页时出错: {str(e)}")
            return False

    def setup_logger(self):
        """设置日志配置，支持exe打包"""
        # 设置日志目录
        base_path = get_base_path()
        log_path = os.path.join(base_path, 'logs')
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
        # 如果浏览器已经初始化，直接返回
        if self.browser and self.context and self.page:
            logger.info("浏览器已经初始化，跳过重复初始化")
            return

        # 设置playwright浏览器路径，支持exe打包
        base_path = get_base_path()
        playwright_browsers_path = os.path.join(base_path, 'ms-playwright')

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
                userSide = self.page.locator(".text-avatar").all()
                logger.info(f"找到用户头像元素数量: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 0 or self.page.locator(".text-avatar").is_visible(timeout=5000):
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
            self.page = self.context.new_page()
            self.is_logged_in = False

        # 设置页面超时时间
        self.page.set_default_timeout(20000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

        logger.info("浏览器初始化完成")

    def login(self):
        """
        等待用户手动登录，最多等待5分钟
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

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
                        user_avatar = self.page.locator(".text-avatar").first
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

    def close(self):
        """
        关闭浏览器、playwright和数据库连接
        """
        try:
            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 检查浏览器是否已初始化
            if hasattr(self, 'page') and self.page:
                self.page.close()
            if hasattr(self, 'context') and self.context:
                self.context.close()
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

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

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            # 设置更广泛的目标API关键词
            target_apis = ['api/gsearch/search_for_author_square']

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                logger.info(f"捕获到目标API: {url}")
                    try:
                        # 检查响应状态
                        if response.status != 200:
                        logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                            return

                        try:
                        # 检查浏览器是否仍然有效
                        if not hasattr(self, 'page') or not self.page or self.page.is_closed():
                            logger.warning(f"页面已关闭，跳过API数据处理: {url}")
                                return

                        data = response.json()

                        # 找到匹配的API类型
                        matched_api = None
                        for api in target_apis:
                            if api in url:
                                matched_api = api
                                break

                        # 存储有效的API数据
                        if matched_api:
                            # 检查当前是否处于点击按钮后的状态
                            if hasattr(self, 'button_clicked') and self.button_clicked:
                                self.api_data[url] = {
                                    'url': url,
                                    'data': data,
                                    'api_type': matched_api,
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'status': response.status
                                }
                                logger.info(f"成功存储API数据: {url}, 当前API数据总数: {len(self.api_data)}")

                                # 检查数据结构
                                if 'authors' in data:
                                    authors_count = len(data['authors'])
                                    logger.info(f"API数据包含 {authors_count} 个作者信息")

                                    # 立即处理数据并保存到数据库
                                    self._process_api_data_immediately(url, data)

                        # 标记API响应已处理
                        self.api_response_processed = True
                                else:
                                    logger.warning(
                                        f"API数据结构异常: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                            else:
                                logger.info(f"捕获到API数据，但尚未点击按钮，暂不处理: {url}")

                    except ValueError:
                        logger.warning(f"无效的JSON响应: {url}")
                    except Exception as json_error:
                        logger.warning(f"JSON解析失败: {str(json_error)}, URL: {url}")

                    except Exception as e:
                    # 如果是浏览器关闭错误，不记录为错误
                    if "Target page, context or browser has been closed" in str(e):
                        logger.info(f"浏览器已关闭，跳过API数据处理: {url}")
                    else:
                        logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def _safe_json_dumps(self, data):
        """安全地将数据转换为JSON字符串，处理各种异常情况"""
        try:
            if data is None:
                return ""
            elif isinstance(data, (dict, list)):
                return json.dumps(data, ensure_ascii=False)
            elif isinstance(data, str):
                # 如果已经是字符串，尝试解析并重新序列化以确保格式正确
                try:
                    parsed = json.loads(data)
                    return json.dumps(parsed, ensure_ascii=False)
                except (json.JSONDecodeError, ValueError):
                    # 如果解析失败，直接返回原字符串（可能是已经格式化的JSON）
                    return data
            else:
                # 其他类型转换为字符串
                return str(data)
        except Exception as e:
            logger.warning(f"JSON序列化失败，使用字符串表示: {str(e)}")
            return str(data) if data is not None else ""

    def _process_all_api_data(self):
        """处理所有API数据"""
        try:
            if not self.api_data:
                self.logger.warning("没有API数据需要处理")
                return

            self.logger.info(f"开始处理 {len(self.api_data)} 个API响应")

            for api_url, response_data in self.api_data.items():
                if 'data' not in response_data:
                    continue

                api_data = response_data['data']
                if 'search_for_author_square' in api_url:
                    authors_added = 0
                    authors_skipped = 0

                    for author_data in api_data['authors']:
                        # 检查是否已存在
                        existing_data = session.query(DouyinSearchList).filter(
                            DouyinSearchList.star_id == author_data['star_id']).first()
                        if existing_data:
                            authors_skipped += 1
                    continue

                        # 创建新记录 - 将字典类型转换为JSON字符串
                        detail = DouyinSearchList(
                            attribute_datas=self._safe_json_dumps(author_data.get('attribute_datas', {})),
                            extra_data=self._safe_json_dumps(author_data.get('extra_data', {})),
                            items=self._safe_json_dumps(author_data.get('items', [])),
                            star_id=str(author_data.get('star_id', '')),
                            task_infos=self._safe_json_dumps(author_data.get('task_infos', {})),
                        )
                        session.add(detail)
                        authors_added += 1

                    # 提交数据库事务
                    session.commit()
                    self.logger.info(f"API数据处理完成: 新增 {authors_added} 条，跳过 {authors_skipped} 条重复数据")

        except Exception as e:
            self.logger.error(f"处理API数据时出错: {str(e)}")
            session.rollback()

    def _process_api_data_immediately(self, url, data):
        """立即处理API数据，避免延迟导致的数据丢失"""
        try:
            if 'search_for_author_square' in url:
                if data and 'authors' in data:
                    authors_added = 0
                    authors_skipped = 0

                    for author_data in data['authors']:
                        # 检查是否已存在
                        existing_data = session.query(DouyinSearchList).filter(
                            DouyinSearchList.star_id == author_data['star_id']).first()
                        if existing_data:
                            authors_skipped += 1
                            continue

                # 创建新记录
                        detail = DouyinSearchList(
                            attribute_datas=self._safe_json_dumps(author_data.get('attribute_datas', {})),
                            extra_data=self._safe_json_dumps(author_data.get('extra_data', {})),
                            items=self._safe_json_dumps(author_data.get('items', [])),
                            star_id=str(author_data.get('star_id', '')),
                            task_infos=self._safe_json_dumps(author_data.get('task_infos', {})),
                        )
                        session.add(detail)
                        authors_added += 1

                    # 提交数据库事务
            session.commit()
                    logger.info(f"立即处理API数据完成: 新增 {authors_added} 条，跳过 {authors_skipped} 条重复数据")
                else:
                    logger.warning(f"API数据结构异常，无法处理: {url}")

        except Exception as e:
            logger.error(f"立即处理API数据时出错: {str(e)}")
            session.rollback()

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


def main():
    """
    主函数 - 抖音搜索自动化程序
    """
    spider = None
    try:
        logger.info("=== 抖音搜索自动化程序启动 ===")
        spider = DouYinSpider()

        # 3. 初始化浏览器和登录
        spider.setup_browser()
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
                return False

        # 4. 处理Excel数据（搜索抖音用户）
        result = spider.scrape_user_notes("搜索达人")
        if result == 0:
            logger.error("处理Excel数据失败")
            return False

        # 等待一段时间，确保所有API响应都被处理
        logger.info("等待所有API响应处理完成...")

        # 检查是否有API数据需要处理
        if spider.api_data:
            logger.info(f"检测到 {len(spider.api_data)} 个API响应，开始处理")
            # 处理API数据
            spider._process_all_api_data()
            # 开始自动翻页
            spider._process_all_pages()
        else:
            logger.info("没有检测到API数据，程序将退出")

        logger.info("抖音搜索程序执行完成")
        return True

        except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
            return False
        except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False
        finally:
        # 确保资源被正确释放
        if spider:
            try:
                spider.close()
                logger.info("资源清理完成")
            except Exception as e:
                logger.error(f"清理资源时出错: {str(e)}")


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
