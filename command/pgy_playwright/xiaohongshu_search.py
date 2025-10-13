import json
import os
import time
import random

from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from unitl.common import Common

"""
    获取SpiderQianguaHotNote表中博主姓名，去小红书完善博主信息
"""

# 搜索关键词列表
SEARCH_KEYWORDS = [
    "眼妆博主", "变装博主", "非遗", "大平层", "别墅", 
    "微胖穿搭", "大码穿搭", "沉浸式护肤", "好物分享", 
    "宅家写字", "男大vlog", "女大vlog", "北大vlog", "清华vlog"
]

class XiaohongshuSpider:
    def __init__(self):
        self.logger = logger.bind(class_name=self.__class__.__name__)
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.xiaohongshu.com"
        self.is_logged_in = False
        self.found_match = False
        self.api_data = []
        self.cookie_file = os.path.join(self.data_dir, 'cookies.json')
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
        self.note_ids = set()  # 存储所有不重复的ID
        self.waiting_for_api = False  # 是否正在等待API响应
        self.last_api_time = 0  # 最后一次收到API响应的时间
        self.max_retries = 3
        self.current_page = 1
        self.max_pages = 10  # 设置最大爬取页数
        self.last_height = 0  # 记录上次滚动高度
        self.first_api_received = False  # 添加标志来跟踪第一次API响应
        self.current_keyword_index = 0  # 当前搜索关键词的索引

    def setup_browser(self):
        """初始化浏览器"""
        try:
            self.playwright = sync_playwright().start()
            # 配置浏览器选项
            browser_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-web-security',  # 添加跨域支持
                '--disable-features=IsolateOrigins,site-per-process'  # 禁用站点隔离
            ]

            self.browser = self.playwright.chromium.launch(
                headless=False,
                args=browser_args
            )

            # 创建上下文
            context_options = {
                'viewport': {'width': 1280, 'height': 560},
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'bypass_csp': True  # 绕过内容安全策略
            }

            self.context = self.browser.new_context(**context_options)

            # 设置请求拦截
            self.context.route("**/*", lambda route: route.continue_())
            
            # 添加响应监听器
            self.context.on("response", self._handle_api_response)

            # 尝试加载已保存的Cookie
            if self._load_cookies():
                # 验证Cookie是否有效
                self.page = self.context.new_page()
                self.page.goto(self.base_url)
                self.common.random_sleep(2, 3)
                try:
                    # 检查是否存在用户头像元素
                    userSide = self.page.locator(".user.side-bar-component").all()
                    user_len = len(userSide)
                    if user_len > 1 or self.page.locator(".user.side-bar-component").is_visible(timeout=50000):
                        self.is_logged_in = True
                        self.logger.info("Cookie有效，已自动登录")
                    else:
                        self.logger.info("Cookie已失效，需要重新登录")
                except Exception as e:
                    self.logger.error(f"验证Cookie时出错: {str(e)}")
                    self.logger.info("Cookie已失效，需要重新登录")
            else:
                self.page = self.context.new_page()

            # 设置页面超时时间
            self.page.set_default_timeout(20000)

        except Exception as e:
            self.logger.error(f"初始化浏览器时出错: {str(e)}")
            raise Exception("浏览器初始化失败")

    def _is_valid_note_id(self, note_id):
        """检查是否为有效的笔记ID"""
        # 标准小红书笔记ID格式为24位字符
        return isinstance(note_id, str) and len(note_id) == 24 and '#' not in note_id

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            
            # 修改URL匹配条件，使用更宽松的匹配
            if 'search/notes' in url:
                try:
                    response_data = response.json()
                    
                    if response_data and 'data' in response_data:
                        # 处理数据，提取ID
                        if 'items' in response_data['data']:
                            valid_ids_count = 0
                            for item in response_data['data']['items']:
                                if isinstance(item, dict) and 'note_card' in item:
                                    note_card = item['note_card']
                                    if isinstance(note_card, dict) and 'user' in note_card:
                                        user = note_card['user']
                                        if isinstance(user, dict) and 'user_id' in user:
                                            user_id = user['user_id']
                                            if self._is_valid_note_id(user_id):  # Reusing the validation function since format is similar
                                                self.note_ids.add(user_id)
                                                valid_ids_count += 1
                            
                            self.last_api_time = time.time()
                            self.waiting_for_api = False
                            self.logger.info(f"收到notes接口响应，本次新增 {valid_ids_count} 个有效用户ID，当前共有 {len(self.note_ids)} 个唯一用户ID")
                        else:
                            self.logger.warning("响应数据中没有items字段")
                    else:
                        self.logger.warning("响应数据中没有data字段")
                except Exception as e:
                    self.logger.error(f"处理notes接口响应失败: {str(e)}")
                    self.logger.error(f"响应数据: {response.text()}")
        except Exception as e:
            self.logger.error(f"处理API响应时出错: {str(e)}")

    def wait_for_api_response(self, timeout=10):
        """等待API响应"""
        self.waiting_for_api = True
        start_time = time.time()
        current_ids_count = len(self.note_ids)

        while self.waiting_for_api and time.time() - start_time < timeout:
            if len(self.note_ids) >= current_ids_count:  # 如果ID数量增加，说明收到了新数据
                return True
            time.sleep(1)
            self.logger.info(f"等待API响应中... 已等待 {int(time.time() - start_time)} 秒")

        return not self.waiting_for_api

    def _check_is_bottom(self):
        """检查是否到达页面底部"""
        try:
            # 获取页面总高度
            total_height = self.page.evaluate("document.documentElement.scrollHeight")
            # 获取可视区域高度
            viewport_height = self.page.evaluate("window.innerHeight")
            # 获取当前滚动位置
            current_scroll = self.page.evaluate("window.pageYOffset")
            
            # 如果当前滚动位置 + 可视区域高度 >= 页面总高度，说明到达底部
            is_bottom = (current_scroll + viewport_height) >= total_height
            if is_bottom:
                self.logger.info("已到达页面底部")
            return is_bottom
        except Exception as e:
            self.logger.error(f"检查页面底部时出错: {str(e)}")
            return False

    def search_next_keyword(self):
        """切换到下一个关键词并进行搜索"""
        if self.current_keyword_index >= len(SEARCH_KEYWORDS):
            self.logger.info("所有关键词都已搜索完毕")
            return False

        current_keyword = SEARCH_KEYWORDS[self.current_keyword_index]
        self.logger.info(f"开始搜索关键词: {current_keyword}")

        # 清空之前的搜索结果
        self.note_ids.clear()
        self.waiting_for_api = False

        # 点击搜索框
        search_box = self.page.locator('.search-input').first
        search_box.click()
        self.common.random_sleep(0.8, 1.7)

        # 清空搜索框
        search_box.fill("")
        self.common.random_sleep(0.3, 1.2)

        # 输入搜索内容
        for char in current_keyword:
            if random.random() < 0.2:
                time.sleep(random.uniform(0.4, 0.8))
            else:
                time.sleep(random.uniform(0.1, 0.3))
            search_box.type(char)

        self.common.random_sleep(0.7, 1.5)

        # 点击搜索按钮
        self.waiting_for_api = True
        search_button = self.page.locator('.input-button').first
        search_button.click()

        # 等待搜索结果加载
        self.logger.info("等待搜索结果加载...")
        self.page.wait_for_selector('div[class*="search-layout__main"]', timeout=10000)
        self.logger.info("搜索结果页面已加载")

        return True

    def scrape_user_notes(self):
        """抓取用户笔记"""
        if not self.is_logged_in:
            self.logger.error("未登录状态，无法抓取数据")
            return False

        try:
            while self.current_keyword_index < len(SEARCH_KEYWORDS):
                if not self.search_next_keyword():
                    break

                # 等待第一次API响应
                self.logger.info("等待第一次notes接口响应...")
                if not self.wait_for_api_response(timeout=15):
                    self.logger.error("未收到第一次notes接口响应")
                    continue

                # 开始滚动加载更多数据
                no_new_data_count = 0
                last_scroll_height = 0
                
                while True:
                    current_height = self.page.evaluate("document.documentElement.scrollHeight")
                    
                    if current_height == last_scroll_height:
                        if self._check_is_bottom():
                            self.logger.info("已到达页面底部，准备切换下一个关键词")
                            break
                    
                    last_scroll_height = current_height
                    self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    self.logger.info("页面已滚动，等待新的notes接口响应...")
                    
                    self.waiting_for_api = True
                    
                    if not self.wait_for_api_response():
                        no_new_data_count += 1
                        if no_new_data_count >= 3:
                            if self._check_is_bottom():
                                self.logger.info("已到达页面底部，准备切换下一个关键词")
                                break
                            self.logger.info("连续3次未收到新数据，准备切换下一个关键词")
                            break
                    else:
                        no_new_data_count = 0
                    
                    self.common.random_sleep(3, 4)

                # 保存当前关键词的结果
                self._save_note_ids(SEARCH_KEYWORDS[self.current_keyword_index])
                
                # 移动到下一个关键词
                self.current_keyword_index += 1
                self.common.random_sleep(2, 3)

            self.logger.info("所有关键词搜索完成")
            return True

        except Exception as e:
            self.logger.error(f"抓取笔记时出错: {str(e)}")
            return False

    def _save_note_ids(self, keyword):
        """保存笔记ID到文件"""
        try:
            if self.note_ids:
                output_file = os.path.join(self.data_dir, f'note_ids_{keyword}_{int(time.time())}.json')
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'keyword': keyword,
                        'ids': sorted(list(self.note_ids)),
                        'count': len(self.note_ids)
                    }, f, ensure_ascii=False, indent=2)
                self.logger.info(f"已保存关键词 '{keyword}' 的 {len(self.note_ids)} 个ID到文件: {output_file}")
        except Exception as e:
            self.logger.error(f"保存ID到文件时出错: {str(e)}")

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
            # 访问首页
            self.page.goto(self.base_url)
            # 页面加载后等待
            self.common.random_sleep(3, 5)

            # 等待并点击登录按钮
            self.logger.info("等待登录按钮出现...")
            login_button = self.page.wait_for_selector("#login-btn", timeout=10000)
            if not login_button:
                self.logger.error("未找到登录按钮")
                return False
            login_button.click()

            # 等待扫码登录界面加载
            self.logger.info("等待扫码登录界面加载...")
            qr_code = self.page.wait_for_selector(".qrcode-img", timeout=10000)
            if not qr_code:
                self.logger.error("未找到二维码")
                return False

            # 获取二维码图片
            qr_src = qr_code.get_attribute("src")
            if not qr_src:
                self.logger.error("未获取到二维码图片地址")
                return False

            # 等待登录成功
            self.logger.info("等待扫码登录...")
            try:
                # 等待个人头像出现，表示登录成功
                avatar = self.page.wait_for_selector(".channel-list >> .user", timeout=60000)  # 给用户60秒扫码时间
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

            self.logger.info("所有资源已正确关闭")
        except Exception as e:
            self.logger.error(f"关闭资源时出错: {str(e)}")

    def _save_cookies(self):
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f)
            logger.info("Cookie数据已保存")
        except Exception as e:
            logger.error(f"保存Cookie时出错: {str(e)}")

    def _load_cookies(self):
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                self.context.add_cookies(cookies)
                logger.info("已加载保存的Cookie")
                return True
            return False
        except Exception as e:
            logger.error(f"加载Cookie时出错: {str(e)}")
            return False

if __name__ == '__main__':
    spider = XiaohongshuSpider()
    try:
        if not spider.is_logged_in:
            spider.login()
        
        if spider.is_logged_in:
            spider.scrape_user_notes()
    finally:
        spider.close()
    