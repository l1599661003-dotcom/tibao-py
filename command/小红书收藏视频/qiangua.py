import json
import os
import cv2
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from core.database_text_fangpian import session
from models.models_tibao import XiaohongshuMonth
from unitl.common import Common

"""
    将存好热门的视频链接收藏到小红书收藏夹
"""
class QianGuaSpider:
    def __init__(self):
        self.setup_logger()
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.xiaohongshu.com"
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
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

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        logger.add(os.path.join(log_path, "qiangua_{time}.log"), rotation="1 day", retention="7 days")

    def setup_browser(self):
        """初始化浏览器"""
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
            viewport={'width': 1024, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )

        # 尝试加载已保存的Cookie
        if self._load_cookies():
            # 验证Cookie是否有效
            self.page = self.context.new_page()
            self.page.goto(self.base_url)
            self.common.random_sleep(2, 3)
            try:
                # 检查是否存在用户头像元素
                userSide = self.page.locator(".user.side-bar-component").all()
                logger.info(f"user: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 1 or self.page.locator(".user.side-bar-component").is_visible(timeout=50000):
                    self.is_logged_in = True
                    logger.info("Cookie有效，已自动登录")
                else:
                    logger.info("Cookie已失效，需要重新登录")
            except:
                logger.info("Cookie已失效，需要重新登录")
        else:
            self.page = self.context.new_page()

        # 设置页面超时时间
        self.page.set_default_timeout(20000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

    def login(self):
        """
        使用扫码方式登录小红书
        :return: 是否登录成功
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

            logger.info("开始登录流程...")
            # 访问首页
            self.page.goto(self.base_url)
            self.common.random_sleep()

            # 等待并点击登录按钮
            logger.info("等待登录按钮出现...")
            login_button = self.page.wait_for_selector("text=登录", timeout=10000)
            if not login_button:
                logger.error("未找到登录按钮")
                return False
            login_button.click()
            self.common.random_sleep()

            # 等待登录成功
            logger.info("等待扫码登录...")
            try:
                # 等待个人头像出现，表示登录成功
                avatar = self.page.wait_for_selector(".channel-list >> .user", timeout=60000)  # 给用户60秒扫码时间
                if avatar:
                    logger.info("登录成功！")
                    self.is_logged_in = True

                    # 登录成功后保存Cookie
                    self._save_cookies()

                    return True
                else:
                    logger.error("登录失败，未检测到登录成功状态")
                    return False
            except PlaywrightTimeoutError:
                logger.error("登录超时，请重试")
                return False

        except Exception as e:
            logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def scrape_user_notes(self, zjName):
        """抓取指定用户的笔记信息"""
        try:
            logger.info(f"开始抓取专辑 {zjName}")

            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return None
            logger.info('开始抓取 千瓜数据')
            
            # 使用 SQLAlchemy session 查询
            urls = session.query(XiaohongshuMonth).filter(
                XiaohongshuMonth.status != 1,
                XiaohongshuMonth.data == '2024-10'
            ).order_by(XiaohongshuMonth.id.desc()).all()
            
            logger.info(f"数据列表")
            if len(urls) > 0:
                for url in urls:
                    self.page.goto(url.xiaohongshu_url)  # 使用 SQLAlchemy 模型的属性
                    self.common.random_sleep()
                    # 滚动到页面底部
                    self.page.evaluate("""
                        window.scrollTo({
                            top: document.documentElement.scrollHeight,
                            behavior: 'smooth'
                        });
                    """)
                    try:
                        # 处理收藏按钮
                        self.page.wait_for_selector('.collect-wrapper')
                        fav = self.page.locator('.collect-wrapper >> svg use')
                        collected = fav.get_attribute('xlink:href')
                        if collected == '#collect':
                            fav = self.page.locator('.collect-wrapper')
                            fav.click()
                        else:
                            # 更新状态使用 SQLAlchemy
                            url.status = 1
                            session.commit()
                            continue
                        self.common.random_sleep(1, 2)
                        # 显示加入专辑
                        join = self.page.wait_for_selector("text=加入专辑", timeout=3000)
                        join.click()
                        self.common.random_sleep(1, 2)
                        # 选择要加入的专辑
                        named = self.page.wait_for_selector(f"text={zjName}", timeout=3000)
                        named.click()
                        
                        # 更新状态使用 SQLAlchemy
                        url.status = 1
                        session.commit()
                        
                    except Exception as e:
                        logger.error(f"抓取过程出错: {str(e)}")
                        self.update_monitor_status(
                            status="出错",
                            fail_count=self.monitor_data.get('fail_count', 0) + 1
                        )
                        # 发生错误时回滚
                        session.rollback()
            
            # 保存进度和Cookie
            self._save_cookies()
            
        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            self.update_monitor_status(
                status="出错",
                fail_count=self.monitor_data.get('fail_count', 0) + 1
            )
            session.rollback()
            return None
        finally:
            # 确保会话被正确处理
            session.close()

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
                
            # 确保关闭数据库会话
            session.close()
            
            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")
            logger.error(f"响应URL: {url}")

    def _save_cookies(self):
        """
        保存当前会话的Cookie
        """
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f)
            logger.info("Cookie数据已保存")
        except Exception as e:
            logger.error(f"保存Cookie时出错: {str(e)}")

    def _load_cookies(self):
        """
        加载保存的Cookie
        :return: 是否成功加载Cookie
        """
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

