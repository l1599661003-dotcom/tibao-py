import json
import os
import sys
import configparser
import time
from datetime import datetime
import cv2
import schedule
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback
from core.database_text_tibao_2 import session
from models.models_tibao import (TrainingBloggerDetails, TrainingBloggerDetailsPeizhi
)
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
            'id': config.get('PGY_LOGIN', 'id'),
            'base_url': config.get('PGY_LOGIN', 'base_url')
        },
        'DELAY_CONFIG': {
            'between_requests': tuple(map(int, config.get('DELAY', 'between_requests').split(','))),
            'page_load_wait': tuple(map(int, config.get('DELAY', 'page_load_wait').split(','))),
            'api_wait': tuple(map(int, config.get('DELAY', 'api_wait').split(','))),
            'login_wait': tuple(map(int, config.get('DELAY', 'login_wait').split(','))),
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

        # 获取配置信息并立即提取到普通变量中，避免数据库会话问题
        try:
            peizhi = session.query(TrainingBloggerDetailsPeizhi).filter(
                TrainingBloggerDetailsPeizhi.id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).first()

            if peizhi:
                # 立即提取所有需要的配置信息到普通变量中
                self.peizhi_email = peizhi.email
                self.peizhi_password = peizhi.password
                self.peizhi_month = peizhi.month
                self.peizhi_video_price = peizhi.video_price
                self.peizhi_graphic_price = peizhi.graphic_price
                self.peizhi_start_id = peizhi.start_id
                self.peizhi_end_id = peizhi.end_id
                logger.info(f"成功加载配置信息: ID={peizhi.id}, 月份={peizhi.month}")
            else:
                logger.error(f"未找到ID为{self.config['PGY_LOGIN_CONFIG']['id']}的配置信息")
                raise ValueError(f"配置信息不存在: ID={self.config['PGY_LOGIN_CONFIG']['id']}")

        except Exception as e:
            logger.error(f"加载配置信息失败: {str(e)}")
            raise

        # 配置信息加载完成后，关闭数据库会话
        session.close()

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
            logger.info("将使用系统默认的playwright浏览器路径")

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
                logger.info("正在验证Cookie有效性...")
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
            try:
                # 访问首页
                self.page.goto(self.base_url)
                self.common.random_sleep()

                # 等待并点击第一个登录按钮
                logger.info("等待第一个登录按钮出现...")
                first_login_button = self.page.wait_for_selector("text=账号登录", timeout=10000)
                if not first_login_button:
                    logger.error("未找到第一个登录按钮")
                    return False
                first_login_button.click()
                self.common.random_sleep(1, 2)

                # 等待并点击弹窗中的账号登录按钮
                logger.info("等待弹窗中的账号登录按钮...")
                second_login_button = self.page.wait_for_selector("text=账号登录 >> nth=1", timeout=10000)
                if not second_login_button:
                    logger.error("未找到弹窗中的账号登录按钮")
                    return False
                second_login_button.click()
                self.common.random_sleep(1, 2)

                # 等待邮箱输入框出现并输入邮箱
                logger.info("正在输入账号密码...")
                email_input = self.page.wait_for_selector("input.css-1dbyz17.css-xno39g.dyn", timeout=5000)
                email_input.fill(self.peizhi_email)
                self.common.random_sleep(1, 2)  # 模拟人工输入间隔

                # 等待密码输入框出现并输入密码
                password_input = self.page.wait_for_selector("input.css-1dbyz17.css-cct1ew.dyn", timeout=5000)
                password_input.fill(self.peizhi_password)
                self.common.random_sleep(1, 2)  # 模拟人工输入间隔

                # 点击登录按钮
                submit_button = self.page.wait_for_selector("button.css-r7neow.css-wp7z9d.dyn.beer-login-btn",
                                                            timeout=5000)
                submit_button.click()

                logger.info("等待登录成功...")
                # 等待个人头像出现，表示登录成功
                avatar = self.page.wait_for_selector(".home_head_user_info", timeout=60000)
                if avatar:
                    logger.info("登录成功！")
                    self.is_logged_in = True

                    # 登录成功后保存Cookie
                    self._save_cookies()

                    return True
                else:
                    logger.error("登录失败，未检测到登录成功状态")
                    return False
            except PlaywrightTimeoutError as e:
                logger.error("登录超时，请重试")
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

            # 访问页面
            page_url = f"https://pgy.xiaohongshu.com/solar/infra/notification-list"
            logger.info(f"开始访问页面: {page_url}")

            self.page.goto(page_url)

            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
            except Exception as e:
                logger.error(f"等待页面加载完成时出错: {str(e)}")

            # 开始分页处理
            page_count = 0
            max_pages = 50  # 最大处理页数，防止无限循环
            total_extracted_ids = 0  # 统计总共提取到的ID数量
            consecutive_duplicate_pages = 0  # 连续重复页面计数

            while page_count < max_pages:
                # 等待页面加载和API响应
                if page_count >= 2:
                    self.common.random_sleep(30, 40)
                page_count += 1
                logger.info(f"正在处理第 {page_count} 页")

                # 处理当前页的API数据
                logger.info(f"检查API数据是否存在: {bool(self.api_data)}, 数量: {len(self.api_data)}")

                if self.api_data:
                    # 创建api_data的副本进行遍历
                    api_data_copy = dict(self.api_data)
                    # 处理消息列表API数据
                    for api_url, response_data in api_data_copy.items():
                        if 'api/adsmessage/solar/message/list' in api_url and 'data' in response_data:
                            try:
                                api_data = response_data['data']
                                if api_data['code'] == 0 and 'data' in api_data:
                                    message_list = api_data['data'].get('kols', [])


                            except Exception as e:
                                logger.error(f"处理第 {page_count} 页消息列表数据时出错: {str(e)}")
                                continue
                else:
                    logger.warning(f"第 {page_count} 页没有获取到API数据")

                # 清空当前页的API数据，准备处理下一页
                self.api_data.clear()

                # 尝试点击下一页
                has_next_page = self._click_next_page()

                # 如果点击失败，尝试重试一次
                if not has_next_page:
                    logger.info("第一次点击失败，尝试重试...")
                    self.common.random_sleep(2, 3)
                    has_next_page = self._click_next_page()

                if not has_next_page:
                    logger.info("已到最后一页，停止处理")
                    break

            logger.info(f"分页处理完成，共处理了 {page_count} 页，总共提取到 {total_extracted_ids} 个ID")

        except Exception as e:
            logger.error(f"处理数据时出错: {str(e)}")
            session.rollback()

            # 保存进度和Cookie
            self._save_cookies()
        finally:
            # 确保会话被正确处理
            session.close()

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
                "div.d-pagination.notice_pagination div.d-pagination-page:has(span svg path[d='M19 12L31 24L19 36'])").first

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

            # 等待页面加载和新的API响应
            self.common.random_sleep(2, 3)

            # 等待页面网络空闲
            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
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
            try:
                from core.database_text_tibao_2 import session as db_session
                db_session.close()
            except:
                pass

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应，只捕获指定的API请求"""
        try:
            url = response.url
            # 从配置获取需要捕获的API路径
            target_apis = ['cooperator/blogger/v2']

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
                logger.info(f"找到Cookie文件: {self.cookie_file}")
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

    def _click_ignore_button(self):
        """
        点击"已读"按钮
        """
        try:
            # 检查是否存在'已读'按钮
            ignore_button_elements = self.page.locator("button:has-text('已读')").all()

            if len(ignore_button_elements) > 0:
                # 找到第一个'已读'按钮
                ignore_button = ignore_button_elements[0]

                # 检查按钮是否可见
                if ignore_button.is_visible(timeout=3000):
                    ignore_button.click()
                    logger.info("成功点击'已读'按钮")
                    return True
                else:
                    logger.debug("'已读'按钮不可见")
            else:
                logger.debug("未找到'已读'按钮")

        except Exception as e:
            logger.debug(f"查找'已读'按钮时出错: {str(e)}")
            return False

        return False


def run_spider_task():
    """
    执行爬虫任务 - 只在异常时重启版本
    """
    spider = None
    try:
        logger.info("开始执行蒲公英数据抓取任务...")

        # 1. 初始化爬虫实例
        spider = PGYSpider()

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
        logger.info(f"调度器启用状态: {scheduler_config['enable_scheduler']}")
        logger.info(f"执行时间: 每天 {scheduler_config['daily_time']}")
        logger.info(f"单次执行模式: {scheduler_config['run_once']}")

        if scheduler_config['run_once']:
            # 单次执行模式
            logger.info("单次执行模式，立即开始任务...")
            success = run_spider_task()
            if not success:
                logger.info("程序异常停止，将在1小时后重启...")
                time.sleep(3600)
                return main()  # 递归重启
            return success

        elif scheduler_config['enable_scheduler']:
            # 调度器模式
            logger.info(f"已设置定时任务，将在每天 {scheduler_config['daily_time']} 执行")

            # 运行调度器
            while True:
                try:
                    schedule.run_pending()
                    time.sleep(scheduler_config['check_interval'])
                except Exception as e:
                    logger.error(f"调度器运行出错: {str(e)}")
                    logger.info("调度器异常停止，将在1小时后重启...")
                    time.sleep(3600)
                    continue

        else:
            # 调度器未启用，直接执行一次
            logger.info("调度器未启用，直接执行任务...")
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
    except KeyboardInterrupt:
        logger.info("收到键盘中断信号，程序退出")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        sys.exit(1)