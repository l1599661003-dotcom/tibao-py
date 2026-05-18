import json
import os
import random
import time
import sys
import configparser
from datetime import datetime

import requests

from models.models_tibao import DouyinSearchList
from core.database_text_tibao_2 import session
from loguru import logger
from playwright.sync_api import sync_playwright
from unitl.common import Common


def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(
            os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../..")


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        os.path.join(get_base_path(), 'xingtu_spider_config.ini'),
        'xingtu_spider_config.ini'
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"已加载配置文件: {config_path}")
            break

    if not config_loaded:
        logger.error("未找到配置文件 xingtu_spider_config.ini")
        raise FileNotFoundError("配置文件不存在")

    # 解析配置
    return {
        'category': {
            'name': config.get('CATEGORY', 'category_name', fallback='时尚'),
            'selector': config.get('CATEGORY', 'category_selector', fallback="span:has-text('时尚')"),
            'computer': config.get('CATEGORY', 'computer')
        },
        'price_range': {
            'min_price': config.getint('PRICE_RANGE', 'min_price', fallback=24000),
            'max_price': config.getint('PRICE_RANGE', 'max_price', fallback=30000)
        }
    }


class XingtuSpider:
    """星图数据抓取器 - 简化版"""

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

        self.base_url = 'https://www.xingtu.cn/ad/creator/market'
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.common = Common()
        self.current_kol = None  # 当前正在处理的KOL信息
        self.api_response_processed = False  # 标记API响应是否已处理
        self.button_clicked = False  # 标记是否已点击确定按钮
        self.current_page = 1  # 当前页码

        # 加载配置
        self.config = load_config()
        logger.info(
            f"配置加载完成 - 品类: {self.config['category']['name']}, 价格区间: {self.config['price_range']['min_price']}-{self.config['price_range']['max_price']}")

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 数据统计
        self.total_authors = 0
        self.processed_pages = 0

        # 企业微信webhook地址
        self.webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b3b0cdf5-62b6-49d7-80d7-6f741c3c2c4d"

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

    def send_wechat_notification(self, message):
        """发送企业微信通知"""
        try:
            data = {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }
            response = requests.post(self.webhook_url, json=data, timeout=5)
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info("✅ 企业微信通知发送成功")
                    return True
                else:
                    logger.warning(f"企业微信通知发送失败: {result}")
                    return False
            else:
                logger.warning(f"企业微信通知发送失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"发送企业微信通知时出错: {str(e)}")
            return False

    def check_and_handle_captcha(self):
        """检测并处理验证码"""
        try:
            logger.info("检测是否出现验证码...")

            # 常见的验证码元素选择器
            captcha_selectors = [
                'div[class*="captcha"]',  # 通用验证码容器
                'div[class*="verify"]',  # 验证容器
                'div[class*="slider"]',  # 滑块验证码
                'iframe[src*="captcha"]',  # iframe验证码
                'div.secsdk-captcha',  # 字节系验证码
                'div.verification',  # 验证弹窗
                'div.verify-wrap',  # 验证包装
            ]

            # 检查是否出现验证码
            captcha_found = False
            for selector in captcha_selectors:
                try:
                    captcha_element = self.page.locator(selector).first
                    if captcha_element.is_visible(timeout=1000):
                        logger.warning(f"⚠️  检测到验证码！选择器: {selector}")
                        captcha_found = True
                        # 发送企业微信通知
                        try:
                            self.send_wechat_notification(
                                f"{self.config['category']['computer']}🔒 星图数据抓取检测到验证码！请尽快手动完成验证，程序已暂停等待...")
                        except:
                            pass
                        break
                except:
                    continue

            if captcha_found:
                logger.warning("=" * 60)
                logger.warning("🔒 检测到验证码，请手动完成验证！")
                logger.warning("验证完成后程序将自动继续...")
                logger.warning("=" * 60)

                # 等待验证码消失，最多等待5分钟
                max_wait_time = 300  # 5分钟
                check_interval = 3  # 每3秒检查一次
                elapsed_time = 0

                while elapsed_time < max_wait_time:
                    time.sleep(check_interval)
                    elapsed_time += check_interval

                    # 检查验证码是否已消失
                    all_disappeared = True
                    for selector in captcha_selectors:
                        try:
                            element = self.page.locator(selector).first
                            if element.is_visible(timeout=500):
                                all_disappeared = False
                                break
                        except:
                            continue

                    if all_disappeared:
                        logger.info(f"✅ 验证码已完成！(等待了 {elapsed_time} 秒)")
                        # 发送完成通知
                        try:
                            self.send_wechat_notification(f"✅ 验证码已完成！程序继续执行 (等待了 {elapsed_time} 秒)")
                        except:
                            pass
                        time.sleep(2)  # 等待页面稳定
                        return True

                    # 每30秒提示一次
                    if elapsed_time % 30 == 0:
                        logger.info(f"仍在等待验证码完成... (已等待 {elapsed_time}/{max_wait_time} 秒)")

                logger.error("❌ 验证码等待超时（5分钟）")
                return False
            else:
                logger.info("✓ 未检测到验证码")
                return True

        except Exception as e:
            logger.error(f"检测验证码时出错: {str(e)}")
            return True  # 出错时继续执行，避免阻塞

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
                self.common.random_sleep(1, 2)  # 减少等待时间

                # 检查是否存在用户头像元素
                userSide = self.page.locator(".text-avatar").all()
                logger.info(f"找到用户头像元素数量: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 0 or self.page.locator(".text-avatar").is_visible(timeout=3000):  # 减少超时时间
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
                self.common.random_sleep(1, 2)  # 减少等待时间

                # 等待5分钟，每5秒检查一次登录状态
                max_wait_time = 300  # 5分钟 = 300秒
                check_interval = 5  # 每5秒检查一次，提高响应速度
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

    def scrape_data(self):
        """抓取数据主流程"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态")
                return False

            min_price = self.config['price_range']['min_price']
            max_price = self.config['price_range']['max_price']
            category = self.config['category']['name']

            logger.info(f"开始抓取数据 - 品类: {category}, 价格区间: {min_price}-{max_price}")

            # 设置筛选条件
            self._set_filters(min_price, max_price)

            # 处理所有页面
            self._process_all_pages()

            logger.info(f"🎉 抓取完成！共处理 {self.processed_pages} 页，累计新增 {self.total_authors} 条数据")
            return True

        except Exception as e:
            logger.error(f"抓取数据失败: {e}")
            return False

    def _set_filters(self, min_price, max_price):
        """设置筛选条件"""
        try:
            category = self.config['category']['name']

            # 点击品类标签
            self.page.locator(self.config['category']['selector']).first.click()
            time.sleep(1)

            # # 点击全选
            # self.page.locator("span:has-text('全选')").first.click()
            # time.sleep(2)
            #
            # # 再次点击品类标签
            # self.page.locator(self.config['category']['selector']).first.click()
            # time.sleep(1)

            # 点击达人报价
            self.page.locator("span:has-text('达人报价')").first.click()
            time.sleep(2)

            # 点击报价区间下拉框
            price_item = self.page.locator("div.price-group-item:has(div.label:has-text('报价区间'))").first
            price_item.locator("div.xt-dropdown").first.click()
            time.sleep(1)

            # 设置价格区间
            inputs = self.page.locator("div.input-wrapper.el-input input[type='number']")
            inputs.nth(0).fill(str(min_price))
            inputs.nth(1).fill(str(max_price))
            time.sleep(1)

            # 点击第一个确定按钮
            self.page.locator("div.custom-actions button.submit-btn:has-text('确定')").first.click()
            time.sleep(2)

            # 清空API数据缓存（关键！）
            self.api_data = {}
            # 重置按钮点击状态
            self.button_clicked = False

            # 点击第二个确定按钮
            self.page.locator("footer button.el-button--primary:has-text('确定')").first.click()

            # 设置按钮点击标志
            self.button_clicked = True
            logger.info("已设置按钮点击标志，后续API响应将被处理")

            try:
                self.page.wait_for_load_state('networkidle', timeout=3000)  # 减少超时时间
            except Exception as e:
                logger.warning(f"等待页面加载完成时出错: {str(e)}")

            # 等待API数据加载
            time.sleep(2)  # 减少等待时间

            # 检测并处理验证码
            if not self.check_and_handle_captcha():
                logger.error("验证码处理失败")
                raise Exception("验证码处理失败")

            logger.info(f"筛选条件设置完成 - 品类: {category}, 价格: {min_price}-{max_price}")

        except Exception as e:
            logger.error(f"设置筛选条件失败: {e}")
            raise

    def _process_all_pages(self):
        """处理所有页面"""
        page_num = 1

        # 处理第一页（点击确定后的当前页）
        logger.info(f"开始处理第 {page_num} 页数据...")
        self._process_current_page_data(page_num)

        # 自动翻页处理后续页面
        while self._click_next_page():
            page_num += 1
            self.processed_pages = page_num
            logger.info(f"正在处理第 {page_num} 页数据...")

            # 等待页面网络空闲
            try:
                self.page.wait_for_load_state('networkidle', timeout=3000)  # 减少超时时间
            except Exception as e:
                logger.warning(f"等待网络空闲时出错: {str(e)}")

            # 处理当前页数据
            self._process_current_page_data(page_num)

            wait_time = random.uniform(10, 15)
            time.sleep(wait_time)
            if page_num == 50:
                continue

        logger.info(f"所有页面处理完成，共处理 {self.processed_pages} 页")

    def _process_current_page_data(self, page_num):
        """处理当前页面的数据"""
        try:
            if not self.api_data:
                logger.warning(f"第 {page_num} 页没有API数据")
                return 0

            logger.info(f"处理第 {page_num} 页的 {len(self.api_data)} 个API响应")

            # 处理API数据并保存到数据库
            api_data_copy = dict(self.api_data)
            total_authors_added = 0
            total_authors_skipped = 0

            for api_url, response_data in api_data_copy.items():
                if 'data' not in response_data:
                    continue

                api_data = response_data['data']
                if 'q' in api_url:
                    authors_added = 0
                    authors_skipped = 0

                    # 确保authors字段存在
                    if 'authors' not in api_data:
                        logger.warning(
                            f"API数据缺少authors字段: {list(api_data.keys()) if isinstance(api_data, dict) else 'Not a dict'}")
                        continue

                    for author_data in api_data['authors']:
                        # 检查是否已存在
                        existing_data = session.query(DouyinSearchList).filter(
                            DouyinSearchList.star_id == author_data['star_id']).first()
                        if existing_data:
                            authors_skipped += 1
                            existing_data.import_status = 6
                            continue

                        # 创建新记录
                        detail = DouyinSearchList(
                            attribute_datas=json.dumps(author_data.get('attribute_datas', {}), ensure_ascii=False),
                            extra_data=json.dumps(author_data.get('extra_data', {}), ensure_ascii=False),
                            items=json.dumps(author_data.get('items', []), ensure_ascii=False),
                            star_id=str(author_data.get('star_id', '')),
                            task_infos=json.dumps(author_data.get('task_infos', {}), ensure_ascii=False),
                            category=self.config['category']['name'],
                            created_at=datetime.now(),
                            updated_at=datetime.now(),
                            import_status=6,
                        )
                        session.add(detail)
                        authors_added += 1

                    # 累计总数
                    total_authors_added += authors_added
                    total_authors_skipped += authors_skipped

                    # 批量提交事务，提高效率
                    try:
                        session.commit()
                        logger.info(f"API响应处理完成: 新增 {authors_added} 条，跳过 {authors_skipped} 条重复数据")
                    except Exception as commit_error:
                        logger.error(f"提交事务时出错: {str(commit_error)}")
                        session.rollback()

            # 清空已处理的API数据，避免重复处理
            for url in api_data_copy.keys():
                if url in self.api_data:
                    del self.api_data[url]

            logger.info(
                f"第 {page_num} 页数据处理完成: 总计新增 {total_authors_added} 条，跳过 {total_authors_skipped} 条重复数据")
            return total_authors_added

        except Exception as e:
            logger.error(f"处理第 {page_num} 页数据时出错: {str(e)}")
            session.rollback()  # 出错时回滚事务
            return 0

    def _click_next_page(self):
        """点击下一页按钮，返回是否成功点击"""
        try:
            # 查找下一页按钮（根据您之前提供的HTML结构）
            next_page_button = self.page.locator("button.btn-next").first

            # 检查按钮是否存在
            if not next_page_button.is_visible(timeout=2000):  # 减少超时时间
                logger.info("未找到下一页按钮，可能已到最后一页")
                return False

            # 检查按钮是否被禁用
            button_disabled = next_page_button.get_attribute("disabled")
            if button_disabled is not None:
                logger.info("下一页按钮被禁用，确认已到最后一页")
                return False

            # 点击下一页按钮
            logger.info("点击下一页按钮")
            next_page_button.click()

            # 检测并处理验证码
            if not self.check_and_handle_captcha():
                logger.warning('验证码处理失败，跳过当前页')
                return False

            # 等待网络空闲
            try:
                self.page.wait_for_load_state('networkidle', timeout=3000)  # 减少超时时间
            except Exception as e:
                logger.warning(f"等待网络空闲时出错: {str(e)}")

            return True

        except Exception as e:
            logger.error(f"点击下一页时出错: {str(e)}")
            return False

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
                                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'status': response.status
                                }
                                logger.info(f"成功存储API数据: {url}, 当前API数据总数: {len(self.api_data)}")

                                # 检查数据结构
                                if 'authors' in data:
                                    authors_count = len(data['authors'])
                                    logger.info(f"API数据包含 {authors_count} 个作者信息")

                                    # 立即处理数据并保存到数据库
                                    self._process_api_data_immediately(url, data)
                                else:
                                    logger.warning(
                                        f"API数据结构异常: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")

                            # 标记API响应已处理
                            self.api_response_processed = True
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
                            attribute_datas=json.dumps(author_data.get('attribute_datas', {}), ensure_ascii=False),
                            extra_data=json.dumps(author_data.get('extra_data', {}), ensure_ascii=False),
                            items=json.dumps(author_data.get('items', []), ensure_ascii=False),
                            star_id=str(author_data.get('star_id', '')),
                            task_infos=json.dumps(author_data.get('task_infos', {}), ensure_ascii=False),
                            category=self.config['category']['name'],
                        )
                        session.add(detail)
                        authors_added += 1

                    # 提交数据库事务
                    session.commit()
                    self.total_authors += authors_added
                    logger.info(f"立即处理API数据完成: 新增 {authors_added} 条，跳过 {authors_skipped} 条重复数据")
                else:
                    logger.warning(f"API数据结构异常，无法处理: {url}")

        except Exception as e:
            logger.error(f"立即处理API数据时出错: {str(e)}")
            session.rollback()

    def _save_author_data(self, author_data):
        """保存作者数据"""
        try:
            star_id = str(author_data.get('star_id', ''))
            nick_name = ""

            # 尝试获取昵称用于日志显示
            try:
                attribute_datas = author_data.get('attribute_datas', {})
                if isinstance(attribute_datas, dict):
                    nick_name = attribute_datas.get('nick_name', '')
                elif isinstance(attribute_datas, str):
                    import json
                    attr_data = json.loads(attribute_datas)
                    nick_name = attr_data.get('nick_name', '')
            except:
                nick_name = "未知"

            # 检查是否已存在
            existing = session.query(DouyinSearchList).filter(
                DouyinSearchList.star_id == star_id
            ).first()

            if existing:
                logger.info(f"📋 数据已存在，跳过: {nick_name} (ID: {star_id})")
                return False

            # 创建新记录
            new_record = DouyinSearchList(
                star_id=star_id,
                attribute_datas=json.dumps(author_data.get('attribute_datas', {}), ensure_ascii=False),
                extra_data=json.dumps(author_data.get('extra_data', {}), ensure_ascii=False),
                items=json.dumps(author_data.get('items', []), ensure_ascii=False),
                task_infos=json.dumps(author_data.get('task_infos', {}), ensure_ascii=False),
                category=self.config['category']['name'],
            )

            session.add(new_record)
            logger.info(f"✅ 新增数据: {nick_name} (ID: {star_id}) - 品类: {self.config['category']['name']}")
            return True

        except Exception as e:
            logger.error(f"❌ 保存数据失败: {e}")
            session.rollback()
            return False

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


def main():
    """
    主函数 - 星图数据抓取程序
    """
    spider = None
    try:
        logger.info("=== 星图数据抓取程序启动 ===")
        spider = XingtuSpider()

        # 3. 初始化浏览器和登录
        spider.setup_browser()
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        # 4. 抓取数据
        result = spider.scrape_data()
        if not result:
            logger.error("抓取数据失败")
            return False

        logger.info("星图数据抓取程序执行完成")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        import traceback
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
        session.close()


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
