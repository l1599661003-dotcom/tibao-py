import json
import os
import sys
import configparser
from datetime import datetime
import cv2
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback
from core.database_text_tibao_2 import session
from models.models_tibao import (
    TrainingBloggerDetailsSpider,
    TrainingBloggers, TrainingBloggerDetails
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
            'id': config.get('PGY_LOGIN', 'id')
        },
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

        self.base_url = 'https://pgy.xiaohongshu.com/solar/pre-trade/home'

        # 获取配置信息并立即提取到普通变量中，避免数据库会话问题
        try:
            peizhi = session.query(TrainingBloggerDetailsSpider).filter(
                TrainingBloggerDetailsSpider.spider_id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).first()
            
            if not peizhi:
                raise ValueError(f"未找到ID为{self.config['PGY_LOGIN_CONFIG']['id']}的配置信息")
            
            self.peizhi_email = peizhi.email
            self.peizhi_password = peizhi.password

        except Exception as e:
            logger.error(f"加载配置信息失败: {str(e)}")
            raise

        # 配置信息加载完成后，不要关闭数据库会话，后续还需要使用

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
        
        # 添加当前页码和类型属性
        self.current_page = 1
        self.current_type = 1

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
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                # 尝试重新登录
                login_success = self.login()
                if not login_success:
                    logger.error("重新登录失败，程序退出")
                    return None
            peizhi = session.query(TrainingBloggerDetailsSpider).filter(
                TrainingBloggerDetailsSpider.spider_id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).all()
            # 访问页面
            page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/note/kol"
            logger.info(f"开始访问页面: {page_url}")
            try:
                self.page.goto(page_url)
            except Exception as e:
                logger.error(f"访问页面失败: {str(e)}")

            # 检查并点击"已读"按钮
            self._click_ignore_button()
            self.common.random_sleep(2, 3)
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.warning(f"等待网络空闲时出错: {str(e)}")

            try:
                ignore_button_elements = self.page.locator("span:has-text('合作报价')").first
                if ignore_button_elements and ignore_button_elements.is_visible(timeout=3000):
                    ignore_button_elements.click()
            except Exception as e:
                logger.warning(f"点击合作报价表头时出错: {str(e)}")

            notes_type = ""
            notes_type1 = ""
            if peizhi and len(peizhi) > 0 and hasattr(peizhi[0], 'remark'):
                if peizhi[0].remark == '1':
                    notes_type = "图文笔记"
                    notes_type1 = "pic"
                elif peizhi[0].remark == '2':
                    notes_type = "视频笔记"
                    notes_type1 = "video"
            else:
                logger.warning("未找到有效的配置记录或remark字段")
                return None
            # 点击图文笔记下面的"请选择"下拉框
            try:
                filters_item = self.page.locator(f"div.filters-item:has(span:has-text('{notes_type}'))").first

                if filters_item and filters_item.is_visible(timeout=5000):
                    # 在同一个 filters-item 内查找"请选择"下拉框
                    dropdown = filters_item.locator("input[placeholder='请选择']").first

                    if dropdown and dropdown.is_visible(timeout=5000):
                        dropdown.click()
                        self.common.random_sleep(2, 3)
                        self._input_range_values(
                            f"div.d-popover.filters-item-custom.header-cover.{notes_type1} div.d-input-number-main")

                        # 点击确定按钮 - 需要在页面上查找，而不是在dropdown_menu列表中查找
                        confirm_button = self.page.locator("button:has-text('确定')").last
                        if confirm_button and confirm_button.is_visible(timeout=3000):
                            confirm_button.click()
                            self.common.random_sleep(2, 3)
            except Exception as e:
                logger.warning(f"点击{notes_type}下拉框时出错: {str(e)}")
            if peizhi and len(peizhi) > 0:
                # 循环处理每条配置记录的粉丝量范围
                for i, pz in enumerate(peizhi):
                    # 每次循环都点击粉丝量表头
                    try:
                        ignore_button_elements = self.page.locator("span:has-text('粉丝量')").first
                        if ignore_button_elements and ignore_button_elements.is_visible(timeout=3000):
                            ignore_button_elements.click()
                    except Exception as e:
                        logger.warning(f"第 {i+1} 条配置：点击粉丝量表头时出错: {str(e)}")
                    try:
                        logger.info(f"处理第 {i+1} 条配置: 粉丝量范围 {pz.start_id} - {pz.end_id}")
                        self.api_data.clear()  # 清空之前的数据
                        
                        self._input_range_values(
                            "div.d-popover div.filter-select-popover div.d-input-number-main",
                            str(pz.start_id),
                            str(pz.end_id)
                        )
                        
                        # 等待数据加载
                        self.common.random_sleep(5, 10)
                        
                        try:
                            self.page.wait_for_load_state('networkidle', timeout=10000)
                        except Exception as e:
                            logger.warning(f"等待网络空闲时出错: {str(e)}")
                            
                    except Exception as e:
                        logger.warning(f"处理第 {i+1} 条配置时出错: {str(e)}")
                        continue

                    # 等待数据完全加载
                    logger.info(f"等待第 {i+1} 条配置的API数据加载...")
                    max_wait_time = 120  # 最大等待2分钟
                    wait_count = 0
                    while len(self.api_data) == 0 and wait_count < max_wait_time:
                        self.common.random_sleep(2, 3)
                        wait_count += 1
                        if wait_count % 15 == 0:
                            logger.info(f"等待API数据加载中... ({wait_count}/{max_wait_time}秒)")

                    if len(self.api_data) == 0:
                        logger.warning(f"第 {i+1} 条配置等待 {max_wait_time} 秒后仍未获取到API数据，跳过此配置")
                        continue

                    # 处理API数据
                    if self.api_data:
                        # 创建api_data的副本进行遍历
                        api_data_copy = dict(self.api_data)
                        for api_url, response_data in api_data_copy.items():
                            if 'data' not in response_data:
                                continue

                            api_data = response_data['data']

                            if 'v2' in api_url:
                                self.common.random_sleep(5, 10)
                                # 处理第一页数据
                                self.current_page = 1
                                logger.info(f"开始处理第 {self.current_page} 页数据...")
                                self._process_v2(api_data)
                                self.api_data.clear()

                                # 处理后续页面
                                page_count = 1
                                consecutive_failures = 0  # 连续失败次数
                                max_consecutive_failures = 3  # 最大连续失败次数
                                
                                while self._click_next_page():
                                    self.current_page += 1
                                    page_count += 1
                                    logger.info(f"正在处理第 {self.current_page} 页数据... (总计第 {page_count} 页)")

                                    # 等待API数据加载，增加等待时间
                                    self.common.random_sleep(15, 45)

                                    # 检查API数据是否加载完成
                                    max_wait_time = 90  # 增加最大等待时间到90秒
                                    wait_count = 0
                                    while len(self.api_data) == 0 and wait_count < max_wait_time:
                                        self.common.random_sleep(2, 3)
                                        wait_count += 1
                                        if wait_count % 15 == 0:
                                            logger.info(f"等待API数据加载中... ({wait_count}/{max_wait_time}秒)")

                                    if len(self.api_data) == 0:
                                        consecutive_failures += 1
                                        logger.warning(f"第 {self.current_page} 页等待 {max_wait_time} 秒后仍未获取到API数据，连续失败次数: {consecutive_failures}")
                                        
                                        if consecutive_failures >= max_consecutive_failures:
                                            logger.error(f"连续 {max_consecutive_failures} 次失败，停止分页")
                                            break
                                        else:
                                            logger.info(f"继续尝试下一页...")
                                            continue
                                    else:
                                        consecutive_failures = 0  # 重置失败计数

                                    # 处理当前页数据
                                    current_page_processed = False
                                    for api_url, response_data in self.api_data.items():
                                        if 'v2' in api_url and 'data' in response_data:
                                            api_data = response_data['data']
                                            self._process_v2(api_data)
                                            current_page_processed = True
                                            logger.info(f"第 {self.current_page} 页数据处理完成")
                                            break

                                    if not current_page_processed:
                                        consecutive_failures += 1
                                        logger.warning(f"第 {self.current_page} 页没有找到有效的API数据，连续失败次数: {consecutive_failures}")
                                        
                                        if consecutive_failures >= max_consecutive_failures:
                                            logger.error(f"连续 {max_consecutive_failures} 次失败，停止分页")
                                            break
                                        else:
                                            logger.info(f"继续尝试下一页...")
                                            continue
                                    else:
                                        consecutive_failures = 0  # 重置失败计数

                                    # 清空当前页的API数据，准备处理下一页
                                    self.api_data.clear()
                                    
                                    # 如果不是最后一页，等待一下再处理下一页
                                    self.common.random_sleep(5, 8)
                                
                                logger.info(f"粉丝量范围 {pz.start_id} - {pz.end_id} 的分页处理完成，共处理 {page_count} 页")
                                
                                # 分页处理完成后，等待一下再处理下一条配置
                                if i < len(peizhi) - 1:
                                    logger.info(f"等待处理下一条配置...")
                                    self.common.random_sleep(10, 15)
                    else:
                        logger.warning(f"第 {i+1} 条配置没有获取到API数据")

                    # 保存进度和Cookie
                    self._save_cookies()
                
                logger.info("所有博主数据处理完成")

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            self.update_monitor_status(
                status="出错",
                fail_count=self.monitor_data.get('fail_count', 0) + 1
            )
            raise  # 重新抛出异常，让上层处理重启逻辑

    def _process_v2(self, api_data):
        """处理博主数据并更新到TrainingBloggers表"""
        try:
            if api_data.get('code') != 0:
                logger.warning(f"API响应状态异常: {api_data.get('code')}")
                return

            if 'data' not in api_data or 'kols' not in api_data['data']:
                logger.warning("API响应数据结构异常")
                return
        except Exception as e:
            logger.error(f"检查API数据时出错: {str(e)}")
            return

        data = api_data['data']

        try:
            processed_count = 0
            for item in data['kols']:
                user_id = item.get('userId')
                if not user_id:
                    continue
                
                # 检查是否已存在相同的记录
                existing_record = session.query(TrainingBloggers).filter(
                    TrainingBloggers.blogger_dandelion_id == user_id
                ).first()
                existing_record1 = session.query(TrainingBloggerDetails).filter(
                    TrainingBloggerDetails.blogger_dandelion_id == user_id
                ).first()

                # 准备更新数据
                update_data = {
                    'account_id': item.get('redId', ''),  # 小红书ID
                    'nickname': item.get('name', ''),  # 昵称
                    'blogger_dandelion_id': user_id,  # 蒲公英ID
                    'followers_count': item.get('fansNum', 0),  # 粉丝量
                    'organization_name': item.get('noteSign', {}).get('name', '') if item.get('noteSign') else '',  # 机构名称
                    'graphic_price': item.get('picturePrice', 0),  # 图文报价
                    'video_price': item.get('videoPrice', 0),  # 视频报价
                    'tags': self._extract_tags(item.get('contentTags', [])),  # 标签
                    'page': self.current_page,  # 页数
                    'month': datetime.now().strftime('%Y-%m'),  # 动态获取当前月份
                    'updated_at': datetime.now(),  # 更新时间
                    'type': self.current_type,  # 类型
                }

                if existing_record:
                    # 更新已存在的记录
                    for key, value in update_data.items():
                        if hasattr(existing_record, key):
                            setattr(existing_record, key, value)
                    
                    # 设置创建时间（如果不存在）
                    if not existing_record.created_at:
                        existing_record.created_at = datetime.now()
                    
                    session.commit()
                    processed_count += 1
                else:
                    # 创建新记录
                    update_data['created_at'] = datetime.now()  # 设置创建时间
                    new_record = TrainingBloggers(**update_data)
                    session.add(new_record)
                    session.commit()
                    processed_count += 1
                    
                if existing_record1:
                    # 更新已存在的记录
                    for key, value in update_data.items():
                        if hasattr(existing_record1, key):
                            setattr(existing_record1, key, value)

                    session.commit()
                else:
                    # 创建新记录
                    update_data['created_at'] = datetime.now()  # 设置创建时间
                    new_record = TrainingBloggerDetails(**update_data)
                    session.add(new_record)
                    session.commit()

            logger.info(f"本页共处理 {processed_count} 条博主数据")
        except Exception as e:
            logger.error(f"保存博主数据失败: {str(e)}")
            session.rollback()

    def _extract_tags(self, content_tags):
        """提取标签信息"""
        if not content_tags:
            return ''
        
        try:
            tags = []
            for tag in content_tags:
                if isinstance(tag, dict):
                    taxonomy1 = tag.get('taxonomy1Tag')
                    taxonomy2 = tag.get('taxonomy2Tag')
                    if taxonomy1:
                        tags.append(taxonomy1)
                    if taxonomy2:
                        tags.append(taxonomy2)
            
            return ','.join(tags) if tags else ''
        except Exception as e:
            logger.warning(f"提取标签信息时出错: {str(e)}")
            return ''

    # 点击下一页按钮
    def _click_next_page(self):
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

            # 检查按钮是否被禁用
            button_class = next_page_button.get_attribute("class") or ""
            if "disabled" in button_class:
                logger.info("下一页按钮被禁用，确认已到最后一页")
                return False

            # 点击下一页按钮
            logger.info("点击下一页按钮")
            next_page_button.click()

            # 等待页面网络空闲，但不要因为超时而中断
            try:
                self.page.wait_for_load_state('networkidle', timeout=15000)  # 增加超时时间到15秒
                logger.info("网络空闲状态达到，继续等待API数据")
            except Exception as e:
                logger.warning(f"等待网络空闲时出错: {str(e)}，但继续等待API数据")
                # 即使网络空闲等待失败，也要继续等待API数据

            # 额外等待时间，确保API数据完全加载
            self.common.random_sleep(8, 12)

            # 检查是否有新的API响应，增加重试机制
            max_retries = 12  # 增加重试次数
            retry_count = 0
            
            while retry_count < max_retries:
                api_count_after = len(self.api_data)
                logger.info(f"第 {retry_count + 1} 次检查 - 点击前API数量: {api_count_before}, 点击后API数量: {api_count_after}")
                
                if api_count_after > api_count_before:
                    logger.info(f"成功获取到新的API数据，数量: {api_count_after - api_count_before}")
                    return True
                
                # 如果还没有新数据，再等待一下
                if retry_count < max_retries - 1:
                    logger.info(f"等待API数据加载，第 {retry_count + 1} 次重试...")
                    self.common.random_sleep(6, 10)
                
                retry_count += 1
            
            # 如果多次检查后仍没有新数据，检查页面是否有分页按钮
            logger.warning("多次检查后仍没有新的API响应，检查页面状态...")
            
            # 检查是否还有下一页按钮
            try:
                next_page_button_after = self.page.locator(
                    "div.d-pagination div.d-pagination-page:has(span svg path[d='M19 12L31 24L19 36'])").first
                
                if next_page_button_after and next_page_button_after.is_visible(timeout=3000):
                    button_class_after = next_page_button_after.get_attribute("class") or ""
                    if "disabled" not in button_class_after:
                        logger.warning("页面仍有下一页按钮且未禁用，继续分页")
                        # 即使没有API响应，也继续分页
                        return True
                    else:
                        logger.info("下一页按钮已被禁用，确认已到最后一页")
                        return False
                else:
                    logger.info("未找到下一页按钮，确认已到最后一页")
                    return False
                    
            except Exception as e:
                logger.warning(f"检查页面状态时出错: {str(e)}")
                # 如果检查页面状态失败，为了安全起见，返回False
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
                    
                    logger.info(f"成功捕获API响应: {url}, 状态: {response.status}, 当前API数据总数: {len(self.api_data)}")

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

    def _input_range_values(self, selector_type, first = "2000", end = "9999999"):
        """输入范围值到两个输入框并点击确定按钮"""
        try:
            # 等待一下让下拉选项完全展开
            self.common.random_sleep(2, 3)
            try:
                # 查找下拉菜单 - 放宽选择器限制，允许更深层嵌套
                dropdown_menu = self.page.locator(selector_type).all()
                
                if dropdown_menu and len(dropdown_menu) >= 2:
                    # 第一个输入框 - 最小值
                    min_input = dropdown_menu[0].locator("input.d-text").first
                    if min_input and min_input.is_visible(timeout=3000):
                        min_input.click(force=True)
                        min_input.fill("")
                        min_input.fill(first)
                        self.common.random_sleep(1, 2)

                    # 第二个输入框 - 最大值
                    max_input = dropdown_menu[1].locator("input.d-text").first
                    if max_input and max_input.is_visible(timeout=3000):
                        max_input.click(force=True)
                        max_input.fill("")
                        max_input.fill(end)
                        self.common.random_sleep(1, 2)
                    
                    # 点击确定按钮 - 需要在页面上查找，而不是在dropdown_menu列表中查找
                    confirm_button = self.page.locator("button:has-text('确定')").last
                    if confirm_button and confirm_button.is_visible(timeout=3000):
                        confirm_button.click()
                        # 等待页面响应
                        self.common.random_sleep(2, 3)
                        return True
                    else:
                        logger.warning("未找到确定按钮或不可见")
                else:
                    logger.warning(f"未找到足够的下拉菜单元素，当前数量: {len(dropdown_menu) if dropdown_menu else 0}")
            
            except Exception as e:
                logger.warning(f"失败: {str(e)}")

            return False
            
        except Exception as e:
            logger.error(f"输入范围值时出错: {str(e)}")
            return False


def run_spider_task():
    """
    执行爬虫任务 - 只在异常时重启版本
    """
    spider = None
    start_time = datetime.now()
    
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

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"数据抓取任务完成，程序正常结束，总耗时: {duration}")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        logger.info(f"程序异常停止，运行时长: {duration}")
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
    run_spider_task()