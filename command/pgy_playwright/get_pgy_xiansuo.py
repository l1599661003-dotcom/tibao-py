import json
import os
import re
import sys
import configparser
import time
import requests
from datetime import datetime, timedelta
import cv2
from loguru import logger
from playwright.sync_api import sync_playwright
import traceback
from core.database_text_tibao_2 import session
from models.models_tibao import FpPgyInvitationsInfo, FpPgyInvitationsMessage
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
            'base_url': config.get('PGY_LOGIN', 'base_url')
        },
        'PGY_USER_ID': {
            'user_id': config.get('PGY_USER', 'user_id'),
        },
        'BROWSER_CONFIG': {
            'headless': config.getboolean('BROWSER', 'headless'),
            'viewport': {
                'width': config.getint('BROWSER', 'viewport_width'),
                'height': config.getint('BROWSER', 'viewport_height')
            },
            'user_agent': config.get('BROWSER', 'user_agent'),
            'timeout': config.getint('BROWSER', 'timeout'),
            'args': [
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        },
        'DELAY_CONFIG': {
            'between_requests': tuple(map(int, config.get('DELAY', 'between_requests').split(','))),
            'page_load_wait': tuple(map(int, config.get('DELAY', 'page_load_wait').split(','))),
            'api_wait': tuple(map(int, config.get('DELAY', 'api_wait').split(','))),
            'login_wait': tuple(map(int, config.get('DELAY', 'login_wait').split(','))),
        },
        'SCHEDULER_CONFIG': {
            'enable_scheduler': config.getboolean('SCHEDULER', 'enable_scheduler'),
            'daily_time': config.get('SCHEDULER', 'daily_time'),
            'run_once': config.getboolean('SCHEDULER', 'run_once'),
            'check_interval': config.getint('SCHEDULER', 'check_interval')
        },
        'LOOP_CONFIG': {
            'cycle_interval_hours': config.getint('LOOP', 'cycle_interval_hours', fallback=1),
            'enable_loop': config.getboolean('LOOP', 'enable_loop', fallback=True)
        },
        'USER_NAME': {
            'username': config.get('USERNAME', 'username', fallback='未知用户'),
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
        self.current_user_data = {}
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

        self.playwright = sync_playwright().start()
        # 配置浏览器选项
        self.browser = self.playwright.chromium.launch(
            headless=self.config['BROWSER_CONFIG']['headless'],
            args=self.config['BROWSER_CONFIG']['args']
        )
        # 创建上下文
        self.context = self.browser.new_context(
            viewport=self.config['BROWSER_CONFIG']['viewport'],
            user_agent=self.config['BROWSER_CONFIG']['user_agent']
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
        self.page.set_default_timeout(self.config['BROWSER_CONFIG']['timeout'])
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

    def login(self):
        """
        等待用户手动登录，最多等待5分钟
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

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
                        if 'api/solar/user/info' in api_url and 'data' in response_data:
                            api_data = response_data['data']
                            if api_data['code'] == 0 and 'data' in api_data:
                                message_list = api_data['data']
                                self.current_user_data.update({
                                    'platform_user_id' : message_list['redId'],
                                    'platform_nickname' : message_list['nickName'],
                                })

                        if 'api/adsmessage/solar/message/list' in api_url and 'data' in response_data:
                            try:
                                api_data = response_data['data']
                                if api_data['code'] == 0 and 'data' in api_data:
                                    message_list = api_data['data'].get('messageList', [])

                                    extracted_count = 0
                                    saved_count = 0
                                    duplicate_count = 0
                                    
                                    for message in message_list:
                                        content = message.get('content', '')
                                        kol_id = message.get('id', '')
                                        # 从content中提取ID
                                        extracted_id = self._extract_id_from_content(content)

                                        if extracted_id:
                                            extracted_count += 1
                                            total_extracted_ids += 1

                                            # 检查是否已存在相同的记录
                                            if self._check_message_exists(self.current_user_data['platform_user_id'], extracted_id, kol_id):
                                                duplicate_count += 1
                                                continue

                                            # 检查是否已经在FpPgyInvitationsInfo表中存在
                                            status = self._check_invitation_info_exists(kol_id, extracted_id)
                                            
                                            # 保存到数据库
                                            if self._save_message_to_database(self.current_user_data['platform_user_id'], extracted_id, content, kol_id, status, self.current_user_data['platform_nickname']):
                                                saved_count += 1
                                        else:
                                            continue

                                    logger.info(f"第 {page_count} 页统计: 提取到 {extracted_count} 个ID, 保存 {saved_count} 个, 重复 {duplicate_count} 个")
                                    
                                    # 判断当前页是否主要是重复数据
                                    if extracted_count > 0 and saved_count == 0:
                                        consecutive_duplicate_pages += 1
                                        logger.info(f"第 {page_count} 页主要是重复数据，连续重复页面数: {consecutive_duplicate_pages}")
                                        
                                        # 如果连续3页都是重复数据，停止翻页
                                        if consecutive_duplicate_pages >= 3:
                                            logger.info(f"连续 {consecutive_duplicate_pages} 页都是重复数据，停止翻页处理")
                                            return
                                    else:
                                        # 如果有新数据，重置连续重复计数
                                        consecutive_duplicate_pages = 0
                                    
                                    # 检查当前页重复数据是否达到5条，如果是则停止翻页
                                    if duplicate_count >= 5:
                                        logger.info(f"第 {page_count} 页重复数据达到5条，停止翻页处理")
                                        return

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

    def scrape_user_notes1(self):
        """抓取博主信息 - 重构版本，匹配PHP逻辑"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return None

            messages = session.query(FpPgyInvitationsMessage).filter(
                FpPgyInvitationsMessage.status == 0,
                FpPgyInvitationsMessage.platform_user_id == self.config['PGY_USER_ID']['user_id']
            ).all()

            for message in messages:
                # 等待页面加载和API响应
                self.api_data.clear()
                self.current_user_data.clear()
                agency_name = self.extract_agency_name(message.platform_content)
                # 访问页面
                page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/invite-detail?id={message.platform_message_id}"
                logger.info(f"开始访问页面: {page_url}")
                self.page.goto(page_url)
                self.common.random_sleep(25, 35)
                # 等待页面加载完成
                try:
                    self.page.wait_for_load_state('networkidle', timeout=10000)
                except Exception as e:
                    logger.error(f"等待页面加载完成时出错: {str(e)}")

                if self.api_data:
                    # 创建api_data的副本进行遍历
                    api_data_copy = dict(self.api_data)
                    for api_url, response_data in api_data_copy.items():
                        if 'get_invite_info' in api_url and 'data' in response_data:
                            api_data = response_data['data']
                            if api_data['code'] == 0:
                                # 检查是否存在invite字段
                                if 'invite' in api_data:
                                    invite_info = api_data['invite']
                                elif 'data' in api_data and 'invite' in api_data['data']:
                                    invite_info = api_data['data']['invite']
                                else:
                                    logger.warning(f"API响应中没有找到invite字段: {api_data}")
                                    continue
                                try:
                                    existing_record = session.query(FpPgyInvitationsInfo).filter(
                                        FpPgyInvitationsInfo.message_id == message.platform_message_id
                                    ).first()
                                    if existing_record:
                                        logger.info(
                                            f"邀请信息已存在: kol_id={message.platform_kol_id}, extracted_id={message.platform_message_id}")
                                        continue
                                    if invite_info.get('kolName', '') == 2:
                                        message.status = 2
                                        session.commit()
                                        logger.info(f"{message.platform_message_id}博主不感兴趣，跳过")
                                        continue
                                    self.current_user_data.update({
                                        'message_id' : self._extract_id_from_content(message.platform_content),
                                        'invitation_details': message.platform_content,
                                        'company_remarks': agency_name,
                                        'recorded_at': message.created_at.strftime(
                                            '%Y-%m-%d %H:%M:%S') if message.created_at else '',
                                        'blogger_name': invite_info.get('kolName', ''),
                                        'blogger_link': f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{invite_info.get('kolId', '')}",
                                        'blogger_intent': invite_info.get('kolIntention', ''),
                                        'brand_name': invite_info.get('cooperateBrandName', ''),
                                        'cooperation_type': invite_info.get('inviteType', ''),
                                        'product_name': invite_info.get('productName', ''),
                                        'expected_publish_period_start': invite_info.get('expectedPublishTimeStart',
                                                                                         ''),
                                        'expected_publish_period_end': invite_info.get('expectedPublishTimeEnd',
                                                                                       ''),
                                        'cooperation_content': invite_info.get('inviteContent', ''),
                                        'invitation_initiation_time': invite_info.get('inviteCreateTime', ''),
                                        'organization': message.platform_nickname,
                                        'clue_type': '邀约',
                                        'platform_user_id': invite_info.get('kolId', ''),
                                        'account_source': message.platform_nickname,  # 添加账号来源标识
                                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    })
                                    # 点击眼睛图标并获取联系方式
                                    contact_value = self._click_eyes()
                                    self.common.random_sleep(10, 15)

                                    if contact_value:
                                        logger.info(f"成功获取到联系方式: {contact_value}")
                                        self.current_user_data.update({
                                            'contact_information': contact_value,
                                        })
                                    logger.info(f"最终数据整合{self.current_user_data}")

                                    # 将数据插入到FpPgyInvitationsInfo表中
                                    try:
                                        # 数据预处理和验证
                                        def safe_datetime_parse(date_str, format_str):
                                            """安全的时间解析函数"""
                                            if not date_str:
                                                return None
                                            try:
                                                return datetime.strptime(date_str, format_str)
                                            except (ValueError, TypeError):
                                                logger.warning(f"时间格式解析失败: {date_str}")
                                                return None

                                        def safe_str(value):
                                            """安全的字符串转换"""
                                            if value is None:
                                                return ''
                                            return str(value)

                                        invitation_info = FpPgyInvitationsInfo(
                                            message_id=safe_str(
                                                self.current_user_data.get('message_id', '')),
                                            invitation_details=safe_str(
                                                self.current_user_data.get('invitation_details', '')),
                                            company_remarks=safe_str(
                                                self.current_user_data.get('company_remarks', '')),
                                            recorded_at=safe_datetime_parse(
                                                self.current_user_data.get('recorded_at', ''), '%Y-%m-%d %H:%M:%S'),
                                            blogger_name=safe_str(self.current_user_data.get('blogger_name', '')),
                                            blogger_link=safe_str(self.current_user_data.get('blogger_link', '')),
                                            blogger_intent=safe_str(
                                                self.current_user_data.get('blogger_intent', '')),
                                            brand_name=safe_str(self.current_user_data.get('brand_name', '')),
                                            cooperation_type=safe_str(
                                                self.current_user_data.get('cooperation_type', '')),
                                            product_name=safe_str(self.current_user_data.get('product_name', '')),
                                            expected_publish_period_start=safe_datetime_parse(
                                                self.current_user_data.get('expected_publish_period_start', ''),
                                                '%Y-%m-%d'),
                                            expected_publish_period_end=safe_datetime_parse(
                                                self.current_user_data.get('expected_publish_period_end', ''),
                                                '%Y-%m-%d'),
                                            cooperation_content=safe_str(
                                                self.current_user_data.get('cooperation_content', '')),
                                            contact_information=safe_str(
                                                self.current_user_data.get('contact_information', '')),
                                            invitation_initiation_time=safe_datetime_parse(
                                                self.current_user_data.get('invitation_initiation_time', ''),
                                                '%Y-%m-%d %H:%M:%S'),
                                            organization=safe_str(self.current_user_data.get('organization', '')),
                                            clue_type=safe_str(self.current_user_data.get('clue_type', '')),
                                            platform_user_id=safe_str(
                                                self.current_user_data.get('platform_user_id', '')),
                                            account_source=safe_str(
                                                self.current_user_data.get('account_source', '')),
                                            created_at=datetime.now(),
                                            is_handle=2
                                        )

                                        session.add(invitation_info)
                                        session.commit()
                                        logger.info(f"插入的数据详情: ID: {invitation_info.id}, 博主={invitation_info.blogger_name}, 品牌={invitation_info.brand_name}, 联系方式={invitation_info.contact_information}")

                                        # 立即同步到API
                                        try:
                                            serialized_data = serialize_record(invitation_info)
                                            if sync_single_record_to_api(serialized_data):
                                                logger.info(f"数据同步到API成功，记录ID: {invitation_info.id}")
                                            else:
                                                logger.error(f"数据同步到API失败，记录ID: {invitation_info.id}")
                                        except Exception as sync_error:
                                            logger.error(f"同步数据到API时出错: {str(sync_error)}")

                                    except Exception as db_error:
                                        logger.error(f"插入数据库时出错: {str(db_error)}")
                                        session.rollback()
                                        continue

                                except Exception as e:
                                    logger.error(f"更新数据时出错: {str(e)}")
                                    logger.error(f"invite_info内容: {invite_info}")
                                    continue

                    self.common.random_sleep(3, 6)  # 防止请求过快
                    message.status = 1
                    session.commit()
                    logger.info(f"已更新消息状态为已处理: {message.platform_message_id}")

                self.common.random_sleep(3, 6)  # 防止请求过快

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            self.update_monitor_status(
                status="出错",
                fail_count=self.monitor_data.get('fail_count', 0) + 1
            )
            session.rollback()
            return None
        finally:
            # 确保会话被正确处理
            session.close()

    def extract_agency_name(self, message_content: str) -> str:
        """
        从消息内容中提取机构名称

        Args:
            message_content: 消息内容

        Returns:
            机构名称
        """
        # 使用正则表达式匹配括号内的内容
        match = re.search(r'（(.+?)）', message_content)
        if match:
            return match.group(1)
        return ''

    def _click_eyes(self):
        """
        点击联系方式对应的眼睛图标

        Returns:
            bool: 是否成功点击眼睛图标
        """
        try:

            # 方法1: 通过文本内容定位联系方式，然后找到同级的item-right中的眼睛图标
            contact_selector = "div.item-left span:has-text('联系方式')"

            # 检查是否存在联系方式元素
            contact_elements = self.page.locator(contact_selector).all()

            # 获取第一个联系方式元素
            contact_element = contact_elements[0]

            # 找到该元素的父级容器（item-left）
            item_left = contact_element.locator("xpath=ancestor::div[contains(@class, 'item-left')]")

            # 找到同级的item-right容器
            item_right = item_left.locator("xpath=following-sibling::div[contains(@class, 'item-right')]")

            # 在item-right中查找眼睛图标（SVG）
            eyes_icon = item_right.locator("span.d-icon svg").first

            # 检查眼睛图标是否存在
            if not eyes_icon.is_visible(timeout=3000):
                logger.warning("未找到眼睛图标，尝试其他选择器...")

                # 备用方法：直接查找SVG图标
                eyes_icon = item_right.locator("svg").first

                if not eyes_icon.is_visible(timeout=3000):
                    logger.error("未找到眼睛图标")
                    return False

            # 点击眼睛图标
            eyes_icon.click()

            # 等待页面响应
            self.common.random_sleep(2, 3)

            # 等待页面网络空闲
            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
            except Exception as e:
                logger.warning(f"等待网络空闲时出错: {str(e)}")

            # 获取联系方式的值
            contact_value = self._get_contact_info()
            return contact_value

        except Exception as e:
            logger.error(f"点击眼睛图标时出错: {str(e)}")
            return None

    def _get_contact_info(self):
        """
        获取联系方式的值

        Returns:
            str: 联系方式的值，如果未找到则返回None
        """
        try:

            # 方法1: 查找包含联系方式的文本内容
            contact_text_selector = "span.d-text:has-text('联系方式')"
            contact_text_elements = self.page.locator(contact_text_selector).all()

            if len(contact_text_elements) > 0:
                # 找到包含"联系方式"的元素，然后查找同级的item-right中的文本
                for contact_element in contact_text_elements:
                    # 找到该元素的父级容器（item-left）
                    item_left = contact_element.locator("xpath=ancestor::div[contains(@class, 'item-left')]")

                    # 找到同级的item-right容器
                    item_right = item_left.locator("xpath=following-sibling::div[contains(@class, 'item-right')]")

                    # 在item-right中查找文本内容
                    text_elements = item_right.locator("span.d-text").all()

                    for text_element in text_elements:
                        text_content = text_element.text_content().strip()
                        if text_content and text_content != "联系方式":
                            return text_content

            # 方法2: 直接查找item-right中的文本内容
            item_right_elements = self.page.locator("div.item-right").all()

            for item_right in item_right_elements:
                # 查找该item-right中的文本内容
                text_elements = item_right.locator("span.d-text").all()

                for text_element in text_elements:
                    text_content = text_element.text_content().strip()
                    if text_content and text_content != "联系方式" and len(text_content) > 0:
                        logger.info(f"方法2找到联系方式值: {text_content}")
                        return text_content

            # 方法3: 查找所有包含文本的span元素
            all_text_spans = self.page.locator("span.d-text").all()

            for span in all_text_spans:
                text_content = span.text_content().strip()
                if text_content and text_content != "联系方式" and len(text_content) > 0:
                    # 检查这个span是否在item-right中
                    parent_item_right = span.locator("xpath=ancestor::div[contains(@class, 'item-right')]")
                    if parent_item_right.count() > 0:
                        logger.info(f"方法3找到联系方式值: {text_content}")
                        return text_content

            logger.warning("未找到联系方式的值")
            return None

        except Exception as e:
            logger.error(f"获取联系方式值时出错: {str(e)}")
            return None
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
        """处理API响应，只捕获指定的API请求"""
        try:
            url = response.url
            # 从配置获取需要捕获的API路径
            target_apis = [
                'api/adsmessage/solar/message/list',
                'api/solar/user/info',
                'api/solar/invite/get_invite_info'
            ]

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
            next_page_button = self.page.locator("div.d-pagination.notice_pagination div.d-pagination-page:has(span svg path[d='M19 12L31 24L19 36'])").first
            
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

    def _extract_id_from_content(self, content):
        """
        从content中提取ID

        Args:
            content: 消息内容字符串

        Returns:
            str: 提取到的ID，如果没有找到则返回None
        """
        try:
            if not content:
                return None

            # 查找包含id=的链接
            import re
            # 匹配 id= 后面的32位字符（字母和数字的组合）
            pattern = r'id=([a-f0-9]{32})'
            match = re.search(pattern, content)

            if match:
                return match.group(1)
            else:
                return None

        except Exception as e:
            logger.error(f"提取ID时出错: {str(e)}")
            return None

    def _save_message_to_database(self, platform_user_id, platform_message_id, content, kol_id, status, platform_nickname):
        """
        保存消息到数据库

        Args:
            platform_user_id: 平台用户ID
            platform_message_id: 平台消息ID

        Returns:
            bool: 是否成功保存
        """
        try:
            # 检查是否已存在相同的记录
            existing_record = session.query(FpPgyInvitationsMessage).filter(
                FpPgyInvitationsMessage.platform_user_id == platform_user_id,
                FpPgyInvitationsMessage.platform_message_id == platform_message_id,
                FpPgyInvitationsMessage.platform_kol_id == kol_id,
            ).first()

            if existing_record:
                logger.info(f"记录已存在，跳过保存: platform_user_id={platform_user_id}, platform_message_id={platform_message_id}")
                return False

            # 创建新记录
            new_message = FpPgyInvitationsMessage(
                platform_user_id=platform_user_id,
                platform_message_id=platform_message_id,
                platform_kol_id=kol_id,
                platform_nickname=platform_nickname,
                platform_content=content,
                status=status,
                created_at=datetime.now()
            )

            session.add(new_message)
            session.commit()
            logger.info(f"成功保存到数据库: platform_user_id={platform_user_id}, platform_message_id={platform_message_id}")
            return True

        except Exception as e:
            logger.error(f"保存到数据库时出错: {str(e)}")
            session.rollback()
            return False

    def _check_invitation_info_exists(self, kol_id, extracted_id):
        """
        检查邀请信息是否已存在

        Args:
            kol_id: 博主ID
            extracted_id: 提取的邀请ID

        Returns:
            int: 1表示已存在，0表示不存在
        """
        try:
            # 查询FpPgyInvitationsInfo表中是否存在相同的邀请ID
            existing_record = session.query(FpPgyInvitationsInfo).filter(
                FpPgyInvitationsInfo.platform_user_id == kol_id,
                FpPgyInvitationsInfo.invitation_details.like(f'%id={extracted_id}%')
            ).first()

            if existing_record:
                logger.info(f"邀请信息已存在: kol_id={kol_id}, extracted_id={extracted_id}")
                return 1
            else:
                return 0

        except Exception as e:
            logger.error(f"检查邀请信息是否存在时出错: {str(e)}")
            return 0

    def _check_message_exists(self, platform_user_id, platform_message_id, kol_id):
        """
        检查消息是否已存在

        Args:
            platform_user_id: 平台用户ID
            platform_message_id: 平台消息ID

        Returns:
            bool: 是否存在
        """
        try:
            existing_record = session.query(FpPgyInvitationsMessage).filter(
                FpPgyInvitationsMessage.platform_user_id == platform_user_id,
                FpPgyInvitationsMessage.platform_message_id == platform_message_id,
                FpPgyInvitationsMessage.platform_kol_id == kol_id
            ).first()

            return existing_record is not None

        except Exception as e:
            logger.error(f"检查消息是否存在时出错: {str(e)}")
            return False

def serialize_record(record):
    """
    序列化SQLAlchemy记录对象为字典
    
    Args:
        record: SQLAlchemy记录对象
        
    Returns:
        dict: 序列化后的字典
    """
    result = {}
    for column in record.__table__.columns:
        value = getattr(record, column.name)
        
        # 处理datetime类型
        if isinstance(value, datetime):
            value = value.strftime('%Y-%m-%d %H:%M:%S')
        
        # 处理None值
        if value is None:
            value = ''
        
        result[column.name] = value
    
    return result

def sync_single_record_to_api(record_data):
    """
    同步单条记录到API
    
    Args:
        record_data: 记录数据字典
        
    Returns:
        bool: 是否同步成功
    """
    try:
        url = "http://47.104.76.46:19000/api/v1/sync/spider/data"
        headers = {"Content-Type": "application/json"}
        
        payload = {
            "tb_name": "fp_pgy_invitations_info",
            "tb_data": [record_data],
            "client_id": "1"
        }
        
        response = requests.post(
            url, 
            headers=headers, 
            data=json.dumps(payload, ensure_ascii=False), 
            timeout=30
        )
        
        if response.status_code == 200:
            return True
        else:
            logger.error(f"单条数据同步失败，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"单条数据同步异常: {str(e)}")
        return False

def run_spider_task():
    """
    执行爬虫任务 - 循环执行版本
    """
    spider = None
    cycle_count = 0
    notification_sent = False  # 标记是否已发送通知
    
    try:
        # 1. 初始化爬虫实例
        spider = PGYSpider()

        # 2. 执行登录
        logger.info("开始登录流程...")
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        logger.info("登录成功，开始循环执行数据抓取...")

        # 3. 循环执行抓取任务
        cycle_interval_hours = spider.config.get('LOOP_CONFIG', {}).get('cycle_interval_hours', 1)
        cycle_interval_seconds = cycle_interval_hours
        
        while True:
            cycle_count += 1
            
            try:
                try:
                    spider.scrape_user_notes()
                except Exception as e:
                    logger.error(f"❌ scrape_user_notes 执行失败: {str(e)}")
                    logger.error(f"错误详情: {traceback.format_exc()}")
                    # 继续执行下一个任务，不中断整个循环
                
                time.sleep(20)
                # 执行第二阶段的抓取任务
                try:
                    spider.scrape_user_notes1()
                except Exception as e:
                    logger.error(f"❌ scrape_user_notes1 执行失败: {str(e)}")
                    logger.error(f"错误详情: {traceback.format_exc()}")
                    # 继续执行下一轮循环，不中断整个程序
                
                logger.info(f"第 {cycle_count} 轮数据抓取完成")
                
                # 等待指定时间后继续下一轮
                logger.info(f"等待 {cycle_interval_hours} 秒后开始下一轮抓取...")
                logger.info(f"下次执行时间: {(datetime.now() + timedelta(hours=cycle_interval_hours)).strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(cycle_interval_seconds)
                
            except KeyboardInterrupt:
                logger.warning("用户手动中断程序")
                break
            except Exception as e:
                logger.error(f"第 {cycle_count} 轮执行出现未预期错误: {str(e)}")
                logger.error(f"错误详情: {traceback.format_exc()}")
                logger.info(f"等待 {cycle_interval_hours} 秒后重试...")
                time.sleep(cycle_interval_seconds)  # 即使出错也等待指定时间后重试
                continue

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        
        # 只在程序异常停止时发送一次通知
        if not notification_sent:
            username = spider.config.get('USER_NAME', {}).get('username', '未知用户') if spider else '未知用户'
            error_message = f"🚨 {username}的线索程序异常停止\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n错误: {str(e)}"
            send_wecom_message(error_message)
            notification_sent = True
        
        return False
    finally:
        # 确保资源被正确释放
        if spider:
            try:
                spider.close()
                logger.info("资源清理完成")
            except Exception as e:
                logger.error(f"清理资源时出错: {str(e)}")


# def main():
#     """
#     主函数 - 调度器版本
#     """
#     try:
#         # 加载配置
#         config = load_config()
#         scheduler_config = config['SCHEDULER_CONFIG']
#
#         logger.info("=== 蒲公英数据抓取调度器启动 ===")
#         logger.info(f"调度器启用状态: {scheduler_config['enable_scheduler']}")
#         logger.info(f"执行时间: 每天 {scheduler_config['daily_time']}")
#         logger.info(f"单次执行模式: {scheduler_config['run_once']}")
#
#         if scheduler_config['run_once']:
#             # 单次执行模式
#             logger.info("单次执行模式，立即开始任务...")
#             success = run_spider_task()
#             return success
#
#         elif scheduler_config['enable_scheduler']:
#             # 调度器模式
#             logger.info("调度器模式启动...")
#
#             # 设置定时任务
#             schedule.every().day.at(scheduler_config['daily_time']).do(run_spider_task)
#
#             logger.info(f"已设置定时任务，将在每天 {scheduler_config['daily_time']} 执行")
#             logger.info("调度器运行中，按 Ctrl+C 停止...")
#
#             # 运行调度器
#             while True:
#                 schedule.run_pending()
#                 import time
#                 time.sleep(scheduler_config['check_interval'])
#
#         else:
#             # 调度器未启用，直接执行一次
#             logger.info("调度器未启用，直接执行任务...")
#             success = run_spider_task()
#             return success
#
#     except KeyboardInterrupt:
#         logger.info("调度器已停止")
#         return True
#     except Exception as e:
#         logger.error(f"调度器启动失败: {str(e)}")
#         logger.error(f"错误详情: {traceback.format_exc()}")
#         return False

def send_wecom_message(content):
    """
    发送企业微信机器人通知
    
    Args:
        content: 要发送的消息内容
    """
    try:
        webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=4a0b3ddf-ab21-4c0f-8cde-86e299ca889e"

        headers = {
            "Content-Type": "application/json"
        }

        data = {
            "msgtype": "text",
            "text": {
                "content": content
            }
        }
        response = requests.post(webhook_url, headers=headers, data=json.dumps(data), timeout=10)
        if response.status_code == 200:
            logger.info("企业微信通知发送成功")
        else:
            logger.error(f"企业微信通知发送失败: {response.status_code} - {response.text}")
            
    except requests.exceptions.Timeout:
        logger.error("企业微信通知发送超时")
    except requests.exceptions.RequestException as e:
        logger.error(f"企业微信通知发送网络错误: {str(e)}")
    except Exception as e:
        logger.error(f"企业微信通知发送异常: {str(e)}")


if __name__ == "__main__":
    logger.info("=== 蒲公英数据抓取程序启动 ===")
    
    try:
        # 直接调用循环执行函数
        run_spider_task()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        # 用户手动中断不发送通知
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
    finally:
        logger.info("程序结束")