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
from models.models_tibao import (
    TrainingBloggerDetails,
    FpOutBloggerNoteDetail,
    FpOutBloggerFansProfile,
    FpOutBloggerFansSummary,
    FpOutBloggerFansHistory,
    FpOutBloggerInfo,
    FpOutBloggerDataSummary,
    TrainingBloggerDetailsPeizhi, TrainingBloggers
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
        base_path = os.path.abspath(".")
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

        self.base_url = 'https://pgy.xiaohongshu.com'

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
        from core.database_text_tibao_2 import session as new_session

        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                # 尝试重新登录
                login_success = self.login()
                if not login_success:
                    logger.error("重新登录失败，程序退出")
                    return None

            # 查询需要更新的博主数据 - 匹配SQL查询逻辑
            urls = new_session.query(TrainingBloggers).filter(
                TrainingBloggers.month == self.peizhi_month,
                TrainingBloggers.status == 0,
                (TrainingBloggers.video_price >= self.peizhi_video_price) | (TrainingBloggers.graphic_price >= self.peizhi_graphic_price),
                # TrainingBloggerDetails.video_price >= self.peizhi_video_price,
                # TrainingBloggerDetails.graphic_price >= self.peizhi_graphic_price,
                TrainingBloggers.id >= self.peizhi_start_id,
                TrainingBloggers.id <= self.peizhi_end_id,
                TrainingBloggers.blogger_dandelion_id.isnot(None),
                TrainingBloggers.blogger_dandelion_id != ''
            ).all()

            if len(urls) <= 0:
                logger.info("没有找到需要处理的博主数据")
                return

            logger.info(f"找到 {len(urls)} 个博主数据")

            for url in urls:
                if not url.blogger_dandelion_id:
                    continue

                try:
                    # 清空之前的数据
                    self.api_data.clear()

                    logger.info(f"正在处理博主: {url.nickname}")

                    # 访问页面
                    page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{url.blogger_dandelion_id}"
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

                    # 检查并点击"已读"按钮
                    self._click_ignore_button()

                    self.common.random_sleep(30, 40)

                    if self.api_data:
                        # 创建api_data的副本进行遍历，参考原版
                        api_data_copy = dict(self.api_data)
                        for api_url, response_data in api_data_copy.items():
                            if 'data' not in response_data:
                                continue

                            api_data = response_data['data']

                            if 'fans_profile' in api_url:
                                self._process_fans_profile(api_data, url.blogger_dandelion_id, new_session)
                            elif 'data_summary' in api_url:
                                self._process_data_summary(api_data, url.blogger_dandelion_id, new_session)
                            elif 'fans_history' in api_url:
                                self._process_fans_history(api_data, url.blogger_dandelion_id, new_session)
                            elif 'fans_summary' in api_url:
                                self._process_fans_summary(api_data, url.blogger_dandelion_id, new_session)
                            elif 'blogger' in api_url:
                                self._process_blogger(api_data, url.blogger_dandelion_id, new_session)

                        # 所有 API 数据处理完毕后，点击笔记按钮
                        try:
                            self.api_data.clear()
                            # 点击合作笔记按钮
                            if self._click_eyes():
                                self.common.random_sleep(10, 20)
                                # 处理笔记详情数据
                                notes_data_copy = dict(self.api_data)
                                should_continue_pagination = True

                                for api_url, response_data in notes_data_copy.items():
                                    if 'notes_detail' in api_url and 'data' in response_data:
                                        api_data = response_data['data']
                                        # 检查是否应该继续分页
                                        should_continue_pagination = self._process_notes_detail(api_data,
                                                                                                url.blogger_dandelion_id,
                                                                                                new_session)
                                        break

                                # 如果应该继续分页，点击下一页
                                if should_continue_pagination:
                                    while self._click_next_page():
                                        # 等待API数据加载
                                        self.common.random_sleep(10, 20)

                                        # 检查是否应该继续分页
                                        should_continue_pagination = True
                                        for api_url, response_data in self.api_data.items():
                                            if 'notes_detail' in api_url and 'data' in response_data:
                                                api_data = response_data['data']
                                                should_continue_pagination = self._process_notes_detail(api_data,
                                                                                                        url.blogger_dandelion_id,
                                                                                                        new_session)
                                                break

                                        if not should_continue_pagination:
                                            logger.info("没有上个月的数据，停止分页")
                                            break

                                        # 清空当前页的API数据，准备处理下一页
                                        self.api_data.clear()
                                else:
                                    logger.info("当前页没有上个月的数据，停止分页")

                        except Exception as db_error:
                            logger.error(f"更新数据库时出错: {str(db_error)}")
                            new_session.rollback()
                            # 继续处理下一个博主，不退出程序

                    else:
                        logger.info(f"未捕获到博主 {url.nickname} 的API请求")

                    # 处理完成后，将当前博主的status更新为1
                    try:
                        url.status = 1
                        new_session.commit()
                        logger.info(f"博主 {url.nickname} 处理完成，status已更新为1")
                    except Exception as status_error:
                        logger.error(f"更新博主 {url.nickname} 的status失败: {str(status_error)}")
                        new_session.rollback()

                except Exception as e:
                    logger.error(f"处理博主 {url.nickname} 数据时出错: {str(e)}")
                    new_session.rollback()
                    # 继续处理下一个博主，不退出程序
                    continue

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
            new_session.rollback()
            raise  # 重新抛出异常，让上层处理重启逻辑
        finally:
            # 确保会话被正确处理
            try:
                new_session.close()
            except:
                pass

    def _process_fans_profile(self, api_data, user_id, session):
        """处理粉丝画像数据"""
        if api_data['code'] != 0:
            return

        data = api_data['data']
        logger.info(f"粉丝画像数据: {data}")

        try:
            # 检查是否已存在相同的记录
            existing_record = session.query(FpOutBloggerFansProfile).filter(
                FpOutBloggerFansProfile.user_id == user_id,
                FpOutBloggerFansProfile.dateKey == data.get('dateKey')
            ).first()

            if existing_record:
                # 更新已存在的记录
                existing_record.ages = json.dumps(data.get('ages', []), ensure_ascii=False) if data.get(
                    'ages') else None
                existing_record.gender = json.dumps(data.get('gender', {}), ensure_ascii=False) if data.get(
                    'gender') else None
                existing_record.interests = json.dumps(data.get('interests', []), ensure_ascii=False) if data.get(
                    'interests') else None
                existing_record.provinces = json.dumps(data.get('provinces', []), ensure_ascii=False) if data.get(
                    'provinces') else None
                existing_record.cities = json.dumps(data.get('cities', []), ensure_ascii=False) if data.get(
                    'cities') else None
                existing_record.devices = json.dumps(data.get('devices', []), ensure_ascii=False) if data.get(
                    'devices') else None
                session.commit()
                logger.info(f"成功更新粉丝画像数据到数据库，用户ID: {user_id}")
            else:
                # 创建新的粉丝画像记录
                fans_profile = FpOutBloggerFansProfile(
                    user_id=user_id,
                    ages=json.dumps(data.get('ages', []), ensure_ascii=False) if data.get('ages') else None,
                    gender=json.dumps(data.get('gender', {}), ensure_ascii=False) if data.get('gender') else None,
                    interests=json.dumps(data.get('interests', []), ensure_ascii=False) if data.get(
                        'interests') else None,
                    provinces=json.dumps(data.get('provinces', []), ensure_ascii=False) if data.get(
                        'provinces') else None,
                    cities=json.dumps(data.get('cities', []), ensure_ascii=False) if data.get('cities') else None,
                    dateKey=data.get('dateKey'),
                    devices=json.dumps(data.get('devices', []), ensure_ascii=False) if data.get('devices') else None
                )

                session.add(fans_profile)
                session.commit()
                logger.info(f"成功保存粉丝画像数据到数据库，用户ID: {user_id}")

        except Exception as e:
            logger.error(f"保存粉丝画像数据失败: {str(e)}")
            session.rollback()

    def _process_data_summary(self, api_data, user_id, session):
        """处理数据摘要数据"""
        if api_data['code'] != 0:
            return

        data = api_data['data']
        logger.info(f"数据摘要数据: {data}")

        try:
            # 检查是否已存在相同的记录
            existing_record = session.query(FpOutBloggerDataSummary).filter(
                FpOutBloggerDataSummary.platform_user_id == user_id
            ).first()

            if existing_record:
                # 更新已存在的记录
                existing_record._id = data.get('_id')
                existing_record.mValidRawReadFeedNum = str(data.get('mValidRawReadFeedNum', ''))
                existing_record.mEngagementNumCompare = str(data.get('mEngagementNumCompare', ''))
                existing_record.picReadCost = str(data.get('picReadCost', ''))
                existing_record.noteType = json.dumps(data.get('noteType', []), ensure_ascii=False) if data.get(
                    'noteType') else None
                existing_record.readMedianBeyondRate = str(data.get('readMedianBeyondRate', ''))
                existing_record.responseRate = str(data.get('responseRate', ''))
                existing_record.videoReadBeyondRate = str(data.get('videoReadBeyondRate', ''))
                existing_record.kolAdvantageHover = str(data.get('kolAdvantageHover', ''))
                existing_record.estimateVideoEngageCostCompare = str(data.get('estimateVideoEngageCostCompare', ''))
                existing_record.estimateVideoEngageCost = str(data.get('estimateVideoEngageCost', ''))
                existing_record.estimatePictureCpuvCompare = str(data.get('estimatePictureCpuvCompare', ''))
                existing_record.noteNumber = str(data.get('noteNumber', ''))
                existing_record.interactionMedian = str(data.get('interactionMedian', ''))
                existing_record.pictureReadBeyondRate = str(data.get('pictureReadBeyondRate', ''))
                existing_record.kolAdvantage = str(data.get('kolAdvantage', ''))
                existing_record.estimatePictureCpmCompare = str(data.get('estimatePictureCpmCompare', ''))
                existing_record.estimatePictureCpm = str(data.get('estimatePictureCpm', ''))
                existing_record.estimateVideoCpmCompare = str(data.get('estimateVideoCpmCompare', ''))
                existing_record.picReadCostCompare = str(data.get('picReadCostCompare', ''))
                existing_record.isActive = str(data.get('isActive', ''))
                existing_record.videoReadCost = str(data.get('videoReadCost', ''))
                existing_record.fans30GrowthRate = str(data.get('fans30GrowthRate', ''))
                existing_record.mAccumImpCompare = str(data.get('mAccumImpCompare', ''))
                existing_record.mValidRawReadFeedCompare = str(data.get('mValidRawReadFeedCompare', ''))
                existing_record.estimatePictureCpuv = str(data.get('estimatePictureCpuv', ''))
                existing_record.pictureCase = str(data.get('pictureCase', ''))
                existing_record.estimatePictureEngageCostCompare = str(data.get('estimatePictureEngageCostCompare', ''))
                existing_record.dateKey = str(data.get('dateKey', ''))
                existing_record.pictureReadCost = str(data.get('pictureReadCost', ''))
                existing_record.fans30GrowthBeyondRate = str(data.get('fans30GrowthBeyondRate', ''))
                existing_record.mCpuvNum = str(data.get('mCpuvNum', ''))
                existing_record.mCpuvNumCompare = str(data.get('mCpuvNumCompare', ''))
                existing_record.estimateVideoCpuvCompare = str(data.get('estimateVideoCpuvCompare', ''))
                existing_record.interactionBeyondRate = str(data.get('interactionBeyondRate', ''))
                existing_record.easyConnect = str(data.get('easyConnect', ''))
                existing_record.mAccumImpNum = str(data.get('mAccumImpNum', ''))
                existing_record.estimateVideoCpm = str(data.get('estimateVideoCpm', ''))
                existing_record.estimatePictureEngageCost = str(data.get('estimatePictureEngageCost', ''))
                existing_record.mEngagementNum = str(data.get('mEngagementNum', ''))
                existing_record.tradeNames = json.dumps(data.get('tradeNames', []), ensure_ascii=False) if data.get(
                    'tradeNames') else None
                existing_record.readMedian = str(data.get('readMedian', ''))
                existing_record.activeDayInLast7 = str(data.get('activeDayInLast7', ''))
                existing_record.inviteNum = str(data.get('inviteNum', ''))
                existing_record.mEngagementNumOld = str(data.get('mEngagementNumOld', ''))
                existing_record.videoReadCostV2 = str(data.get('videoReadCostV2', ''))
                existing_record.videoReadCostCompare = str(data.get('videoReadCostCompare', ''))
                existing_record.estimateVideoCpuv = str(data.get('estimateVideoCpuv', ''))
                existing_record.videoCase = str(data.get('videoCase', ''))
                existing_record.creator_id = str(data.get('creator_id', ''))
                existing_record.create_time = str(data.get('create_time', ''))
                existing_record.sync_status = str(data.get('sync_status', ''))
                existing_record.source_type = str(data.get('source_type', ''))
                existing_record.task_id = str(data.get('task_id', ''))
                existing_record.client_task_log_id = str(data.get('client_task_log_id', ''))
                existing_record.client_id = str(data.get('client_id', ''))
                existing_record.created_at = str(data.get('created_at', ''))
                existing_record.updated_at = str(data.get('updated_at', ''))
                session.commit()
                logger.info(f"成功更新数据摘要数据到数据库，用户ID: {user_id}")
            else:
                # 创建新的数据摘要记录
                data_summary = FpOutBloggerDataSummary(
                    _id=data.get('_id'),
                    mValidRawReadFeedNum=str(data.get('mValidRawReadFeedNum', '')),
                    mEngagementNumCompare=str(data.get('mEngagementNumCompare', '')),
                    picReadCost=str(data.get('picReadCost', '')),
                    noteType=json.dumps(data.get('noteType', []), ensure_ascii=False) if data.get('noteType') else None,
                    readMedianBeyondRate=str(data.get('readMedianBeyondRate', '')),
                    responseRate=str(data.get('responseRate', '')),
                    videoReadBeyondRate=str(data.get('videoReadBeyondRate', '')),
                    kolAdvantageHover=str(data.get('kolAdvantageHover', '')),
                    estimateVideoEngageCostCompare=str(data.get('estimateVideoEngageCostCompare', '')),
                    estimateVideoEngageCost=str(data.get('estimateVideoEngageCost', '')),
                    estimatePictureCpuvCompare=str(data.get('estimatePictureCpuvCompare', '')),
                    noteNumber=str(data.get('noteNumber', '')),
                    interactionMedian=str(data.get('interactionMedian', '')),
                    pictureReadBeyondRate=str(data.get('pictureReadBeyondRate', '')),
                    kolAdvantage=str(data.get('kolAdvantage', '')),
                    estimatePictureCpmCompare=str(data.get('estimatePictureCpmCompare', '')),
                    estimatePictureCpm=str(data.get('estimatePictureCpm', '')),
                    estimateVideoCpmCompare=str(data.get('estimateVideoCpmCompare', '')),
                    picReadCostCompare=str(data.get('picReadCostCompare', '')),
                    isActive=str(data.get('isActive', '')),
                    videoReadCost=str(data.get('videoReadCost', '')),
                    fans30GrowthRate=str(data.get('fans30GrowthRate', '')),
                    mAccumImpCompare=str(data.get('mAccumImpCompare', '')),
                    mValidRawReadFeedCompare=str(data.get('mValidRawReadFeedCompare', '')),
                    estimatePictureCpuv=str(data.get('estimatePictureCpuv', '')),
                    pictureCase=str(data.get('pictureCase', '')),
                    estimatePictureEngageCostCompare=str(data.get('estimatePictureEngageCostCompare', '')),
                    dateKey=str(data.get('dateKey', '')),
                    pictureReadCost=str(data.get('pictureReadCost', '')),
                    fans30GrowthBeyondRate=str(data.get('fans30GrowthBeyondRate', '')),
                    mCpuvNum=str(data.get('mCpuvNum', '')),
                    mCpuvNumCompare=str(data.get('mCpuvNumCompare', '')),
                    estimateVideoCpuvCompare=str(data.get('estimateVideoCpuvCompare', '')),
                    interactionBeyondRate=str(data.get('interactionBeyondRate', '')),
                    easyConnect=str(data.get('easyConnect', '')),
                    mAccumImpNum=str(data.get('mAccumImpNum', '')),
                    estimateVideoCpm=str(data.get('estimateVideoCpm', '')),
                    estimatePictureEngageCost=str(data.get('estimatePictureEngageCost', '')),
                    mEngagementNum=str(data.get('mEngagementNum', '')),
                    tradeNames=json.dumps(data.get('tradeNames', []), ensure_ascii=False) if data.get(
                        'tradeNames') else None,
                    readMedian=str(data.get('readMedian', '')),
                    activeDayInLast7=str(data.get('activeDayInLast7', '')),
                    inviteNum=str(data.get('inviteNum', '')),
                    mEngagementNumOld=str(data.get('mEngagementNumOld', '')),
                    videoReadCostV2=str(data.get('videoReadCostV2', '')),
                    videoReadCostCompare=str(data.get('videoReadCostCompare', '')),
                    estimateVideoCpuv=str(data.get('estimateVideoCpuv', '')),
                    videoCase=str(data.get('videoCase', '')),
                    creator_id=str(data.get('creator_id', '')),
                    platform_user_id=user_id,
                    create_time=str(data.get('create_time', '')),
                    sync_status=str(data.get('sync_status', '')),
                    source_type=str(data.get('source_type', '')),
                    task_id=str(data.get('task_id', '')),
                    client_task_log_id=str(data.get('client_task_log_id', '')),
                    client_id=str(data.get('client_id', '')),
                    created_at=str(data.get('created_at', '')),
                    updated_at=str(data.get('updated_at', '')),
                    type=1
                )

                session.add(data_summary)
                session.commit()
                logger.info(f"成功保存数据摘要数据到数据库，用户ID: {user_id}")

        except Exception as e:
            logger.error(f"保存数据摘要数据失败: {str(e)}")
            session.rollback()

    def _process_blogger(self, api_data, user_id, session):
        """处理博主数据"""
        if api_data['code'] != 0:
            return

        data = api_data['data']
        logger.info(f"博主数据: {data}")

        try:
            # 检查是否已存在相同的记录
            existing_record = session.query(FpOutBloggerInfo).filter(
                FpOutBloggerInfo.userId == user_id
            ).first()

            if existing_record:
                # 更新已存在的记录
                existing_record.valid = data.get('valid')
                existing_record.headPhoto = data.get('headPhoto')
                existing_record.name = data.get('name')
                existing_record.redId = data.get('redId')
                existing_record.location = data.get('location')
                existing_record.homePageDisplay = data.get('homePageDisplay')
                existing_record.travelAreaList = json.dumps(data.get('travelAreaList', []),
                                                            ensure_ascii=False) if data.get('travelAreaList') else None
                existing_record.type = json.dumps(data.get('type', []), ensure_ascii=False) if data.get(
                    'type') else None
                existing_record.personalTags = json.dumps(data.get('personalTags', []), ensure_ascii=False) if data.get(
                    'personalTags') else None
                existing_record.fansCount = data.get('fansCount')
                existing_record.likeCollectCountInfo = data.get('likeCollectCountInfo')
                existing_record.businessNoteCount = data.get('businessNoteCount')
                existing_record.totalNoteCount = data.get('totalNoteCount')
                existing_record.recommend = data.get('recommend')
                existing_record.picturePrice = data.get('picturePrice')
                existing_record.videoPrice = data.get('videoPrice')
                existing_record.lowerPrice = data.get('lowerPrice')
                existing_record.userType = data.get('userType')
                existing_record.showPrice = data.get('showPrice')
                existing_record.pictureState = data.get('pictureState')
                existing_record.videoState = data.get('videoState')
                existing_record.isCollect = data.get('isCollect')
                existing_record.cooperateState = data.get('cooperateState')
                existing_record.pictureInCart = data.get('pictureInCart')
                existing_record.videoInCart = data.get('videoInCart')
                existing_record.note = data.get('note')
                existing_record.live = data.get('live')
                existing_record.cps = data.get('cps')
                existing_record.noteSign = json.dumps(data.get('noteSign', {}), ensure_ascii=False) if data.get(
                    'noteSign') else None
                existing_record.liveSign = json.dumps(data.get('liveSign', {}), ensure_ascii=False) if data.get(
                    'liveSign') else None
                existing_record.goodRecommendPermission = data.get('goodRecommendPermission')
                existing_record.cpc = data.get('cpc')
                existing_record.pictureCpcBasePrice = data.get('pictureCpcBasePrice')
                existing_record.pictureCpcPerPrice = data.get('pictureCpcPerPrice')
                existing_record.pictureCpcEstimateNum = data.get('pictureCpcEstimateNum')
                existing_record.videoCpcState = data.get('videoCpcState')
                existing_record.videoCpcBasePrice = data.get('videoCpcBasePrice')
                existing_record.videoCpcPerPrice = data.get('videoCpcPerPrice')
                existing_record.videoCpcEstimateNum = data.get('videoCpcEstimateNum')
                existing_record.pictureCpcInCart = data.get('pictureCpcInCart')
                existing_record.videoCpcInCart = data.get('videoCpcInCart')
                existing_record.contentTags = json.dumps(data.get('contentTags', []), ensure_ascii=False) if data.get(
                    'contentTags') else None
                existing_record.featureTags = json.dumps(data.get('featureTags', []), ensure_ascii=False) if data.get(
                    'featureTags') else None
                existing_record.industryTag = data.get('industryTag')
                existing_record.gender = data.get('gender')
                existing_record.picPriceRemind = data.get('picPriceRemind')
                existing_record.videoPriceRemind = data.get('videoPriceRemind')
                existing_record.currentLevel = data.get('currentLevel')
                existing_record.nextLevel = data.get('nextLevel')
                existing_record.priceState = data.get('priceState')
                existing_record.resemblance = data.get('resemblance')
                existing_record.noteList = json.dumps(data.get('noteList', []), ensure_ascii=False) if data.get(
                    'noteList') else None
                existing_record.tradeType = data.get('tradeType')
                existing_record.clickMidNum = data.get('clickMidNum')
                existing_record.clickMidNumMcn = data.get('clickMidNumMcn')
                existing_record.interMidNum = data.get('interMidNum')
                existing_record.fansNum = data.get('fansNum')
                existing_record.matchNoteNumber = data.get('matchNoteNumber')
                existing_record.authorityList = json.dumps(data.get('authorityList', []),
                                                           ensure_ascii=False) if data.get('authorityList') else None
                existing_record.processingAuthorities = json.dumps(data.get('processingAuthorities', []),
                                                                   ensure_ascii=False) if data.get(
                    'processingAuthorities') else None
                existing_record.pictureShowState = data.get('pictureShowState')
                existing_record.videoShowState = data.get('videoShowState')
                existing_record.classifyCode = data.get('classifyCode')
                existing_record.predictiveExposure = data.get('predictiveExposure')
                existing_record.efficiencyValidUser = data.get('efficiencyValidUser')
                existing_record.pictureReadCost = data.get('pictureReadCost')
                existing_record.videoReadCost = data.get('videoReadCost')
                existing_record.pictureClickMidNum = data.get('pictureClickMidNum')
                existing_record.pictureInterMidNum = data.get('pictureInterMidNum')
                existing_record.videoClickMidNum = data.get('videoClickMidNum')
                existing_record.videoFinishRate = data.get('videoFinishRate')
                existing_record.videoInterMidNum = data.get('videoInterMidNum')
                existing_record.fans30GrowthRate = data.get('fans30GrowthRate')
                existing_record.fans30GrowthNum = data.get('fans30GrowthNum')
                existing_record.nextPicturePrice = data.get('nextPicturePrice')
                existing_record.nextVideoPrice = data.get('nextVideoPrice')
                existing_record.fansRiseNum = data.get('fansRiseNum')
                existing_record.fansEngageNum = data.get('fansEngageNum')
                existing_record.overflowNum = data.get('overflowNum')
                existing_record.newHighQuality = data.get('newHighQuality')
                existing_record.isIndustryRecommend = data.get('isIndustryRecommend')
                existing_record.picturePriceGtZero = data.get('picturePriceGtZero')
                existing_record.videoPriceGtZero = data.get('videoPriceGtZero')
                existing_record.lowActive = data.get('lowActive')
                existing_record.fansActiveIn28dLv = data.get('fansActiveIn28dLv')
                existing_record.fansEngageNum30dLv = data.get('fansEngageNum30dLv')
                existing_record.hundredLikePercent30 = data.get('hundredLikePercent30')
                existing_record.thousandLikePercent30 = data.get('thousandLikePercent30')
                existing_record.pictureHundredLikePercent30 = data.get('pictureHundredLikePercent30')
                existing_record.pictureThousandLikePercent30 = data.get('pictureThousandLikePercent30')
                existing_record.videoHundredLikePercent30 = data.get('videoHundredLikePercent30')
                existing_record.videoThousandLikePercent30 = data.get('videoThousandLikePercent30')
                existing_record.cooperType = data.get('cooperType')
                existing_record.buyerType = data.get('buyerType')
                existing_record.promiseImpNum = data.get('promiseImpNum')
                existing_record.kolType = data.get('kolType')
                existing_record.showPromiseTag = data.get('showPromiseTag')
                existing_record.activityList = json.dumps(data.get('activityList', []), ensure_ascii=False) if data.get(
                    'activityList') else None
                existing_record.controlState = data.get('controlState')
                existing_record.clothingIndustryPrice = json.dumps(data.get('clothingIndustryPrice', {}),
                                                                   ensure_ascii=False) if data.get(
                    'clothingIndustryPrice') else None
                existing_record.fbState = data.get('fbState')
                existing_record.forecastReadUvLower = data.get('forecastReadUvLower')
                existing_record.forecastReadUvUpper = data.get('forecastReadUvUpper')
                existing_record.forecastGroupCoverRateLower = data.get('forecastGroupCoverRateLower')
                existing_record.forecastGroupCoverRateUpper = data.get('forecastGroupCoverRateUpper')
                existing_record.intentionInfo = json.dumps(data.get('intentionInfo', {}),
                                                           ensure_ascii=False) if data.get('intentionInfo') else None
                existing_record.seedAndHarcest = json.dumps(data.get('seedAndHarcest', {}),
                                                            ensure_ascii=False) if data.get('seedAndHarcest') else None
                existing_record.liveImg = data.get('liveImg')
                existing_record.liveId = data.get('liveId')
                existing_record.liveGMV = data.get('liveGMV')
                existing_record.isStar = data.get('isStar')
                existing_record.top2CategoryList = json.dumps(data.get('top2CategoryList', []),
                                                              ensure_ascii=False) if data.get(
                    'top2CategoryList') else None
                existing_record.hasBuyerAuth = data.get('hasBuyerAuth')
                existing_record.sellerRealIncomeAmt90d = data.get('sellerRealIncomeAmt90d')
                existing_record.estimatePictureCpm = data.get('estimatePictureCpm')
                existing_record.estimatePictureCpmCompare = data.get('estimatePictureCpmCompare')
                existing_record.estimateVideoCpm = data.get('estimateVideoCpm')
                existing_record.estimateVideoCpmCompare = data.get('estimateVideoCpmCompare')
                existing_record.estimatePictureEngageCost = data.get('estimatePictureEngageCost')
                existing_record.estimatePictureEngageCostCompare = data.get('estimatePictureEngageCostCompare')
                existing_record.estimateVideoEngageCost = data.get('estimateVideoEngageCost')
                existing_record.estimateVideoEngageCostCompare = data.get('estimateVideoEngageCostCompare')
                existing_record.inviteReply48hNumRatio = data.get('inviteReply48hNumRatio')
                existing_record.recommendReason = data.get('recommendReason')
                existing_record.kolHeadLabel = data.get('kolHeadLabel')
                existing_record.accumCoopImpMedinNum30d = data.get('accumCoopImpMedinNum30d')
                existing_record.estimateCpuv30d = data.get('estimateCpuv30d')
                existing_record.accumPicCommonImpMedinNum30d = data.get('accumPicCommonImpMedinNum30d')
                existing_record.accumVideoCommonImpMedinNum30d = data.get('accumVideoCommonImpMedinNum30d')
                existing_record.accumCommonImpMedinNum30d = data.get('accumCommonImpMedinNum30d')
                existing_record.marketTarget = data.get('marketTarget')
                existing_record.readMidCoop30 = data.get('readMidCoop30')
                existing_record.interMidCoop30 = data.get('interMidCoop30')
                existing_record.mEngagementNum = data.get('mEngagementNum')
                existing_record.mEngagementNumMcn = data.get('mEngagementNumMcn')
                existing_record.mCpuvNum30d = data.get('mCpuvNum30d')
                existing_record.user_desc = data.get('user_desc')
                existing_record.source_type = data.get('source_type')
                session.commit()
                logger.info(f"成功更新博主数据到数据库，用户ID: {user_id}")
            else:
                # 创建新的博主记录
                blogger_info = FpOutBloggerInfo(
                    userId=data.get('userId'),
                    valid=data.get('valid'),
                    headPhoto=data.get('headPhoto'),
                    name=data.get('name'),
                    redId=data.get('redId'),
                    location=data.get('location'),
                    homePageDisplay=data.get('homePageDisplay'),
                    travelAreaList=json.dumps(data.get('travelAreaList', []), ensure_ascii=False) if data.get(
                        'travelAreaList') else None,
                    type=json.dumps(data.get('type', []), ensure_ascii=False) if data.get('type') else None,
                    personalTags=json.dumps(data.get('personalTags', []), ensure_ascii=False) if data.get(
                        'personalTags') else None,
                    fansCount=data.get('fansCount'),
                    likeCollectCountInfo=data.get('likeCollectCountInfo'),
                    businessNoteCount=data.get('businessNoteCount'),
                    totalNoteCount=data.get('totalNoteCount'),
                    recommend=data.get('recommend'),
                    picturePrice=data.get('picturePrice'),
                    videoPrice=data.get('videoPrice'),
                    lowerPrice=data.get('lowerPrice'),
                    userType=data.get('userType'),
                    showPrice=data.get('showPrice'),
                    pictureState=data.get('pictureState'),
                    videoState=data.get('videoState'),
                    isCollect=data.get('isCollect'),
                    cooperateState=data.get('cooperateState'),
                    pictureInCart=data.get('pictureInCart'),
                    videoInCart=data.get('videoInCart'),
                    note=data.get('note'),
                    live=data.get('live'),
                    cps=data.get('cps'),
                    noteSign=json.dumps(data.get('noteSign', {}), ensure_ascii=False) if data.get('noteSign') else None,
                    liveSign=json.dumps(data.get('liveSign', {}), ensure_ascii=False) if data.get('liveSign') else None,
                    goodRecommendPermission=data.get('goodRecommendPermission'),
                    cpc=data.get('cpc'),
                    pictureCpcBasePrice=data.get('pictureCpcBasePrice'),
                    pictureCpcPerPrice=data.get('pictureCpcPerPrice'),
                    pictureCpcEstimateNum=data.get('pictureCpcEstimateNum'),
                    videoCpcState=data.get('videoCpcState'),
                    videoCpcBasePrice=data.get('videoCpcBasePrice'),
                    videoCpcPerPrice=data.get('videoCpcPerPrice'),
                    videoCpcEstimateNum=data.get('videoCpcEstimateNum'),
                    pictureCpcInCart=data.get('pictureCpcInCart'),
                    videoCpcInCart=data.get('videoCpcInCart'),
                    contentTags=json.dumps(data.get('contentTags', []), ensure_ascii=False) if data.get(
                        'contentTags') else None,
                    featureTags=json.dumps(data.get('featureTags', []), ensure_ascii=False) if data.get(
                        'featureTags') else None,
                    industryTag=data.get('industryTag'),
                    gender=data.get('gender'),
                    picPriceRemind=data.get('picPriceRemind'),
                    videoPriceRemind=data.get('videoPriceRemind'),
                    currentLevel=data.get('currentLevel'),
                    nextLevel=data.get('nextLevel'),
                    priceState=data.get('priceState'),
                    resemblance=data.get('resemblance'),
                    noteList=json.dumps(data.get('noteList', []), ensure_ascii=False) if data.get('noteList') else None,
                    tradeType=data.get('tradeType'),
                    clickMidNum=data.get('clickMidNum'),
                    clickMidNumMcn=data.get('clickMidNumMcn'),
                    interMidNum=data.get('interMidNum'),
                    fansNum=data.get('fansNum'),
                    matchNoteNumber=data.get('matchNoteNumber'),
                    authorityList=json.dumps(data.get('authorityList', []), ensure_ascii=False) if data.get(
                        'authorityList') else None,
                    processingAuthorities=json.dumps(data.get('processingAuthorities', []),
                                                     ensure_ascii=False) if data.get('processingAuthorities') else None,
                    pictureShowState=data.get('pictureShowState'),
                    videoShowState=data.get('videoShowState'),
                    classifyCode=data.get('classifyCode'),
                    predictiveExposure=data.get('predictiveExposure'),
                    efficiencyValidUser=data.get('efficiencyValidUser'),
                    pictureReadCost=data.get('pictureReadCost'),
                    videoReadCost=data.get('videoReadCost'),
                    pictureClickMidNum=data.get('pictureClickMidNum'),
                    pictureInterMidNum=data.get('pictureInterMidNum'),
                    videoClickMidNum=data.get('videoClickMidNum'),
                    videoFinishRate=data.get('videoFinishRate'),
                    videoInterMidNum=data.get('videoInterMidNum'),
                    fans30GrowthRate=data.get('fans30GrowthRate'),
                    fans30GrowthNum=data.get('fans30GrowthNum'),
                    nextPicturePrice=data.get('nextPicturePrice'),
                    nextVideoPrice=data.get('nextVideoPrice'),
                    fansRiseNum=data.get('fansRiseNum'),
                    fansEngageNum=data.get('fansEngageNum'),
                    overflowNum=data.get('overflowNum'),
                    newHighQuality=data.get('newHighQuality'),
                    isIndustryRecommend=data.get('isIndustryRecommend'),
                    picturePriceGtZero=data.get('picturePriceGtZero'),
                    videoPriceGtZero=data.get('videoPriceGtZero'),
                    lowActive=data.get('lowActive'),
                    fansActiveIn28dLv=data.get('fansActiveIn28dLv'),
                    fansEngageNum30dLv=data.get('fansEngageNum30dLv'),
                    hundredLikePercent30=data.get('hundredLikePercent30'),
                    thousandLikePercent30=data.get('thousandLikePercent30'),
                    pictureHundredLikePercent30=data.get('pictureHundredLikePercent30'),
                    pictureThousandLikePercent30=data.get('pictureThousandLikePercent30'),
                    videoHundredLikePercent30=data.get('videoHundredLikePercent30'),
                    videoThousandLikePercent30=data.get('videoThousandLikePercent30'),
                    cooperType=data.get('cooperType'),
                    buyerType=data.get('buyerType'),
                    promiseImpNum=data.get('promiseImpNum'),
                    kolType=data.get('kolType'),
                    showPromiseTag=data.get('showPromiseTag'),
                    activityList=json.dumps(data.get('activityList', []), ensure_ascii=False) if data.get(
                        'activityList') else None,
                    controlState=data.get('controlState'),
                    clothingIndustryPrice=json.dumps(data.get('clothingIndustryPrice', {}),
                                                     ensure_ascii=False) if data.get('clothingIndustryPrice') else None,
                    fbState=data.get('fbState'),
                    forecastReadUvLower=data.get('forecastReadUvLower'),
                    forecastReadUvUpper=data.get('forecastReadUvUpper'),
                    forecastGroupCoverRateLower=data.get('forecastGroupCoverRateLower'),
                    forecastGroupCoverRateUpper=data.get('forecastGroupCoverRateUpper'),
                    intentionInfo=json.dumps(data.get('intentionInfo', {}), ensure_ascii=False) if data.get(
                        'intentionInfo') else None,
                    seedAndHarcest=json.dumps(data.get('seedAndHarcest', {}), ensure_ascii=False) if data.get(
                        'seedAndHarcest') else None,
                    liveImg=data.get('liveImg'),
                    liveId=data.get('liveId'),
                    liveGMV=data.get('liveGMV'),
                    isStar=data.get('isStar'),
                    top2CategoryList=json.dumps(data.get('top2CategoryList', []), ensure_ascii=False) if data.get(
                        'top2CategoryList') else None,
                    hasBuyerAuth=data.get('hasBuyerAuth'),
                    sellerRealIncomeAmt90d=data.get('sellerRealIncomeAmt90d'),
                    estimatePictureCpm=data.get('estimatePictureCpm'),
                    estimatePictureCpmCompare=data.get('estimatePictureCpmCompare'),
                    estimateVideoCpm=data.get('estimateVideoCpm'),
                    estimateVideoCpmCompare=data.get('estimateVideoCpmCompare'),
                    estimatePictureEngageCost=data.get('estimatePictureEngageCost'),
                    estimatePictureEngageCostCompare=data.get('estimatePictureEngageCostCompare'),
                    estimateVideoEngageCost=data.get('estimateVideoEngageCost'),
                    estimateVideoEngageCostCompare=data.get('estimateVideoEngageCostCompare'),
                    inviteReply48hNumRatio=data.get('inviteReply48hNumRatio'),
                    recommendReason=data.get('recommendReason'),
                    kolHeadLabel=data.get('kolHeadLabel'),
                    accumCoopImpMedinNum30d=data.get('accumCoopImpMedinNum30d'),
                    estimateCpuv30d=data.get('estimateCpuv30d'),
                    accumPicCommonImpMedinNum30d=data.get('accumPicCommonImpMedinNum30d'),
                    accumVideoCommonImpMedinNum30d=data.get('accumVideoCommonImpMedinNum30d'),
                    accumCommonImpMedinNum30d=data.get('accumCommonImpMedinNum30d'),
                    marketTarget=data.get('marketTarget'),
                    readMidCoop30=data.get('readMidCoop30'),
                    interMidCoop30=data.get('interMidCoop30'),
                    mEngagementNum=data.get('mEngagementNum'),
                    mEngagementNumMcn=data.get('mEngagementNumMcn'),
                    mCpuvNum30d=data.get('mCpuvNum30d'),
                    user_desc=data.get('user_desc'),
                    source_type=data.get('source_type'),
                )

                session.add(blogger_info)
                session.commit()
                logger.info(f"成功保存博主数据到数据库，用户ID: {user_id}")

        except Exception as e:
            logger.error(f"保存博主数据失败: {str(e)}")
            session.rollback()

    def _process_fans_history(self, api_data, user_id, session):
        """处理粉丝历史数据"""
        if api_data['code'] != 0:
            return

        data = api_data['data']
        logger.info(f"粉丝历史数据: {data}")

        try:
            # 检查是否已存在相同的记录
            existing_record = session.query(FpOutBloggerFansHistory).filter(
                FpOutBloggerFansHistory.user_id == user_id,
                FpOutBloggerFansHistory.dateKey == data.get('dateKey')
            ).first()

            if existing_record:
                # 更新已存在的记录
                existing_record.num = data.get('num')
                existing_record.date_type = data.get('date_type')
                existing_record.increase_type = data.get('increase_type')
                existing_record.updated_at = data.get('updated_at')
                session.commit()
                logger.info(f"成功更新粉丝历史数据到数据库，用户ID: {user_id}")
            else:
                # 创建新的粉丝历史记录
                fans_history = FpOutBloggerFansHistory(
                    num=data.get('num'),
                    dateKey=data.get('dateKey'),
                    user_id=user_id,
                    date_type=data.get('date_type'),
                    increase_type=data.get('increase_type'),
                    updated_at=data.get('updated_at')
                )

                session.add(fans_history)
                session.commit()
                logger.info(f"成功保存粉丝历史数据到数据库，用户ID: {user_id}")

        except Exception as e:
            logger.error(f"保存粉丝历史数据失败: {str(e)}")
            session.rollback()

    def _process_fans_summary(self, api_data, user_id, session):
        """处理粉丝概要数据"""
        if api_data['code'] != 0:
            return

        data = api_data['data']
        logger.info(f"粉丝概要数据: {data}")

        try:
            # 检查是否已存在相同的记录
            existing_record = session.query(FpOutBloggerFansSummary).filter(
                FpOutBloggerFansSummary.user_id == user_id
            ).first()

            if existing_record:
                # 更新已存在的记录
                existing_record.fansNum = data.get('fansNum')
                existing_record.fansIncreaseNum = data.get('fansIncreaseNum')
                existing_record.fansGrowthRate = float(data.get('fansGrowthRate', 0)) if data.get(
                    'fansGrowthRate') else None
                existing_record.fansGrowthBeyondRate = float(data.get('fansGrowthBeyondRate', 0)) if data.get(
                    'fansGrowthBeyondRate') else None
                existing_record.activeFansL28 = data.get('activeFansL28')
                existing_record.activeFansRate = float(data.get('activeFansRate', 0)) if data.get(
                    'activeFansRate') else None
                existing_record.activeFansBeyondRate = float(data.get('activeFansBeyondRate', 0)) if data.get(
                    'activeFansBeyondRate') else None
                existing_record.engageFansRate = float(data.get('engageFansRate', 0)) if data.get(
                    'engageFansRate') else None
                existing_record.engageFansL30 = data.get('engageFansL30')
                existing_record.engageFansBeyondRate = float(data.get('engageFansBeyondRate', 0)) if data.get(
                    'engageFansBeyondRate') else None
                existing_record.readFansIn30 = data.get('readFansIn30')
                existing_record.readFansRate = float(data.get('readFansRate', 0)) if data.get('readFansRate') else None
                existing_record.readFansBeyondRate = float(data.get('readFansBeyondRate', 0)) if data.get(
                    'readFansBeyondRate') else None
                existing_record.payFansUserRate30d = float(data.get('payFansUserRate30d', 0)) if data.get(
                    'payFansUserRate30d') else None
                existing_record.payFansUserNum30d = data.get('payFansUserNum30d')
                session.commit()
                logger.info(f"成功更新粉丝概要数据到数据库，用户ID: {user_id}")
            else:
                # 创建新的粉丝概要记录
                fans_summary = FpOutBloggerFansSummary(
                    fansNum=data.get('fansNum'),
                    fansIncreaseNum=data.get('fansIncreaseNum'),
                    fansGrowthRate=float(data.get('fansGrowthRate', 0)) if data.get('fansGrowthRate') else None,
                    fansGrowthBeyondRate=float(data.get('fansGrowthBeyondRate', 0)) if data.get(
                        'fansGrowthBeyondRate') else None,
                    activeFansL28=data.get('activeFansL28'),
                    activeFansRate=float(data.get('activeFansRate', 0)) if data.get('activeFansRate') else None,
                    activeFansBeyondRate=float(data.get('activeFansBeyondRate', 0)) if data.get(
                        'activeFansBeyondRate') else None,
                    engageFansRate=float(data.get('engageFansRate', 0)) if data.get('engageFansRate') else None,
                    engageFansL30=data.get('engageFansL30'),
                    engageFansBeyondRate=float(data.get('engageFansBeyondRate', 0)) if data.get(
                        'engageFansBeyondRate') else None,
                    readFansIn30=data.get('readFansIn30'),
                    readFansRate=float(data.get('readFansRate', 0)) if data.get('readFansRate') else None,
                    readFansBeyondRate=float(data.get('readFansBeyondRate', 0)) if data.get(
                        'readFansBeyondRate') else None,
                    payFansUserRate30d=float(data.get('payFansUserRate30d', 0)) if data.get(
                        'payFansUserRate30d') else None,
                    payFansUserNum30d=data.get('payFansUserNum30d'),
                    user_id=user_id
                )

                session.add(fans_summary)
                session.commit()
                logger.info(f"成功保存粉丝概要数据到数据库，用户ID: {user_id}")

        except Exception as e:
            logger.error(f"保存粉丝概要数据失败: {str(e)}")
            session.rollback()

    def _process_notes_detail(self, api_data, user_id, session):
        """处理笔记详情数据"""
        if api_data['code'] != 0:
            return True

        data = api_data['data']
        logger.info(f"笔记详情数据: {data}")

        # 检查是否包含上个月的数据
        if 'list' in data and data['list']:
            current_date = datetime.now()
            last_month_start = datetime(current_date.year, current_date.month - 1,
                                        1) if current_date.month > 1 else datetime(current_date.year - 1, 12, 1)

            # 保存笔记数据到数据库
            saved_count = 0
            for item in data['list']:
                # 解析日期
                item_date = datetime.strptime(item['date'], '%Y-%m-%d')

                # 检查是否是上个月的数据
                if item_date >= last_month_start:
                    logger.info("没有上个月的数据，停止分页")
                    return False

                # 检查是否已存在相同的记录
                existing_record = session.query(FpOutBloggerNoteDetail).filter(
                    FpOutBloggerNoteDetail.noteId == item.get('noteId'),
                    FpOutBloggerNoteDetail.user_id == user_id
                ).first()

                if existing_record:
                    # 更新已存在的记录
                    existing_record.readNum = item.get('readNum')
                    existing_record.likeNum = item.get('likeNum')
                    existing_record.collectNum = item.get('collectNum')
                    existing_record.isAdvertise = str(item.get('isAdvertise', False))
                    existing_record.isVideo = str(item.get('isVideo', False))
                    existing_record.imgUrl = item.get('imgUrl')
                    existing_record.title = item.get('title')
                    existing_record.brandName = item.get('brandName')
                    existing_record.date = item.get('date')
                    existing_record.advertise_switch = 1
                    existing_record.order_type = 1
                    existing_record.note_type = 3
                    session.commit()
                    logger.debug(f"成功更新笔记记录: {item.get('noteId')}")
                else:
                    # 创建新的笔记记录
                    note_detail = FpOutBloggerNoteDetail(
                        readNum=item.get('readNum'),
                        likeNum=item.get('likeNum'),
                        collectNum=item.get('collectNum'),
                        isAdvertise=str(item.get('isAdvertise', False)),
                        isVideo=str(item.get('isVideo', False)),
                        noteId=item.get('noteId'),
                        imgUrl=item.get('imgUrl'),
                        title=item.get('title'),
                        brandName=item.get('brandName'),
                        date=item.get('date'),
                        user_id=user_id,
                        advertise_switch=1,
                        order_type=1,
                        note_type=3
                    )

                    session.add(note_detail)
                    saved_count += 1

            # 提交数据库事务
            try:
                session.commit()
                logger.info(f"成功保存 {saved_count} 条笔记记录到数据库")
            except Exception as e:
                logger.error(f"提交数据库事务失败: {str(e)}")
                session.rollback()

        return True

    def _click_eyes(self):
        """
        点击笔记数据下的合作笔记按钮
        """
        try:
            logger.info("开始查找合作笔记按钮...")

            # 方法1: 通过完整的CSS选择器定位合作笔记按钮
            cooperate_note_selector = "div.note-case-wrapper div.note-type__select div.d-segment div.d-space.d-space-horizontal.d-space-align-center div:has-text('合作笔记')"

            # 检查是否存在合作笔记按钮
            cooperate_note_elements = self.page.locator(cooperate_note_selector).all()

            if len(cooperate_note_elements) > 0:
                # 找到第一个合作笔记按钮
                cooperate_note_button = cooperate_note_elements[0]

                # 检查按钮是否可见
                if cooperate_note_button.is_visible(timeout=3000):
                    logger.info("找到合作笔记按钮，准备点击...")
                    cooperate_note_button.click()
                    logger.info("成功点击合作笔记按钮")

                    # 等待页面网络空闲
                    try:
                        self.page.wait_for_load_state('networkidle', timeout=1000)
                    except Exception as e:
                        logger.warning(f"等待网络空闲时出错: {str(e)}")

                    return True
                else:
                    logger.warning("合作笔记按钮不可见")
            else:
                logger.warning("未找到合作笔记按钮")

        except Exception as e:
            logger.error(f"点击合作笔记按钮时出错: {str(e)}")
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