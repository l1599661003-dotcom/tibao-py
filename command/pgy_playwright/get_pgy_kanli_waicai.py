import json
import os
import sys
import configparser
import time
from datetime import datetime
import schedule
from loguru import logger
from playwright.sync_api import sync_playwright
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback

from core.database_text_tibao_2 import session
from models.models_tibao import (
    KolProfileDataWaicai,
    KolProfileDataWaicaiPeizhiPeizhi,
    FpOutBloggerNoteDetail,
    FpOutBloggerFansProfile,
    FpOutBloggerFansSummary,
    FpOutBloggerFansHistory,
    FpOutBloggerInfo,
    FpOutBloggerDataSummary
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
            logger.info(f"已加载配置文件: {config_path}")
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
            peizhi = session.query(KolProfileDataWaicaiPeizhiPeizhi).filter(
                KolProfileDataWaicaiPeizhiPeizhi.id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).first()

            if peizhi:
                # 立即提取所有需要的配置信息到普通变量中
                self.peizhi_email = peizhi.email
                self.peizhi_password = peizhi.password
            else:
                logger.error(f"未找到ID为{self.config['PGY_LOGIN_CONFIG']['id']}的配置信息")
                raise ValueError(f"配置信息不存在: ID={self.config['PGY_LOGIN_CONFIG']['id']}")

        except Exception as e:
            logger.error(f"加载配置信息失败: {str(e)}")
            raise
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
        self.current_user_data = {}  # 当前用户数据

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
                'width': 1012,
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

            # 创建新的会话来查询数据
            from core.database_text_tibao_2 import session as query_session

            peizhi = query_session.query(KolProfileDataWaicaiPeizhiPeizhi).filter(
                KolProfileDataWaicaiPeizhiPeizhi.id == int(self.config['PGY_LOGIN_CONFIG']['id'])
            ).first()

            # 查询需要更新的博主数据 - 匹配PHP查询逻辑
            urls = query_session.query(KolProfileDataWaicai).filter(
                KolProfileDataWaicai.id >= peizhi.start_id,
                KolProfileDataWaicai.id <= peizhi.end_id,
                KolProfileDataWaicai.spider_status == 0
            ).all()

            logger.info(f"找到 {len(urls)} 个博主数据")
            if not urls:
                return

            # 提取博主数据，避免会话绑定问题
            blogger_data = []
            for url in urls:
                if not url.blogger_id:
                    continue
                blogger_data.append({
                    'id': url.id,
                    'blogger_id': url.blogger_id,
                    'nickname': url.nickname
                })

            # 关闭查询会话
            try:
                query_session.close()
                logger.debug("成功关闭查询会话")
            except Exception as e:
                logger.warning(f"关闭查询会话时出错: {str(e)}")

            for blogger in blogger_data:
                if not blogger['blogger_id']:
                    continue

                try:
                    # 清空之前的数据
                    self.api_data.clear()
                    self.current_user_data.clear()

                    logger.info(f"正在处理博主: {blogger['nickname']}")
                    # 访问页面
                    page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{blogger['blogger_id']}"
                    logger.info(f"开始访问页面: {page_url}")

                    try:
                        self.page.goto(page_url)
                    except Exception as nav_error:
                        # 其他类型的导航错误
                        logger.error(f"导航错误: {str(nav_error)}")
                        continue

                    try:
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception as e:
                        logger.warning(f"等待页面加载完成时出错: {str(e)}")

                    self._click_ignore_button()
                    self.common.random_sleep(30, 40)

                    if self.api_data:
                        # 创建api_data的副本进行遍历，参考原版
                        api_data_copy = dict(self.api_data)

                        for api_url, response_data in api_data_copy.items():
                            if 'data' not in response_data:
                                continue

                            api_data = response_data['data']
                            # 根据不同的API进行不同的处理
                            if 'fans_profile' in api_url:
                                self._process_fans_profile(api_data, blogger['blogger_id'])
                            elif 'fans_summary' in api_url:
                                self._process_fans_summary(api_data, blogger['blogger_id'])
                            elif 'fans_overall_new_history' in api_url:
                                self._process_fans_history(api_data, blogger['blogger_id'])
                            elif 'notes_detail' in api_url:
                                self._process_notes_detail(api_data, blogger['blogger_id'])
                            elif 'blogger' in api_url:
                                self._process_blogger(api_data, blogger['blogger_id'])
                            elif 'notes_rate' in api_url:
                                # 确保价格数据已获取
                                graphic_price = self.current_user_data.get('graphic_price', 0)
                                video_price = self.current_user_data.get('video_price', 0)
                                self._process_notes_rate(api_data, graphic_price, video_price, 'daily',
                                                         blogger['blogger_id'])

                        # 3. 点击下一步按钮，获取数据摘要
                        try:
                            self.api_data.clear()
                            once = self.page.locator("button:has-text('下一步')").first
                            if once:
                                once.click()

                            # 点击"按成本"按钮
                            dropdown_container = self.page.locator('.d-spinner-container')
                            switch_button = dropdown_container.locator('button:has-text("按成本")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                                # 等待页面加载完成
                                try:
                                    self.page.wait_for_load_state('networkidle', timeout=5000)
                                except Exception as e:
                                    logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                # 处理数据摘要API
                                data_summary_copy = dict(self.api_data)
                                for api_url, response_data in data_summary_copy.items():
                                    try:
                                        if not response_data or not isinstance(response_data, dict):
                                            continue

                                        if 'data_summary' in api_url and 'data' in response_data:
                                            api_data = response_data.get('data', {})
                                            if api_data and isinstance(api_data, dict):
                                                self._process_data_summary(api_data, blogger['blogger_id'])
                                                break
                                    except Exception as e:
                                        logger.warning(f"处理数据摘要API时出错: {str(e)}")
                                        continue

                        except Exception as e:
                            logger.error(f"处理数据摘要步骤时出错: {str(e)}")

                        self.common.random_sleep(10, 15)
                        # 4. 点击"合作笔记"按钮，获取合作笔记数据
                        try:
                            self.api_data.clear()
                            dropdown_container = self.page.locator('.d-spinner-nested-loading')
                            switch_button = dropdown_container.locator('button:has-text("合作笔记")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                            # 等待页面加载完成
                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(10, 15)

                            # 处理合作笔记API
                            notes_rate_copy = dict(self.api_data)
                            for api_url, response_data in notes_rate_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'notes_rate' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            # 确保价格数据已获取
                                            graphic_price = self.current_user_data.get('graphic_price', 0)
                                            video_price = self.current_user_data.get('video_price', 0)
                                            self._process_notes_rate(api_data, graphic_price, video_price, 'coop',
                                                                     blogger['blogger_id'])
                                            break
                                except Exception as e:
                                    logger.warning(f"处理合作笔记API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理合作笔记步骤时出错: {str(e)}")

                        try:
                            self.api_data.clear()
                            dropdown_container = self.page.locator('.d-spinner-nested-loading')
                            switch_button = dropdown_container.locator('button:has-text("图文＋视频")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                            # 等待页面加载完成
                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(10, 15)

                            # 处理合作笔记API
                            notes_rate_copy = dict(self.api_data)
                            for api_url, response_data in notes_rate_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'notes_rate' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            # 确保价格数据已获取
                                            graphic_price = self.current_user_data.get('graphic_price', 0)
                                            video_price = self.current_user_data.get('video_price', 0)
                                            self._process_notes_rate(api_data, graphic_price, video_price, 'coop',
                                                                     blogger['blogger_id'])
                                            break
                                except Exception as e:
                                    logger.warning(f"处理合作笔记API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理合作笔记步骤时出错: {str(e)}")

                        logger.info(f"当前用户数据: {self.current_user_data}")

                        # 所有 API 数据处理完毕后，统一更新数据库
                        if self.current_user_data:
                            try:
                                # 创建新的会话来处理数据库更新
                                from core.database_text_tibao_2 import session as update_session

                                # 重新查询博主记录
                                url_record = update_session.query(KolProfileDataWaicai).filter(
                                    KolProfileDataWaicai.id == blogger['id']
                                ).first()

                                if url_record:
                                    # 添加额外的字段
                                    self.current_user_data.update({
                                        'updated_at': datetime.now()
                                    })

                                    # 更新博主信息
                                    logger.debug(f"准备更新博主数据，当前数据: {self.current_user_data}")
                                    for key, value in self.current_user_data.items():
                                        if hasattr(url_record, key):
                                            old_value = getattr(url_record, key)
                                            setattr(url_record, key, value)
                                            logger.debug(f"更新字段 {key}: {old_value} -> {value}")
                                        else:
                                            logger.debug(f"字段 {key} 在数据库模型中不存在，跳过更新")

                                        # 提交更新
                                        try:
                                            update_session.commit()
                                            logger.info(f"成功更新博主 {url_record.nickname} 的所有数据")
                                        except Exception as commit_error:
                                            logger.error(f"提交数据库更新时出错: {str(commit_error)}")
                                            update_session.rollback()
                                    else:
                                        logger.warning(f"未找到博主记录，ID: {blogger['id']}")

                            except Exception as db_error:
                                logger.error(f"更新数据库时出错: {str(db_error)}")
                                update_session.rollback()
                            finally:
                                try:
                                    update_session.close()
                                except:
                                    pass

                except Exception as e:
                    logger.error(f"处理博主 {blogger['nickname']} 数据时出错: {str(e)}")
                    logger.error(f"错误详情: {traceback.format_exc()}")
                    continue

            # 保存进度和Cookie
            self._save_cookies()

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            self.update_monitor_status(
                status="出错",
                fail_count=self.monitor_data.get('fail_count', 0) + 1
            )
            return None

    def _process_blogger(self, api_data, blogger_id):
        """处理博主基本信息"""
        try:
            if api_data['code'] == 0:
                data = api_data['data']

                personal_string = ''
                personal_list = []
                content_tags = data.get("contentTags", [])
                featureTags = data.get("featureTags", [])
                personal_tags = data.get("personalTags", [])  # 修正字段名

                # 安全处理个人标签
                if personal_tags and isinstance(personal_tags, list):
                    for tag in personal_tags:
                        if tag and str(tag).strip():  # 确保tag不为空且去除空白
                            personal_list.append(str(tag).strip())
                personal_string = "、".join(personal_list)

                tags = set()
                # 安全处理内容标签
                if content_tags and isinstance(content_tags, list):
                    for tag in content_tags:
                        if isinstance(tag, dict):
                            taxonomy1 = tag.get("taxonomy1Tag")
                            taxonomy2 = tag.get("taxonomy2Tag")
                            if taxonomy1 and str(taxonomy1).strip():
                                tags.add(str(taxonomy1).strip())
                            if taxonomy2 and str(taxonomy2).strip():
                                tags.add(str(taxonomy2).strip())

                # 安全处理特征标签
                if featureTags and isinstance(featureTags, list):
                    for tag in featureTags:
                        if tag and str(tag).strip():
                            tags.add(str(tag).strip())

                level_map = {
                    0: "异常",
                    1: "普通",
                    2: "优秀"
                }
                currentLevel = data.get("currentLevel", 0)
                current_level = level_map.get(currentLevel, "")

                tag_string = "、".join(tags)

                # 安全处理价格信息
                picture_price = 0
                video_price = 0
                video_price_raw = data.get('videoPrice')
                picture_price_raw = data.get('picturePrice')

                if video_price_raw is not None:
                    try:
                        video_price = float(video_price_raw)
                    except (ValueError, TypeError):
                        logger.warning(f"视频价格转换失败: {video_price_raw}")
                        video_price = 0

                if picture_price_raw is not None:
                    try:
                        picture_price = float(picture_price_raw)
                    except (ValueError, TypeError):
                        logger.warning(f"图文价格转换失败: {picture_price_raw}")
                        picture_price = 0

                # 安全获取博主机构信息
                agency_name = self._safe_get_nested(data, ['noteSign', 'name'], "")

                # 安全获取其他字段
                nickname = data.get('name', '')
                xhs_id = data.get('redId', '')
                fans_count = data.get('fansCount', 0)
                like_collect_count = data.get('likeCollectCountInfo', '')
                region = data.get('location', '')

                # 验证粉丝数量
                if fans_count is not None:
                    try:
                        fans_count = int(fans_count)
                    except (ValueError, TypeError):
                        logger.warning(f"粉丝数量转换失败: {fans_count}")
                        fans_count = 0

                # 更新用户数据
                self.current_user_data.update({
                    'blogger_id': blogger_id,  # 博主ID
                    'dandelion_link': f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{blogger_id}",
                    'xhs_link': f"https://www.xiaohongshu.com/user/profile/{blogger_id}",
                    'nickname': str(nickname) if nickname else '',  # 昵称
                    'xhs_id': str(xhs_id) if xhs_id else '',  # 小红书ID
                    'fans_count': fans_count,  # 粉丝数量
                    'graphic_price': picture_price,  # 图文一口价
                    'video_price': video_price,  # 视频一口价
                    'like_collect_count': str(like_collect_count) if like_collect_count else '',  # 获赞与收藏
                    'region': str(region) if region else '',  # 所在地区
                    'agency': agency_name,  # 博主机构
                    'dandelion_status': current_level,  # 账号状态
                    'tags': tag_string,  # 标签
                    'kol_persona': personal_string,  # 人设
                    'spider_status': 1
                })

                # 同步到 FpOutBloggerInfo 表
                self._sync_blogger_to_fp_out_blogger_info(data, blogger_id)

        except Exception as e:
            logger.error(f"处理博主基本信息时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _sync_blogger_to_fp_out_blogger_info(self, data, user_id):
        """同步博主数据到 FpOutBloggerInfo 表"""
        try:
            from core.database_text_tibao_2 import session as new_session

            # 检查是否已存在相同的记录
            existing_record = new_session.query(FpOutBloggerInfo).filter(
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
                new_session.commit()
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
                    noteList=json.dumps(data.get('noteList', []), ensure_ascii=False) if data.get(
                        'noteList') else None,
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

                new_session.add(blogger_info)
                new_session.commit()

        except Exception as e:
            logger.error(f"同步博主数据到 FpOutBloggerInfo 表失败: {str(e)}")
            new_session.rollback()
        finally:
            try:
                new_session.close()
                logger.debug("成功关闭数据库会话")
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")

    def _process_fans_profile(self, api_data, blogger_id):
        """处理粉丝画像数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("粉丝画像API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝画像数据为空或格式错误")
                return

            # 处理年龄分布（确保有5个年龄段）
            ages = data.get('ages', [])
            if not isinstance(ages, list):
                ages = []

            # 处理性别分布
            gender = data.get('gender', {})
            if not isinstance(gender, dict):
                gender = {}

            # 处理兴趣 top5
            interests = data.get('interests', [])
            if not isinstance(interests, list):
                interests = []

            interest_data = ""
            try:
                valid_interests = []
                for p in interests[:5]:
                    if p and isinstance(p, dict):
                        name = p.get('name')
                        percent = p.get('percent')
                        if name and percent is not None:
                            try:
                                percent_val = float(percent)
                                valid_interests.append(f"{str(name)} ({round(percent_val * 100, 1)}%)")
                            except (ValueError, TypeError):
                                continue
                interest_data = " 、".join(valid_interests)
            except Exception as e:
                logger.warning(f"处理兴趣数据时出错: {str(e)}")
                interest_data = ""

            # 处理城市 top3
            cities = data.get('cities', [])
            if not isinstance(cities, list):
                cities = []

            city_data = ""
            try:
                valid_cities = []
                for p in cities[:3]:
                    if p and isinstance(p, dict):
                        name = p.get('name')
                        percent = p.get('percent')
                        if name and percent is not None:
                            try:
                                percent_val = float(percent)
                                valid_cities.append(f"{str(name)} ({round(percent_val * 100, 1)}%)")
                            except (ValueError, TypeError):
                                continue
                city_data = " 、".join(valid_cities)
            except Exception as e:
                logger.warning(f"处理城市数据时出错: {str(e)}")
                city_data = ""

            # 处理设备数据
            devices = data.get('devices', [])
            if not isinstance(devices, list):
                devices = []

            apple_percent = 0
            huawei_percent = 0
            try:
                for d in devices:
                    if d and isinstance(d, dict):
                        name = d.get("name", "")
                        percent = d.get("percent", 0)
                        if isinstance(name, str) and percent is not None:
                            try:
                                if "apple" in name.lower():
                                    apple_percent = float(percent)
                                elif "huawei" in name.lower():
                                    huawei_percent = float(percent)
                            except (ValueError, TypeError):
                                continue
            except Exception as e:
                logger.warning(f"处理设备数据时出错: {str(e)}")

            apple_percent_str = f"苹果:{apple_percent * 100:.2f}%,华为:{huawei_percent * 100:.2f}%"

            # 安全处理年龄数据，避免索引越界
            def safe_age_percent(ages_list, index, default="0.00"):
                """安全获取年龄百分比"""
                try:
                    if (ages_list and isinstance(ages_list, list) and
                            len(ages_list) > index and
                            ages_list[index] and
                            isinstance(ages_list[index], dict)):
                        percent = ages_list[index].get('percent', 0)
                        if percent is not None:
                            try:
                                return f"{float(percent):.2f}"
                            except (ValueError, TypeError):
                                return default
                    return default
                except (ValueError, TypeError, IndexError):
                    return default

            # 安全获取性别比例
            female_ratio = "0.00"
            try:
                if gender and isinstance(gender, dict):
                    female_val = gender.get('female')
                    if female_val is not None:
                        female_ratio = f"{float(female_val):.2f}"
            except (ValueError, TypeError):
                female_ratio = "0.00"

            # 合并所有数据
            self.current_user_data.update({
                "age_lt18_ratio": safe_age_percent(ages, 0),
                "age_18_24_ratio": safe_age_percent(ages, 1),
                "age_25_34_ratio": safe_age_percent(ages, 2),
                "age_35_44_ratio": safe_age_percent(ages, 3),
                "age_gt44_ratio": safe_age_percent(ages, 4),
                "device_type": apple_percent_str,
                "region_distribution": city_data,
                "female_fans_ratio": female_ratio,
                "user_interest": interest_data,
            })

            # 同步到 FpOutBloggerFansProfile 表
            self._sync_fans_profile_to_fp_out_blogger_fans_profile(data, blogger_id)

        except Exception as e:
            logger.error(f"处理粉丝画像数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _sync_fans_profile_to_fp_out_blogger_fans_profile(self, data, user_id):
        """同步粉丝画像数据到 FpOutBloggerFansProfile 表"""
        try:
            from core.database_text_tibao_2 import session as new_session

            # 检查是否已存在相同的记录
            existing_record = new_session.query(FpOutBloggerFansProfile).filter(
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
                new_session.commit()
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

                new_session.add(fans_profile)
                new_session.commit()

        except Exception as e:
            logger.error(f"同步粉丝画像数据到 FpOutBloggerFansProfile 表失败: {str(e)}")
            new_session.rollback()
        finally:
            try:
                new_session.close()
                logger.debug("成功关闭数据库会话")
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")

    def _process_data_summary(self, api_data, blogger_id):
        """处理数据摘要"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("数据摘要API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("数据摘要数据为空或格式错误")
                return

            # 处理行业名称
            trade_names = data.get('tradeNames', [])
            trade_name = ""
            try:
                if isinstance(trade_names, list) and trade_names:
                    # 过滤掉空值和None，确保数据质量
                    valid_trade_names = []
                    for name in trade_names:
                        if name and str(name).strip():
                            valid_trade_names.append(str(name).strip())
                    trade_name = ", ".join(valid_trade_names)
            except Exception as e:
                logger.warning(f"处理行业名称时出错: {str(e)}")
                trade_name = ""

            # 安全获取数值，避免类型错误
            def safe_numeric_value(value, default=0):
                """安全获取数值，处理None和类型转换"""
                try:
                    if value is None:
                        return default
                    if value == '':
                        return default
                    return float(value)
                except (ValueError, TypeError):
                    return default

            # 安全获取各个字段值
            try:
                self.current_user_data.update({
                    'reply_rate_48h': safe_numeric_value(data.get('responseRate')),
                    'cooperate_industry': trade_name,  # 合作行业
                    'est_graphic_cpm': safe_numeric_value(data.get('estimateVideoCpm')),
                    'est_video_cpm': safe_numeric_value(data.get('estimatePictureCpm')),
                    'est_graphic_cpc': safe_numeric_value(data.get('pictureReadCost')),
                    'est_video_cpc': safe_numeric_value(data.get('videoReadCostV2')),
                    'est_graphic_cpe': safe_numeric_value(data.get('estimatePictureEngageCost')),
                    'est_video_cpe': safe_numeric_value(data.get('estimateVideoEngageCost')),
                })

            except Exception as e:
                logger.error(f"更新数据摘要字段时出错: {str(e)}")

            # 同步到 FpOutBloggerDataSummary 表
            self._sync_data_summary_to_fp_out_blogger_data_summary(data, blogger_id)

        except Exception as e:
            logger.error(f"处理数据摘要时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _sync_data_summary_to_fp_out_blogger_data_summary(self, data, user_id):
        """同步数据摘要到 FpOutBloggerDataSummary 表"""
        try:
            from core.database_text_tibao_2 import session as new_session

            # 检查是否已存在相同的记录
            existing_record = new_session.query(FpOutBloggerDataSummary).filter(
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
                new_session.commit()
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

                new_session.add(data_summary)
                new_session.commit()

        except Exception as e:
            logger.error(f"同步数据摘要到 FpOutBloggerDataSummary 表失败: {str(e)}")
            new_session.rollback()
        finally:
            try:
                new_session.close()
                logger.debug("成功关闭数据库会话")
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")

    def _process_notes_rate(self, api_data, graphic_price, video_price, type, blogger_id):
        """处理数据摘要"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning(f"{type}笔记率API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning(f"{type}笔记率数据为空或格式错误")
                return

            # 安全获取数值，避免除零错误
            def safe_get_numeric(key, default=0, convert_to_int=False):
                """安全获取数值"""
                try:
                    value = data.get(key, default)
                    if value is None or value == '':
                        return default
                    
                    # 转换为float
                    float_value = float(value)
                    
                    # 如果需要转换为整数，进行四舍五入
                    if convert_to_int:
                        return round(float_value)
                    
                    return float_value
                except (ValueError, TypeError):
                    return default

            # 获取原始数值，不进行类型转换
            imp_median = safe_get_numeric('impMedian', 0, convert_to_int=True)
            read_median = safe_get_numeric('readMedian', 0, convert_to_int=True)
            engage_median = safe_get_numeric('mEngagementNum', 0, convert_to_int=True)

            # 记录原始值用于调试
            logger.debug(f"{type}数据 - impMedian: {data.get('impMedian')} -> {imp_median}")
            logger.debug(f"{type}数据 - readMedian: {data.get('readMedian')} -> {read_median}")
            logger.debug(f"{type}数据 - mEngagementNum: {data.get('mEngagementNum')} -> {engage_median}")

            # 计算CPM、CPC、CPE，添加安全检查
            def safe_division(numerator, denominator, multiplier=1):
                """安全除法，避免除零错误，保留两位小数"""
                try:
                    if denominator and denominator > 0:
                        return round((numerator / denominator) * multiplier, 2)
                    return 0
                except (ZeroDivisionError, TypeError):
                    return 0

            # 计算各项指标
            cpm_value = min(
                safe_division(graphic_price, imp_median, 1000),
                safe_division(video_price, imp_median, 1000)
            )

            cpc_value = min(
                safe_division(graphic_price, read_median),
                safe_division(video_price, read_median)
            )

            cpe_value = min(
                safe_division(graphic_price, engage_median),
                safe_division(video_price, engage_median)
            )

            # 更新用户数据
            # 根据type构建正确的字段名，确保与数据库字段名匹配
            if type == 'daily':
                update_data = {
                        'daily_exposure_median': imp_median,
                        'daily_read_median': read_median,
                        'daily_engage_median': engage_median,
                        'daily_cpm': cpm_value,
                        'daily_cpc': cpc_value,
                        'daily_cpe': cpe_value,
                    }
            else:  # coop
                update_data = {
                    'coop_exposure_median': imp_median,
                    'coop_read_median': read_median,
                    'coop_engage_median': engage_median,
                    'coop_cpm': cpm_value,
                    'coop_cpc': cpc_value,
                    'coop_cpe': cpe_value,
            }

            # 如果是日常数据，添加额外字段
            if type == 'daily':
                try:
                    hundred_like = safe_get_numeric('hundredLikePercent', 0)
                    thousand_like = safe_get_numeric('thousandLikePercent', 0)
                    update_data.update({
                        'hundred_like_ratio': hundred_like,
                        'thousand_like_ratio': thousand_like,
                    })
                except Exception as e:
                    logger.warning(f"处理日常数据额外字段时出错: {str(e)}")
                    update_data.update({
                        'hundred_like_ratio': 0,
                        'thousand_like_ratio': 0,
                    })

            # 记录更新前的值用于调试
            logger.debug(f"更新前 {type} 字段值: {update_data}")
            
            # 更新用户数据
            self.current_user_data.update(update_data)
            
            # 记录更新后的值用于调试
            logger.debug(f"更新后 current_user_data 中的 {type} 字段值:")
            for key in update_data.keys():
                logger.debug(f"  {key}: {self.current_user_data.get(key)}")

        except Exception as e:
            logger.error(f"处理{type}笔记率数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_fans_summary(self, api_data, blogger_id):
        """处理粉丝概要数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("粉丝概要API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝概要数据为空或格式错误")
                return

            # 安全获取活跃粉丝率
            active_fans_rate = data.get('activeFansRate')
            active_fans_ratio = 0.0

            try:
                if active_fans_rate is not None:
                    # 确保activeFansRate是有效的数值
                    rate_value = float(active_fans_rate)
                    if 0 <= rate_value <= 100:  # 验证百分比范围
                        active_fans_ratio = rate_value / 100
                    else:
                        logger.warning(f"activeFansRate值超出范围: {rate_value}%")
                        active_fans_ratio = 0.0
                else:
                    logger.warning("未找到activeFansRate字段")
                    active_fans_ratio = 0.0

            except (ValueError, TypeError) as e:
                logger.warning(f"activeFansRate值转换失败: {active_fans_rate}, 错误: {str(e)}")
                active_fans_ratio = 0.0
            except Exception as e:
                logger.warning(f"处理活跃粉丝率时出现未知错误: {str(e)}")
                active_fans_ratio = 0.0

            # 更新用户数据
            try:
                self.current_user_data.update({
                    'active_fans_ratio': active_fans_ratio,
                })
            except Exception as e:
                logger.error(f"更新活跃粉丝率字段时出错: {str(e)}")

            # 同步到 FpOutBloggerFansSummary 表
            self._sync_fans_summary_to_fp_out_blogger_fans_summary(data, blogger_id)

        except Exception as e:
            logger.error(f"处理粉丝概要数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _sync_fans_summary_to_fp_out_blogger_fans_summary(self, data, user_id):
        """同步粉丝概要数据到 FpOutBloggerFansSummary 表"""
        try:
            from core.database_text_tibao_2 import session as new_session

            # 检查是否已存在相同的记录
            existing_record = new_session.query(FpOutBloggerFansSummary).filter(
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
                new_session.commit()
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

                new_session.add(fans_summary)
                new_session.commit()

        except Exception as e:
            logger.error(f"同步粉丝概要数据到 FpOutBloggerFansSummary 表失败: {str(e)}")
            new_session.rollback()
        finally:
            try:
                new_session.close()
                logger.debug("成功关闭数据库会话")
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")

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

    def _process_notes_detail(self, api_data, blogger_id):
        """处理笔记详情数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("笔记详情API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("笔记详情数据为空或格式错误")
                return

            # 安全获取笔记列表
            note_list = data.get('list', [])
            if not isinstance(note_list, list):
                logger.warning("笔记列表不是数组格式")
                return

            if not note_list:
                logger.info("笔记列表为空")
                return

            brand_set = set()
            brand_date_map = set()
            last_publish_date = None
            likeNum = 0
            video_num = 0

            # 处理每个笔记项
            for item in note_list:
                if not isinstance(item, dict):
                    continue

                try:
                    # 安全获取点赞数
                    item_like_num = item.get("likeNum", 0)
                    if item_like_num is not None:
                        try:
                            like_val = float(item_like_num)
                            if like_val > 0:
                                likeNum += like_val
                        except (ValueError, TypeError):
                            logger.warning(f"点赞数转换失败: {item_like_num}")

                    # 检查是否为视频
                    isVideo = item.get("isVideo", False)
                    if isVideo:
                        video_num += 1

                    # 处理品牌信息
                    brandName = item.get("brandName")
                    if brandName and str(brandName).strip():
                        brand_set.add(str(brandName).strip())

                        # 处理品牌合作日期
                        date_str = item.get("date")
                        if date_str and str(date_str).strip():
                            try:
                                date_obj = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
                                date_str_formatted = f"{date_obj.month}月{date_obj.day}日"
                                brandName_str = f"{str(brandName).strip()}:{date_str_formatted}"
                                brand_date_map.add(brandName_str)
                            except Exception as e:
                                logger.warning(f"解析品牌合作日期失败: {date_str}, 错误: {str(e)}")

                    # 处理笔记发布日期
                    date_str = item.get('date')
                    if date_str and str(date_str).strip():
                        try:
                            note_date = datetime.strptime(str(date_str).strip(), "%Y-%m-%d").date()
                            if not last_publish_date or note_date > last_publish_date:
                                last_publish_date = note_date
                        except Exception as e:
                            logger.warning(f"解析笔记日期失败: {date_str}, 错误: {str(e)}")
                            continue

                except Exception as e:
                    logger.warning(f"处理笔记项时出错: {str(e)}")
                    continue

            # 构造"已合作品牌"格式
            brand_list = sorted(list(brand_set))  # 排序确保一致性
            brand_names_str = "、".join(brand_list) if brand_list else ""

            # 构造"品牌合作日期"格式
            brand_dates_list = sorted(list(brand_date_map))  # 排序确保一致性
            brand_dates_str = "、".join(brand_dates_list) if brand_dates_list else ""

            # 安全计算比例，避免除零错误
            video_note_ratio = 0.0
            note_like_median = 0.0

            try:
                if len(note_list) > 0:
                    video_note_ratio = round(video_num / len(note_list), 4)
                    note_like_median = round(likeNum / len(note_list), 2)
            except (ZeroDivisionError, TypeError) as e:
                logger.warning(f"计算比例时出错: {str(e)}")
                video_note_ratio = 0.0
                note_like_median = 0.0

            # 更新用户数据
            try:
                self.current_user_data.update({
                    'video_note_ratio': video_note_ratio,  # 视频笔记比例
                    'note_like_median': note_like_median,  # 笔记平均点赞数
                    'cooperated_brands': brand_names_str,  # 品牌名
                    'cooperated_brands_with_date': brand_dates_str,  # 已合作品牌及日期
                    'last_note_update_time': last_publish_date,
                })

            except Exception as e:
                logger.error(f"更新笔记详情字段时出错: {str(e)}")

            # 同步笔记详情到 FpOutBloggerNoteDetail 表
            self._sync_notes_detail_to_fp_out_blogger_note_detail(data, blogger_id)

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _sync_notes_detail_to_fp_out_blogger_note_detail(self, data, user_id):
        """同步笔记详情数据到 FpOutBloggerNoteDetail 表"""
        try:
            from core.database_text_tibao_2 import session as new_session

            # 安全获取笔记列表
            note_list = data.get('list', [])
            if not isinstance(note_list, list):
                logger.warning("笔记列表不是数组格式")
                return

            if not note_list:
                logger.info("笔记列表为空")
                return

            saved_count = 0
            for item in note_list:
                if not isinstance(item, dict):
                    continue

                try:
                    # 检查是否已存在相同的记录
                    existing_record = new_session.query(FpOutBloggerNoteDetail).filter(
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
                        new_session.commit()
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

                        new_session.add(note_detail)
                        saved_count += 1

                except Exception as e:
                    logger.warning(f"处理笔记项时出错: {str(e)}")
                    continue

            # 提交数据库事务
            try:
                new_session.commit()
                logger.info(f"成功保存 {saved_count} 条笔记记录到 FpOutBloggerNoteDetail 表，用户ID: {user_id}")
            except Exception as e:
                logger.error(f"提交数据库事务失败: {str(e)}")
                new_session.rollback()

        except Exception as e:
            logger.error(f"同步笔记详情数据到 FpOutBloggerNoteDetail 表失败: {str(e)}")
            new_session.rollback()
        finally:
            try:
                new_session.close()
                logger.debug("成功关闭数据库会话")
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")

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

            # 关闭所有资源
            if hasattr(self, 'page') and self.page:
                try:
                    self.page.close()
                except Exception as e:
                    logger.warning(f"关闭页面时出错: {str(e)}")

            if hasattr(self, 'context') and self.context:
                try:
                    self.context.close()
                except Exception as e:
                    logger.warning(f"关闭上下文时出错: {str(e)}")

            if hasattr(self, 'browser') and self.browser:
                try:
                    self.browser.close()
                except Exception as e:
                    logger.warning(f"关闭浏览器时出错: {str(e)}")

            if hasattr(self, 'playwright') and self.playwright:
                try:
                    self.playwright.stop()
                except Exception as e:
                    logger.warning(f"停止playwright时出错: {str(e)}")

            # 确保关闭数据库会话
            try:
                session.close()
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _handle_api_response(self, response):
        """处理API响应，只捕获指定的API请求"""
        try:
            if not response:
                logger.warning("API响应对象为空")
                return

            url = response.url
            if not url:
                logger.warning("API响应URL为空")
                return

            # 从配置获取需要捕获的API路径
            target_apis = self.config.get('API_TARGETS', [])
            if not target_apis:
                logger.warning("未配置API目标")
                return

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                try:
                    # 检查响应状态
                    if response.status != 200:
                        logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                        return

                    # 安全解析JSON
                    try:
                        data = response.json()
                    except Exception as json_error:
                        logger.warning(f"JSON解析失败: {str(json_error)}, URL: {url}")
                        return

                    # 验证数据结构
                    if not isinstance(data, dict):
                        logger.warning(f"API响应数据格式异常，期望dict，实际: {type(data)}, URL: {url}")
                        return

                    # 找到匹配的API类型
                    matched_api = None
                    for api in target_apis:
                        if api in url:
                            matched_api = api
                            break

                    if not matched_api:
                        logger.warning(f"未找到匹配的API类型: {url}")
                        return

                    # 存储API数据
                    try:
                        self.api_data[url] = {
                            'url': url,
                            'data': data,
                            'api_type': matched_api,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'status': response.status
                        }
                        logger.debug(f"成功捕获API响应: {matched_api}, URL: {url}")
                    except Exception as e:
                        logger.error(f"存储API数据时出错: {str(e)}")

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
                    logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

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

    def parse_percentage(self, value, default=0.0):
        """
        将字符串或数字形式的百分比转换为小数

        Args:
            value: 要转换的值，可以是字符串(如 "12.34" 或 "12.34%")或数字
            default: 转换失败时返回的默认值

        Returns:
            float: 转换后的小数值
        """
        try:
            if value is None:
                return default

            # 如果是字符串，去除百分号和空白
            if isinstance(value, str):
                value = value.strip().rstrip('%')

            # 转换为float并除以100
            return float(value) / 100

        except (ValueError, TypeError, AttributeError):
            return default

    def _validate_api_data(self, api_data, required_fields=None):
        """
        验证API数据的有效性

        Args:
            api_data: API响应数据
            required_fields: 必需字段列表

        Returns:
            bool: 数据是否有效
        """
        try:
            if not isinstance(api_data, dict):
                logger.warning("API数据不是字典格式")
                return False

            if api_data.get('code') != 0:
                logger.warning(f"API响应码异常: {api_data.get('code')}")
                return False

            if 'data' not in api_data:
                logger.warning("API数据缺少data字段")
                return False

            if required_fields:
                data = api_data.get('data', {})
                missing_fields = [field for field in required_fields if field not in data]
                if missing_fields:
                    logger.warning(f"API数据缺少必需字段: {missing_fields}")
                    return False

            return True

        except Exception as e:
            logger.error(f"验证API数据时出错: {str(e)}")
            return False

    def _safe_get_nested(self, data, keys, default=""):
        """
        安全获取嵌套字典中的值

        Args:
            data: 数据字典
            keys: 键的列表，如 ['noteSign', 'name']
            default: 默认值

        Returns:
            获取到的值或默认值
        """
        try:
            current = data
            for key in keys:
                if current is None or not isinstance(current, dict):
                    return default
                current = current.get(key)
                if current is None:
                    return default
            return current if current is not None else default
        except Exception:
            return default

    def _safe_get_numeric(self, data, key, default=0, min_value=None, max_value=None):
        """
        安全获取数值，支持范围验证

        Args:
            data: 数据字典
            key: 键名
            default: 默认值
            min_value: 最小值
            max_value: 最大值

        Returns:
            float: 安全获取的数值
        """
        try:
            value = data.get(key, default)
            if value is None:
                return default

            # 转换为float
            numeric_value = float(value)

            # 范围验证
            if min_value is not None and numeric_value < min_value:
                logger.warning(f"数值 {key}={numeric_value} 小于最小值 {min_value}")
                return default

            if max_value is not None and numeric_value > max_value:
                logger.warning(f"数值 {key}={numeric_value} 大于最大值 {max_value}")
                return default

            return numeric_value

        except (ValueError, TypeError) as e:
            logger.warning(f"转换数值 {key}={value} 失败: {str(e)}")
            return default

    def _process_fans_history(self, api_data, blogger_id):
        """处理粉丝历史数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("粉丝历史API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝历史数据为空或格式错误")
                return

            logger.info(f"处理粉丝历史数据: {data}")

            # 处理粉丝历史列表数据
            fans_list = data.get('list', [])
            if isinstance(fans_list, list) and fans_list:
                processed_count = 0
                for fan_record in fans_list:
                    if isinstance(fan_record, dict):
                        # 为每个记录添加时间戳
                        fan_record['updated_at'] = datetime.now()
                        # 同步到 FpOutBloggerFansHistory 表
                        self._sync_fans_history_to_fp_out_blogger_fans_history(fan_record, blogger_id)
                        processed_count += 1
                logger.info(f"成功处理 {processed_count} 条粉丝历史记录")
            else:
                logger.warning("粉丝历史列表数据为空或格式错误")
        except Exception as e:
            logger.error(f"处理粉丝历史数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _sync_fans_history_to_fp_out_blogger_fans_history(self, data, user_id):
        """同步粉丝历史数据到 FpOutBloggerFansHistory 表"""
        if not user_id:
            logger.warning("用户ID为空，无法同步粉丝历史数据")
            return

        from core.database_text_tibao_2 import session as new_session
        try:
            # 检查是否已存在相同的记录
            existing_record = new_session.query(FpOutBloggerFansHistory).filter(
                FpOutBloggerFansHistory.user_id == user_id,
                FpOutBloggerFansHistory.dateKey == data.get('dateKey')
            ).first()

            if existing_record:
                # 更新已存在的记录
                existing_record.num = data.get('num')
                existing_record.date_type = data.get('date_type')
                existing_record.increase_type = data.get('increase_type')
                existing_record.updated_at = data.get('updated_at')
                new_session.commit()
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

                new_session.add(fans_history)
                new_session.commit()

        except Exception as e:
            logger.error(f"同步粉丝历史数据到 FpOutBloggerFansHistory 表失败: {str(e)}")
            new_session.rollback()
        finally:
            try:
                new_session.close()
                logger.debug("成功关闭数据库会话")
            except Exception as e:
                logger.warning(f"关闭数据库会话时出错: {str(e)}")


def run_spider_task():
    """
    执行爬虫任务 - 被调度器调用的函数
    """
    spider = None
    try:
        logger.info("开始执行蒲公英数据抓取任务...")

        # 1. 初始化爬虫实例
        spider = PGYSpider()

        # 2. 执行登录
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        # 3. 执行抓取和数据更新
        spider.scrape_user_notes()

        logger.info("数据抓取任务完成")
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


def main():
    """
    主函数 - 调度器版本
    """
    try:
        # 加载配置
        config = load_config()
        scheduler_config = config['SCHEDULER_CONFIG']

        logger.info("=== 蒲公英数据抓取调度器启动 ===")
        logger.info(f"调度器启用状态: {scheduler_config['enable_scheduler']}")
        logger.info(f"执行时间: 每天 {scheduler_config['daily_time']}")
        logger.info(f"单次执行模式: {scheduler_config['run_once']}")

        if scheduler_config['run_once']:
            # 单次执行模式
            logger.info("单次执行模式，立即开始任务...")
            success = run_spider_task()
            return success

        elif scheduler_config['enable_scheduler']:

            # 设置定时任务
            schedule.every().day.at(scheduler_config['daily_time']).do(run_spider_task)

            # 运行调度器
            while True:
                schedule.run_pending()
                import time
                time.sleep(scheduler_config['check_interval'])

        else:
            # 调度器未启用，直接执行一次
            logger.info("调度器未启用，直接执行任务...")
            success = run_spider_task()
            return success

    except KeyboardInterrupt:
        logger.info("调度器已停止")
        return True
    except Exception as e:
        logger.error(f"调度器启动失败: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    run_spider_task()