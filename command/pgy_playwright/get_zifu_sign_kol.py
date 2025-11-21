import configparser
import json
import os
import sys
import time
from datetime import datetime
import cv2
from loguru import logger

from core.database_text_tibao_2 import session
from playwright.sync_api import sync_playwright
import traceback

from models.models import Creator, CreatorNoteDetail, BloggerInfo
from models.models_tibao import FpCreatorFansSummary, FpCreatorNoteRate
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
            'page_size': config.get('PGY_LOGIN', 'page_size')
        },
    }

class PGYSpider:
    def __init__(self):
        # 加载配置
        self.setup_logger()
        self.config = load_config()

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
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.creator_id = 0
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
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                logger.info("等待5分钟后重试...")
                time.sleep(300)  # 5分钟 = 300秒
                return
            # creator_data = session.query(CreatorBusinessOut).filter(CreatorBusinessOut.creator_mcn == 6, CreatorBusinessOut.sign_status == 1).all()
            page = int(self.config['PGY_LOGIN_CONFIG']['id'])
            page_size = int(self.config['PGY_LOGIN_CONFIG']['page_size'])

            creator_data = session.query(BloggerInfo) \
                .filter(BloggerInfo.status == 0) \
                .offset((page - 1) * page_size) \
                .limit(page_size) \
                .all()

            if len(creator_data) > 0:
                logger.info(f"找到 {len(creator_data)} 个博主数据，开始处理...")

                for item in creator_data:
                    # if not item.platform_user_id:
                    #     continue

                    try:
                        # 清空之前的数据
                        self.api_data.clear()

                        # logger.info(f"正在处理博主: {item.creator_nickname}")
                        logger.info(f"正在处理博主: {item.nickname}")

                        # 访问页面
                        # page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{item.platform_user_id:}"
                        page_url = item.pgy_link
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

                        self.common.random_sleep(30, 35)

                        if self.api_data:
                            # 创建api_data的副本进行遍历，参考原版
                            api_data_copy = dict(self.api_data)
                            for api_url, response_data in api_data_copy.items():
                                if 'data' not in response_data:
                                    continue

                                api_data = response_data['data']
                                # if 'blogger' in api_url:
                                #     self._process_blogger(api_data, item)
                                if 'notes_detail' in api_url:
                                    self._process_notes_detail(api_data, item)
                                elif 'fans_summary' in api_url:
                                    self._process_fans_summary(api_data, item)
                                elif 'notes_rate' in api_url:
                                    self._process_notes_rate(api_data, item, 0)

                            self.api_data.clear()
                            # 点击按钮操作
                            dropdown_container = self.page.locator('.d-spinner-nested-loading')
                            switch_button = dropdown_container.locator('button:has-text("合作笔记")').first
                            if not switch_button.is_visible(timeout=5000):
                                logger.warning(f"按钮不可见: {'button:has-text("合作笔记")'}")
                                return False
                            switch_button.click()
                            logger.info(f"成功点击按钮: {'button:has-text("合作笔记")'}")
                            for api_url, response_data in api_data_copy.items():
                                if 'data' not in response_data:
                                    continue

                                api_data = response_data['data']

                                if 'notes_rate' in api_url:
                                    self._process_notes_rate(api_data, item, 1)

                            self.common.random_sleep(10, 15)

                            # 所有 API 数据处理完毕后，点击笔记按钮
                            try:
                                self.api_data.clear()
                                # 处理第一页笔记详情数据
                                notes_data_copy = dict(self.api_data)
                                should_continue_pagination = True

                                # 处理第一页数据
                                for api_url, response_data in notes_data_copy.items():
                                    if 'notes_detail' in api_url and 'data' in response_data:
                                        api_data = response_data['data']
                                        # 检查是否应该继续分页
                                        should_continue_pagination = self._process_notes_detail(api_data, item)
                                        break

                                # 如果应该继续分页，继续处理后续页面
                                if should_continue_pagination:
                                    logger.info("开始处理分页数据...")
                                    page_count = 1

                                    while self._click_next_page():
                                        page_count += 1
                                        logger.info(f"正在处理第 {page_count} 页数据...")

                                        # 等待API数据加载
                                        self.common.random_sleep(7, 10)

                                        # 处理当前页数据
                                        for api_url, response_data in self.api_data.items():
                                            if 'notes_detail' in api_url and 'data' in response_data:
                                                api_data = response_data['data']
                                                should_continue_pagination = self._process_notes_detail(api_data, item)
                                                break

                                        # 如果不应该继续分页，退出循环
                                        if not should_continue_pagination:
                                            logger.info(f"第 {page_count} 页数据距离今天超过90天，停止分页")
                                            break

                                        # 清空当前页的API数据，准备处理下一页
                                        self.api_data.clear()

                                    logger.info(f"分页处理完成，共处理了 {page_count} 页数据")
                                else:
                                    logger.info("第一页数据距离今天超过90天，无需分页")

                            except Exception as db_error:
                                logger.error(f"更新数据库时出错: {str(db_error)}")
                                # 继续处理下一个博主，不退出程序
                            item.status = 1
                            session.commit()

                        else:
                            # logger.info(f"未捕获到博主 {item.creator_nickname} 的API请求")
                            logger.info(f"未捕获到博主 {item.nickname} 的API请求")

                    except Exception as e:
                        # logger.error(f"处理博主 {item.creator_nickname} 数据时出错: {str(e)}")
                        logger.error(f"处理博主 {item.nickname} 数据时出错: {str(e)}")
                        continue
            else:
                logger.info("没有找到新签约博主数据")

            # 保存进度和Cookie
            self._save_cookies()

            logger.info("本轮数据处理完成，等待5分钟后继续查询...")
            time.sleep(300)  # 5分钟 = 300秒

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            self.update_monitor_status(
                status="出错",
                fail_count=self.monitor_data.get('fail_count', 0) + 1
            )
            logger.info("发生错误，等待5分钟后重试...")
            return

    def _process_blogger(self, api_data, url):
        self.creator_id = url.id
        if api_data['code'] == 0:
            data = api_data.get('data', {}) or {}
            note_sign = data.get('noteSign') or {}
            tag_str = ''
            tags = data.get('contentTags', [])
            if tags and isinstance(tags, list):
                first_tag = tags[0]  # 取第一个标签对象
                taxonomy1 = first_tag.get('taxonomy1Tag', '')
                taxonomy2_list = first_tag.get('taxonomy2Tags', [])
                taxonomy2 = taxonomy2_list[0] if taxonomy2_list else ''
                tag_str = f"{taxonomy1}-{taxonomy2}" if taxonomy1 or taxonomy2 else ''
            else:
                tag_str = ''

            try:
                session.query(Creator).filter(Creator.id == url.id).update({
                    Creator.creator_nickname: data.get('name') or '',
                    Creator.platform_account_id: data.get('redId') or '',
                    Creator.creator_location: data.get('location') or '',
                    Creator.fans_count: data.get('fansCount') or 0,
                    Creator.like_collect_count: data.get('likeCollectCountInfo') or 0,
                    Creator.picture_price: data.get('picturePrice') or 0.00,
                    Creator.video_price: data.get('videoPrice') or 0.00,
                    Creator.creator_gender: data.get('gender') or 0,
                    Creator.creator_avatar: data.get('headPhoto') or '',
                    Creator.account_level: data.get('currentLevel') or 0,
                    Creator.content_field: tag_str,
                    Creator.mcn_name: note_sign.get('name') or '',
                    Creator.status:2
                })
                session.commit()

            except Exception as e:
                session.rollback()
                print(f"[错误] 更新 Creator 失败, ID={url.id}, 错误详情: {e}")

    def _process_notes_detail(self, api_data, url):
        """处理笔记详情数据"""
        if api_data['code'] != 0:
            return False  # 返回False表示不应该继续分页

        try:
            data = api_data.get('data', {}) or {}
            note_list = data.get('list', [])
            # 将当前页的每个笔记数据添加到列表中
            if note_list and isinstance(note_list, list):
                for item in data['list']:
                    item_data = dict(item)
                    existing = session.query(CreatorNoteDetail).filter(
                        # CreatorNoteDetail.platform_user_id == url.platform_user_id,
                        CreatorNoteDetail.platform_user_id == url.id,
                        CreatorNoteDetail.note_id == item_data.get('noteId')
                    ).first()

                    if existing:
                        # 如果已存在，更新数据
                        existing.note_title = item_data.get('title')
                        existing.brand_name = item_data.get('brandName')
                        existing.note_date = item_data.get('date')
                        existing.img_url = item_data.get('imgUrl')
                        existing.is_advertise = item_data.get('isAdvertise', False)
                        existing.is_video = item_data.get('isVideo', False)
                        existing.like_num = item_data.get('likeNum')
                        existing.collect_num = item_data.get('collectNum')
                        existing.read_num = item_data.get('readNum')
                        existing.update_time=int(datetime.now().timestamp())
                    else:
                        # 创建新记录
                        note_detail = CreatorNoteDetail(
                            # platform_user_id=url.platform_user_id,
                            platform_user_id=url.id,
                            note_id=item_data.get('noteId'),
                            note_title=item_data.get('title'),
                            brand_name=item_data.get('brandName'),
                            note_date=item_data.get('date'),
                            img_url=item_data.get('imgUrl'),
                            is_advertise=item_data.get('isAdvertise', False),
                            is_video=item_data.get('isVideo', False),
                            like_num=int(item_data.get('likeNum')),
                            collect_num=int(item_data.get('collectNum')),
                            read_num=int(item_data.get('readNum')),
                            create_time=int(datetime.now().timestamp()),
                            update_time = int(datetime.now().timestamp())
                        )
                        session.add(note_detail)
                session.commit()

            # 检查最后一条数据的时间，判断是否应该继续分页
            if note_list:
                should_continue = True
            else:
                should_continue = False
            return should_continue

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")
            session.rollback()
            return True

    def _process_fans_summary(self, api_data, url):
        """处理笔记详情数据"""
        self.creator_id = url.id
        if api_data['code'] == 0:
            data = api_data.get('data', {}) or {}
            item_data = dict(data)
            existing = session.query(FpCreatorFansSummary).filter(
                # FpCreatorFansSummary.platform_user_id == url.platform_user_id,
                FpCreatorFansSummary.platform_user_id == url.id,
            ).first()

            if existing:
                # 如果已存在，更新数据
                existing.fans_increase_num = item_data.get('fansIncreaseNum')
                existing.fans_growth_rate = item_data.get('fansGrowthRate')
                existing.active_fans_rate = item_data.get('activeFansRate')
                existing.read_fans_rate = item_data.get('readFansRate')
                existing.engage_fans_rate = item_data.get('engageFansRate')
                existing.pay_fans_user_rate_30d = item_data.get('payFansUserRate30d')
                existing.fans_growth_beyond_rate = item_data.get('fansGrowthBeyondRate')
                existing.active_fans_beyond_rate = item_data.get('activeFansBeyondRate')
                existing.read_fans_beyond_rate = item_data.get('readFansBeyondRate')
                existing.engage_fans_beyond_rate = item_data.get('engageFansBeyondRate')
                existing.pay_fans_user_num_30d = item_data.get('payFansUserNum30d')
                existing.read_fans_in_30 = item_data.get('readFansIn30')
                existing.fans_num = item_data.get('fansNum')
                existing.engage_fans_l30 = item_data.get('engageFansL30')
                existing.active_fans_l28 = item_data.get('activeFansL28')
                existing.update_time = int(datetime.now().timestamp())
            else:
                # 创建新记录
                note_detail = FpCreatorFansSummary(
                    # platform_user_id=url.platform_user_id,
                    platform_user_id=url.id,
                    fans_increase_num = item_data.get('fansIncreaseNum'),
                    fans_growth_rate = item_data.get('fansGrowthRate'),
                    active_fans_rate = item_data.get('activeFansRate'),
                    read_fans_rate = item_data.get('readFansRate'),
                    engage_fans_rate = item_data.get('engageFansRate'),
                    pay_fans_user_rate_30d = item_data.get('payFansUserRate30d'),
                    fans_growth_beyond_rate = item_data.get('fansGrowthBeyondRate'),
                    active_fans_beyond_rate = item_data.get('activeFansBeyondRate'),
                    read_fans_beyond_rate = item_data.get('readFansBeyondRate'),
                    engage_fans_beyond_rate = item_data.get('engageFansBeyondRate'),
                    pay_fans_user_num_30d = item_data.get('payFansUserNum30d'),
                    read_fans_in_30 = item_data.get('readFansIn30'),
                    fans_num = item_data.get('fansNum'),
                    engage_fans_l30 = item_data.get('engageFansL30'),
                    active_fans_l28 = item_data.get('activeFansL28'),
                    create_time=int(datetime.now().timestamp()),
                    update_time=int(datetime.now().timestamp())
                )
                session.add(note_detail)
                session.commit()

    def _process_notes_rate(self, api_data, url, business):
        if api_data['code'] == 0:
            data = api_data.get('data', {}) or {}
            item_data = dict(data)
            existing = session.query(FpCreatorNoteRate).filter(
                FpCreatorNoteRate.platform_user_id == url.id,
                FpCreatorNoteRate.business == business,
            ).first()
            if existing:
                # 如果已存在，更新数据
                existing.imp_median = item_data.get('impMedian')
                existing.read_median = item_data.get('readMedian')
                existing.mengagement_num = item_data.get('mengagementNum')
                existing.like_median = item_data.get('likeMedian')
                existing.collect_median = item_data.get('collectMedian')
                existing.comment_median = item_data.get('commentMedian')
                existing.share_median = item_data.get('shareMedian')
                existing.mfollow_cnt = item_data.get('mfollowCnt')
                existing.interaction_rate = item_data.get('interactionRate')
                existing.video_full_view_rate = item_data.get('videoFullViewRate')
                existing.picture3s_view_rate = item_data.get('picture3sViewRate')
                existing.thousand_like_percent = item_data.get('thousandLikePercent')
                existing.hundred_like_percent = item_data.get('hundredLikePercent')
                existing.business = business
                existing.update_time = int(datetime.now().timestamp())
            else:
                # 创建新记录
                note_detail = FpCreatorNoteRate(
                    platform_user_id=url.id,
                    business=business,

                    # ====== 新增补全：你上面 update 中用到的字段 ======
                    imp_median=item_data.get('impMedian'),
                    read_median=item_data.get('readMedian'),
                    mengagement_num=item_data.get('mengagementNum'),
                    like_median=item_data.get('likeMedian'),
                    collect_median=item_data.get('collectMedian'),
                    comment_median=item_data.get('commentMedian'),
                    share_median=item_data.get('shareMedian'),
                    mfollow_cnt=item_data.get('mfollowCnt'),
                    interaction_rate=item_data.get('interactionRate'),
                    video_full_view_rate=item_data.get('videoFullViewRate'),
                    picture3s_view_rate=item_data.get('picture3sViewRate'),
                    thousand_like_percent=item_data.get('thousandLikePercent'),
                    hundred_like_percent=item_data.get('hundredLikePercent'),

                    create_time=int(datetime.now().timestamp()),
                    update_time=int(datetime.now().timestamp())
                )
                session.add(note_detail)
                session.commit()

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

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应，只捕获指定的API请求"""
        try:
            url = response.url
            # 从配置获取需要捕获的API路径
            target_apis = [
                'solar/cooperator/user/blogger',
                'notes_detail',
                'fans_summary',
                'notes_rate'
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


def run_spider_task():
    """
    执行爬虫任务 - 只在异常时重启版本
    """
    spider = None
    try:
        # 1. 初始化爬虫实例
        spider = PGYSpider()
        logger.info("爬虫实例初始化成功")

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
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        sys.exit(1)