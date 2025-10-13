import json
import os
import signal
import time
from datetime import datetime
import sys
from pathlib import Path
import random

import keyboard
import pandas as pd
import schedule
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text, and_, or_, desc, Column, Integer, String, TIMESTAMP, DateTime, Numeric, \
    Text, Boolean, Float, BigInteger, ForeignKey, DECIMAL
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session

from unitl.common import Common

"""
    获取SpiderQianguaHotNote表中博主姓名，去小红书完善博主信息
"""

# 数据库连接配置
DATABASE_URL = 'mysql+pymysql://fpdev:fpdev@47.104.13.93:3306/fangpian'

# 创建数据库引擎
engine = create_engine(DATABASE_URL, isolation_level="READ UNCOMMITTED")

# 创建会话工厂
Session = sessionmaker(bind=engine)
ScopedSession = scoped_session(Session)

# 创建会话
session = Session()

# 创建声明性基类
Base = declarative_base()


class SpiderQianguaHotNote(Base):
    __tablename__ = 'fp_spider_qiangua_hot_note'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='ID')
    kol_name = Column(String(128), comment='博主名称')
    note_title = Column(String(128), comment='笔记标题')
    xiaohongshu_user_id = Column(String(32), comment='博主user_id')
    xiaohongshu_note_id = Column(String(32), comment='笔记小红书id')
    xsec_token = Column(String(256), comment='笔记token')
    kol_img = Column(String(128), comment='博主头像')
    kol_image_id = Column(String(128), comment='博主头像id')
    kol_type = Column(String(128), comment='博主类型')
    note_like = Column(BigInteger, comment='点赞')
    note_collect = Column(BigInteger, comment='收藏')
    note_issue_time = Column(String(128), comment='笔记发布时间')
    note_comment = Column(BigInteger, comment='评论')
    note_share = Column(BigInteger, comment='分享')
    note_read = Column(BigInteger, comment='预估阅读')
    note_interact = Column(BigInteger, comment='互动量')
    note_classify = Column(String(128), comment='笔记分类')
    note_image = Column(String(255), comment='笔记封面')
    note_image_id = Column(String(255), comment='笔记封面id')
    note_tags = Column(String(255), comment='笔记标签')
    note_tag_classify = Column(String(128), comment='笔记标签分类')
    hot_note_24h = Column(String(128), comment='排名时间（近24小时）')
    note_type = Column(Integer, comment='笔记类型')
    hot_date = Column(Integer, comment='日期0点时间戳')
    status = Column(Integer, comment='抓取状态')
    create_time = Column(Integer, comment='创建时间')
    update_time = Column(Integer, comment='更新时间')
    video_url = Column(String(255), comment='视频URL')
    note_video_text = Column(Text, comment='视频文本内容')


class XiaohongshuSpider:
    def __init__(self):
        self.logger = logger.bind(class_name=self.__class__.__name__)
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.xiaohongshu.com"
        self.is_logged_in = False
        self.found_match = False  # 标志位，表示是否找到匹配的笔记
        self.api_data = {}  # 存储API数据
        self.notes_data = {}  # 存储API数据
        self.video_data = {}  # 存储API数据
        self.matched_notes = {}  # 存储API数据
        self.kol_image_id = ''  # 存储API数据
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

            # 设置响应监听器
            self.page.on("response", self._handle_api_response)

        except Exception as e:
            self.logger.error(f"初始化浏览器时出错: {str(e)}")
            raise Exception("浏览器初始化失败")

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url

            # 过滤静态资源和非目标API请求
            if not any(api in url for api in ['usersearch', 'user_posted', 'feed']) or \
                    url.endswith(('.png', '.jpg', '.gif', '.css', '.js')):
                return

            # 根据响应状态码进行处理
            if response.status >= 400:
                self.logger.warning(f"API响应异常状态码: {response.status} for {url}")
                return

            try:
                response_data = response.json()
            except Exception as e:
                self.logger.error(f"解析API响应JSON失败: {str(e)}")
                return

            # 添加随机延迟，模拟真实用户行为
            self.common.random_sleep(0.5, 1.5)

            # 处理不同类型的API响应
            if 'api/sns/web/v1/search/usersearch' in url:
                self._handle_user_search_response(response_data)
            elif '/api/sns/web/v1/user_posted' in url:
                self._handle_user_posted_response(response_data)
            elif 'api/sns/web/v1/feed' in url:
                self._handle_feed_response(response_data)

        except Exception as e:
            self.logger.error(f"处理API响应时出错: {str(e)}")

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
            login_button = self.page.wait_for_selector("text=登录", timeout=10000)
            if not login_button:
                self.logger.error("未找到登录按钮")
                return False
            login_button.click()
            self.common.random_sleep(2, 3)

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

    def scrape_user_notes(self, kol_name, kol_image_id):
        """
        抓取指定KOL的笔记信息并匹配更新数据库
        :param kol_name: KOL名称
        :param kol_image_id: KOL图片id
        :return: 返回值说明：
                 True - 成功完成抓取
                 False - 一般性错误，可以重试
                 'SKIP' - 应该跳过这个博主，不再重试
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return False

            # 重置所有状态
            self._reset_state()
            self.common.random_sleep(3, 4)
            self.logger.info(f"开始处理博主: {kol_name}")

            # 点击搜索框 - 添加随机位置点击
            search_box = self.page.locator('.search-input').first
            search_box.click()

            # 随机等待时间
            self.common.random_sleep(0.8, 1.7)  # 更随机的等待时间

            # 清空搜索框
            search_box.fill("")
            self.common.random_sleep(0.3, 1.2)  # 更自然的等待

            # 模拟更自然的输入
            for char in kol_name:
                # 在快速和慢速输入之间随机变化，模拟真实打字
                if random.random() < 0.2:  # 20%几率停顿一下
                    time.sleep(random.uniform(0.4, 0.8))
                else:
                    time.sleep(random.uniform(0.1, 0.3))
                search_box.type(char)

            # 输入后随机停顿
            self.common.random_sleep(0.7, 1.5)

            # 点击搜索按钮
            search_button = self.page.locator('.input-button').first
            search_button.click()

            # 等待搜索结果加载
            self.page.wait_for_selector('div[class*="content-container"]', timeout=10000)
            self.common.random_sleep(2, 3)

            self.kol_image_id = kol_image_id

            user_tab = self.page.locator('text=用户').first
            if not user_tab.is_visible():
                user_tab = self.page.locator('div[class*="channel"]:has-text("用户")').first

            user_tab.click()

            self.common.random_sleep(2, 3)
            # 尝试不同的选择器来等待用户卡片/头像
            self.page.wait_for_selector('img[class*="user-image"]', timeout=5000)

            # 点击用户标签后，等待API数据
            wait_start = time.time()
            while not self.api_data and time.time() - wait_start < 15:  # 最多等待15秒
                self.common.random_sleep(5, 5)

            if not self.api_data:
                self.logger.error("等待用户搜索数据超时")
                return False

            # 现在我们有了API数据，可以执行导航
            navigation_result = self.search_and_navigate_to_blogger(self.api_data)
            
            # 如果返回'SKIP'，直接返回，不再继续处理
            if navigation_result == 'SKIP':
                return 'SKIP'
            
            # 如果导航失败，返回False以允许重试
            if not navigation_result:
                self.logger.error("导航到用户主页失败")
                return False

            # 如果导航成功，继续处理
            try:
                # 等待笔记列表加载
                self.page.wait_for_selector(".note-item", timeout=10000)
                wait_start = time.time()
                while not self.notes_data and time.time() - wait_start < 10:
                    self.common.random_sleep(2, 3)

                if not self.notes_data:
                    self.logger.error("等待笔记数据超时")
                    return False

                if self._process_user_posted_data(self.notes_data):
                    try:
                        matched_link = self.page.wait_for_selector(
                            f'a[href*="/user/profile/{self.matched_notes["user_id"]}/{self.matched_notes["note_id"]}"]',
                            timeout=5000
                        )
                        self.common.random_sleep(2, 3)

                        # 1. 先清空之前的视频数据
                        self.video_data = {}

                        # 2. 点击笔记并等待页面加载
                        matched_link.click()

                        # 3. 等待笔记详情页面加载完成
                        try:
                            # 等待笔记详情页的特定元素出现
                            self.page.wait_for_selector('.author-container', timeout=10000)
                        except Exception as e:
                            self.logger.error(f"等待笔记详情页面加载失败: {str(e)}")
                            return False

                        # 4. 等待feed API响应
                        wait_start = time.time()
                        while not self.video_data and time.time() - wait_start < 15:  # 增加等待时间到15秒
                            try:
                                # 检查页面是否包含视频元素
                                video_element = self.page.locator('video').first
                                if video_element and video_element.is_visible():
                                    break
                            except:
                                pass
                            self.common.random_sleep(1, 2)  # 缩短每次检查的间隔

                        if not self.video_data:
                            self.logger.error("等待视频数据超时")
                            # 尝试重新触发加载
                            try:
                                self.page.reload()
                                self.common.random_sleep(2, 3)
                                # 再次等待feed API响应
                                wait_start = time.time()
                                while not self.video_data and time.time() - wait_start < 10:
                                    self.common.random_sleep(1, 2)
                            except Exception as e:
                                self.logger.error(f"重新加载页面失败: {str(e)}")
                                return False

                        # 确保模态窗口已关闭
                        close_buttons = self.page.locator('div[class*="close"]').all()
                        for btn in close_buttons:
                            if btn.is_visible():
                                btn.click()
                                self.common.random_sleep(1, 2)
                                break

                        # 等待页面返回正常状态
                        self.common.random_sleep(2, 3)
                        # 第二步：如果有匹配的笔记，立即处理
                        if self.matched_notes:
                            # 只处理第一个匹配的笔记
                            db_note = self.matched_notes['db_note']
                            match_info = self.matched_notes

                            try:
                                # 更新基本信息
                                db_note.xiaohongshu_user_id = match_info['user_id']
                                db_note.kol_img = match_info['user_img']
                                db_note.xiaohongshu_note_id = match_info['note_id']
                                db_note.note_image = match_info['url_default']
                                db_note.xsec_token = match_info['xsec_token']
                                db_note.status = 1
                                db_note.video_url = match_info['video_url']
                                db_note.update_time = int(datetime.now().timestamp())

                                # 标记匹配成功 - 设置全局标志
                                self.found_match = True

                                # 提交数据库更新
                                session.commit()
                                return True

                            except Exception as e:
                                self.logger.error(f"处理匹配笔记时出错: {str(e)}")
                                session.rollback()
                    except Exception as e:
                        self.logger.error(f"处理匹配笔记时出错: {str(e)}")

                # 继续处理笔记数据...
                self.logger.info("开始处理用户笔记数据")

            except PlaywrightTimeoutError as e:
                self.logger.error(f"等待笔记列表超时: {str(e)}")
                return False
            except Exception as e:
                self.logger.error(f"处理笔记数据时出错: {str(e)}")
                return False

            # 判断最终结果
            if self.found_match:
                self.logger.info(f"成功找到并处理了博主 {kol_name} 的匹配笔记")
                return True
            else:
                self.logger.info(f"未找到博主 {kol_name} 的匹配笔记，跳过当前博主")
                return 'SKIP'  # 如果没有找到匹配的笔记，也返回SKIP

        except Exception as e:
            self.logger.error(f"抓取KOL {kol_name} 笔记时出错: {str(e)}")
            return False

    def update_monitor_status(self, **kwargs):
        """更新监控状态"""
        logger.info(kwargs)
        if kwargs.get('completed_count'):
            self.monitor_data['completed_count'] = kwargs.get('completed_count')
        if kwargs.get('fail_count'):
            self.monitor_data['fail_count'] = kwargs.get('fail_count')

    def save_data(self, user_id, data):
        """
        保存抓取的数据到CSV文件
        """
        try:
            filename = os.path.join(self.data_dir, f'user_{user_id}_{datetime.now().strftime("%Y%m%d")}.csv')
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"数据已保存到 {filename}")
        except Exception as e:
            logger.error(f"保存数据时出错: {str(e)}")

    def close(self):
        """
        关闭浏览器和playwright
        """
        try:
            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 关闭数据库连接
            try:
                session.commit()
            except:
                session.rollback()
            finally:
                session.close()

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

    def _extract_video_url_from_feed_data(self, data):
        """从feed API响应中提取视频URL"""
        try:
            self.logger.info("开始从feed API数据中提取视频URL")
            # 路径1: 通过items字段
            items = data.get('data', {}).get('items', [])
            if items and len(items) > 0:
                for item in items:
                    note_card = item.get('note_card', {})
                    if note_card.get('type') == 'video':
                        video = note_card.get('video', {})
                        media = video.get('media', {})
                        stream = media.get('stream', {})

                        # 先尝试h265格式
                        h265 = stream.get('h265', [])
                        if h265 and len(h265) > 0:
                            backup_urls = h265[0].get('backup_urls', [])
                            if backup_urls and len(backup_urls) > 0:
                                return backup_urls[0]

                        # 再尝试h264格式
                        h264 = stream.get('h264', [])
                        if h264 and len(h264) > 0:
                            backup_urls = h264[0].get('backup_urls', [])
                            if backup_urls and len(backup_urls) > 0:
                                return backup_urls[0]
                    else:
                        return None
        except Exception as e:
            self.logger.error(f"提取视频URL时出错: {str(e)}")
            return None

    def _process_user_posted_data(self, response_data):
        """处理用户笔记数据并更新数据库"""
        try:

            if not response_data or 'data' not in response_data:
                self.logger.error("API响应数据格式不正确")
                return False

            notes_data = response_data.get('data', {}).get('notes', [])
            if not notes_data:
                return False

            # 第一步：找出匹配的笔记并保存基本信息
            for note in notes_data:
                try:
                    # 获取笔记信息
                    user = note.get('user', {})
                    user_id = user.get('user_id')
                    user_img = user.get('avatar')
                    if not user_id:
                        continue

                    display_title = note.get('display_title', '')
                    note_id = note.get('note_id')
                    if not note_id:
                        continue

                    xsec_token = note.get('xsec_token')
                    if not xsec_token:
                        continue

                    url_default = note.get('cover', {}).get('url_default', '')

                    # 从url_default中提取图片ID
                    image_id = self.extract_image_id(url_default)

                    if not image_id:
                        continue

                    # 查找匹配的记录
                    try:
                        db_notes = session.query(SpiderQianguaHotNote).filter(
                            SpiderQianguaHotNote.xiaohongshu_note_id.is_(None),
                            (
                                    (SpiderQianguaHotNote.note_title != '') &
                                    (SpiderQianguaHotNote.note_title == display_title)
                            ) |
                            (
                                    (SpiderQianguaHotNote.note_image_id != '') &
                                    (SpiderQianguaHotNote.note_image_id == image_id)
                            )
                        ).all()

                        for db_note in db_notes:
                            # 判断匹配类型
                            title_matched = display_title == db_note.note_title
                            image_id_matched = image_id == db_note.note_image_id

                            self.matched_notes = {}
                            if title_matched or image_id_matched:
                                # 将匹配信息保存起来，稍后处理
                                self.matched_notes = {
                                    'db_note': db_note,
                                    'user_id': user_id,
                                    'user_img': user_img,
                                    'note_id': note_id,
                                    'xsec_token': xsec_token,
                                    'url_default': url_default,
                                    'display_title': display_title,
                                    'image_id': image_id,
                                    'title_matched': title_matched,
                                    'image_id_matched': image_id_matched
                                }
                                self.logger.info(
                                    f"找到匹配笔记: {note_id} (标题匹配: {title_matched}, 图片ID匹配: {image_id_matched})")
                                return True

                    except Exception as e:
                        self.logger.error(f"查询数据库时出错: {str(e)}")
                        session.rollback()
                        continue

                except Exception as e:
                    self.logger.error(f"处理笔记数据时出错: {str(e)}")
                    continue

            # 如果没有匹配的笔记，返回False
            return False

        except Exception as e:
            self.logger.error(f"处理API响应数据时出错: {str(e)}")
            session.rollback()
            return False

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

    def extract_image_id(self, image_url):
        """从图片URL中提取图片ID"""
        try:
            if not image_url:
                return None

            # 常见的图片ID格式 (1040xxx 或 1000gxxx)
            import re
            match = re.search(r'(1040[^?/]+|1000g[^?/]+)', image_url)
            if match:
                image_id = match.group(1)
                # 如果ID末尾有.jpg等后缀，去掉.及后面的内容
                if '.' in image_id:
                    image_id = image_id.split('.')[0]
                return image_id
            return None
        except Exception as e:
            self.logger.error(f"提取图片ID时出错: {str(e)}")
            return None

    def search_and_navigate_to_blogger(self, data):
        """
        搜索并导航到博主页面
        :param data: 用户搜索API返回的数据
        :return: 返回值说明：
                 True - 成功导航到用户主页
                 False - 一般性错误，可以重试
                 'SKIP' - 未找到匹配用户或无法导航，应该跳过这个博主
        """
        try:
            # 确保数据格式正确
            if not data or 'data' not in data or 'users' not in data['data']:
                self.logger.error("API响应数据格式不正确")
                return False

            self.common.random_sleep(2, 3)
            self.logger.info(f"用户接口数据: {data}")

            users = data['data']['users']

            # 遍历用户列表查找匹配的用户
            found_by_image_id = False
            matched_user_data = None
            kol_image_id = self.extract_image_id(self.kol_image_id)

            for user in users:
                user_image = user.get('image', '')
                user_image_id = self.extract_image_id(user_image)
                user_name = user.get('name', '')
                user_id = user.get('id')
                xsec_token = user.get('xsec_token')

                if kol_image_id and user_image_id:
                    # 去掉两个ID中可能存在的.jpg等后缀再比较
                    if kol_image_id == user_image_id:
                        found_by_image_id = True
                        matched_user_data = {
                            'id': user_id,
                            'xsec_token': xsec_token,
                            'name': user_name
                        }
                        break

            # 如果没有找到匹配的用户，使用第一个用户
            # if not found_by_image_id and users:
            #     first_user = users[0]
            #     matched_user_data = {
            #         'id': first_user.get('id'),
            #         'xsec_token': first_user.get('xsec_token'),
            #         'name': first_user.get('name')
            #     }
            #     self.logger.info(f"未找到匹配图片ID的用户，使用第一个用户: {matched_user_data['name']}")
            
            # 如果没有找到匹配的用户，返回SKIP
            if not matched_user_data:
                self.logger.info("未找到匹配的用户，跳过此博主")
                return 'SKIP'

            # 如果找到了用户数据，构造并访问URL
            if matched_user_data and matched_user_data['id'] and matched_user_data['xsec_token']:
                try:
                    # 构造用户主页URL
                    profile_url = f"https://www.xiaohongshu.com/user/profile/{matched_user_data['id']}?xsec_token={matched_user_data['xsec_token']}&xsec_source=pc_search&tab=fav"
                    self.logger.info(f"构造用户主页URL: {profile_url}")

                    # 访问用户主页
                    self.page.goto(profile_url)
                    self.common.random_sleep(2, 3)

                    # 切换到笔记标签
                    notes_tab = self.page.locator('text=笔记').first
                    if notes_tab and notes_tab.is_visible():
                        notes_tab.click()
                        self.common.random_sleep(1, 2)
                        return True
                    else:
                        self.logger.error("无法找到或点击笔记标签，跳过此博主")
                        return 'SKIP'
                except Exception as e:
                    self.logger.error(f"导航到用户页面时出错: {str(e)}")
                    return 'SKIP'
            else:
                self.logger.error("未能获取有效的用户数据，跳过此博主")
                return 'SKIP'

        except Exception as e:
            self.logger.error(f"搜索过程中出错: {str(e)}")
            return False

    def run(self):
        """运行爬虫主程序"""
        try:
            # 尝试登录
            if not self.is_logged_in:
                logger.info("尝试登录...")
                if not self.login():
                    logger.error("登录失败，程序退出")
                    return False

            total_success = 0
            total_fail = 0
            batch_count = 0

            while True:
                # 获取需要爬取的KOL列表
                kols, record_ids, kol_image_ids = get_kols_to_scrape()
                if not kols:
                    if batch_count == 0:
                        logger.warning("没有找到需要更新的KOL数据")
                    else:
                        logger.info(f"所有数据处理完成，共处理 {batch_count} 批数据")
                        logger.info(f"总计：成功 {total_success} 条，失败 {total_fail} 条")
                    break

                batch_count += 1

                # 处理每个KOL
                success_count = 0
                fail_count = 0
                for index, (kol, record_id, kol_image_id) in enumerate(zip(kols, record_ids, kol_image_ids), 1):
                    try:
                        logger.info(f"批次 {batch_count} 进度: {index}/{len(kols)}")
                        retry_count = 0
                        success = False

                        # 处理单个KOL，包含重试逻辑
                        while retry_count <= MAX_RETRIES:
                            result = self.scrape_user_notes(kol, kol_image_id)
                            
                            # 如果返回'SKIP'，直接跳过这个博主，不再重试
                            if isinstance(result, str) and result == 'SKIP':
                                logger.info(f"跳过博主 {kol}")
                                try:
                                    # 更新数据库中的状态
                                    session.query(SpiderQianguaHotNote).filter(
                                        SpiderQianguaHotNote.id == record_id
                                    ).update(
                                        {
                                            "status": 2,  # 状态2表示跳过
                                            "update_time": int(datetime.now().timestamp())
                                        },
                                        synchronize_session=False
                                    )
                                    session.commit()
                                except SQLAlchemyError as e:
                                    logger.error(f"更新博主 {kol} 状态时数据库错误: {str(e)}")
                                    session.rollback()
                                except Exception as e:
                                    logger.error(f"更新博主 {kol} 状态时出错: {str(e)}")
                                    session.rollback()
                                
                                fail_count += 1
                                break
                                
                            if result:  # 如果成功
                                success = True
                                break
                                
                            # 如果是一般性错误，且还有重试机会，继续重试
                            retry_count += 1
                            if retry_count <= MAX_RETRIES:
                                logger.warning(f"第 {retry_count} 次重试处理 KOL: {kol}")
                                time.sleep(RETRY_DELAY)

                        if success:
                            success_count += 1
                            logger.info(f"成功处理KOL: {kol}")
                        else:
                            fail_count += 1
                            if retry_count > MAX_RETRIES:
                                logger.error(f"处理KOL {kol} 失败，已达到最大重试次数")

                    except Exception as e:
                        logger.error(f"处理KOL {kol} 时出错: {str(e)}")
                        fail_count += 1
                        continue

                total_success += success_count
                total_fail += fail_count

                logger.info(f"第 {batch_count} 批数据处理完成")
                logger.info(f"累计结果：成功 {total_success} 条，失败 {total_fail} 条")

                # 每批数据处理完后短暂休息，避免请求过于频繁
                time.sleep(2)

            return True

        except Exception as e:
            logger.error(f"爬虫运行出错: {str(e)}")
            return False
        finally:
            self.close()
            logger.info("爬虫资源已关闭")

    def add_random_scroll(self):
        try:
            # 获取页面高度
            height = self.page.evaluate('() => document.body.scrollHeight')
            if height > 500:
                # 随机滚动到不同位置
                scroll_to = random.randint(100, min(height - 200, 800))
                self.page.evaluate(f'window.scrollTo(0, {scroll_to})')
                self.common.random_sleep(0.5, 1.2)
                # 50%概率再滚动一次
                if random.random() < 0.5:
                    scroll_to2 = random.randint(scroll_to - 100, min(height - 100, scroll_to + 300))
                    self.page.evaluate(f'window.scrollTo(0, {scroll_to2})')
                    self.common.random_sleep(0.3, 0.8)
        except Exception as e:
            self.logger.debug(f"随机滚动出错: {str(e)}")  # 使用debug级别避免记录太多信息

    def _reset_state(self):
        """重置所有状态变量"""
        self.found_match = False
        self.api_data = {}
        self.notes_data = {}
        self.video_data = {}
        self.matched_notes = {}
        self.kol_image_id = ''

    def _handle_user_search_response(self, response_data):
        """处理用户搜索API响应"""
        try:
            if response_data and 'data' in response_data and 'users' in response_data['data']:
                self.api_data = response_data
                user_count = len(response_data['data']['users'])
            else:
                self.logger.warning("用户搜索API返回数据格式不符合预期")
        except Exception as e:
            self.logger.error(f"处理用户搜索响应时出错: {str(e)}")

    def _handle_user_posted_response(self, response_data):
        """处理用户笔记API响应"""
        try:
            if response_data and 'data' in response_data:
                self.notes_data = response_data
                notes_count = len(response_data.get('data', {}).get('notes', []))
            else:
                self.logger.warning("用户笔记API返回数据格式不符合预期")
        except Exception as e:
            self.logger.error(f"处理用户笔记响应时出错: {str(e)}")

    def _handle_feed_response(self, response_data):
        """处理feed API响应"""
        try:
            if response_data and 'data' in response_data:
                self.video_data = response_data
                video_url = self._extract_video_url_from_feed_data(response_data)
                self.logger.info(f"成功提取视频URL: {video_url}")
                if self.matched_notes:
                    self.matched_notes['video_url'] = video_url
                else:
                    self.logger.warning("未能从feed数据中提取到视频URL")
            else:
                self.logger.warning("Feed API返回数据格式不符合预期")
        except Exception as e:
            self.logger.error(f"处理feed响应时出错: {str(e)}")


# 全局变量用于控制程序运行
running = True
MAX_RETRIES = 1
RETRY_DELAY = 5
BATCH_SIZE = 50  # 每次处理的数据量


def signal_handler(signum, frame):
    """处理进程信号"""
    global running
    logger.info("收到退出信号，准备安全退出...")
    running = False


def get_kols_to_scrape():
    """
    获取需要爬取的KOL列表，使用事务和行锁保证线程安全
    返回：(kol列表, 记录ID列表, kol图片ID列表)
    """
    local_session = sessionmaker(bind=engine)()
    try:
        # 开启事务
        with local_session.begin():
            today_start = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_timestamp = int(today_start.timestamp())

            # 使用SELECT FOR UPDATE锁定要处理的记录
            records = local_session.query(
                SpiderQianguaHotNote.id,
                SpiderQianguaHotNote.kol_name,
                SpiderQianguaHotNote.kol_image_id
            ).filter(
                and_(
                    or_(
                        SpiderQianguaHotNote.xiaohongshu_user_id.is_(None),
                        SpiderQianguaHotNote.xiaohongshu_note_id.is_(None)
                    ),
                    SpiderQianguaHotNote.hot_date == today_start_timestamp,
                    SpiderQianguaHotNote.status.is_(None)  # 只获取未处理的记录
                )
            ).order_by(
                desc(SpiderQianguaHotNote.note_interact)
            ).limit(BATCH_SIZE).with_for_update(skip_locked=True).all()

            if not records:
                return [], [], []  # 修改这里，返回三个空列表

            # 提取KOL名称和记录ID
            kols = []
            kol_image_ids = []
            record_ids = []

            for record in records:
                if record.kol_name:  # 确保KOL名称不为空
                    kols.append(record.kol_name)
                    kol_image_ids.append(record.kol_image_id)
                    record_ids.append(record.id)

            # 立即将这些记录的状态更新为0（处理中）
            if record_ids:
                local_session.query(SpiderQianguaHotNote).filter(
                    SpiderQianguaHotNote.id.in_(record_ids)
                ).update(
                    {"status": 0},
                    synchronize_session=False
                )

            # 提交事务，释放锁
            local_session.commit()
            return kols, record_ids, kol_image_ids

    except SQLAlchemyError as e:
        logger.error(f"数据库操作出错: {str(e)}")
        local_session.rollback()
        return [], [], []  # 这里也要修改为返回三个空列表
    except Exception as e:
        logger.error(f"获取KOL列表时出错: {str(e)}")
        local_session.rollback()
        return [], [], []  # 这里也要修改为返回三个空列表
    finally:
        local_session.close()


def main():
    """主任务函数"""
    spider = None
    try:
        # 配置日志
        logger.add(
            "logs/spider_{time}.log",
            rotation="1 day",
            retention="7 days",
            level="INFO",
            encoding="utf-8"
        )

        # 运行爬虫
        spider = XiaohongshuSpider()
        spider.run()

    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
    finally:
        if spider:
            logger.info("爬虫资源已关闭")


def run_scheduler():
    """运行调度器"""
    # 注册信号处理
    main()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 设置定时任务
    schedule.every().day.at('06:20').do(main)

    logger.info("自动化调度已启动，每天06:20运行，按 Ctrl+C 退出...")

    # 主循环
    while running:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.error(f"调度器运行出错: {str(e)}")
            time.sleep(60)  # 出错后等待1分钟再继续


if __name__ == '__main__':
    try:
        run_scheduler()
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        sys.exit(1)