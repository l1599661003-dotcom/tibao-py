import json
import os
from datetime import datetime, timedelta
import requests  # 用于发送飞书请求
import cv2
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy import text, func
import traceback
from urllib.parse import urlparse, parse_qsl

from core.localhost_fp_project import session
from models.models_tibao import (
    KolMediaAccount, 
    BloggerSigningHistory,
    KolBusinessMediaAccount,
    AdminUser,
    AdminUserGroup,
    KolOrder
)
from service.feishu_service import get_feishu_token
from unitl.common import Common

"""
    更新账号信息,博主变现，粉丝情况,从蒲公英抓取数据
"""
class PGYSpider:
    def __init__(self):
        self.setup_logger()
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://pgy.xiaohongshu.com"
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
        self.current_user_data = {}  # 添加一个临时存储当前用户数据的字典
        self.notes_rate_processed = False  # 添加到清空数据的地方

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        logger.add(os.path.join(log_path, "pgy_{time}.log"), rotation="1 day", retention="7 days")

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
            viewport={'width': 1912, 'height': 768},
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
                userSide = self.page.locator(".home_head_user_info").all()
                logger.info(f"user: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 1 or self.page.locator(".home_head_user_info").is_visible(timeout=50000):
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
        使用账号密码登录蒲公英
        :param username: 登录账号
        :param password: 登录密码
        :return: 是否登录成功
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
                email_input.fill("W224564785@163.com")
                self.common.random_sleep(1, 2)  # 模拟人工输入间隔
                
                # 等待密码输入框出现并输入密码
                password_input = self.page.wait_for_selector("input.css-1dbyz17.css-cct1ew.dyn", timeout=5000)
                password_input.fill("Aa123456!")
                self.common.random_sleep(1, 2)  # 模拟人工输入间隔
                
                # 点击登录按钮
                submit_button = self.page.wait_for_selector("button.css-r7neow.css-wp7z9d.dyn.beer-login-btn", timeout=5000)
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
        """抓取博主信息"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return None
            logger.info('开始抓取 蒲公英数据')

            # 使用 SQLAlchemy session 查询
            urls = session.query(KolMediaAccount).filter(
                KolMediaAccount.is_enable.in_([1, 2]),
                ~KolMediaAccount.id.in_([6519])
            ).all()

            session.query(KolMediaAccount).update({"is_update": 1})
            session.commit()
            logger.info(f"找到 {len(urls)} 个博主数据")
            if len(urls) <= 0:
                return
            for url in urls:
                try:
                    # 清空之前的数据
                    self.api_data.clear()
                    self.current_user_data.clear()  # 清空上一个用户的数据

                    logger.info(f"正在处理博主: {url.nickname}")
                    logger.info(f"访问链接: {url.dandelion_platform_link}")

                    # 访问页面
                    self.page.goto(url.dandelion_platform_link)

                    # 等待页面加载完成
                    self.page.wait_for_load_state('networkidle')
                    self.common.random_sleep(3, 4)  # 等待API请求完成

                    # 处理捕获到的API数据
                    if self.api_data:
                        # 创建api_data的副本进行遍历
                        api_data_copy = dict(self.api_data)
                        
                        # 1. 首先处理blogger API获取基本信息
                        for api_url, response_data in api_data_copy.items():
                            if 'blogger' in api_url and 'data' in response_data:
                                api_data = response_data['data']
                                if api_data['code'] == 0:
                                    data = api_data['data']
                                    notesign = ''

                                    # 获取当前博主信息
                                    info = session.query(KolMediaAccount).filter(
                                        KolMediaAccount.dandelion_platform_id == url.dandelion_platform_id
                                    ).first()

                                    # 处理签约信息
                                    if url.sign_type != '自孵化':  # sign_type 需要从其他地方获取
                                        if data.get('noteSign'):
                                            notesign = data['noteSign']['name']
                                            if url.is_enable == 1 and '方片' not in notesign:
                                                is_enable = 0

                                                # 记录签约历史
                                                self.record_history(info.id, is_enable)

                                                # 获取飞书 token
                                                feishu_token = get_feishu_token()

                                                # 获取签约信息
                                                qy = session.query(KolBusinessMediaAccount).filter(
                                                    KolBusinessMediaAccount.media_account_id == info.id
                                                ).first()
                                                qy1 = session.query(AdminUser).filter(
                                                    AdminUser.id == qy.lead_director_id
                                                ).first()
                                                qy2 = session.query(AdminUserGroup).filter(
                                                    AdminUserGroup.id == qy1.group
                                                ).first()

                                                # 获取垂类赛道信息
                                                pendant_query = text("""
                                                    SELECT GROUP_CONCAT(CONCAT(p2.name, '-->', p.name) SEPARATOR ',') as name
                                                    FROM tibao_2.pendant_account_joins as j
                                                    LEFT JOIN tibao_2.pendants as p ON p.id = j.pendant_id
                                                    LEFT JOIN tibao_2.pendants as p2 ON p2.id = p.p_id
                                                    WHERE j.account_id = :account_id
                                                """)
                                                pendant_result = session.execute(pendant_query,
                                                                                 {'account_id': info.id}).first()
                                                pendant = pendant_result.name if pendant_result and pendant_result.name else ''

                                                title = qy2.title if qy2 else ''

                                                # 构建飞书消息数据
                                                feishu_data = {
                                                    'fields': {
                                                        '博主id': int(info.id),
                                                        '账号名称': str(info.nickname),
                                                        '垂类赛道': str(pendant),
                                                        '粉丝数量': int(info.followers),
                                                        '图文报备': int(info.graphic_price),
                                                        '视频报备': int(info.video_price),
                                                        '30天报备条数': int(info.pgy_orders_30_days),
                                                        '90天报备条数': int(info.pgy_orders_90_days),
                                                        '主页链接': str(info.xhs_url),
                                                        '签约经纪人': str(qy1.name) if qy1 else '',
                                                        '组别': str(title),
                                                        '所属机构': str(info.notesign)
                                                    }
                                                }

                                                # 发送飞书消息
                                                try:
                                                    response = requests.post(
                                                        'https://open.feishu.cn/open-apis/bitable/v1/apps/G6uVbL6rRa8MltsSlvbcFaW0nig/tables/tbleF8mABc5B8HBF/records',
                                                        headers={
                                                            'Content-Type': 'application/json; charset=utf-8',
                                                            'Authorization': f'Bearer {feishu_token}'
                                                        },
                                                        json=feishu_data,
                                                        verify=False
                                                    )
                                                    response_data = response.json()
                                                    logger.info(f"飞书消息发送结果: {response_data}")
                                                except Exception as e:
                                                    logger.error(f"发送飞书消息失败: {str(e)}")
                                        else:
                                            if url.is_enable == 1:
                                                is_enable = 0
                                                # 记录签约历史
                                                self.record_history(info.id, is_enable)

                                                # 获取飞书 token
                                                feishu_token = get_feishu_token()

                                                # 获取签约信息
                                                qy = session.query(KolBusinessMediaAccount).filter(
                                                    KolBusinessMediaAccount.media_account_id == info.id
                                                ).first()
                                                qy1 = session.query(AdminUser).filter(
                                                    AdminUser.id == qy.lead_director_id
                                                ).first()
                                                qy2 = session.query(AdminUserGroup).filter(
                                                    AdminUserGroup.id == qy1.group
                                                ).first()

                                                # 获取垂类赛道信息
                                                pendant_query = text("""
                                                    SELECT GROUP_CONCAT(CONCAT(p2.name, '-->', p.name) SEPARATOR ',') as name
                                                    FROM tibao_2.pendant_account_joins as j
                                                    LEFT JOIN tibao_2.pendants as p ON p.id = j.pendant_id
                                                    LEFT JOIN tibao_2.pendants as p2 ON p2.id = p.p_id
                                                    WHERE j.account_id = :account_id
                                                """)
                                                pendant_result = session.execute(pendant_query,
                                                                                 {'account_id': info.id}).first()
                                                pendant = pendant_result.name if pendant_result and pendant_result.name else ''

                                                title = qy2.title if qy2 else ''

                                                # 构建飞书消息数据
                                                feishu_data = {
                                                    'fields': {
                                                        '博主id': int(info.id),
                                                        '账号名称': str(info.nickname),
                                                        '垂类赛道': str(pendant),
                                                        '粉丝数量': int(info.followers),
                                                        '图文报备': int(info.graphic_price),
                                                        '视频报备': int(info.video_price),
                                                        '30天报备条数': int(info.pgy_orders_30_days),
                                                        '90天报备条数': int(info.pgy_orders_90_days),
                                                        '主页链接': str(info.xhs_url),
                                                        '签约经纪人': str(qy1.name) if qy1 else '',
                                                        '组别': str(title),
                                                        '所属机构': str(info.notesign)
                                                    }
                                                }

                                                # 发送飞书消息
                                                try:
                                                    response = requests.post(
                                                        'https://open.feishu.cn/open-apis/bitable/v1/apps/G6uVbL6rRa8MltsSlvbcFaW0nig/tables/tbleF8mABc5B8HBF/records',
                                                        headers={
                                                            'Content-Type': 'application/json; charset=utf-8',
                                                            'Authorization': f'Bearer {feishu_token}'
                                                        },
                                                        json=feishu_data,
                                                        verify=False
                                                    )
                                                    response_data = response.json()
                                                    logger.info(f"飞书消息发送结果: {response_data}")
                                                except Exception as e:
                                                    logger.error(f"发送飞书消息失败: {str(e)}")
                                    else:
                                        # 自孵化逻辑
                                        if data.get('noteSign'):
                                            notesign = data['noteSign']['name']
                                        elif data.get('liveSign'):
                                            notesign = data['liveSign']['name']

                                        if not notesign:
                                            view_id = "vew276azNv"
                                            app_token = "GvoobItcNaf47jsqyiHcg4GNnWd"
                                            table_id = "tblPoUb8Dp2gR9zP"

                                            # 获取飞书 token
                                            feishu_token = get_feishu_token()

                                            # 删除现有记录
                                            self.delete_feishu_records(app_token, table_id, feishu_token, view_id)

                                            # 添加新记录
                                            array_data = {
                                                "records": [{
                                                    "fields": {
                                                        "id": float(info.id),
                                                        "账号名称": str(info.nickname)
                                                    }
                                                }]
                                            }

                                            self.add_feishu_records(array_data, app_token, table_id)

                                    # 处理价格信息
                                    picture_price = 0
                                    video_price = 0
                                    if data.get('videoShowState'):
                                        video_price = data.get('videoPrice', 0)
                                    if data.get('pictureShowState'):
                                        picture_price = data.get('picturePrice', 0)

                                    # 更新用户数据
                                    self.current_user_data.update({
                                        'gender': data.get('gender', ''),
                                        'is_enable': url.is_enable,
                                        'nickname': data.get('name', ''),
                                        'notesign': notesign,
                                        'talent_id': data.get('redId', ''),
                                        'shipping_address': data.get('location', ''),
                                        'followers': data.get('fansCount', 0),
                                        'like_count': data.get('likeCollectCountInfo', ''),
                                        'graphic_price': picture_price,
                                        'video_price': video_price,
                                        'currentLevel': data.get('currentLevel', '')
                                    })
                        for api_url, response_data in api_data_copy.items():
                            try:
                                if 'data' not in response_data:
                                    continue

                                api_data = response_data['data']

                                # 根据不同的API进行不同的处理
                                if 'notes_rate' in api_url and not self.notes_rate_processed:
                                    try:
                                        # 使用trans-data-item类定位容器
                                        dropdown_container = self.page.locator('.trans-data-item')
                                        
                                        # 1. 获取日常笔记30天数据（默认页面状态）
                                        self.page.wait_for_load_state('networkidle')
                                        self.common.random_sleep(3, 4)

                                        # 2. 切换到合作笔记30天
                                        switch_button = dropdown_container.locator('button:has-text("合作笔记")').first
                                        switch_button.click()
                                        self.page.wait_for_load_state('networkidle')
                                        self.common.random_sleep(3, 4)

                                        # 2. 切换全流量开关两次，确保状态正确
                                        ad_switch = dropdown_container.page.wait_for_selector('text=全流量')
                                        ad_switch.click()
                                        natural_flow = dropdown_container.page.wait_for_selector('text=仅自然流量')
                                        natural_flow.click()
                                        self.common.random_sleep(2, 3)

                                        ad_switch = dropdown_container.page.wait_for_selector('text=仅自然流量')
                                        ad_switch.click()
                                        natural_flow = dropdown_container.page.wait_for_selector('text=全流量')
                                        natural_flow.click()
                                        self.common.random_sleep(2, 3)

                                        # 3. 切换到合作笔记90天
                                        dropdown = dropdown_container.locator('.d-select.--color-text-title.--color-bg-fill:has-text("近30日")')
                                        dropdown.click()
                                        self.common.random_sleep(2, 3)

                                        option_90d = dropdown_container.page.wait_for_selector('text=近90日',timeout=10000)
                                        option_90d.click()
                                        self.page.wait_for_load_state('networkidle')
                                        self.common.random_sleep(3, 4)

                                        # 4. 切换回日常笔记（保持90天）
                                        switch_button = dropdown_container.locator('button:has-text("日常笔记")').first
                                        switch_button.click()
                                        self.page.wait_for_load_state('networkidle')
                                        self.common.random_sleep(4, 5)

                                        # 处理收集到的API数据
                                        api_data_map = {
                                            "daily_30": None,  # 日常笔记30天
                                            "coop_30": None,  # 合作笔记30天
                                            "daily_90": None,  # 日常笔记90天
                                            "coop_90": None  # 合作笔记90天
                                        }

                                        # 分类API响应
                                        for rate_url, rate_data in self.api_data.items():
                                            if 'notes_rate' not in rate_url:
                                                continue

                                            params = dict(parse_qsl(urlparse(rate_url).query))
                                            business = params.get('business', '')
                                            date_type = params.get('dateType', '')
                                            advertise_switch = params.get('advertiseSwitch', '')

                                            # 全流量数据
                                            if advertise_switch == '1':
                                                if business == '0' and date_type == '1':
                                                    api_data_map["daily_30"] = rate_data['data']
                                                elif business == '1' and date_type == '1':
                                                    api_data_map["coop_30"] = rate_data['data']
                                                elif business == '0' and date_type == '2':
                                                    api_data_map["daily_90"] = rate_data['data']
                                                elif business == '1' and date_type == '2':
                                                    api_data_map["coop_90"] = rate_data['data']
                                            # 自然流量数据
                                            elif advertise_switch == '0' and business == '1' and date_type == '1':
                                                data = rate_data['data']['data']
                                                # 更新自然流量相关数据
                                                self.current_user_data.update({
                                                    'read_median': int(data.get('readMedian', 0)),
                                                    'interaction_median': int(data.get('mEngagementNum', 0)),
                                                    'coop_pic_text_exposure_median': int(data.get('impMedian', 0))
                                                })
                                        # 处理数据并更新current_user_data
                                        if api_data_map["daily_30"]:
                                            data = api_data_map["daily_30"]['data']
                                            self.current_user_data.update({
                                                'daily_pic_video_exposure_median': int(data.get('impMedian', 0)),
                                                'daily_pic_video_reading_median': int(data.get('readMedian', 0)),
                                                'daily_pic_video_interaction_median': int(data.get('mEngagementNum', 0)),
                                                'daily_pic_video_interaction_rate': self.parse_percentage(data.get('interactionRate')),
                                                'daily_pic_video_likes_note_ratio': self.parse_percentage(data.get('hundredLikePercent')),
                                                'daily_pic_video_hundred_likes_note_ratio': self.parse_percentage(data.get('thousandLikePercent')),
                                                'daily_pic_video_completion_rate': self.parse_percentage(data.get('videoFullViewRate')),
                                                'daily_pic_video_three_sec_reading_rate': self.parse_percentage(data.get('picture3sViewRate')),
                                            })
                                            self._calculate_metrics(data, self.current_user_data.get('video_price', 0),
                                                                  self.current_user_data.get('graphic_price', 0),
                                                                  'daily_pic_video')

                                        if api_data_map["coop_30"]:
                                            data = api_data_map["coop_30"]['data']
                                            self.current_user_data.update({
                                                'cooperation_pic_video_exposure_median': int(data.get('impMedian', 0)),
                                                'cooperation_pic_video_reading_median': int(data.get('readMedian', 0)),
                                                'cooperation_pic_video_interaction_median': int(data.get('mEngagementNum', 0)),
                                                'cooperation_pic_video_interaction_rate': self.parse_percentage(data.get('interactionRate')),
                                                'cooperation_pic_video_likes_note_ratio': self.parse_percentage(data.get('hundredLikePercent')),
                                                'cooperation_pic_video_hundred_likes_note_ratio': self.parse_percentage(data.get('thousandLikePercent')),
                                                'cooperation_pic_video_completion_rate': self.parse_percentage(data.get('videoFullViewRate')),
                                                'cooperation_pic_video_three_sec_reading_rate': self.parse_percentage(data.get('picture3sViewRate')),
                                            })
                                            self._calculate_metrics(data, self.current_user_data.get('video_price', 0),
                                                                  self.current_user_data.get('graphic_price', 0),
                                                                  'cooperation_pic_video')

                                        if api_data_map["daily_90"]:
                                            data = api_data_map["daily_90"]['data']
                                            self.current_user_data.update({
                                                'daily_video_exposure_median': int(data.get('impMedian', 0)),
                                                'daily_video_reading_median': int(data.get('readMedian', 0)),
                                                'daily_video_interaction_median': int(data.get('mEngagementNum', 0)),
                                                'daily_video_interaction_rate': self.parse_percentage(data.get('interactionRate')),
                                                'daily_video_hundred_likes_note_ratio': self.parse_percentage(data.get('hundredLikePercent')),
                                                'daily_video_thousand_likes_note_ratio': self.parse_percentage(data.get('thousandLikePercent')),
                                                'daily_video_completion_rate': self.parse_percentage(data.get('videoFullViewRate')),
                                                'daily_video_three_sec_reading_rate': self.parse_percentage(data.get('picture3sViewRate')),
                                                'daily_video_notenumber': int(data.get('noteNumber', 0)),
                                            })
                                            self._calculate_metrics(data, self.current_user_data.get('video_price', 0),
                                                                  self.current_user_data.get('graphic_price', 0),
                                                                  'daily_90')

                                        if api_data_map["coop_90"]:
                                            data = api_data_map["coop_90"]['data']
                                            self.current_user_data.update({
                                                'daily_pic_text_exposure_median': int(data.get('impMedian', 0)),
                                                'daily_pic_text_reading_median': int(data.get('readMedian', 0)),
                                                'daily_pic_text_interaction_median': int(data.get('mEngagementNum', 0)),
                                                'daily_pic_text_interaction_rate': self.parse_percentage(data.get('interactionRate')),
                                                'daily_pic_text_likes_note_ratio': self.parse_percentage(data.get('hundredLikePercent')),
                                                'daily_pic_text_hundred_likes_note_ratio': self.parse_percentage(data.get('thousandLikePercent')),
                                                'daily_pic_text_completion_rate': self.parse_percentage(data.get('videoFullViewRate')),
                                                'daily_pic_text_three_sec_reading_rate': self.parse_percentage(data.get('picture3sViewRate')),
                                                'daily_pic_text_notenumber': int(data.get('noteNumber', 0)),
                                            })
                                            self._calculate_metrics(data, self.current_user_data.get('video_price', 0),
                                                                  self.current_user_data.get('graphic_price', 0),
                                                                  'daily_pic_text')

                                        self.notes_rate_processed = True  # 设置标记为已处理

                                    except Exception as e:
                                        logger.error(f"处理笔记数据时出错: {str(e)}")
                                        logger.error(traceback.format_exc())

                                elif 'fans_profile' in api_url:
                                    self._process_fans_profile(api_data)

                                elif 'data_summary' in api_url:
                                    self._process_data_summary(api_data)

                                elif 'fans_overall_new_history' in api_url:
                                    self._process_fans_history(api_data)

                                elif 'fans_summary' in api_url:
                                    self._process_fans_summary(api_data)

                                elif 'notes_detail' in api_url:
                                    self._process_notes_detail(api_data, url)

                                elif 'cost_effective' in api_url:
                                    self._process_cost_effective(api_data)

                            except Exception as api_error:
                                logger.error(f"处理API {api_url} 数据时出错: {str(api_error)}")
                                continue
                        logger.info('数据库更新数据')
                        logger.info(self.current_user_data)
                        self.notes_rate_processed = False
                        # 所有 API 数据处理完毕后，统一更新数据库
                        if self.current_user_data:
                            try:
                                # 添加额外的字段
                                self.current_user_data.update({
                                    'last_update_time': datetime.now(),
                                    'is_update': 2,
                                    'xhs_url': f"https://www.xiaohongshu.com/user/profile/{url.dandelion_platform_id}"
                                })

                                # 获取当前数据库中的记录
                                media = session.query(KolMediaAccount).filter(
                                    KolMediaAccount.id == url.id
                                ).first()

                                # 检查昵称是否变更
                                if media and 'nickname' in self.current_user_data and self.current_user_data['nickname'] != media.nickname:
                                    # 插入昵称变更记录
                                    session.execute(
                                        text("""
                                            INSERT INTO tibao_2.kol_media_account_history
                                            (media_account_id, old_nickname, new_nickname, created_at, updated_at)
                                            VALUES (:media_account_id, :old_nickname, :new_nickname, :created_at, :updated_at)
                                        """),
                                        {
                                            'media_account_id': media.id,
                                            'old_nickname': media.nickname,
                                            'new_nickname': self.current_user_data['nickname'],
                                            'created_at': datetime.now(),
                                            'updated_at': datetime.now()
                                        }
                                    )

                                # 更新数据
                                session.query(KolMediaAccount).filter(
                                    KolMediaAccount.id == url.id
                                ).update(self.current_user_data)
                                session.commit()
                                logger.info(f"成功更新博主 {url.nickname} 的所有数据")

                            except Exception as db_error:
                                logger.error(f"更新数据库时出错: {str(db_error)}")
                                session.rollback()
                        else:
                            logger.info(f"未捕获到博主 {url.nickname} 的API请求")

                    self.common.random_sleep(5, 6)  # 防止请求过快

                except Exception as e:
                    logger.error(f"处理博主 {url.nickname} 数据时出错: {str(e)}")
                    continue

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
        """处理API响应，只捕获指定的API请求"""
        try:
            url = response.url
            # 定义需要捕获的API路径
            target_apis = [
                'api/solar/kol/data_v3/notes_rate',
                'api/solar/kol/data/fans_profile',
                'api/solar/kol/data_v3/data_summary',
                'api/solar/cooperator/user/blogger/',
                'api/solar/kol/data/fans_overall_new_history',
                'api/solar/kol/data_v3/fans_summary',
                'api/solar/kol/data_v2/notes_detail',
                'api/solar/kol/data_v2/cost_effective'
            ]

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                try:
                    data = response.json()

                    # 存储API数据
                    self.api_data[url] = {
                        'url': url,
                        'data': data,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

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

    def delete_feishu_records(self, app_token, table_id, token, view_id):
        """删除飞书表格中的记录"""
        try:
            response = requests.get(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records',
                headers={'Authorization': f'Bearer {token}'},
                params={'view_id': view_id},
                verify=False
            )
            records = response.json().get('data', {}).get('items', [])

            for record in records:
                record_id = record.get('record_id')
                if record_id:
                    requests.delete(
                        f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}',
                        headers={'Authorization': f'Bearer {token}'},
                        verify=False
                    )
        except Exception as e:
            logger.error(f"删除飞书记录失败: {str(e)}")

    def add_feishu_records(self, data, app_token, table_id):
        """添加飞书表格记录"""
        try:
            feishu_token = get_feishu_token

            response = requests.post(
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create',
                headers={
                    'Content-Type': 'application/json; charset=utf-8',
                    'Authorization': f'Bearer {feishu_token}'
                },
                json=data,
                verify=False
            )
            logger.info(f"添加飞书记录结果: {response.json()}")
        except Exception as e:
            logger.error(f"添加飞书记录失败: {str(e)}")

    def record_history(self, creator_id, status=None):
        """
        记录博主签约历史
        :param creator_id: 博主ID
        :param status: 状态，如果为None则使用博主当前状态
        :return: 创建的历史记录
        """
        try:
            # 获取博主账号信息
            account = session.query(KolMediaAccount).filter(
                KolMediaAccount.id == creator_id
            ).first()

            # 如果未提供状态，使用博主当前状态
            if status is None:
                status = account.is_enable

            # 获取签约人信息
            operator = session.query(KolBusinessMediaAccount).filter(
                KolBusinessMediaAccount.media_account_id == creator_id
            ).first()

            # 设置操作人ID
            operator_id = operator.id if operator else None

            # 获取签约人组别
            pm_group = None
            if operator_id:
                admin_user = session.query(AdminUser).filter(
                    AdminUser.id == operator_id
                ).first()
                if admin_user:
                    pm_group = admin_user.pm_group

            # 创建历史记录
            new_history = BloggerSigningHistory(
                creator_id=creator_id,
                operator_id=operator_id,
                action_time=datetime.now(),
                status=status,
                group=pm_group
            )

            # 添加并提交到数据库
            session.add(new_history)
            session.commit()
            logger.info(f"成功记录签约历史 - 博主ID: {creator_id}, 状态: {status}")
            return new_history

        except Exception as e:
            logger.error(f"记录签约历史失败: {str(e)}")
            session.rollback()
            return None

    def _calculate_metrics(self, data, video_price, graphic_price, prefix):
        """计算CPE、CPM、CPC、CPR指标"""
        try:
            metrics = {}
            engagement_num = int(data.get('mEngagementNum', 0))
            read_median = int(data.get('readMedian', 0))
            imp_median = int(data.get('impMedian', 0))

            if prefix == 'daily_pic_video':
                # 视频指标
                if engagement_num > 0:
                    metrics['daily_pic_video_video_cpe'] = format(video_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['daily_pic_video_video_cpc'] = format(video_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['daily_pic_video_video_cpm'] = format((video_price / imp_median) * 1000, '.2f')
                        metrics['daily_pic_video_video_cpr'] = format(read_median / imp_median, '.2f')
                # 图文指标
                if engagement_num > 0:
                    metrics['daily_pic_video_text_cpe'] = format(graphic_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['daily_pic_video_text_cpc'] = format(graphic_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['daily_pic_video_text_cpm'] = format((graphic_price / imp_median) * 1000, '.2f')
                        metrics['daily_pic_video_text_cpr'] = format(read_median / imp_median, '.2f')

            elif prefix == 'cooperation_pic_video':
                # 视频指标
                if engagement_num > 0:
                    metrics['cooperation_pic_video_video_cpe'] = format(video_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['cooperation_pic_video_video_cpc'] = format(video_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['cooperation_pic_video_video_cpm'] = format((video_price / imp_median) * 1000, '.2f')
                        metrics['cooperation_pic_video_video_cpr'] = format(read_median / imp_median, '.2f')
                # 图文指标
                if engagement_num > 0:
                    metrics['cooperation_pic_video_text_cpe'] = format(graphic_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['cooperation_pic_video_text_cpc'] = format(graphic_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['cooperation_pic_video_text_cpm'] = format((graphic_price / imp_median) * 1000, '.2f')
                        metrics['cooperation_pic_video_text_cpr'] = format(read_median / imp_median, '.2f')

            elif prefix == 'daily_90':
                # 视频指标
                if engagement_num > 0:
                    metrics['daily_video_cpe'] = format(video_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['daily_video_cpc'] = format(video_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['daily_video_cpm'] = format((video_price / imp_median) * 1000, '.2f')
                        metrics['daily_video_cpr'] = format(read_median / imp_median, '.2f')
                # 图文指标
                if engagement_num > 0:
                    metrics['coop_video_cpe'] = format(graphic_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['coop_video_cpc'] = format(graphic_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['coop_video_cpm'] = format((graphic_price / imp_median) * 1000, '.2f')
                        metrics['coop_video_cpr'] = format(read_median / imp_median, '.2f')

            elif prefix == 'daily_pic_text':
                # 图文指标
                if engagement_num > 0:
                    metrics['daily_pic_text_cpe'] = format(video_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['daily_pic_text_cpc'] = format(video_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['daily_pic_text_cpm'] = format((video_price / imp_median) * 1000, '.2f')
                        metrics['daily_pic_text_cpr'] = format(read_median / imp_median, '.2f')
                # 视频指标
                if engagement_num > 0:
                    metrics['coop_pic_text_cpe'] = format(graphic_price / engagement_num, '.2f')
                if read_median > 0:
                    metrics['coop_pic_text_cpc'] = format(graphic_price / read_median, '.2f')
                    if imp_median > 0:
                        metrics['coop_pic_text_cpm'] = format((graphic_price / imp_median) * 1000, '.2f')
                        metrics['coop_pic_text_cpr'] = format(read_median / imp_median, '.2f')

            # 设置默认值为'0.00'
            for key in metrics:
                if not metrics[key]:
                    metrics[key] = '0.00'

            self.current_user_data.update(metrics)

        except Exception as e:
            logger.error(f"计算{prefix}指标时出错: {str(e)}")
            logger.error(f"错误的数据结构: {data}")

    def _process_notes_detail(self, api_data, blogger):
        """处理笔记详情数据"""
        if api_data['code'] != 0:
            return

        try:
            data = api_data['data']

            # 计算不同时间段的订单数量
            current_date = datetime.now()
            thirty_days_ago = current_date - timedelta(days=30)
            ninety_days_ago = current_date - timedelta(days=90)

            day30 = 0
            day90 = 0
            all_orders = 0

            # 处理品牌名称列表，过滤掉None值
            brand_names = []
            for item in data.get('list', []):
                brand_name = item.get('brandName')
                if brand_name:  # 只添加非空的品牌名称
                    brand_names.append(brand_name)

            # 统计蒲公英平台订单数据
            for item in data.get('list', []):
                try:
                    item_date = datetime.strptime(item['date'], '%Y-%m-%d')

                    if thirty_days_ago <= item_date <= current_date:
                        day30 += 1

                    if ninety_days_ago <= item_date <= current_date:
                        day90 += 1

                    all_orders += 1
                except (ValueError, KeyError) as e:
                    logger.warning(f"处理订单日期时出错: {str(e)}")
                    continue

            try:
                # 获取当前博主信息
                account = session.query(KolMediaAccount).filter(
                    KolMediaAccount.dandelion_platform_id == blogger.dandelion_platform_id
                ).first()

                if account:
                    # 使用原生SQL查询来避免模型字段不匹配的问题
                    zf_order30 = session.execute(text("""
                        SELECT COUNT(*) as count
                        FROM kol_orders
                        WHERE media_account_id = :account_id
                        AND stage NOT IN (11, 12)
                        AND created_at >= :thirty_days_ago
                    """), {
                        'account_id': account.id,
                        'thirty_days_ago': thirty_days_ago
                    }).scalar()

                    zf_order90 = session.execute(text("""
                        SELECT COUNT(*) as count
                        FROM kol_orders
                        WHERE media_account_id = :account_id
                        AND stage NOT IN (11, 12)
                        AND created_at >= :ninety_days_ago
                    """), {
                        'account_id': account.id,
                        'ninety_days_ago': ninety_days_ago
                    }).scalar()

                    zf_all = session.execute(text("""
                        SELECT COUNT(*) as count
                        FROM kol_orders
                        WHERE media_account_id = :account_id
                        AND stage NOT IN (11, 12)
                    """), {
                        'account_id': account.id
                    }).scalar()

                    # 更新用户数据
                    self.current_user_data.update({
                        'brand_name': ','.join(brand_names),  # 使用过滤后的品牌名称列表
                        'pgy_total_orders': all_orders,
                        'pgy_orders_30_days': day30,
                        'pgy_orders_90_days': day90,
                        'system_total_orders': zf_all or 0,
                        'system_orders_30_days': zf_order30 or 0,
                        'system_orders_90_days': zf_order90 or 0,
                    })
                else:
                    logger.error(f"未找到博主信息: {blogger.dandelion_platform_id}")
                    # 至少保存蒲公英平台的数据
                    self.current_user_data.update({
                        'brand_name': ','.join(brand_names),  # 使用过滤后的品牌名称列表
                        'pgy_total_orders': all_orders,
                        'pgy_orders_30_days': day30,
                        'pgy_orders_90_days': day90,
                    })

            except Exception as e:
                logger.error(f"处理系统订单数据时出错: {str(e)}")
                # 至少保存蒲公英平台的数据
                self.current_user_data.update({
                    'brand_name': ','.join(brand_names),  # 使用过滤后的品牌名称列表
                    'pgy_total_orders': all_orders,
                    'pgy_orders_30_days': day30,
                    'pgy_orders_90_days': day90,
                })

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")
            # 设置默认值
            self.current_user_data.update({
                'brand_name': '',
                'pgy_total_orders': 0,
                'pgy_orders_30_days': 0,
                'pgy_orders_90_days': 0,
                'system_total_orders': 0,
                'system_orders_30_days': 0,
                'system_orders_90_days': 0,
            })

    def _process_fans_profile(self, api_data):
        """处理粉丝画像数据"""
        if api_data['code'] == 0:
            data = api_data['data']
            try:
                # 处理年龄分布
                self.current_user_data.update({
                    'age_less_than_18': format(data['ages'][0]['percent'], '.2f'),  # 年龄<18
                    'age_18_to_24': format(data['ages'][1]['percent'], '.2f'),  # 年龄18_24
                    'age_25_to_34': format(data['ages'][2]['percent'], '.2f'),  # 年龄25_34
                    'age_35_to_44': format(data['ages'][3]['percent'], '.2f'),  # 年龄35_44
                    'age_greater_than_44': format(data['ages'][4]['percent'], '.2f'),  # 年龄>44
                })

                # 处理性别分布
                self.current_user_data.update({
                    'male_fan_percentage': format(data['gender']['male'], '.2f'),  # 男粉丝占比
                    'female_fan_percentage': format(data['gender']['female'], '.2f'),  # 女粉丝占比
                })

                # 处理用户兴趣 top5
                for i in range(5):
                    interest = data['interests'][i]
                    self.current_user_data[f'interest_top{i + 1}'] = (
                        f"{interest['name']}({format(interest['percent'] * 100, '.2f')}%)"
                    )

                # 处理省份 top3
                for i in range(3):
                    province = data['provinces'][i]
                    self.current_user_data[f'province_top{i + 1}'] = (
                        f"{province['name']}({format(province['percent'] * 100, '.2f')}%)"
                    )

                # 处理城市 top3
                for i in range(3):
                    city = data['cities'][i]
                    self.current_user_data[f'city_top{i + 1}'] = (
                        f"{city['name']}({format(city['percent'] * 100, '.2f')}%)"
                    )

                # 处理设备 top3
                for i in range(3):
                    device = data['devices'][i]
                    self.current_user_data[f'device_top{i + 1}'] = (
                        f"{device['name']}({format(device['percent'] * 100, '.2f')}%)"
                    )

            except Exception as e:
                logger.error(f"处理粉丝画像数据时出错: {str(e)}")

    def _process_data_summary(self, api_data):
        """处理数据摘要"""
        if api_data['code'] == 0:
            data = api_data['data']
            try:
                self.current_user_data.update({
                    'notes_published': data.get('noteNumber', 0),  # 发布笔记数量
                    'content_categories': ','.join([item.get('contentTag', '') for item in data.get('noteType', [])]),  # 内容类目及占比
                    'cooperated_industries': ','.join(data.get('tradeNames', [])),  # 合作行业
                })
            except Exception as e:
                logger.error(f"处理数据摘要时出错: {str(e)}")

    def _process_fans_history(self, api_data):
        """处理粉丝历史数据"""
        if api_data['code'] == 0:
            self.current_user_data.update({
                'followers_increase': api_data['data']['fansNumInc'],
                'followers_change_rate': float(f"{float(api_data['data']['fansNumIncRate']):.2f}")
            })

    def _process_fans_summary(self, api_data):
        """处理粉丝概要数据"""
        if api_data['code'] == 0:
            self.current_user_data.update({
                'active_followers_percentage': float(api_data['data']['activeFansRate']) / 100,
                'active_followers_benchmark_exceed': float(api_data['data']['activeFansBeyondRate']),
                'engaged_followers_percentage': float(api_data['data']['engageFansRate']),
                'engaged_followers_benchmark_exceed': float(api_data['data']['engageFansBeyondRate']),
                'reading_followers_percentage': float(api_data['data']['readFansRate']),
                'reading_followers_benchmark_exceed': float(api_data['data']['readFansBeyondRate']),
            })

    def _process_cost_effective(self, api_data):
        """处理成本效益数据"""
        try:
            if not api_data or api_data.get('data') is None:
                self.current_user_data.update({
                    'video_read_cost': 0,
                    'picture_read_cost': 0,
                })
            else:
                data = api_data['data']
                self.current_user_data.update({
                    'video_read_cost': data.get('videoReadCost', 0),
                    'picture_read_cost': data.get('pictureReadCost', 0),
                })
        except Exception as e:
            logger.error(f"处理成本效益数据时出错: {str(e)}")
            self.current_user_data.update({
                'video_read_cost': 0,
                'picture_read_cost': 0,
            })

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

