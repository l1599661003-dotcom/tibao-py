import time
import json
import os
from datetime import datetime
import signal
import sys
import configparser
import calendar
from decimal import Decimal

import schedule
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import random
from sqlalchemy.exc import SQLAlchemyError

from core.localhost_fp_project import session
from models.models import QgBloggerRank, QgBrandInfo, QgNoteInfo

"""
    获取千瓜MCN商业收入榜数据
"""


class QianguaMcnRankSpider:
    def __init__(self):
        self.setup_logger()
        self.base_url = "https://app.qian-gua.com"
        self.mcn_rank_url = "https://app.qian-gua.com/#/mcn/rank"
        self.is_logged_in = False
        self.api_data = {}
        self.current_organization = None
        self.cookie_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.json')
        self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mcn_rank_config.ini')
        self.load_config()
        self.setup_browser()

    """设置日志"""

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(os.path.join(log_path, "qiangua_mcn_rank_{time}.log"), rotation="1 day", retention="7 days")

    """加载配置文件"""

    def load_config(self):
        try:
            config = configparser.ConfigParser()
            config.read(self.config_file, encoding='utf-8')

            # 读取机构列表
            org_str = config.get('MCN', 'organizations')
            self.organizations = [org.strip() for org in org_str.split(',')]

            # 读取查询月份列表
            months_str = config.get('DATE', 'query_months')
            self.query_months = [month.strip() for month in months_str.split(',')]

            # 读取设置
            self.max_brand_records = config.getint('SETTINGS', 'max_brand_records')
            self.max_note_records = config.getint('SETTINGS', 'max_note_records')
            self.scroll_delay_min = config.getint('SETTINGS', 'scroll_delay_min')
            self.scroll_delay_max = config.getint('SETTINGS', 'scroll_delay_max')
            self.click_delay_min = config.getfloat('SETTINGS', 'click_delay_min', fallback=0.8)
            self.click_delay_max = config.getfloat('SETTINGS', 'click_delay_max', fallback=1.8)

            logger.info(f"配置加载成功: 机构数量={len(self.organizations)}, 查询月份={self.query_months}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def human_delay(self, min_sec=None, max_sec=None):
        """模拟人工延迟,避免频繁操作"""
        try:
            min_delay = self.click_delay_min if min_sec is None else min_sec
            max_delay = self.click_delay_max if max_sec is None else max_sec
            if max_delay < min_delay:
                min_delay, max_delay = max_delay, min_delay
            delay = random.uniform(min_delay, max_delay)
            logger.debug(f"模拟人工延时 {delay:.2f} 秒")
            time.sleep(delay)
        except Exception as e:
            logger.debug(f"模拟延时失败: {e}, 使用默认1秒")
            time.sleep(1)

    """初始化浏览器"""

    def setup_browser(self):
        self.playwright = sync_playwright().start()
        # 使用本地Chrome浏览器并指定用户数据目录
        # 这样可以使用你的Chrome配置,避免滑块验证
        user_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chrome_user_data')
        os.makedirs(user_data_dir, exist_ok=True)

        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",  # 使用Chrome而不是Chromium
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            no_viewport=True,  # 不设置固定viewport，允许窗口最大化
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            args=[
                '--disable-blink-features=AutomationControlled',  # 隐藏自动化特征
                '--no-sandbox',
                '--disable-web-security',
                '--start-maximized',  # 启动时最大化
            ]
        )
        self.browser = None  # 使用persistent context时不需要browser对象
        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self.page.set_default_timeout(20000)
        self.page.on("response", self._handle_api_response)

    """关闭所有弹出框"""

    def close_popups(self):
        try:
            # 使用JavaScript关闭所有可能的弹出框
            self.page.evaluate('''
                (() => {
                    const closeButtons = document.querySelectorAll('.el-icon-close, .lei-icon-close, [class*="close-btn"]');
                    closeButtons.forEach(btn => {
                        if (btn.offsetParent !== null) {
                            btn.click();
                        }
                    });
                })()
            ''')
            confirm_button = self.page.locator('button:has-text("确认")')

            if confirm_button.is_visible(timeout=1000):
                confirm_button.click()
                self.human_delay()
        except Exception as e:
            logger.error(f"关闭弹出框时出错: {str(e)}")

    """检查登录状态"""

    def check_login_status(self):
        try:
            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            # 打印页面内容用于调试
            html_content = self.page.content()
            if 'avatar' in html_content or 'user-container' in html_content:
                self.is_logged_in = True
                logger.info("通过页面内容检测到用户相关元素,已登录")
                return True

            self.is_logged_in = False
            logger.info("未检测到用户头像或登录状态")
            return False

        except Exception as e:
            self.is_logged_in = False
            logger.error(f"检查登录状态失败: {str(e)}")
            return False

    def login(self):
        """执行登录操作"""
        try:
            logger.info("开始登录...")
            self.page.click("text=登录/注册")
            self.human_delay(1.5, 2.5)

            self.page.click("text=手机登录")
            self.human_delay(1.5, 2.5)

            # 输入账号密码
            self.page.fill("input[placeholder='请输入手机号']", '13151572333')
            self.human_delay(1.0, 1.8)
            self.page.fill("input[placeholder='请输入登录密码']", '12345678abc')
            self.human_delay(1.0, 1.8)

            # 勾选协议
            self.page.click('.el-checkbox__inner')
            self.human_delay(0.8, 1.4)

            # 点击登录按钮
            self.page.click('button[class="el-button el-button--primary"][style="width: 200px;"]')
            self.human_delay(1.0, 2.0)

            # 等待滑块出现并提示用户
            logger.info("已点击登录按钮,等待滑块验证...")
            logger.info("请手动完成滑块验证并点击登录!")
            self.human_delay(1.5, 2.5)

            # 等待用户手动完成滑块验证和登录,最多等待60秒
            logger.info("等待用户手动完成滑块验证和登录(最多等待60秒)...")
            wait_time = 0
            max_wait_time = 60

            while wait_time < max_wait_time:
                try:
                    # 每隔2秒检查一次登录状态
                    time.sleep(2)
                    wait_time += 2

                    # 检查是否登录成功
                    if self.check_login_status():
                        logger.info(f"登录成功! (等待了 {wait_time} 秒)")
                        return True

                    # 每10秒提示一次
                    if wait_time % 10 == 0:
                        logger.info(f"仍在等待用户完成登录... (已等待 {wait_time}/{max_wait_time} 秒)")

                except Exception as e:
                    logger.debug(f"检查登录状态时出错: {str(e)}")
                    continue

            logger.error(f"等待超时({max_wait_time}秒),登录失败")
            return False
        except Exception as e:
            logger.error(f"登录过程出错: {str(e)}")
            return False

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if response.request.resource_type in ['fetch', 'xhr']:
                # 拦截三个接口
                api_name = None
                if 'GetMcnRankData' in url:
                    api_name = 'GetMcnRankData'
                elif 'GetMcnBrandList' in url:
                    api_name = 'GetMcnBrandList'
                elif 'GetMcnBrandNoteList' in url:
                    api_name = 'GetMcnBrandNoteList'

                if api_name:
                    try:
                        timestamp = str(int(time.time() * 1000))
                    except:
                        timestamp = str(int(time.time() * 1000))

                    if response.status == 200:
                        try:
                            response_data = response.json()
                            logger.info(f"收到{api_name}接口响应: timestamp={timestamp}")

                            # 保存API数据
                            if api_name not in self.api_data:
                                self.api_data[api_name] = []

                            self.api_data[api_name].append({
                                'url': url,
                                'data': response_data,
                                'timestamp': timestamp,
                                'processed': False
                            })

                            if api_name == 'GetMcnBrandList':
                                item_list = response_data.get('Data', {}).get('ItemList', [])
                                logger.info(f"GetMcnBrandList接口返回 {len(item_list)} 条品牌数据")
                            elif api_name == 'GetMcnBrandNoteList':
                                logger.info("GetMcnBrandNoteList接口数据:")
                                logger.info(json.dumps(response_data, ensure_ascii=False, indent=2))
                            elif api_name == 'GetMcnRankData':
                                item_list = response_data.get('Data', {}).get('ItemList', [])
                                logger.info(f"捕获 {len(item_list)} 条MCN排行数据")
                            else:
                                logger.info(f"{api_name} 接口数据")

                        except Exception as e:
                            logger.error(f"解析{api_name}接口响应数据时出错: {str(e)}")
                    else:
                        logger.warning(f"{api_name}接口请求状态码异常: {response.status}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def save_cookies(self):
        """保存cookies到文件"""
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w') as f:
                json.dump(cookies, f)
            logger.info("Cookies已保存到文件")
        except Exception as e:
            logger.error(f"保存cookies时出错: {str(e)}")

    def save_rank_data_to_db(self, org_name, min_timestamp=None):
        """处理MCN排行数据写入数据库"""
        try:
            rank_entries = self.api_data.get('GetMcnRankData', [])
            if not rank_entries:
                logger.warning(f"未捕获机构 {org_name} 的MCN排行数据,跳过入库")
                return False

            inserted = 0
            updated = 0

            processed_entries = []
            min_ts = 0
            if min_timestamp is not None:
                try:
                    min_ts = int(min_timestamp)
                except (TypeError, ValueError):
                    min_ts = 0

            for entry in rank_entries:
                if entry.get('processed'):
                    continue

                try:
                    entry_ts = int(entry.get('timestamp'))
                except (TypeError, ValueError):
                    entry_ts = 0

                if min_ts and entry_ts and entry_ts < min_ts:
                    continue

                response_data = entry.get('data') or {}
                item_list = (response_data.get('Data') or {}).get('ItemList') or []
                if not item_list:
                    processed_entries.append(entry)
                    continue

                for item in item_list:

                    tags_text = item.get('BloggerTags')
                    if not tags_text:
                        tag_list = item.get('BloggerTagList') or []
                        tags_text = ','.join(
                            tag.get('Name') for tag in tag_list if tag.get('Name')
                        )

                    increase_value = item.get('IncreaseRankValue')
                    try:
                        increase_value_decimal = (
                            Decimal(str(increase_value)).quantize(Decimal('0.00'))
                            if increase_value is not None
                            else Decimal('0.00')
                        )
                    except Exception:
                        increase_value_decimal = Decimal('0.00')

                    payload = {
                        'nickname': item.get('NickName') or '',
                        'rank_number': item.get('RankNumber') or 0,
                        'change_number': item.get('ChangeNumber') or 0,
                        'rank_value': item.get('RankValue') or 0,
                        'rank_value_attach': item.get('RankValueAttach') or 0,
                        'increase_rank_value': increase_value_decimal,
                        'mcn_user_id': item.get('McnUserId'),
                        'small_avatar': item.get('SmallAvatar'),
                        'blogger_tags': tags_text,
                        'blogger_count': item.get('BloggerCount') or 0,
                        'note_count': item.get('NoteCount') or 0,
                        'like_collect': item.get('LikeCollect') or 0,
                        'fans_count': item.get('FansCount') or 0,
                        'brand_count': item.get('BrandCount') or 0,
                        'institute_name': item.get('InstituteName') or org_name,
                        'is_certification': 1 if item.get('IsCertification') else 0,
                        'current_user_is_favorite': 1 if item.get('CurrentUserIsFavorite') else 0,
                    }

                    record = session.query(QgBloggerRank).filter(QgBloggerRank.nickname == item.get('NickName')).first()

                    if record:
                        for field, value in payload.items():
                            setattr(record, field, value)
                        updated += 1
                    else:
                        session.add(QgBloggerRank(**payload))
                        inserted += 1

                processed_entries.append(entry)

            if inserted or updated:
                session.commit()
                logger.info(
                    f"机构 {org_name or '未知机构'} 笔记数据写入数据库完成: 新增 {inserted} 条, 更新 {updated} 条"
                )
            else:
                logger.info(f"机构 {org_name or '未知机构'} 笔记数据无更新,跳过提交")

            for entry in processed_entries:
                entry['processed'] = True

            self.api_data['GetMcnBrandNoteList'] = [entry for entry in note_entries if not entry.get('processed')]
            return inserted > 0 or updated > 0
        except SQLAlchemyError as db_err:
            session.rollback()
            logger.error(f"机构 {org_name} 排行数据入库失败: {db_err}")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"机构 {org_name} 排行数据处理异常: {str(e)}")
            return False


    def save_brand_data_to_db(self, org_name, mcn_index, year_month):
        """处理品牌列表数据写入数据库"""
        try:
            brand_entries = self.api_data.get('GetMcnBrandList', [])
            if not brand_entries:
                logger.warning(f"未捕获机构 {org_name} 的品牌数据,跳过入库")
                return False

            inserted = 0
            updated = 0
            processed_entries = []

            for entry in brand_entries:
                if entry.get('processed'):
                    continue

                response_data = entry.get('data') or {}
                item_list = (response_data.get('Data') or {}).get('ItemList') or []
                if not item_list:
                    processed_entries.append(entry)
                    continue

                brand_ids = [item.get('BrandId') for item in item_list if item.get('BrandId') is not None]
                existing_map = {}
                if brand_ids:
                    existing_records = (
                        session.query(QgBrandInfo)
                        .filter(QgBrandInfo.brand_id.in_(brand_ids))
                        .all()
                    )
                    existing_map = {record.brand_id: record for record in existing_records}

                for item in item_list:
                    brand_id = item.get('BrandId')
                    if brand_id is None:
                        continue

                    amount_value = item.get('Amount')
                    try:
                        amount_value = int(amount_value) if amount_value is not None else 0
                    except (TypeError, ValueError):
                        amount_value = 0

                    payload = {
                        'blogger_id': mcn_index,
                        'month': year_month,
                        'brand_id': brand_id,
                        'brand_id_key': item.get('BrandIdKey'),
                        'brand_name': item.get('BrandName') or item.get('BrandNickName') or '未知品牌',
                        'brand_logo': item.get('BrandLogo'),
                        'brand_intro': item.get('BrandIntro'),
                        'note_count': item.get('NoteCount') or 0,
                        'active_count': item.get('ActiveCount') or 0,
                        'amount_desc': item.get('AmountDesc'),
                        'amount': amount_value,
                    }

                    record = existing_map.get(brand_id)
                    if record:
                        for field, value in payload.items():
                            setattr(record, field, value)
                        updated += 1
                    else:
                        session.add(QgBrandInfo(**payload))
                        inserted += 1

                processed_entries.append(entry)

            if inserted or updated:
                session.commit()
                logger.info(
                    f"机构 {org_name} 品牌数据写入数据库完成: 新增 {inserted} 条, 更新 {updated} 条"
                )
            else:
                logger.info(f"机构 {org_name} 品牌数据无更新,跳过提交")

            for entry in processed_entries:
                entry['processed'] = True

            self.api_data['GetMcnBrandList'] = [entry for entry in brand_entries if not entry.get('processed')]
            return inserted > 0 or updated > 0
        except SQLAlchemyError as db_err:
            session.rollback()
            logger.error(f"机构 {org_name} 品牌数据入库失败: {db_err}")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"机构 {org_name} 品牌数据处理异常: {str(e)}")
            return False


    def save_note_data_to_db(self, org_name=None):
        """处理笔记列表数据写入数据库"""
        try:
            org_display = org_name or self.current_organization or '未知机构'

            note_entries = self.api_data.get('GetMcnBrandNoteList', [])
            if not note_entries:
                wait_start = time.time()
                while time.time() - wait_start < 3:
                    note_entries = self.api_data.get('GetMcnBrandNoteList', [])
                    if note_entries:
                        break
                    time.sleep(0.2)

            if not note_entries:
                logger.warning(f"未捕获机构 {org_display} 的笔记数据,跳过入库")
                return False

            inserted = 0
            updated = 0
            processed_entries = []

            def parse_datetime(value):
                if not value:
                    return None
                if isinstance(value, str):
                    value = value.replace('Z', '+00:00')
                    try:
                        return datetime.fromisoformat(value)
                    except ValueError:
                        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
                            try:
                                return datetime.strptime(value, fmt)
                            except ValueError:
                                continue
                return None

            def to_int(value):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return 0

            for entry in note_entries:
                if entry.get('processed'):
                    continue

                response_data = entry.get('data') or {}
                item_list = (response_data.get('Data') or {}).get('ItemList') or []
                if not item_list:
                    processed_entries.append(entry)
                    continue

                note_ids = [item.get('NoteId') for item in item_list if item.get('NoteId') is not None]
                existing_map = {}
                if note_ids:
                    existing_records = (
                        session.query(QgNoteInfo)
                        .filter(QgNoteInfo.note_id.in_(note_ids))
                        .all()
                    )
                    existing_map = {record.note_id: record for record in existing_records}

                for item in item_list:
                    note_id = item.get('NoteId')
                    if note_id is None:
                        continue

                    amount_value = to_int(item.get('Amount'))
                    publish_time = parse_datetime(item.get('PublishTime'))
                    update_time_raw = parse_datetime(item.get('UpdateTime'))

                    pub_date = None
                    pub_date_value = item.get('PubDate')
                    if pub_date_value:
                        try:
                            pub_date = datetime.strptime(pub_date_value, '%Y-%m-%d').date()
                        except ValueError:
                            pub_date = None

                    payload = {
                        'note_id': note_id,
                        'date_code': item.get('DateCode'),
                        'note_id_key': item.get('NoteIdKey'),
                        'unique_id': item.get('Id'),
                        'user_id': item.get('UserId'),
                        'title': item.get('Title'),
                        'cover_image': item.get('CoverImage'),
                        'blogger_id': item.get('BloggerId'),
                        'blogger_id_key': item.get('BloggerIdKey'),
                        'blogger_nickname': item.get('BloggerNickName'),
                        'blogger_prop': item.get('BloggerProp'),
                        'publish_time': publish_time,
                        'note_type': item.get('NoteType'),
                        'is_business': 1 if item.get('IsBusiness') else 0,
                        'note_type_desc': item.get('NoteTypeDesc'),
                        'props': to_int(item.get('Props')),
                        'pub_date': pub_date,
                        'update_time_raw': update_time_raw,
                        'video_duration': item.get('VideoDuration'),
                        'gender': to_int(item.get('Gender')) if item.get('Gender') is not None else None,
                        'big_avatar': item.get('BigAvatar'),
                        'small_avatar': item.get('SmallAvatar'),
                        'tag_name': item.get('TagName'),
                        'cooperate_binds_name': item.get('CooperateBindsName'),
                        'view_count': to_int(item.get('ViewCount')),
                        'active_count': to_int(item.get('ActiveCount')),
                        'amount': amount_value,
                        'ad_price_desc': item.get('AdPriceDesc'),
                        'ad_price_update_status': to_int(item.get('AdPriceUpdateStatus')),
                        'is_ad_note': 1 if item.get('IsAdNote') else 0,
                    }

                    record = existing_map.get(note_id)
                    if record:
                        for field, value in payload.items():
                            setattr(record, field, value)
                        updated += 1
                    else:
                        session.add(QgNoteInfo(**payload))
                        inserted += 1

                processed_entries.append(entry)

            if inserted or updated:
                session.commit()
                logger.info(
                    f"机构 {org_display} 笔记数据写入数据库完成: 新增 {inserted} 条, 更新 {updated} 条"
                )
            else:
                logger.info(f"机构 {org_display} 笔记数据无更新,跳过提交")

            for entry in processed_entries:
                entry['processed'] = True

            self.api_data['GetMcnBrandNoteList'] = [entry for entry in note_entries if not entry.get('processed')]
            return inserted > 0 or updated > 0
        except SQLAlchemyError as db_err:
            session.rollback()
            logger.error(f"机构 {org_display} 笔记数据入库失败: {db_err}")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"机构 {org_display} 笔记数据处理异常: {str(e)}")
            return False

    def load_cookies(self):
        """从文件加载cookies"""
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r') as f:
                    cookies = json.load(f)
                self.context.add_cookies(cookies)
                logger.info("已从文件加载cookies")
                return True
            return False
        except Exception as e:
            logger.error(f"加载cookies时出错: {str(e)}")
            return False

    def check_and_handle_login(self):
        """检查并处理登录状态"""
        try:
            # 首先访问首页
            self.page.goto(self.base_url)
            time.sleep(2)

            # 关闭弹出框
            self.close_popups()

            # 尝试加载已保存的cookies
            if self.load_cookies():
                # 重新访问首页使cookies生效
                self.page.goto(self.base_url)
                time.sleep(2)

                # 再次关闭可能的弹出框
                self.close_popups()

                # 检查登录状态
                if self.check_login_status():
                    logger.info("使用已保存的cookies登录成功")
                    return True
                else:
                    logger.info("已保存的cookies已过期,需要重新登录")
                    # 清除旧的cookies文件
                    if os.path.exists(self.cookie_file):
                        os.remove(self.cookie_file)
                        logger.info("已删除过期的cookies文件")

            # 如果没有cookies或cookies已过期,执行登录操作
            if self.login():
                # 登录成功后保存cookies
                self.save_cookies()
                return True

            return False
        except Exception as e:
            logger.error(f"检查并处理登录状态时出错: {str(e)}")
            return False

    def click_business_income_tab(self):
        """点击商业收入榜"""
        try:
            logger.info("点击商业收入榜...")
            clicked = self.page.evaluate('''
                () => {
                    const elements = Array.from(document.querySelectorAll('span'));
                    for (const element of elements) {
                        if (element.textContent.trim() === '商业收入榜') {
                            element.click();
                            return true;
                        }
                    }
                    return false;
                }
            ''')

            if clicked:
                logger.info("成功点击商业收入榜")
                self.human_delay(1.5, 2.5)
                self.page.wait_for_load_state('networkidle', timeout=10000)
                return True
            else:
                logger.error("未找到商业收入榜按钮")
                return False
        except Exception as e:
            logger.error(f"点击商业收入榜时出错: {str(e)}")
            return False

    def search_organization(self, org_name):
        """在搜索框中搜索机构"""
        try:
            logger.info(f"搜索机构: {org_name}")

            if 'GetMcnRankData' in self.api_data:
                self.api_data['GetMcnRankData'] = []

            search_input = self.page.locator('.search-box.mr16 .el-autocomplete.s-input .el-input.el-input--medium.el-input-group.el-input-group--append.el-input--suffix input')

            search_input.fill('')
            self.human_delay(0.6, 1.2)

            search_input.fill(org_name)
            self.human_delay(1.0, 1.8)

            search_start_ts = int(time.time() * 1000)
            search_input.press('Enter')
            self.human_delay(1.5, 2.5)

            new_data_received = False
            try:
                self.page.wait_for_event(
                    'response',
                    timeout=10000,
                    predicate=lambda response: (
                        'GetMcnRankData' in response.url
                        and response.request.resource_type in ('xhr', 'fetch')
                    )
                )
                new_data_received = True
            except PlaywrightTimeoutError:
                logger.warning(f"搜索机构 {org_name} 后未捕获新的GetMcnRankData响应")

            self.page.wait_for_load_state('networkidle', timeout=10000)
            self.human_delay(1.0, 1.8)

            logger.info(f"搜索机构 {org_name} 完成")

            if not new_data_received:
                wait_start = time.time()
                while time.time() - wait_start < 3:
                    if self.api_data.get('GetMcnRankData'):
                        break
                    time.sleep(0.2)

            self.save_rank_data_to_db(org_name, min_timestamp=search_start_ts)
            return True
        except Exception as e:
            logger.error(f"搜索机构 {org_name} 时出错: {str(e)}")
            return False

    def click_mcn_item(self, index):
        """点击列表中的第index个机构"""
        try:
            logger.info(f"点击第 {index + 1} 个机构...")

            # 使用JavaScript点击机构
            clicked = self.page.evaluate(f'''
                () => {{
                    const listItems = document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom');
                    if (listItems.length > {index}) {{
                        const item = listItems[{index}];
                        const userContainer = item.querySelector('.list-row .col-item.undefined .col-cell .user-container.fan-user.inst-user.c-mcn-user');
                        if (userContainer) {{
                            userContainer.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            ''')

            if clicked:
                logger.info(f"成功点击第 {index + 1} 个机构")
                self.human_delay(1.5, 2.5)
                return True
            else:
                logger.warning(f"未找到第 {index + 1} 个机构")
                return False
        except Exception as e:
            logger.error(f"点击第 {index + 1} 个机构时出错: {str(e)}")
            return False

    def click_cooperation_brand(self):
        """点击合作品牌标签"""
        try:
            logger.info("点击合作品牌...")

            # 清空之前的API数据
            if 'GetMcnBrandList' in self.api_data:
                self.api_data['GetMcnBrandList'] = []

            # 使用正确的选择器点击合作品牌tab
            clicked = self.page.evaluate('''
                () => {
                    // 查找合作品牌tab: div.el-tabs__nav-wrap.is-top -> .el-tabs__nav-scroll -> [role="tablist"] -> #tab-brand
                    const brandTab = document.querySelector('.el-tabs__nav-wrap.is-top .el-tabs__nav-scroll [role="tablist"] #tab-brand');
                    if (brandTab) {
                        brandTab.click();
                        return true;
                    }
                    return false;
                }
            ''')

            if clicked:
                logger.info("成功点击合作品牌")
                self.human_delay(1.5, 2.5)
                self.page.wait_for_load_state('networkidle', timeout=10000)
                return True
            else:
                logger.error("未找到合作品牌按钮")
                return False
        except Exception as e:
            logger.error(f"点击合作品牌时出错: {str(e)}")
            return False

    def select_date_range_for_month(self, year_month):
        """为指定月份选择日期范围 (从该月第一天到最后一天,如果是当前月则到今天)"""
        try:
            import re
            year, month = map(int, year_month.split('-'))
            target_text = f"{year} 年 {month} 月"
            logger.info(f"选择日期范围: {target_text}")

            # 在弹出框内查找并点击日期输入框(按照简化DOM路径)
            result = self.page.evaluate('''
                () => {
                    const dialog = document.querySelector('.el-dialog__body');
                    if (!dialog) {
                        return {success: false, message: '未找到.el-dialog__body'};
                    }

                    const mcnDetailWrapper = dialog.querySelector('.mcn-detail-wrapper');
                    if (!mcnDetailWrapper) {
                        return {success: false, message: '未找到.mcn-detail-wrapper'};
                    }

                    const tabsContent = mcnDetailWrapper.querySelector('.el-tabs__content');
                    if (!tabsContent) {
                        return {success: false, message: '未找到.el-tabs__content'};
                    }

                    const panesBrand = tabsContent.querySelector('#pane-brand');
                    if (!panesBrand) {
                        return {success: false, message: '未找到#pane-brand'};
                    }

                    const imgPermissionWrapper = panesBrand.querySelector('.img-permission-wrapper');
                    if (!imgPermissionWrapper) {
                        return {success: false, message: '未找到.img-permission-wrapper'};
                    }

                    const brandWrap = imgPermissionWrapper.querySelector('.brand-wrap');
                    if (!brandWrap) {
                        return {success: false, message: '未找到.brand-wrap'};
                    }

                    const datePickerWrapper = brandWrap.querySelector('.date-picker.range-picker-wrapper');
                    if (!datePickerWrapper) {
                        return {success: false, message: '未找到.date-picker.range-picker-wrapper'};
                    }

                    const eventWidthContainer = datePickerWrapper.querySelector('.event-width-container.width-monitoring-wrap');
                    if (!eventWidthContainer) {
                        return {success: false, message: '未找到.event-width-container.width-monitoring-wrap'};
                    }

                    // 获取第三个div
                    const divs = eventWidthContainer.querySelectorAll(':scope > div');
                    if (divs.length < 3) {
                        return {success: false, message: 'event-width-container下的div数量不足3个,只有' + divs.length + '个'};
                    }

                    const thirdDiv = divs[2]; // 索引为2是第三个
                    
                    // 获取日期选择器的位置信息
                    const dateEditor = thirdDiv.querySelector('.el-date-editor--daterange');
                    if (!dateEditor) {
                        return {success: false, message: '未找到日期选择器'};
                    }
                    
                    const rect = dateEditor.getBoundingClientRect();
                    return {
                        success: true, 
                        message: '找到日期选择器',
                        x: rect.left + rect.width / 2,
                        y: rect.top + rect.height / 2,
                        width: rect.width,
                        height: rect.height
                    };
                }
            ''')

            logger.info(f"日期选择器位置: {result['message']}")

            if not result['success']:
                logger.error(f"未找到日期选择器: {result['message']}")
                return False

            # 使用坐标点击日期选择器的中心位置
            click_x = result['x']
            click_y = result['y']
            
            logger.info(f"准备点击坐标: x={click_x}, y={click_y}")
            
            # 使用page.mouse.click点击指定坐标
            self.page.mouse.click(click_x, click_y)
            logger.info(f"已点击日期选择器坐标")
            self.human_delay(1.0, 2.0)
            logger.info("成功打开日期选择器")

            # 检查左右两个面板是否包含目标月份,如果没有则切换
            max_attempts = 24
            attempt = 0

            while attempt < max_attempts:
                # 获取左右两个面板的月份
                panel_info = self.page.evaluate('''
                    () => {
                        const leftPanel = document.querySelector('.el-picker-panel__content.el-date-range-picker__content.is-left');
                        const rightPanel = document.querySelector('.el-picker-panel__content.el-date-range-picker__content.is-right');

                        let leftMonth = null;
                        let rightMonth = null;

                        if (leftPanel) {
                            const leftHeader = leftPanel.querySelector('.el-date-range-picker__header div');
                            if (leftHeader) {
                                leftMonth = leftHeader.textContent.trim();
                            }
                        }

                        if (rightPanel) {
                            const rightHeader = rightPanel.querySelector('.el-date-range-picker__header div');
                            if (rightHeader) {
                                rightMonth = rightHeader.textContent.trim();
                            }
                        }

                        return { leftMonth, rightMonth };
                    }
                ''')

                logger.info(f"当前显示: 左={panel_info['leftMonth']}, 右={panel_info['rightMonth']}")

                # 检查目标月份是否在左右面板中
                if panel_info['leftMonth'] == target_text or panel_info['rightMonth'] == target_text:
                    logger.info(f"找到目标月份: {target_text}")
                    break

                # 判断需要向左还是向右切换
                left_match = re.findall(r'\d+', panel_info['leftMonth']) if panel_info['leftMonth'] else []

                if len(left_match) >= 2:
                    current_year = int(left_match[0])
                    current_month = int(left_match[1])

                    if (current_year < year) or (current_year == year and current_month < month):
                        # 需要向右切换(未来的月份)
                        logger.info("点击右侧箭头切换到下一个月")
                        self.page.click('.el-picker-panel__content.el-date-range-picker__content.is-right .el-date-range-picker__header .el-picker-panel__icon-btn.el-icon-arrow-right')
                    else:
                        # 需要向左切换(过去的月份)
                        logger.info("点击左侧箭头切换到上一个月")
                        self.page.click('.el-picker-panel__content.el-date-range-picker__content.is-left .el-date-range-picker__header .el-picker-panel__icon-btn.el-icon-arrow-left')

                    self.human_delay(0.8, 1.4)
                    attempt += 1
                else:
                    logger.error("无法解析当前月份")
                    return False

            if attempt >= max_attempts:
                logger.error(f"切换月份超时,未找到 {target_text}")
                return False

            # 确定目标月份在左侧还是右侧面板
            panel_side = self.page.evaluate(f'''
                () => {{
                    const leftPanel = document.querySelector('.el-picker-panel__content.el-date-range-picker__content.is-left');
                    const rightPanel = document.querySelector('.el-picker-panel__content.el-date-range-picker__content.is-right');

                    const leftHeader = leftPanel ? leftPanel.querySelector('.el-date-range-picker__header div') : null;
                    const rightHeader = rightPanel ? rightPanel.querySelector('.el-date-range-picker__header div') : null;

                    if (leftHeader && leftHeader.textContent.trim() === '{target_text}') {{
                        return 'left';
                    }} else if (rightHeader && rightHeader.textContent.trim() === '{target_text}') {{
                        return 'right';
                    }}
                    return null;
                }}
            ''')

            if not panel_side:
                logger.error("无法确定目标月份所在的面板")
                return False

            logger.info(f"目标月份在{panel_side}侧面板")

            # 在对应的面板中选择日期
            selector_prefix = f'.el-picker-panel__content.el-date-range-picker__content.is-{panel_side}'

            # 获取该月第一天和最后一天
            date_info = self.page.evaluate(f'''
                () => {{
                    const panel = document.querySelector('{selector_prefix}');
                    if (!panel) return null;

                    const table = panel.querySelector('.el-date-table');
                    if (!table) return null;

                    const rows = table.querySelectorAll('.el-date-table__row');
                    let firstDay = null;
                    let lastDay = null;

                    // 查找第一个available的日期
                    for (const row of rows) {{
                        const cells = row.querySelectorAll('td.available:not(.prev-month):not(.next-month)');
                        if (cells.length > 0 && !firstDay) {{
                            firstDay = cells[0];
                            break;
                        }}
                    }}

                    // 从后往前查找最后一个available的日期
                    for (let i = rows.length - 1; i >= 0; i--) {{
                        const cells = rows[i].querySelectorAll('td.available:not(.prev-month):not(.next-month)');
                        if (cells.length > 0) {{
                            lastDay = cells[cells.length - 1];
                            break;
                        }}
                    }}

                    return {{
                        hasFirst: !!firstDay,
                        hasLast: !!lastDay,
                        firstText: firstDay ? firstDay.textContent.trim() : null,
                        lastText: lastDay ? lastDay.textContent.trim() : null
                    }};
                }}
            ''')

            if not date_info or not date_info['hasFirst'] or not date_info['hasLast']:
                logger.error("未找到有效的日期范围")
                return False

            logger.info(f"找到日期范围: {date_info['firstText']} 到 {date_info['lastText']}")

            # 点击第一天
            clicked_first = self.page.evaluate(f'''
                () => {{
                    const panel = document.querySelector('{selector_prefix}');
                    if (!panel) return false;

                    const table = panel.querySelector('.el-date-table');
                    if (!table) return false;

                    const rows = table.querySelectorAll('.el-date-table__row');
                    for (const row of rows) {{
                        const cells = row.querySelectorAll('td.available:not(.prev-month):not(.next-month)');
                        if (cells.length > 0) {{
                            cells[0].click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            ''')

            if not clicked_first:
                logger.error("点击第一天失败")
                return False

            logger.info("成功点击第一天")
            self.human_delay(0.8, 1.4)

            # 点击最后一天
            clicked_last = self.page.evaluate(f'''
                () => {{
                    const panel = document.querySelector('{selector_prefix}');
                    if (!panel) return false;

                    const table = panel.querySelector('.el-date-table');
                    if (!table) return false;

                    const rows = table.querySelectorAll('.el-date-table__row');
                    for (let i = rows.length - 1; i >= 0; i--) {{
                        const cells = rows[i].querySelectorAll('td.available:not(.prev-month):not(.next-month)');
                        if (cells.length > 0) {{
                            cells[cells.length - 1].click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            ''')

            if not clicked_last:
                logger.error("点击最后一天失败")
                return False

            logger.info("成功点击最后一天")
            self.human_delay(1.0, 2.0)

            logger.info(f"{year}年{month}月日期范围选择完成")
            return True
        except Exception as e:
            logger.error(f"选择日期范围时出错: {str(e)}")
            return False

    def click_sort_by_cost(self):
        """点击预估合作费用排序"""
        try:
            logger.info("点击预估合作费用排序...")

            # 查找所有包含"预估合作费用"文本的sort-title元素并点击
            clicked = self.page.evaluate('''
                () => {
                    // 先尝试在弹出框内查找
                    const dialog = document.querySelector('.el-dialog__body');
                    if (dialog) {
                        const sortTitles = dialog.querySelectorAll('span.sort-title');
                        console.log('在弹出框内找到 ' + sortTitles.length + ' 个 sort-title 元素');

                        for (let i = 0; i < sortTitles.length; i++) {
                            const title = sortTitles[i];
                            const text = title.textContent.trim();
                            console.log('弹出框 sort-title[' + i + ']: "' + text + '"');

                            if (text === '预估合作费用' || text.includes('预估合作费用')) {
                                const clickTarget = title.closest('.self-head-sort-v2') || title.closest('.allow-sort') || title;
                                clickTarget.click();
                                console.log('成功点击弹出框内的预估合作费用排序');
                                return true;
                            }
                        }
                    }

                    // 如果弹出框内没找到,尝试全局查找
                    const allSortTitles = document.querySelectorAll('span.sort-title');
                    console.log('全局找到 ' + allSortTitles.length + ' 个 sort-title 元素');

                    for (let i = 0; i < allSortTitles.length; i++) {
                        const title = allSortTitles[i];
                        const text = title.textContent.trim();
                        console.log('全局 sort-title[' + i + ']: "' + text + '"');

                        if (text === '预估合作费用' || text.includes('预估合作费用')) {
                            const clickTarget = title.closest('.self-head-sort-v2') || title.closest('.allow-sort') || title;
                            clickTarget.click();
                            console.log('成功点击预估合作费用排序');
                            return true;
                        }
                    }

                    console.log('未找到包含"预估合作费用"的 sort-title');
                    return false;
                }
            ''')

            if clicked:
                logger.info("成功点击预估合作费用排序")
                self.human_delay(1.0, 2.0)
                
                # 等待GetMcnBrandList接口响应
                logger.info("等待GetMcnBrandList接口响应...")
                wait_start = time.time()
                max_wait = 10  # 最多等待10秒

                while time.time() - wait_start < max_wait:
                    if 'GetMcnBrandList' in self.api_data and len(self.api_data['GetMcnBrandList']) > 0:
                        # 检查最新的接口是否成功
                        latest_data = self.api_data['GetMcnBrandList'][-1]
                        response_data = latest_data.get('data', {})
                        if response_data.get('Code') == 0:
                            logger.info(f"GetMcnBrandList接口响应成功,已获取数据")
                            break
                        elif response_data.get('Code') == 500:
                            logger.warning(f"GetMcnBrandList接口返回错误: {response_data.get('Msg')}")
                    time.sleep(0.5)
                
                logger.info(f"当前已获取 {len(self.api_data.get('GetMcnBrandList', []))} 条GetMcnBrandList数据")
                return True
            else:
                logger.error("未找到预估合作费用排序按钮")
                return False
        except Exception as e:
            logger.error(f"点击预估合作费用排序时出错: {str(e)}")
            return False

    def scroll_to_load_brands(self, max_records):
        """滚动加载品牌数据"""
        try:
            logger.info(f"开始滚动加载品牌数据,最多加载 {max_records} 条...")

            # 首先滚动el-tabs__content到底部
            logger.info("先滚动el-tabs__content到底部...")
            self.page.evaluate('''
                () => {
                    const tabsContent = document.querySelector('.el-tabs__content');
                    if (tabsContent) {
                        tabsContent.scrollTop = tabsContent.scrollHeight;
                        console.log('已滚动el-tabs__content到底部');
                    }
                }
            ''')
            time.sleep(2)

            # 清空之前可能触发的GetMcnBrandList接口数据
            if 'GetMcnBrandList' in self.api_data:
                self.api_data['GetMcnBrandList'] = []
            
            # 在指定位置持续滚轮滚动，直到GetMcnBrandList接口出现
            logger.info("移动鼠标到品牌列表位置(931, 575)并滚动，等待接口响应...")
            self.page.mouse.move(931, 575)
            time.sleep(0.5)
            
            max_scroll_attempts = 20  # 最多滚动20次
            scroll_attempt = 0
            
            while scroll_attempt < max_scroll_attempts:
                # 向下滚动鼠标滚轮
                self.page.mouse.wheel(0, 300)  # 增大滚动量
                scroll_attempt += 1
                logger.info(f"第 {scroll_attempt} 次鼠标滚轮滚动...")
                time.sleep(1)
                
                # 检查是否有GetMcnBrandList接口响应
                if 'GetMcnBrandList' in self.api_data and len(self.api_data['GetMcnBrandList']) > 0:
                    logger.info(f"检测到GetMcnBrandList接口响应，停止鼠标滚轮滚动")
                    break
            
            if scroll_attempt >= max_scroll_attempts:
                logger.warning("鼠标滚轮滚动达到最大次数，但未检测到接口响应")

            # 然后使用鼠标滚轮滚动品牌列表
            logger.info("开始使用鼠标滚轮滚动品fex牌列表...")
            
            # 先获取初始已有的数据条数
            initial_count = self.page.evaluate('''
                () => {
                    return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                }
            ''')
            logger.info(f"初始已有 {initial_count} 条品牌数据")
            
            prev_count = initial_count
            no_more_data = False
            scroll_count = 0
            consecutive_no_change = 0  # 连续没有变化的次数

            while prev_count < max_records and not no_more_data:
                scroll_count += 1
                
                # 使用鼠标滚轮向下滚动
                self.page.mouse.wheel(0, 500)  # 增大滚动量
                
                # 随机等待
                delay = random.uniform(self.scroll_delay_min, self.scroll_delay_max)
                logger.info(f"第 {scroll_count} 次鼠标滚轮滚动,等待 {delay:.2f} 秒...")
                time.sleep(delay)

                # 检查是否有新数据加载
                new_count = self.page.evaluate('''
                    () => {
                        return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                    }
                ''')

                if new_count == prev_count:
                    consecutive_no_change += 1
                    logger.info(f"当前仍为 {new_count} 条数据，连续 {consecutive_no_change} 次无变化")
                    
                    # 连续3次没有变化才认为没有更多数据
                    if consecutive_no_change >= 3:
                        logger.info("连续3次滚动无新数据,停止滚动")
                        no_more_data = True
                else:
                    consecutive_no_change = 0  # 重置计数器
                    logger.info(f"当前已加载 {new_count} 条品牌数据 (初始 {initial_count} + 新增 {new_count - initial_count})")
                    prev_count = new_count
                    
                if prev_count >= max_records:
                    logger.info(f"已达到最大记录数 {max_records},停止加载")
                    break

            # 统计本次滚动总共获取的API数据
            total_api_count = len(self.api_data.get('GetMcnBrandList', []))
            logger.info(f"滚动完成,共滚动 {scroll_count} 次,获取 {total_api_count} 次GetMcnBrandList接口数据")
            total_api_count = len(self.api_data.get('GetMcnBrandList', []))
            logger.info(f"滚动完成,共滚动 {scroll_count} 次,获取 {total_api_count} 次GetMcnBrandList接口数据")
            
            final_count = self.page.evaluate('''
                () => {
                    return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                }
            ''')

            logger.info(f"品牌数据加载完成,共 {final_count} 条")
            return final_count
        except Exception as e:
            logger.error(f"滚动加载品牌数据时出错: {str(e)}")
            return 0

    def click_brand_item_and_view(self, index):
        """点击品牌列表中的某一项并查看详情"""
        try:
            logger.info(f"点击第 {index + 1} 个品牌并查看详情...")

            # 清空之前的API数据
            if 'GetMcnBrandNoteList' in self.api_data:
                self.api_data['GetMcnBrandNoteList'] = []

            # 点击查看链接 (list-row下第二个div里的col-cell里的div里的a标签)
            clicked = self.page.evaluate(f'''
                () => {{
                    const listItems = document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom');
                    if (listItems.length > {index}) {{
                        const item = listItems[{index}];
                        const listRow = item.querySelector('.list-row');
                        if (listRow) {{
                            // 获取list-row下的第二个div
                            const divs = listRow.querySelectorAll(':scope > div');
                            if (divs.length >= 2) {{
                                const secondDiv = divs[1];
                                const viewLink = secondDiv.querySelector(':scope > div > div > a');
                                if (viewLink) {{
                                    viewLink.click();
                                    console.log('����˲鿴����:', viewLink.textContent);
                                    return true;
                                }}
                            }}
                        }}
                    }}
                    return false;
                }}
            ''')

            if not clicked:
                logger.warning(f"未找到第 {index + 1} 个品牌的查看链接")
                return False

            logger.info(f"成功点击第 {index + 1} 个品牌的查看链接")
            self.human_delay(1.5, 2.5)

            # 点击投放报价
            logger.info("点击投放报价...")
            price_selector = (
                ".el-dialog__body "
                ".index-note-modal.relative-note-list__wrap "
                ".list-con.scroll-list.qg-scrollo-list.scroll-mode "
                ".affix-wrapper .c-affix .list-hd > div:nth-child(4) .sort-title"
            )
            price_button = self.page.locator(price_selector)
            try:
                price_button.wait_for(state="visible", timeout=5000)
                price_button.click()
                self.human_delay(1.0, 2.0)
            except Exception:
                logger.warning("未找到投放报价按钮")
                # 按ESC键关闭弹窗
                self.page.keyboard.press('Escape')
                self.human_delay(0.8, 1.4)
                return False

            logger.info("成功点击投放报价")
            self.human_delay(1.5, 2.5)
            self.page.wait_for_load_state('networkidle', timeout=10000)

            # 滚动一次加载更多数据
            logger.info("滚动加载投放报价数据...")
            note_list_selector = (
                ".el-dialog__body "
                ".index-note-modal.relative-note-list__wrap "
                ".list-con.scroll-list.qg-scrollo-list.scroll-mode "
                ".list-bd.page-component__scroll"
            )
            note_container = self.page.locator(note_list_selector)
            try:
                note_container.wait_for(state="visible", timeout=5000)
                bounding_box = note_container.bounding_box()
                if bounding_box:
                    x = bounding_box["x"] + bounding_box["width"] / 2
                    y = bounding_box["y"] + 10
                    self.page.mouse.move(x, y)
                    self.page.wait_for_timeout(500)
                    self.page.mouse.wheel(0, 800)
                else:
                    logger.warning("未获取到投放报价列表 bounding box，改用 evaluate 滚动")
                    self.page.evaluate('''
                        () => {
                            const container = document.querySelector(
                                '.index-note-modal.relative-note-list__wrap .list-con.scroll-list.qg-scrollo-list.scroll-mode .list-bd.page-component__scroll'
                            );
                            if (container) {
                                container.scrollTop = container.scrollHeight;
                            }
                        }
                    ''')
            except Exception as e:
                logger.warning(f"滚动投放报价数据时出错: {e}")
            self.human_delay(1.0, 2.0)

            # 按一次ESC键返回品牌列表
            logger.info("按ESC键返回品牌列表...")
            self.save_note_data_to_db(self.current_organization)
            self.page.keyboard.press('Escape')
            self.human_delay(0.8, 1.4)

            return True
        except Exception as e:
            logger.error(f"点击第 {index + 1} 个品牌并查看详情时出错: {str(e)}")
            # 尝试返回
            try:
                self.page.keyboard.press('Escape')
                time.sleep(1)
            except:
                pass
            return False

    def process_organization(self, org_name):
        """处理单个机构的数据"""
        try:
            self.current_organization = org_name
            logger.info(f"开始处理机构: {org_name}")

            # 搜索机构
            if not self.search_organization(org_name):
                logger.error(f"搜索机构 {org_name} 失败")
                self.current_organization = None
                return False

            # 获取机构列表数量(最多4条)
            mcn_count = self.page.evaluate('''
                () => {
                    return Math.min(document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length, 4);
                }
            ''')

            logger.info(f"找到 {mcn_count} 个机构,将处理前4个")

            # 循环处理每个机构
            for mcn_index in range(min(mcn_count, 4)):
                logger.info(f"处理第 {mcn_index + 1}/{mcn_count} 个机构")

                # 点击机构
                if not self.click_mcn_item(mcn_index):
                    logger.error(f"点击第 {mcn_index + 1} 个机构失败")
                    continue

                # 点击合作品牌
                if not self.click_cooperation_brand():
                    logger.error("点击合作品牌失败")
                    # 按ESC键返回
                    self.page.keyboard.press('Escape')
                    time.sleep(2)
                    continue

                # 循环处理每个月份
                for month_index, year_month in enumerate(self.query_months):
                    logger.info(f"处理第 {month_index + 1}/{len(self.query_months)} 个月份: {year_month}")

                    # 选择日期范围
                    if not self.select_date_range_for_month(year_month):
                        logger.error(f"选择日期范围失败: {year_month}")
                        continue

                    # 点击预估合作费用排序
                    if not self.click_sort_by_cost():
                        logger.error("点击预估合作费用排序失败")
                        continue

                    # 滚动加载品牌数据
                    brand_count = self.scroll_to_load_brands(self.max_brand_records)

                    if brand_count == 0:
                        logger.warning(f"{year_month} 没有加载到品牌数据")
                        continue

                    self.save_brand_data_to_db(org_name, mcn_index + 1, year_month)

                    # 循环点击每个品牌
                    actual_brand_count = min(brand_count, self.max_brand_records)
                    for brand_index in range(actual_brand_count):
                        logger.info(f"处理第 {brand_index + 1}/{actual_brand_count} 个品牌")
                        self.click_brand_item_and_view(brand_index)
                        time.sleep(2)

                    # 所有品牌处理完成后,按第二次ESC返回
                    logger.info(f"所有品牌处理完成,按第二次ESC返回...")
                    self.page.keyboard.press('Escape')
                    time.sleep(2)

                    logger.info(f"{year_month} 月份处理完成")

                # 处理完所有月份后,按ESC键返回机构搜索页面
                logger.info("处理完当前机构的所有月份,返回机构搜索页面...")
                self.page.keyboard.press('Escape')
                time.sleep(2)

            logger.info(f"机构 {org_name} 处理完成")
            self.current_organization = None
            return True
        except Exception as e:
            logger.error(f"处理机构 {org_name} 时出错: {str(e)}")
            self.current_organization = None
            return False

    def scrape_mcn_rank_data(self):
        """抓取MCN商业收入榜数据"""
        try:
            # 访问MCN排行榜页面
            logger.info("开始访问MCN排行榜页面...")
            self.page.goto(self.mcn_rank_url)

            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            time.sleep(3)

            # 关闭可能的弹出框
            self.close_popups()

            # 点击商业收入榜
            if not self.click_business_income_tab():
                logger.error("点击商业收入榜失败")
                return

            # 处理每个机构
            for org_name in self.organizations:
                logger.info(f"开始处理机构: {org_name}")
                self.process_organization(org_name)
                time.sleep(3)

            logger.info("所有机构处理完成")
        except Exception as e:
            logger.error(f"抓取MCN商业收入榜数据时出错: {str(e)}")

    def run(self):
        """运行爬虫"""
        try:
            logger.info("开始运行爬虫...")

            # 检查并处理登录
            if not self.check_and_handle_login():
                logger.error("登录失败,程序退出")
                return

            # 抓取数据
            self.scrape_mcn_rank_data()

        except Exception as e:
            logger.error(f"运行过程出错: {str(e)}")
        finally:
            self.close()

    def close(self):
        """关闭资源"""
        try:
            # 移除事件监听器
            if hasattr(self, 'page') and self.page:
                try:
                    self.page.remove_listener("response", self._handle_api_response)
                except:
                    pass

            # 使用persistent context时,直接关闭context即可
            if hasattr(self, 'context') and self.context:
                self.context.close()

            # persistent context不需要单独关闭browser
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()

            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()

            logger.info("所有资源已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")


if __name__ == '__main__':
    spider = QianguaMcnRankSpider()
    spider.run()
