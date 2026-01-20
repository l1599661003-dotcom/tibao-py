import time
import json
import os
from datetime import datetime
import configparser
from decimal import Decimal

from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import random
from sqlalchemy.exc import SQLAlchemyError

from core.localhost_fp_project import Session
from models.models import QgBloggerRank

"""
千瓜MCN商业收入榜数据爬虫
功能：从商业收入榜获取数据，逐条点击获取详情中的Amount和NoteCount，更新到数据库
"""


class QianguaBusinessIncomeSpider:
    def __init__(self):
        self.setup_logger()
        self.base_url = "https://app.qian-gua.com"
        self.mcn_rank_url = "https://app.qian-gua.com/#/mcn/rank"
        self.is_logged_in = False
        self.api_data = {}
        self.current_mcn_user_id = None
        self.click_timestamp = 0  # 记录点击时间戳
        self.cookie_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.json')
        self.config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'business_income_config.ini')
        self.load_config()
        self.setup_browser()

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(os.path.join(log_path, "business_income_{time}.log"), rotation="1 day", retention="7 days")

    def load_config(self):
        try:
            config = configparser.ConfigParser()
            config.read(self.config_file, encoding='utf-8')

            # 读取设置
            self.total_pages = config.getint('SETTINGS', 'total_pages', fallback=3)
            self.items_per_page = config.getint('SETTINGS', 'items_per_page', fallback=20)
            self.click_delay_min = config.getfloat('SETTINGS', 'click_delay_min', fallback=0.8)
            self.click_delay_max = config.getfloat('SETTINGS', 'click_delay_max', fallback=1.8)
            self.query_month = config.get('DATE', 'query_month', fallback='2025-12')

            logger.info(f"配置加载成功: 总页数={self.total_pages}, 每页条数={self.items_per_page}, 查询月份={self.query_month}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            # 设置默认值
            self.total_pages = 3
            self.items_per_page = 20
            self.click_delay_min = 0.8
            self.click_delay_max = 1.8
            self.query_month = '2025-12'

    def human_delay(self, min_sec=None, max_sec=None):
        """模拟人工延迟"""
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

    def setup_browser(self):
        self.playwright = sync_playwright().start()
        user_data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'xiaohongshu_notes', 'chrome_user_data')
        os.makedirs(user_data_dir, exist_ok=True)

        self.context = self.playwright.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            no_viewport=True,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-web-security',
                '--start-maximized',
            ]
        )
        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self.page.set_default_timeout(20000)
        self.page.on("response", self._handle_api_response)

    def _handle_api_response(self, response):
        """处理API响应，只捕获指定的API请求"""
        try:
            # 检查页面状态
            if not hasattr(self, 'page') or not self.page or self.page.is_closed():
                return

            url = response.url
            # 从配置获取需要捕获的API路径
            target_apis = ['GetMcnRankData', 'GetMcnNoteStat']

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            # 调试：打印所有请求以便排查
            resource_type = response.request.resource_type
            if is_target_api:
                logger.info(f"捕获到目标API: URL={url}, Type={resource_type}, Status={response.status}")

            # 检查是否是目标API（放宽resource_type限制）
            if is_target_api:
                try:
                    # 再次检查页面状态
                    if self.page.is_closed():
                        return

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

                    logger.info(f"成功捕获 {matched_api} 数据: {url}")

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def close_popups(self):
        try:
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
            logger.debug(f"关闭弹出框时出错: {str(e)}")

    def check_login_status(self):
        try:
            self.page.wait_for_load_state('networkidle', timeout=10000)
            html_content = self.page.content()
            if 'avatar' in html_content or 'user-container' in html_content:
                self.is_logged_in = True
                logger.info("通过页面内容检测到用户相关元素,已登录")
                return True
            self.is_logged_in = False
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

            self.page.fill("input[placeholder='请输入手机号']", '13151572333')
            self.human_delay(1.0, 1.8)
            self.page.fill("input[placeholder='请输入登录密码']", '12345678abc')
            self.human_delay(1.0, 1.8)

            self.page.click('.el-checkbox__inner')
            self.human_delay(0.8, 1.4)

            self.page.click('button[class="el-button el-button--primary"][style="width: 200px;"]')
            self.human_delay(1.0, 2.0)

            logger.info("已点击登录按钮,等待滑块验证...")
            logger.info("请手动完成滑块验证并点击登录!")
            self.human_delay(1.5, 2.5)

            logger.info("等待用户手动完成滑块验证和登录(最多等待60秒)...")
            wait_time = 0
            max_wait_time = 60

            while wait_time < max_wait_time:
                try:
                    time.sleep(2)
                    wait_time += 2
                    if self.check_login_status():
                        logger.info(f"登录成功! (等待了 {wait_time} 秒)")
                        return True
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

    def save_cookies(self):
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w') as f:
                json.dump(cookies, f)
            logger.info("Cookies已保存到文件")
        except Exception as e:
            logger.error(f"保存cookies时出错: {str(e)}")

    def load_cookies(self):
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
        try:
            self.page.goto(self.base_url)
            time.sleep(2)
            self.close_popups()

            if self.load_cookies():
                self.page.goto(self.base_url)
                time.sleep(2)
                self.close_popups()
                if self.check_login_status():
                    logger.info("使用已保存的cookies登录成功")
                    return True
                else:
                    logger.info("已保存的cookies已过期,需要重新登录")
                    if os.path.exists(self.cookie_file):
                        os.remove(self.cookie_file)
                        logger.info("已删除过期的cookies文件")

            if self.login():
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

    def click_month_rank_tab(self):
        """点击月榜"""
        try:
            logger.info("点击月榜")

            # 点击月榜前，清空旧的 GetMcnRankData 数据
            keys_to_delete = [k for k in self.api_data.keys() if 'GetMcnRankData' in k]
            for key in keys_to_delete:
                del self.api_data[key]
            logger.debug(f"已清空 {len(keys_to_delete)} 条旧的 GetMcnRankData 数据")

            clicked = self.page.evaluate('''
                () => {
                    const elements = Array.from(document.querySelectorAll('span, div, label'));
                    for (const element of elements) {
                        if (element.textContent.trim() === '月榜') {
                            const clickableParent = element.closest('.el-radio-button, .el-button, [role="button"], label');
                            if (clickableParent) {
                                clickableParent.click();
                                return true;
                            }
                            element.click();
                            return true;
                        }
                    }
                    return false;
                }
            ''')

            if clicked:
                # 记录点击时间，用于判断新数据
                self.click_timestamp = int(time.time() * 1000)

                # 使用 wait_for_event 同步等待 GetMcnRankData 响应
                logger.info("等待 GetMcnRankData 接口响应...")
                try:
                    self.page.wait_for_event(
                        'response',
                        timeout=30000,  # 最多等待30秒
                        predicate=lambda response: (
                            'GetMcnRankData' in response.url
                            and response.request.resource_type in ('xhr', 'fetch')
                        )
                    )
                    logger.info("成功接收到 GetMcnRankData 响应")
                except PlaywrightTimeoutError:
                    logger.warning("等待 GetMcnRankData 响应超时")

                return True
            else:
                logger.warning("未找到月榜按钮")
                return False
        except Exception as e:
            logger.error(f"点击月榜时出错: {str(e)}")
            return False

    def get_item_count(self):
        """获取当前页面的条目数量"""
        try:
            count = self.page.evaluate('''
                () => {
                    return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                }
            ''')
            return count
        except Exception as e:
            logger.error(f"获取条目数量时出错: {str(e)}")
            return 0

    def click_item_by_index(self, index):
        """点击第index个条目"""
        try:
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
                self.human_delay(3.0, 4.5)
                return True
            else:
                logger.warning(f"未找到第 {index + 1} 个条目")
                return False
        except Exception as e:
            logger.error(f"点击第 {index + 1} 个条目时出错: {str(e)}")
            return False

    def get_monthly_note_data(self):
        """
        获取按月数据 - 使用 wait_for_event 同步等待
        """
        try:
            # 点击商业合作tab
            logger.debug("正在点击商业合作tab...")
            clicked = self.page.evaluate('''
                () => {
                    const businessTab = document.querySelector('.el-tabs__nav-wrap.is-top .el-tabs__nav-scroll [role="tablist"] #tab-business');
                    if (businessTab) {
                        businessTab.click();
                        return true;
                    }
                    return false;
                }
            ''')

            if not clicked:
                logger.error("未找到商业合作按钮")
                return None, None

            logger.info("成功点击商业合作tab")
            self.human_delay(4.0, 6.0)

            # 记录点击按月按钮前的时间戳，用于过滤数据
            click_time = time.time()

            # 点击按月按钮
            logger.debug("正在点击按月按钮...")
            clicked = self.page.evaluate('''
                () => {
                    const monthButtons = document.querySelectorAll('.el-button.el-button--default.el-button--small.el-popover__reference');
                    for (const button of monthButtons) {
                        const text = button.textContent.trim();
                        if (text === '按月') {
                            button.click();
                            return true;
                        }
                    }
                    return false;
                }
            ''')

            if not clicked:
                logger.warning("未找到按月按钮")
                return None, None

            logger.info("已点击按月按钮，等待 GetMcnNoteStat 接口响应...")
            self.human_delay(3.5, 5.0)

            # 使用 wait_for_event 同步等待 GetMcnNoteStat 响应
            try:
                self.page.wait_for_event(
                    'response',
                    timeout=30000,
                    predicate=lambda response: (
                        'GetMcnNoteStat' in response.url
                        and response.request.resource_type in ('xhr', 'fetch')
                    )
                )
                logger.info("成功接收到 GetMcnNoteStat 响应")
            except PlaywrightTimeoutError:
                logger.warning("等待 GetMcnNoteStat 响应超时")

            # 从 api_data 中提取数据（只取点击时间之后的）
            amount = None
            note_count = None

            logger.debug("=== 开始检查 GetMcnNoteStat 数据 ===")
            for api_url, response_data in self.api_data.items():
                if 'GetMcnNoteStat' in api_url and isinstance(response_data, dict):
                    logger.debug(f"检查 URL: {api_url}")

                    # 只处理点击时间之后的数据（通过比较 _= 参数中的时间戳）
                    try:
                        # 从 URL 中提取时间戳
                        import re
                        url_timestamp = int(re.search(r'_=(\d+)', api_url).group(1)) / 1000
                        if url_timestamp <= click_time:
                            logger.debug(f"跳过点击前的数据: url_timestamp={url_timestamp}, click_time={click_time}")
                            continue
                    except Exception as e:
                        logger.debug(f"URL时间戳解析失败: {e}")
                        continue

                    response = response_data.get('data', {})
                    if response and isinstance(response, dict):
                        code = response.get('Code')
                        if code == 200:
                            data = response.get('Data', {})
                            if data:
                                # 提取 Amount 和 NoteCount
                                amount = data.get('Amount', 0)
                                note_count = data.get('NoteCount', 0)
                                logger.info(f"获取按月数据成功: Amount={amount}, NoteCount={note_count}")
                                self.human_delay(2.5, 4.0)
                                break
            logger.debug("=== 检查完成 ===")

            if amount is None:
                logger.warning("未获取到按月数据")

            # 关闭弹窗
            try:
                self.page.keyboard.press('Escape')
                self.human_delay(0.5, 1.0)
            except Exception as e:
                logger.debug(f"关闭弹窗时出错: {str(e)}")

            return amount, note_count

        except Exception as e:
            logger.error(f"获取按月数据时出错: {str(e)}")
            # 尝试关闭弹窗
            try:
                self.page.keyboard.press('Escape')
            except:
                pass
            return None, None
    def click_cooperation_tab(self):
        """点击商业合作tab - 已废弃，使用get_monthly_note_data代替"""
        return self.get_monthly_note_data()

    def click_month_view(self):
        """点击按月切换视图 - 已废弃，使用get_monthly_note_data代替"""
        pass

    def wait_for_note_stat_data(self):
        """等待GetMcnNoteStat接口响应 - 已废弃，使用get_monthly_note_data代替"""
        return None, None

    def close_dialog(self):
        """关闭弹窗 - 已废弃，使用get_monthly_note_data代替"""
        return None, None


    def update_database_with_note_stat(self, mcn_user_id, amount, note_count):
        """使用GetMcnNoteStat的数据更新数据库"""
        db_session = None
        try:
            db_session = Session()

            record = db_session.query(QgBloggerRank).filter(
                QgBloggerRank.mcn_user_id == mcn_user_id,
                QgBloggerRank.month == self.query_month
            ).first()

            if record:
                # 更新rank_value和note_count
                record.rank_value = amount if amount is not None else record.rank_value
                record.note_count = note_count if note_count is not None else record.note_count
                db_session.commit()
                logger.info(f"更新数据库成功: mcn_user_id={mcn_user_id}, rank_value={amount}, note_count={note_count}")
                return True
            else:
                logger.warning(f"未找到mcn_user_id={mcn_user_id}的记录")
                return False

        except SQLAlchemyError as db_err:
            if db_session:
                db_session.rollback()
            logger.error(f"数据库操作失败: {db_err}")
            return False
        except Exception as e:
            if db_session:
                db_session.rollback()
            logger.error(f"更新数据库时出错: {str(e)}")
            return False
        finally:
            if db_session:
                db_session.close()

    def save_rank_data_to_db(self):
        """将GetMcnRankData的数据保存到数据库"""
        db_session = None
        try:
            db_session = Session()

            # 遍历 api_data 找到 GetMcnRankData 的数据（存储在 URL key 中）
            rank_data = None
            for api_url, response_data in self.api_data.items():
                if 'GetMcnRankData' in api_url and isinstance(response_data, dict):
                    rank_data = response_data
                    break

            if not rank_data:
                logger.warning("未捕获GetMcnRankData数据")
                return False

            if rank_data.get('processed'):
                logger.debug("该数据已处理过，跳过")
                return True

            inserted = 0
            updated = 0

            response_data = rank_data.get('data') or {}
            item_list = (response_data.get('Data') or {}).get('ItemList') or []

            if not item_list:
                logger.warning("GetMcnRankData返回的ItemList为空")
                return False

            logger.info(f"开始保存排行数据到数据库，共 {len(item_list)} 条记录")

            for item in item_list:
                mcn_user_id = item.get('McnUserId')
                if not mcn_user_id:
                    logger.debug("跳过缺少mcn_user_id的记录")
                    continue

                nickname = item.get('NickName', 'Unknown')

                tags_text = item.get('BloggerTags')
                if not tags_text:
                    tag_list = item.get('BloggerTagList') or []
                    tags_text = ','.join(tag.get('Name') for tag in tag_list if tag.get('Name'))

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
                    'mcn_user_id': mcn_user_id,
                    'small_avatar': item.get('SmallAvatar'),
                    'blogger_tags': tags_text,
                    'blogger_count': item.get('BloggerCount') or 0,
                    'note_count': item.get('NoteCount') or 0,
                    'like_collect': item.get('LikeCollect') or 0,
                    'fans_count': item.get('FansCount') or 0,
                    'brand_count': item.get('BrandCount') or 0,
                    'institute_name': item.get('InstituteName') or '',
                    'is_certification': 1 if item.get('IsCertification') else 0,
                    'current_user_is_favorite': 1 if item.get('CurrentUserIsFavorite') else 0,
                    'month': self.query_month,
                }

                record = db_session.query(QgBloggerRank).filter(
                    QgBloggerRank.mcn_user_id == mcn_user_id,
                    QgBloggerRank.month == self.query_month
                ).first()

                if record:
                    for field, value in payload.items():
                        setattr(record, field, value)
                    updated += 1
                    logger.debug(f"更新记录: {nickname} (mcn_user_id={mcn_user_id})")
                else:
                    db_session.add(QgBloggerRank(**payload))
                    inserted += 1
                    logger.debug(f"新增记录: {nickname} (mcn_user_id={mcn_user_id})")

            if inserted or updated:
                db_session.commit()
                logger.info(f"排行数据写入数据库完成: 新增 {inserted} 条, 更新 {updated} 条")

            rank_data['processed'] = True
            return True

        except SQLAlchemyError as db_err:
            if db_session:
                db_session.rollback()
            logger.error(f"数据库操作失败: {db_err}")
            return False
        except Exception as e:
            if db_session:
                db_session.rollback()
            logger.error(f"数据处理异常: {str(e)}")
            return False
        finally:
            if db_session:
                db_session.close()

    def click_next_page(self):
        """点击下一页按钮"""
        try:
            clicked = self.page.evaluate('''
                () => {
                    const nextButton = document.querySelector('.btn-next');
                    if (nextButton && !nextButton.classList.contains('disabled')) {
                        nextButton.click();
                        return true;
                    }
                    return false;
                }
            ''')

            if clicked:
                # 记录点击时间，用于判断新数据
                self.click_timestamp = int(time.time() * 1000)

                # 添加人工延迟，等待页面响应
                self.human_delay(4.0, 6.0)

                # 清空 GetMcnRankData 相关的数据
                keys_to_delete = [k for k in self.api_data.keys() if 'GetMcnRankData' in k]
                for key in keys_to_delete:
                    del self.api_data[key]

                # 使用 wait_for_event 同步等待 GetMcnRankData 响应
                logger.info("等待下一页 GetMcnRankData 接口响应...")
                try:
                    self.page.wait_for_event(
                        'response',
                        timeout=30000,
                        predicate=lambda response: (
                            'GetMcnRankData' in response.url
                            and response.request.resource_type in ('xhr', 'fetch')
                        )
                    )
                    logger.info("成功接收到下一页 GetMcnRankData 响应")
                except PlaywrightTimeoutError:
                    logger.warning("等待下一页 GetMcnRankData 响应超时")

                return True
            else:
                logger.warning("下一页按钮不可用")
                return False
        except Exception as e:
            logger.error(f"点击下一页时出错: {str(e)}")
            return False

    def process_page_items(self):
        """处理当前页面的所有条目"""
        try:
            # 1. 先保存排行数据到数据库
            if not self.save_rank_data_to_db():
                logger.warning("保存排行数据失败，但继续处理条目")

            # 2. 获取页面条目数量
            item_count = self.get_item_count()
            if item_count == 0:
                logger.warning("当前页面没有数据")
                return False

            # 3. 从API数据中提取排行条目
            rank_data = None
            for api_url, response_data in self.api_data.items():
                if 'GetMcnRankData' in api_url and isinstance(response_data, dict):
                    rank_data = response_data
                    break

            if not rank_data:
                logger.warning("未获取到排行数据")
                return False

            response_data = rank_data.get('data') or {}
            item_list = (response_data.get('Data') or {}).get('ItemList') or []

            if not item_list:
                logger.warning("排行条目列表为空")
                return False

            logger.info(f"当前页面共有 {item_count} 个条目，API返回 {len(item_list)} 条数据")

            # 4. 处理每个条目
            success_count = 0
            skip_count = 0
            error_count = 0

            for index in range(min(item_count, len(item_list))):
                try:
                    logger.info(f"[{index + 1}/{item_count}] 开始处理第 {index + 1} 个条目")

                    item = item_list[index]
                    mcn_user_id = item.get('McnUserId')
                    nickname = item.get('NickName', 'Unknown')

                    if not mcn_user_id:
                        logger.warning(f"第 {index + 1} 个条目缺少 mcn_user_id，跳过")
                        skip_count += 1
                        continue

                    self.current_mcn_user_id = mcn_user_id
                    logger.debug(f"正在处理博主: {nickname} (mcn_user_id={mcn_user_id})")

                    # 点击条目
                    if not self.click_item_by_index(index):
                        logger.warning(f"点击第 {index + 1} 个条目失败，跳过")
                        skip_count += 1
                        continue

                    # 获取按月数据
                    amount, note_count = self.get_monthly_note_data()

                    if amount is not None or note_count is not None:
                        if self.update_database_with_note_stat(mcn_user_id, amount, note_count):
                            logger.info(f"博主 {nickname} 数据更新成功: Amount={amount}, NoteCount={note_count}")
                            success_count += 1
                        else:
                            logger.warning(f"博主 {nickname} 数据更新失败")
                    else:
                        logger.warning(f"博主 {nickname} 未获取到按月数据")

                    self.human_delay(3.0, 5.0)

                except Exception as e:
                    logger.error(f"处理第 {index + 1} 个条目时出错: {str(e)}")
                    error_count += 1
                    # 尝试关闭弹窗后继续
                    try:
                        self.page.keyboard.press('Escape')
                        self.human_delay(0.5, 1.0)
                    except:
                        pass
                    continue

            # 5. 输出处理结果统计
            logger.info(f"页面处理完成: 成功 {success_count}, 跳过 {skip_count}, 错误 {error_count}")
            return True

        except Exception as e:
            logger.error(f"处理页面条目时出错: {str(e)}")
            return False

    def run(self):
        """运行爬虫"""
        try:
            logger.info("========== 开始运行千瓜商业收入榜爬虫 ==========")

            if not self.check_and_handle_login():
                logger.error("登录失败,程序退出")
                return

            self.page.goto(self.mcn_rank_url)
            self.page.wait_for_load_state('networkidle', timeout=10000)
            time.sleep(3)
            self.close_popups()

            if not self.click_business_income_tab():
                logger.error("点击商业收入榜失败")
                return

            if not self.click_month_rank_tab():
                logger.info("点击月榜失败或已是月榜视图，继续执行")

            for page_num in range(1, self.total_pages + 1):
                logger.info(f"========== 第 {page_num}/{self.total_pages} 页 ==========")

                self.process_page_items()

                if page_num < self.total_pages:
                    if not self.click_next_page():
                        logger.warning("无法点击下一页")
                        break

            logger.info("========== 所有页面处理完成 ==========")

        except Exception as e:
            logger.error(f"运行过程出错: {str(e)}")
        finally:
            self.close()

    def close(self):
        """关闭资源"""
        try:
            if hasattr(self, 'page') and self.page:
                try:
                    self.page.remove_listener("response", self._handle_api_response)
                except:
                    pass

            if hasattr(self, 'context') and self.context:
                self.context.close()

            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()

            logger.info("所有资源已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")


if __name__ == '__main__':
    spider = QianguaBusinessIncomeSpider()
    spider.run()
