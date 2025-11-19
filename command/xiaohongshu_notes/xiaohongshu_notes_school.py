import json
import os
import time
import traceback
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright
import random

from core.localhost_fp_project import session
from models.models import PgyUser, PgyNoteDetail, PgyUserFans

"""
    抓取小红书用户笔记数据
"""


class XiaohongshuNotesSpider:
    def __init__(self):
        self.setup_logger()
        self.base_url = "https://www.xiaohongshu.com"
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.cookie_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.json')
        self.setup_browser()
        self.current_user = None
        self.posted_notes_data = []  # 存储web/v1/user_posted接口数据
        self.note_detail_data = []  # 存储api/sns/web/v1/feed接口数据

    def setup_logger(self):
        """设置日志"""
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(
            os.path.join(log_path, "xiaohongshu_notes_{time}.log"),
            rotation="1 day",
            retention="7 days"
        )

    def setup_browser(self):
        """初始化浏览器"""
        self.playwright = sync_playwright().start()

        # 使用本地Chrome浏览器
        user_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chrome_user_data')
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

        self.browser = None
        self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
        self.page.set_default_timeout(30000)

        # 设置响应监听
        self.page.on("response", self._handle_api_response)

    def human_delay(self, min_sec=0.8, max_sec=2.0):
        """模拟人工延迟"""
        try:
            delay = random.uniform(min_sec, max_sec)
            logger.debug(f"模拟人工延时 {delay:.2f} 秒")
            time.sleep(delay)
        except Exception as e:
            logger.debug(f"模拟延时失败: {e}, 使用默认1秒")
            time.sleep(1)

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url

            # 监听user_posted接口
            if 'web/v1/user_posted' in url:
                if response.status == 200:
                    try:
                        response_data = response.json()
                        logger.info(f"捕获到user_posted接口响应")

                        # 保存user_posted数据
                        self.api_data['user_posted'] = {
                            'url': url,
                            'data': response_data,
                            'timestamp': int(time.time() * 1000)
                        }

                        # 打印notes数量
                        notes = response_data.get('data', {}).get('notes', [])
                        logger.info(f"user_posted接口返回 {len(notes)} 条笔记")

                    except Exception as e:
                        logger.error(f"解析user_posted接口响应数据时出错: {str(e)}")

            # 监听api/sns/web/v1/feed接口
            if 'api/sns/web/v1/feed' in url:
                if response.status == 200:
                    try:
                        response_data = response.json()
                        logger.info(f"捕获到feed接口响应")

                        if 'feed' not in self.api_data:
                            self.api_data['feed'] = []

                        self.api_data['feed'].append({
                            'url': url,
                            'data': response_data,
                            'timestamp': int(time.time() * 1000)
                        })

                        # 打印data.items数据
                        data_items = response_data.get('data', {}).get('items', [])

                    except Exception as e:
                        logger.error(f"解析feed接口响应数据时出错: {str(e)}")

        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def check_login_status(self):
        """检查登录状态"""
        try:
            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=5000)

            # 检查页面中是否存在用户相关元素
            html_content = self.page.content()

            # 检查是否有用户侧边栏元素
            try:
                avatar = self.page.wait_for_selector(".channel-list >> .user", timeout=60000)
                # user_side = self.page.locator(".user.side-bar-component").all()
                if avatar:
                    self.is_logged_in = True
                    logger.info("检测到已登录状态")
                    return True
            except:
                pass

            # 或者检查是否有登录按钮
            try:
                login_button = self.page.locator("text=登录").all()
                if len(login_button) > 0:
                    self.is_logged_in = False
                    logger.info("检测到未登录状态")
                    return False
            except:
                pass

            self.is_logged_in = False
            logger.info("未检测到登录状态")
            return False

        except Exception as e:
            self.is_logged_in = False
            logger.error(f"检查登录状态失败: {str(e)}")
            return False

    def login(self):
        """执行登录操作"""
        try:
            logger.info("开始登录流程...")

            # 等待并点击登录按钮
            login_button = self.page.wait_for_selector("text=登录", timeout=10000)
            if not login_button:
                logger.error("未找到登录按钮")
                return False

            login_button.click()
            self.human_delay(2, 3)

            # 等待用户扫码登录
            logger.info("请扫码登录...")
            logger.info("等待用户完成登录(最多等待60秒)...")

            wait_time = 0
            max_wait_time = 60

            while wait_time < max_wait_time:
                time.sleep(2)
                wait_time += 2

                # 检查是否登录成功
                if self.check_login_status():
                    logger.info(f"登录成功! (等待了 {wait_time} 秒)")
                    logger.info("Cookies已自动保存到chrome_user_data目录（persistent context）")
                    return True

                # 每10秒提示一次
                if wait_time % 10 == 0:
                    logger.info(f"仍在等待用户完成登录... (已等待 {wait_time}/{max_wait_time} 秒)")

            logger.error(f"等待超时({max_wait_time}秒),登录失败")
            return False

        except Exception as e:
            logger.error(f"登录过程出错: {str(e)}")
            return False

    def save_cookies(self):
        """保存cookies"""
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f)
            logger.info("Cookies已保存到文件")
        except Exception as e:
            logger.error(f"保存cookies时出错: {str(e)}")

    def load_cookies(self):
        """加载cookies"""
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
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
            # 访问首页（persistent context会自动加载之前保存的cookies）
            logger.info("访问小红书首页...")
            self.page.goto(self.base_url)
            # 等待页面加载完成
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.warning(f"等待页面加载完成时出错: {str(e)}")

            # 检查登录状态
            if self.check_login_status():
                logger.info("检测到已登录状态（使用persistent context自动保存的cookies）")
                return True
            else:
                logger.info("未检测到登录状态，需要登录")

            # 如果未登录，执行登录操作
            if self.login():
                logger.info("登录成功，cookies已自动保存到chrome_user_data目录")
                return True

            return False

        except Exception as e:
            logger.error(f"检查并处理登录状态时出错: {str(e)}")
            return False

    def click_notes_tab(self):
        """点击笔记tab"""
        try:
            logger.info("点击笔记tab...")

            # 根据提供的选择器查找并点击笔记tab
            clicked = self.page.evaluate('''
                () => {
                    // 查找笔记tab
                    const stickyBox = document.querySelector('div.reds-sticky-box.user-page-sticky');
                    if (!stickyBox) return false;

                    const sticky = stickyBox.querySelector('div.reds-sticky');
                    if (!sticky) return false;

                    const tabsList = sticky.querySelector('div.tertiary.center.reds-tabs-list');
                    if (!tabsList) return false;

                    const subTabList = tabsList.querySelector('div.reds-tab-item.sub-tab-list');
                    if (!subTabList) return false;

                    // 查找包含"笔记"文本的span
                    const spans = subTabList.querySelectorAll('span');
                    for (const span of spans) {
                        if (span.textContent.trim() === '笔记') {
                            span.click();
                            return true;
                        }
                    }

                    return false;
                }
            ''')

            if clicked:
                logger.info("成功点击笔记tab")
                self.human_delay(2, 3)

                # 尝试等待网络空闲，但不抛出异常
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except:
                    logger.info("页面加载超时，但继续执行")

                return True
            else:
                logger.error("未找到笔记tab")
                return False

        except Exception as e:
            logger.error(f"点击笔记tab时出错: {str(e)}")
            return False

    def scroll_to_trigger_load(self):
        """向下滚动一点,触发新section加载"""
        try:
            logger.info("向下滚动触发新section加载...")

            # 滚动到页面底部
            self.page.evaluate('''
                () => {
                    window.scrollTo({
                        top: document.documentElement.scrollHeight,
                        behavior: 'smooth'
                    });
                }
            ''')

            # 等待页面更新
            self.human_delay(2, 3)
            logger.info("滚动完成，等待新section加载")

            return True

        except Exception as e:
            logger.error(f"滚动时出错: {str(e)}")
            return False

    def get_note_cards(self):
        """获取所有笔记卡片，返回包含唯一ID的列表"""
        try:
            # 使用JavaScript获取所有笔记卡片
            cards = self.page.evaluate('''
                () => {
                    // 直接通过ID查找userPostedFeeds容器
                    const userPostedFeeds = document.querySelector('div#userPostedFeeds');
                    if (!userPostedFeeds) {
                        console.log('未找到div#userPostedFeeds');
                        return [];
                    }

                    // 获取所有section.note-item
                    const sections = userPostedFeeds.querySelectorAll('section.note-item');
                    console.log('找到section数量:', sections.length);

                    const cards = [];

                    sections.forEach((section, index) => {
                        // 查找a.cover.mask.ld链接
                        const link = section.querySelector('a.cover.mask.ld');
                        if (link) {
                            // 从href中提取笔记ID
                            // href格式: "/user/profile/620746f00000000010007ae5/68fb56fc0000000003021848?xsec_token=..."
                            const href = link.getAttribute('href');
                            const match = href.match(/\\/user\\/profile\\/[^\\/]+\\/([^?]+)/);
                            const noteId = match ? match[1] : null;

                            cards.push({
                                index: index,
                                href: href,
                                note_id: noteId
                            });
                            console.log('section', index, 'note_id:', noteId);
                        } else {
                            console.log('section', index, '未找到链接');
                        }
                    });

                    return cards;
                }
            ''')

            logger.info(f"找到 {len(cards)} 个笔记卡片")
            return cards

        except Exception as e:
            logger.error(f"获取笔记卡片时出错: {str(e)}")
            return []

    def click_note_card(self, card_index):
        """点击指定索引的笔记卡片"""
        try:
            logger.info(f"点击第 {card_index + 1} 个笔记卡片...")

            # 清空feed接口数据
            if 'feed' in self.api_data:
                self.api_data['feed'] = []

            clicked = self.page.evaluate(f'''
                () => {{
                    // 直接通过ID查找userPostedFeeds容器
                    const userPostedFeeds = document.querySelector('div#userPostedFeeds');
                    if (!userPostedFeeds) {{
                        console.log('未找到div#userPostedFeeds');
                        return false;
                    }}

                    // 获取所有section.note-item
                    const sections = userPostedFeeds.querySelectorAll('section.note-item');
                    console.log('找到section数量:', sections.length);

                    if (sections.length <= {card_index}) {{
                        console.log('索引超出范围:', {card_index}, '总数:', sections.length);
                        return false;
                    }}

                    const section = sections[{card_index}];
                    const link = section.querySelector('a.cover.mask.ld');
                    if (link) {{
                        console.log('找到链接，准备点击');
                        link.click();
                        return true;
                    }} else {{
                        console.log('未找到a.cover.mask.ld链接');
                    }}

                    return false;
                }}
            ''')

            if clicked:
                logger.info(f"成功点击第 {card_index + 1} 个笔记卡片")

                # 延迟模拟人工阅读笔记的时间
                self.human_delay(3, 6)

                return True
            else:
                logger.error(f"未找到第 {card_index + 1} 个笔记卡片")
                return False

        except Exception as e:
            logger.error(f"点击笔记卡片时出错: {str(e)}")
            return False

    def get_fans_count(self):
        """获取博主粉丝数"""
        try:
            logger.info("正在获取粉丝数...")

            # 等待页面加载完成
            time.sleep(2)

            # 使用JavaScript获取粉丝数
            fans_count = self.page.evaluate('''
                () => {
                    // 查找span.count元素
                    const countSpans = document.querySelectorAll('span.count');

                    // 通常第一个count是粉丝数
                    if (countSpans.length > 0) {
                        const fansText = countSpans[1].textContent.trim();
                        console.log('找到粉丝数:', fansText);
                        return fansText;
                    }

                    return null;
                }
            ''')

            if fans_count:
                logger.info(f"✓ 成功获取粉丝数: {fans_count}")
                return fans_count
            else:
                logger.warning("未找到粉丝数元素")

                # 如果没找到，尝试从页面HTML中提取
                page_content = self.page.content()
                import re
                match = re.search(r'<span[^>]*class="count"[^>]*>([^<]+)</span>', page_content)
                if match:
                    fans_count = match.group(1).strip()
                    logger.info(f"✓ 通过正则提取粉丝数: {fans_count}")
                    return fans_count

                return None

        except Exception as e:
            logger.error(f"获取粉丝数失败: {str(e)}")
            return None

    def process_user(self, user):
        """处理单个用户的笔记数据"""
        try:
            self.current_user = user
            user_id = user.userId
            nick_name = user.nick_name
            xsec_token = user.xsec_token

            logger.info(f"开始处理用户: {nick_name} (userId: {user_id})")

            # 构建用户profile页面URL
            profile_url = f"{self.base_url}/user/profile/{user_id}?xsec_token={xsec_token}&xsec_source=pc_search&tab=fav"
            logger.info(f"访问用户profile页面: {profile_url}")

            # 清空API数据
            self.api_data = {}

            # 访问用户页面
            self.page.goto(profile_url)
            self.human_delay(3, 4)

            # 获取粉丝数
            fans_count = self.get_fans_count()
            if fans_count:
                # 保存粉丝数到数据库（每次都新增一条记录，用于历史对比）
                try:
                    new_fans_record = PgyUserFans(
                        user_id=user.id,
                        fans=fans_count,
                        platform_id=1,
                        create_time=datetime.now(),
                        update_time=datetime.now()
                    )
                    session.add(new_fans_record)
                    session.commit()
                    logger.info(f"✓ 已保存用户 {nick_name} 的粉丝数到数据库: {fans_count}")
                except Exception as e:
                    logger.error(f"保存粉丝数到数据库失败: {str(e)}")
                    session.rollback()

            # 点击笔记tab
            if not self.click_notes_tab():
                logger.error(f"点击笔记tab失败，跳过用户 {nick_name}")
                return False

            # 等待页面加载完成
            self.human_delay(2, 3)

            # 等待user_posted接口响应
            logger.info("等待user_posted接口响应...")
            wait_time = 0
            max_wait = 10
            while wait_time < max_wait:
                if 'user_posted' in self.api_data and self.api_data['user_posted']:
                    logger.info(f"已捕获user_posted接口响应（等待了 {wait_time:.1f} 秒）")
                    break
                time.sleep(0.5)
                wait_time += 0.5
            else:
                logger.warning(f"等待user_posted接口响应超时（{max_wait}秒）")

            # 保存user_posted数据到数据库
            if 'user_posted' in self.api_data and self.api_data['user_posted']:
                self.save_user_posted_data_to_db(user.id)

            # 记录已点击过的笔记ID
            clicked_note_ids = set()
            total_clicked = 0
            max_rounds = 20  # 最多循环20轮

            # 获取初始section
            cards = self.get_note_cards()
            if len(cards) == 0:
                logger.warning(f"用户 {nick_name} 没有笔记")
                return False

            logger.info(f"初始加载了 {len(cards)} 个笔记section")

            # 循环处理：点击section -> 滚动 -> 点击新增section
            should_stop = False  # 初始化停止标志
            for round_num in range(max_rounds):
                logger.info(f"===== 第 {round_num + 1} 轮处理 =====")

                # 获取当前所有section
                cards = self.get_note_cards()

                # 提取当前所有笔记ID
                current_note_ids = set()
                for card in cards:
                    if card.get('note_id'):
                        current_note_ids.add(card['note_id'])

                # 找出新增的笔记ID（未点击过的）
                new_note_ids = current_note_ids - clicked_note_ids

                logger.info(f"当前共有 {len(cards)} 个section，其中新增 {len(new_note_ids)} 个未点击的笔记")
                logger.info(f"新增笔记ID: {list(new_note_ids)}")

                # 如果没有新的笔记，结束循环
                if len(new_note_ids) == 0:
                    logger.info("没有新的笔记，结束处理")
                    break

                # 点击新增的笔记
                for card in cards:
                    note_id = card.get('note_id')
                    card_index = card.get('index')

                    # 只点击未点击过的笔记
                    if note_id and note_id in new_note_ids:
                        logger.info(f"处理笔记 {note_id} (索引: {card_index})")

                        # 点击笔记卡片
                        if self.click_note_card(card_index):
                            # 按ESC键关闭详情页
                            logger.info("按ESC键关闭详情页...")
                            self.page.keyboard.press('Escape')
                            self.human_delay(1, 2)

                            # 等待feed接口响应（关闭后再等待）
                            logger.info("等待feed接口响应...")
                            wait_time = 0
                            max_wait = 10
                            while wait_time < max_wait:
                                if 'feed' in self.api_data and len(self.api_data['feed']) > 0:
                                    logger.info(f"已捕获feed接口响应（等待了 {wait_time:.1f} 秒）")
                                    break
                                time.sleep(0.5)
                                wait_time += 0.5
                            else:
                                logger.warning(f"等待feed接口响应超时（{max_wait}秒）")

                            # 保存feed数据（save_feed_data_to_db会自动处理新增和更新）
                            self.save_feed_data_to_db(user.id)

                            # 检查笔记发布时间，如果小于2025-10-12则停止点击
                            feed_entries = self.api_data.get('feed', [])
                            if feed_entries:
                                for entry in feed_entries:
                                    response_data = entry.get('data', {})
                                    note_info = response_data.get('data', {}).get('items', [])
                                    if note_info and len(note_info) > 0:
                                        item = note_info[0]
                                        note_card = item.get('note_card', {})
                                        last_update_time = note_card.get('last_update_time', 0)

                                        if last_update_time:
                                            # 转换毫秒时间戳为日期
                                            note_date = datetime.fromtimestamp(last_update_time / 1000)
                                            cutoff_date = datetime(2025, 10, 12)

                                            logger.info(f"笔记发布时间: {note_date.strftime('%Y-%m-%d')}")

                                            if note_date < cutoff_date:
                                                logger.info(f"笔记发布时间 {note_date.strftime('%Y-%m-%d')} 早于 2025-10-12，停止点击")
                                                should_stop = True
                                                break

                            # 标记为已点击
                            clicked_note_ids.add(note_id)
                            total_clicked += 1

                            # 如果需要停止，跳出循环
                            if should_stop:
                                logger.info("因发布时间过早，停止处理该用户的笔记")
                                break
                        else:
                            logger.error(f"点击笔记 {note_id} 失败")
                            continue

                # 如果因为发布时间过早而停止，跳出外层循环
                if should_stop:
                    break

                # 滚动触发新section加载
                logger.info("向下滚动触发新section加载...")
                self.scroll_to_trigger_load()

            logger.info(f"用户 {nick_name} 处理完成，共点击 {total_clicked} 个笔记")
            logger.info(f"已点击笔记ID列表: {list(clicked_note_ids)}")
            self.current_user = None
            return True

        except Exception as e:
            logger.error(f"处理用户 {user.nick_name} 时出错: {str(e)}")
            self.current_user = None
            return False

    def save_user_posted_data_to_db(self, pgy_id):
        """保存user_posted数据到数据库"""
        try:
            user_posted_data = self.api_data.get('user_posted', {})

            if not user_posted_data:
                logger.warning("未捕获user_posted数据")
                return False

            response_data = user_posted_data.get('data', {})
            notes = response_data.get('data', {}).get('notes', [])

            logger.info(f"准备保存 {len(notes)} 条笔记数据到数据库")

            if not notes:
                logger.warning("notes为空")
                return False

            saved_count = 0
            updated_count = 0

            for note in notes:
                note_id = note.get('note_id', '')
                display_title = note.get('display_title', '')
                note_type = note.get('type', '')
                xsec_token = note.get('xsec_token', '')

                # 提取互动数据
                interact_info = note.get('interact_info', {})
                liked_count = interact_info.get('liked_count', '0')
                # 处理数字格式（可能包含"万"或"w"）
                if isinstance(liked_count, str):
                    like_num = int(liked_count.replace('万', '0000').replace('w', '0000'))
                else:
                    like_num = int(liked_count)

                # 获取当前时间戳
                current_timestamp = int(time.time())

                # 检查数据库中是否已存在（只根据note_id查重）
                existing_record = session.query(PgyNoteDetail).filter(
                    PgyNoteDetail.note_id == note_id
                ).first()

                if existing_record:
                    # 更新已存在的记录（暂时不更新 note_date，等点击后再更新）
                    existing_record.pgy_id = pgy_id
                    existing_record.note_title = display_title
                    existing_record.note_type = note_type
                    existing_record.like_num = like_num
                    existing_record.xsec_token = xsec_token
                    existing_record.platform_id = 1
                    existing_record.update_time = current_timestamp
                    updated_count += 1
                    logger.info(f"✓ 更新笔记数据: {display_title} (ID: {note_id})")
                else:
                    # 插入新记录（note_date稍后在点击时更新）
                    new_record = PgyNoteDetail(
                        pgy_id=pgy_id,
                        note_id=note_id,
                        note_title=display_title,
                        note_type=note_type,
                        like_num=like_num,
                        xsec_token=xsec_token,
                        collect_num=0,
                        share_num=0,
                        note_message='',
                        note_date='',  # 稍后在点击时更新
                        create_time=current_timestamp,
                        update_time=current_timestamp,
                        platform_id=1
                    )
                    session.add(new_record)
                    saved_count += 1
                    logger.info(f"✓ 保存新笔记数据: {display_title} (ID: {note_id})")

                session.commit()

            logger.info(f"✓ user_posted数据保存完成：新增 {saved_count} 条，更新 {updated_count} 条")
            return True

        except Exception as e:
            logger.error(f"保存user_posted数据到数据库时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            session.rollback()
            return False

    def save_feed_data_to_db(self, pgy_id):
        """保存feed数据到数据库"""
        try:
            feed_entries = self.api_data.get('feed', [])

            # 添加调试日志
            logger.info(f"API数据keys: {list(self.api_data.keys())}")
            logger.info(f"捕获到的feed条目数量: {len(feed_entries)}")

            if not feed_entries:
                logger.warning("未捕获feed数据")
                return False

            for entry in feed_entries:
                response_data = entry.get('data', {})

                # 提取笔记信息
                note_info = response_data.get('data', {}).get('items', [])
                logger.info(f"提取到的笔记items数量: {len(note_info)}")

                if not note_info or len(note_info) == 0:
                    logger.warning("note_info为空或长度为0")
                    continue

                # 获取第一个item
                item = note_info[0]
                note_card = item.get('note_card', {})

                # 添加调试日志
                logger.info(f"note_card keys: {list(note_card.keys())}")

                # 提取笔记基本信息
                note_id = note_card.get('note_id', '')
                note_title = note_card.get('title', '')
                note_type = note_card.get('type', '')  # 笔记类型
                desc = note_card.get('desc', '')  # 笔记内容

                logger.info(f"准备保存笔记: note_id={note_id}, title={note_title}")

                # 提取互动数据
                interact_info = note_card.get('interact_info', {})
                like_num = int(interact_info.get('liked_count', '0').replace('万', '0000').replace('w', '0000'))
                collect_num = int(interact_info.get('collected_count', '0').replace('万', '0000').replace('w', '0000'))
                share_num = int(interact_info.get('share_count', '0').replace('万', '0000').replace('w', '0000'))

                # 提取发布时间
                last_update_time = note_card.get('last_update_time', 0)
                if last_update_time:
                    # 转换毫秒时间戳为秒
                    note_date = time.strftime('%Y-%m-%d', time.localtime(last_update_time / 1000))
                else:
                    note_date = ''

                # 获取当前时间戳
                current_timestamp = int(time.time())

                # 检查数据库中是否已存在（只根据note_id查重）
                existing_record = session.query(PgyNoteDetail).filter(
                    PgyNoteDetail.note_id == note_id
                ).first()

                if existing_record:
                    # 更新已存在的记录
                    existing_record.pgy_id = pgy_id
                    existing_record.note_title = note_title
                    existing_record.note_type = note_type
                    existing_record.like_num = like_num
                    existing_record.collect_num = collect_num
                    existing_record.share_num = share_num
                    existing_record.note_message = desc
                    existing_record.note_date = note_date
                    existing_record.update_time = current_timestamp
                    logger.info(f"✓ 更新笔记数据: {note_title} (ID: {note_id})")
                else:
                    # 插入新记录
                    new_record = PgyNoteDetail(
                        pgy_id=pgy_id,
                        note_id=note_id,
                        note_title=note_title,
                        note_type=note_type,
                        like_num=like_num,
                        collect_num=collect_num,
                        share_num=share_num,
                        note_message=desc,
                        note_date=note_date,
                        create_time=current_timestamp,
                        update_time=current_timestamp
                    )
                    session.add(new_record)
                    logger.info(f"✓ 保存新笔记数据: {note_title} (ID: {note_id})")

                session.commit()
                logger.info(f"✓ 数据已提交到数据库")

            return True

        except Exception as e:
            logger.error(f"保存feed数据到数据库时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            session.rollback()
            return False

    def run(self):
        """运行爬虫"""
        try:
            logger.info("开始运行小红书笔记爬虫...")

            # 检查并处理登录
            if not self.check_and_handle_login():
                logger.error("登录失败,程序退出")
                return

            # 查询所有pgy_user数据
            logger.info("查询pgy_user表数据...")
            users = session.query(PgyUser).all()
            logger.info(f"共查询到 {len(users)} 个用户")

            if len(users) == 0:
                logger.warning("未查询到用户数据")
                return

            # 循环处理每个用户
            for index, user in enumerate(users):
                logger.info(f"处理第 {index + 1}/{len(users)} 个用户")
                self.process_user(user)

                # 每处理完一个用户，等待一段时间
                self.human_delay(3, 5)

            logger.info("所有用户处理完成")

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

            # 关闭context
            if hasattr(self, 'context') and self.context:
                self.context.close()

            # 关闭browser
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()

            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()

            # 关闭数据库session
            session.close()

            logger.info("所有资源已关闭")

        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")


if __name__ == '__main__':
    spider = XiaohongshuNotesSpider()
    spider.run()
