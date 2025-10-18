import time
import json
import os
from datetime import datetime
import signal
import sys
import configparser
import calendar

import schedule
from loguru import logger
from playwright.sync_api import sync_playwright
import random

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

            logger.info(f"配置加载成功: 机构数量={len(self.organizations)}, 查询月份={self.query_months}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

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
            viewport={'width': 1512, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            args=[
                '--disable-blink-features=AutomationControlled',  # 隐藏自动化特征
                '--no-sandbox',
                '--disable-web-security',
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
            time.sleep(1)
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
            time.sleep(3)

            self.page.click("text=手机登录")
            time.sleep(3)

            # 输入账号密码
            self.page.fill("input[placeholder='请输入手机号']", '13151572333')
            time.sleep(2)
            self.page.fill("input[placeholder='请输入登录密码']", '12345678abc')
            time.sleep(2)

            # 勾选协议
            self.page.click('.el-checkbox__inner')
            time.sleep(1)

            # 点击登录按钮
            self.page.click('button[class="el-button el-button--primary"][style="width: 200px;"]')

            # 等待滑块出现并提示用户
            logger.info("已点击登录按钮,等待滑块验证...")
            logger.info("请手动完成滑块验证并点击登录!")
            time.sleep(3)

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

                            # 打印接口数据
                            logger.info(f"{api_name} 接口数据:")
                            logger.info(json.dumps(response_data, ensure_ascii=False, indent=2))

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
                time.sleep(3)
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

            # 清空之前的API数据
            if 'GetMcnRankData' in self.api_data:
                self.api_data['GetMcnRankData'] = []

            # 定位搜索框
            search_input = self.page.locator('.search-box.mr16 .el-autocomplete.s-input .el-input.el-input--medium.el-input-group.el-input-group--append.el-input--suffix input')

            # 清空输入框
            search_input.fill('')
            time.sleep(1)

            # 输入机构名称
            search_input.fill(org_name)
            time.sleep(2)

            # 按下回车键
            search_input.press('Enter')
            time.sleep(3)

            # 等待API响应
            self.page.wait_for_load_state('networkidle', timeout=10000)
            time.sleep(2)

            logger.info(f"搜索机构 {org_name} 完成")
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
                time.sleep(3)
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
                time.sleep(3)
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

            # 在弹出框内查找并点击日期输入框(按照完整DOM路径)
            result = self.page.evaluate('''
                () => {
                    const dialog = document.querySelector('.el-dialog__body');
                    if (!dialog) {
                        return {success: false, message: '未找到弹出框(.el-dialog__body)'};
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
                    const dateEditor = thirdDiv.querySelector('.el-date-editor--daterange');
                    if (!dateEditor) {
                        return {success: false, message: '在第三个div中未找到.el-date-editor--daterange'};
                    }

                    const input = dateEditor.querySelector('.el-range-input');
                    if (!input) {
                        return {success: false, message: '在日期选择器中未找到.el-range-input'};
                    }

                    input.click();
                    return {success: true, message: '成功点击日期输入框'};
                }
            ''')

            logger.info(f"日期输入框点击结果: {result['message']}")

            if not result['success']:
                logger.error(f"未找到弹出框内的日期输入框: {result['message']}")
                return False

            time.sleep(2)
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

                    time.sleep(1)
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
            time.sleep(1)

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
            time.sleep(2)

            # 日期选择完成后,向下滚动一点点,触发数据加载
            logger.info("向下滚动页面,触发数据加载...")
            self.page.evaluate('''
                () => {
                    window.scrollBy(0, 100);
                }
            ''')
            time.sleep(1)

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
                        logger.info("GetMcnBrandList接口响应成功")
                        break
                    elif response_data.get('Code') == 500:
                        logger.warning(f"GetMcnBrandList接口返回错误: {response_data.get('Msg')}")
                time.sleep(0.5)

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
                time.sleep(3)
                self.page.wait_for_load_state('networkidle', timeout=10000)
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

            loaded_count = 0
            no_more_data = False

            while loaded_count < max_records and not no_more_data:
                # 获取当前已加载的数据条数
                current_count = self.page.evaluate('''
                    () => {
                        return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                    }
                ''')

                logger.info(f"当前已加载 {current_count} 条品牌数据")

                if current_count >= max_records:
                    logger.info(f"已达到最大记录数 {max_records},停止加载")
                    break

                # 滚动到底部
                prev_count = current_count
                self.page.evaluate('''
                    () => {
                        const scrollContainer = document.querySelector('.list-bd.page-component__scroll');
                        if (scrollContainer) {
                            scrollContainer.scrollTop = scrollContainer.scrollHeight;
                        }
                    }
                ''')

                # 随机等待
                delay = random.uniform(self.scroll_delay_min, self.scroll_delay_max)
                logger.info(f"等待 {delay:.2f} 秒...")
                time.sleep(delay)

                # 检查是否有新数据加载
                new_count = self.page.evaluate('''
                    () => {
                        return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                    }
                ''')

                if new_count == prev_count:
                    logger.info("没有更多数据,停止滚动")
                    no_more_data = True
                else:
                    loaded_count = new_count

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

            # 点击查看链接
            clicked = self.page.evaluate(f'''
                () => {{
                    const listItems = document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom');
                    if (listItems.length > {index}) {{
                        const item = listItems[{index}];
                        const viewLink = item.querySelector('.list-row .col-item.undefined .col-cell div a.text-link.c-split-line');
                        if (viewLink && viewLink.textContent.trim() === '查看') {{
                            viewLink.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            ''')

            if not clicked:
                logger.warning(f"未找到第 {index + 1} 个品牌的查看链接")
                return False

            logger.info(f"成功点击第 {index + 1} 个品牌的查看链接")
            time.sleep(3)

            # 点击投放报价
            logger.info("点击投放报价...")
            clicked_price = self.page.evaluate('''
                () => {
                    const elements = Array.from(document.querySelectorAll('span'));
                    for (const element of elements) {
                        if (element.textContent.trim() === '投放报价') {
                            element.click();
                            return true;
                        }
                    }
                    return false;
                }
            ''')

            if not clicked_price:
                logger.warning("未找到投放报价按钮")
                # 按ESC键关闭弹窗
                self.page.keyboard.press('Escape')
                time.sleep(1)
                return False

            logger.info("成功点击投放报价")
            time.sleep(3)
            self.page.wait_for_load_state('networkidle', timeout=10000)

            # 滚动一次加载更多数据
            logger.info("滚动加载投放报价数据...")
            self.page.evaluate('''
                () => {
                    const scrollContainer = document.querySelector('.list-bd.page-component__scroll');
                    if (scrollContainer) {
                        scrollContainer.scrollTop = scrollContainer.scrollHeight;
                    }
                }
            ''')
            time.sleep(3)

            # 按两次ESC键返回
            logger.info("按ESC键返回...")
            self.page.keyboard.press('Escape')
            time.sleep(1)
            self.page.keyboard.press('Escape')
            time.sleep(2)

            return True
        except Exception as e:
            logger.error(f"点击第 {index + 1} 个品牌并查看详情时出错: {str(e)}")
            # 尝试返回
            try:
                self.page.keyboard.press('Escape')
                time.sleep(1)
                self.page.keyboard.press('Escape')
                time.sleep(1)
            except:
                pass
            return False

    def process_organization(self, org_name):
        """处理单个机构的数据"""
        try:
            logger.info(f"开始处理机构: {org_name}")

            # 搜索机构
            if not self.search_organization(org_name):
                logger.error(f"搜索机构 {org_name} 失败")
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

                    # 循环点击每个品牌
                    actual_brand_count = min(brand_count, self.max_brand_records)
                    for brand_index in range(actual_brand_count):
                        logger.info(f"处理第 {brand_index + 1}/{actual_brand_count} 个品牌")
                        self.click_brand_item_and_view(brand_index)
                        time.sleep(2)

                    logger.info(f"{year_month} 月份处理完成")

                # 处理完所有月份后,按ESC键返回机构搜索页面
                logger.info("处理完当前机构的所有月份,返回机构搜索页面...")
                self.page.keyboard.press('Escape')
                time.sleep(2)

            logger.info(f"机构 {org_name} 处理完成")
            return True
        except Exception as e:
            logger.error(f"处理机构 {org_name} 时出错: {str(e)}")
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
