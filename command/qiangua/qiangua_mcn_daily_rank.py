import time
import json
import os
import sys
from datetime import datetime
import random
from decimal import Decimal
import configparser
import tkinter as tk
from tkinter import ttk

import pandas as pd
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

"""
    获取千瓜MCN商业收入日榜数据
"""


def show_mcn_selection_dialog():
    """显示MCN选择对话框"""
    mcn_names = [
        '大禹', '初颂', '缇苏', '西西里', '橙拉', '门牙', '如涵文化', '方片', 'papitube',
        '古麦', '五月星河', '云森传媒', '雅俗共赏', '仙梓', '妍媸文化', '麦芽传媒',
        '杠上开花', '时刻文化', '美哒文化', '旗鱼跃动', '二咖', '西瓜创想', '灵猫文化',
        '集星文化', '尚世文化', '长沙丁丁', '明诚文化', '掌邦文化', '滕云文化', '鹿鼎文化',
        '丁游文化', '快美BeautyQ', '麦籽网络', '十月知行', '苏颜', '少女派', '最美妆',
        '小雨互动', '壹枝花'
    ]

    selected_mcns = []
    button_widgets = {}  # 存储按钮引用用于改变样式

    def on_mcn_click(mcn_name, btn):
        """处理MCN名字点击事件"""
        if mcn_name not in selected_mcns:
            selected_mcns.append(mcn_name)
            # 改变按钮样式表示已选择
            btn.config(bg='#4CAF50', fg='white', relief='sunken')
            # 更新已选择列表显示
            update_selected_list()

    def update_selected_list():
        """更新已选择列表的显示"""
        selected_text.config(state='normal')
        selected_text.delete(1.0, tk.END)
        if selected_mcns:
            selected_text.insert(1.0, f"已选择 {len(selected_mcns)} 个:\n" + ' → '.join(selected_mcns))
        else:
            selected_text.insert(1.0, "尚未选择任何机构")
        selected_text.config(state='disabled')

    def on_confirm():
        """确认按钮点击事件"""
        if selected_mcns:
            root.quit()
            root.destroy()
        else:
            import tkinter.messagebox as messagebox
            messagebox.showwarning("提示", "请至少选择一个MCN机构!")

    def on_reset():
        """重置按钮点击事件"""
        selected_mcns.clear()
        # 恢复所有按钮样式
        for btn in button_widgets.values():
            btn.config(bg='#E3F2FD', fg='black', relief='raised')
        update_selected_list()

    # 创建主窗口
    root = tk.Tk()
    root.title("千瓜MCN数据抓取 - 选择机构")
    root.geometry("900x700")
    root.configure(bg='#f5f5f5')

    # 创建顶部标题区域
    title_frame = tk.Frame(root, bg='#2196F3', height=80)
    title_frame.pack(fill='x')
    title_frame.pack_propagate(False)

    title_label = tk.Label(
        title_frame,
        text="📊 千瓜MCN数据抓取工具",
        font=("Microsoft YaHei UI", 18, "bold"),
        bg='#2196F3',
        fg='white'
    )
    title_label.pack(pady=20)

    # 创建说明标签
    instruction_frame = tk.Frame(root, bg='#f5f5f5')
    instruction_frame.pack(pady=15)

    instruction_label = tk.Label(
        instruction_frame,
        text="请按顺序点击要查询的MCN机构名称（点击顺序即为查询顺序）",
        font=("Microsoft YaHei UI", 11),
        bg='#f5f5f5',
        fg='#333'
    )
    instruction_label.pack()

    # 创建按钮容器（带滚动）
    canvas_frame = tk.Frame(root, bg='#f5f5f5')
    canvas_frame.pack(pady=10, fill='both', expand=True, padx=20)

    canvas = tk.Canvas(canvas_frame, bg='white', highlightthickness=0)
    scrollbar = tk.Scrollbar(canvas_frame, orient='vertical', command=canvas.yview)

    button_frame = tk.Frame(canvas, bg='white')

    canvas.create_window((0, 0), window=button_frame, anchor='nw')
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side='left', fill='both', expand=True)
    scrollbar.pack(side='right', fill='y')

    # 创建按钮网格
    cols = 5  # 每行5个按钮
    for idx, mcn in enumerate(mcn_names):
        row = idx // cols
        col = idx % cols
        btn = tk.Button(
            button_frame,
            text=mcn,
            width=16,
            height=2,
            font=("Microsoft YaHei UI", 10),
            bg='#E3F2FD',
            fg='black',
            relief='raised',
            bd=2,
            cursor='hand2',
            activebackground='#90CAF9'
        )
        btn.config(command=lambda m=mcn, b=btn: on_mcn_click(m, b))
        btn.grid(row=row, column=col, padx=8, pady=8)
        button_widgets[mcn] = btn

    # 更新canvas滚动区域
    button_frame.update_idletasks()
    canvas.config(scrollregion=canvas.bbox('all'))

    # 创建已选择列表显示框
    selected_frame = tk.Frame(root, bg='#f5f5f5')
    selected_frame.pack(pady=10, fill='x', padx=25)

    selected_label = tk.Label(
        selected_frame,
        text="✓ 已选择列表:",
        font=("Microsoft YaHei UI", 10, "bold"),
        bg='#f5f5f5',
        fg='#333'
    )
    selected_label.pack(anchor='w', pady=(0, 5))

    selected_text = tk.Text(
        selected_frame,
        height=3,
        state='disabled',
        font=("Microsoft YaHei UI", 9),
        bg='#FFF9C4',
        relief='solid',
        bd=1,
        wrap='word'
    )
    selected_text.pack(fill='x')
    update_selected_list()

    # 创建底部操作按钮
    bottom_frame = tk.Frame(root, bg='#f5f5f5')
    bottom_frame.pack(pady=20)

    reset_btn = tk.Button(
        bottom_frame,
        text="🔄 重置",
        width=12,
        height=2,
        font=("Microsoft YaHei UI", 11),
        bg='#FF9800',
        fg='white',
        relief='raised',
        bd=0,
        cursor='hand2',
        activebackground='#F57C00',
        command=on_reset
    )
    reset_btn.pack(side='left', padx=15)

    confirm_btn = tk.Button(
        bottom_frame,
        text="✓ 确认提交",
        width=12,
        height=2,
        font=("Microsoft YaHei UI", 11, "bold"),
        bg='#4CAF50',
        fg='white',
        relief='raised',
        bd=0,
        cursor='hand2',
        activebackground='#388E3C',
        command=on_confirm
    )
    confirm_btn.pack(side='left', padx=15)

    # 居中显示窗口
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    # 运行主循环
    root.mainloop()

    return selected_mcns


def show_rank_type_dialog():
    """显示榜单类型选择对话框"""
    selected_rank_type = []

    def on_rank_click(rank_type):
        """处理榜单类型点击事件"""
        selected_rank_type.clear()
        selected_rank_type.append(rank_type)
        root.quit()
        root.destroy()

    # 创建主窗口
    root = tk.Tk()
    root.title("千瓜MCN数据抓取 - 选择榜单类型")
    root.geometry("550x350")
    root.configure(bg='#f5f5f5')

    # 创建顶部标题区域
    title_frame = tk.Frame(root, bg='#2196F3', height=80)
    title_frame.pack(fill='x')
    title_frame.pack_propagate(False)

    title_label = tk.Label(
        title_frame,
        text="📈 选择榜单类型",
        font=("Microsoft YaHei UI", 18, "bold"),
        bg='#2196F3',
        fg='white'
    )
    title_label.pack(pady=20)

    # 创建说明标签
    instruction_frame = tk.Frame(root, bg='#f5f5f5')
    instruction_frame.pack(pady=20)

    instruction_label = tk.Label(
        instruction_frame,
        text="请选择要查询的榜单类型",
        font=("Microsoft YaHei UI", 12),
        bg='#f5f5f5',
        fg='#333'
    )
    instruction_label.pack()

    # 创建按钮容器
    button_frame = tk.Frame(root, bg='#f5f5f5')
    button_frame.pack(pady=30, expand=True)

    # 创建三个榜单类型按钮
    buttons_info = [
        ("📅 日榜", "日榜", "#FF5722"),
        ("📊 周榜", "周榜", "#2196F3"),
        ("📆 月榜", "月榜", "#4CAF50")
    ]

    for text, rank_type, color in buttons_info:
        btn = tk.Button(
            button_frame,
            text=text,
            width=15,
            height=3,
            font=("Microsoft YaHei UI", 13, "bold"),
            bg=color,
            fg='white',
            relief='raised',
            bd=0,
            cursor='hand2',
            activebackground=color,
            command=lambda r=rank_type: on_rank_click(r)
        )
        btn.pack(side='left', padx=15)

        # 添加鼠标悬停效果
        def on_enter(e, button=btn, clr=color):
            button.config(bg=_darken_color(clr))

        def on_leave(e, button=btn, clr=color):
            button.config(bg=clr)

        btn.bind("<Enter>", on_enter)
        btn.bind("<Leave>", on_leave)

    # 居中显示窗口
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    # 运行主循环
    root.mainloop()

    return selected_rank_type[0] if selected_rank_type else "日榜"


def _darken_color(hex_color):
    """使颜色变暗（用于悬停效果）"""
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    r = int(r * 0.8)
    g = int(g * 0.8)
    b = int(b * 0.8)
    return f'#{r:02x}{g:02x}{b:02x}'


def get_base_dir():
    """获取程序运行目录（支持打包后的exe）"""
    if getattr(sys, 'frozen', False):
        # 如果是打包后的exe，使用exe所在目录
        return os.path.dirname(sys.executable)
    else:
        # 如果是Python脚本，使用脚本所在目录
        return os.path.dirname(os.path.abspath(__file__))


class QianguaMcnDailyRankSpider:
    def __init__(self, rank_type="日榜"):
        self.base_dir = get_base_dir()
        self.setup_logger()
        self.base_url = "https://app.qian-gua.com"
        self.mcn_rank_url = "https://app.qian-gua.com/#/mcn/rank"
        self.is_logged_in = False
        self.api_data = {}
        self.cookie_file = os.path.join(self.base_dir, 'cookies.json')
        self.config_file = os.path.join(self.base_dir, 'daily_rank_config.ini')
        self.export_folder = os.path.join(self.base_dir, 'exports')
        os.makedirs(self.export_folder, exist_ok=True)

        # 保存用户选择的榜单类型
        self.rank_type = rank_type

        # 加载配置
        self.load_config()

        self.setup_browser()

    def setup_logger(self):
        """设置日志"""
        log_path = os.path.join(self.base_dir, 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(
            os.path.join(log_path, "qiangua_daily_rank_{time}.log"),
            rotation="1 day",
            retention="7 days"
        )

    def load_config(self):
        """加载配置文件"""
        try:
            config = configparser.ConfigParser()
            config.read(self.config_file, encoding='utf-8')

            # 读取搜索关键词列表
            keywords_str = config.get('SEARCH', 'keywords', fallback='')
            self.search_keywords = [kw.strip() for kw in keywords_str.split(',') if kw.strip()]

            # 读取设置
            self.click_delay_min = config.getfloat('SETTINGS', 'click_delay_min', fallback=0.8)
            self.click_delay_max = config.getfloat('SETTINGS', 'click_delay_max', fallback=1.8)

            logger.info(f"配置加载成功: 关键词数量={len(self.search_keywords)}, 关键词={self.search_keywords}")
        except FileNotFoundError:
            logger.warning(f"配置文件不存在: {self.config_file}，使用默认配置")
            self.search_keywords = []
            self.click_delay_min = 0.8
            self.click_delay_max = 1.8
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}，使用默认配置")
            self.search_keywords = []
            self.click_delay_min = 0.8
            self.click_delay_max = 1.8

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
        """初始化浏览器"""
        self.playwright = sync_playwright().start()
        user_data_dir = os.path.join(self.base_dir, 'chrome_user_data')
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
        self.page.set_default_timeout(20000)
        self.page.on("response", self._handle_api_response)

    def close_popups(self):
        """关闭所有弹出框"""
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
            logger.error(f"关闭弹出框时出错: {str(e)}")

    def check_login_status(self):
        """检查登录状态"""
        try:
            self.page.wait_for_load_state('networkidle', timeout=10000)
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

            logger.info("已点击登录按钮,等待滑块验证...")
            logger.info("请手动完成滑块验证并点击登录!")
            self.human_delay(1.5, 2.5)

            # 等待用户手动完成滑块验证和登录
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

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if response.request.resource_type in ['fetch', 'xhr']:
                # 只拦截GetMcnRankData接口
                if 'GetMcnRankData' in url:
                    api_name = 'GetMcnRankData'
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

                            item_list = response_data.get('Data', {}).get('ItemList', [])
                            logger.info(f"捕获 {len(item_list)} 条MCN排行数据")

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
                self.human_delay(1.5, 2.5)
                self.page.wait_for_load_state('networkidle', timeout=10000)
                return True
            else:
                logger.error("未找到商业收入榜按钮")
                return False
        except Exception as e:
            logger.error(f"点击商业收入榜时出错: {str(e)}")
            return False

    def click_daily_rank_button(self):
        """点击榜单按钮（根据用户选择的榜单类型）"""
        try:
            logger.info(f"点击{self.rank_type}按钮...")

            # 使用选择器点击榜单按钮
            clicked = self.page.evaluate(f'''
                () => {{
                    const buttons = Array.from(document.querySelectorAll("button"));
                    for (const btn of buttons) {{
                        if (btn.innerText.trim() === "{self.rank_type}") {{
                            btn.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            ''')

            if clicked:
                logger.info(f"成功点击{self.rank_type}按钮")
                self.human_delay(1.5, 2.5)
                self.page.wait_for_load_state('networkidle', timeout=10000)
                return True
            else:
                logger.error(f"未找到{self.rank_type}按钮")
                return False
        except Exception as e:
            logger.error(f"点击{self.rank_type}按钮时出错: {str(e)}")
            return False

    def search_keyword(self, keyword):
        """搜索关键词"""
        try:
            logger.info(f"搜索关键词: {keyword}")

            # 定位搜索框
            search_input = self.page.locator(
                '.search-box.mr16 .el-autocomplete.s-input .el-input.el-input--medium.el-input-group.el-input-group--append.el-input--suffix input'
            )

            # 清空并输入搜索词（这个过程可能触发搜索，但我们不需要这些数据）
            search_input.fill('')
            self.human_delay(0.6, 1.2)

            search_input.fill(keyword)
            self.human_delay(1.0, 1.8)

            # 在按Enter之前清空API数据，这样只有按Enter后的数据才会被保存
            logger.info("清空之前的API数据，准备获取新的搜索结果...")
            if 'GetMcnRankData' in self.api_data:
                self.api_data['GetMcnRankData'] = []

            # 记录搜索开始时间戳
            search_start_ts = int(time.time() * 1000)

            # 按回车搜索
            logger.info(f"按Enter执行搜索: {keyword}")
            search_input.press('Enter')
            self.human_delay(1.5, 2.5)

            # 等待API响应
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
                logger.warning(f"搜索关键词 {keyword} 后未捕获新的GetMcnRankData响应")

            self.page.wait_for_load_state('networkidle', timeout=10000)
            self.human_delay(1.0, 1.8)

            logger.info(f"搜索关键词 {keyword} 完成")

            # 如果没有收到新数据，等待一下再检查
            if not new_data_received:
                wait_start = time.time()
                while time.time() - wait_start < 3:
                    if self.api_data.get('GetMcnRankData'):
                        break
                    time.sleep(0.2)

            return True
        except Exception as e:
            logger.error(f"搜索关键词 {keyword} 时出错: {str(e)}")
            return False

    def extract_rank_data(self):
        """提取MCN排行数据"""
        try:
            rank_entries = self.api_data.get('GetMcnRankData', [])
            if not rank_entries:
                logger.warning("未捕获MCN排行数据")
                return []

            all_data = []

            for entry in rank_entries:
                if entry.get('processed'):
                    continue

                response_data = entry.get('data') or {}
                item_list = (response_data.get('Data') or {}).get('ItemList') or []

                if not item_list:
                    entry['processed'] = True
                    continue

                for item in item_list:
                    # 提取标签信息
                    tags_text = item.get('BloggerTags')
                    if not tags_text:
                        tag_list = item.get('BloggerTagList') or []
                        tags_text = ','.join(
                            tag.get('Name') for tag in tag_list if tag.get('Name')
                        )

                    # 处理涨幅值
                    increase_value = item.get('IncreaseRankValue')
                    try:
                        increase_value_decimal = (
                            Decimal(str(increase_value)).quantize(Decimal('0.00'))
                            if increase_value is not None
                            else Decimal('0.00')
                        )
                    except Exception:
                        increase_value_decimal = Decimal('0.00')

                    # 构造数据字典
                    data_dict = {
                        '昵称': item.get('NickName') or '',
                        '预估商业收入': item.get('RankValue') or 0,
                        '合作品牌数': item.get('BrandCount') or 0,
                        '标签': tags_text,
                        '合作博主数': item.get('BloggerCount') or 0,
                        '合作笔记数': item.get('NoteCount') or 0,
                    }

                    all_data.append(data_dict)

                entry['processed'] = True

            logger.info(f"成功提取 {len(all_data)} 条MCN排行数据")
            return all_data

        except Exception as e:
            logger.error(f"提取MCN排行数据时出错: {str(e)}")
            return []

    def export_to_excel(self, data, filename=None):
        """导出数据到Excel"""
        try:
            if not data:
                logger.warning("没有数据可导出")
                return False

            # 生成文件名
            if filename is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"千瓜MCN{self.rank_type}数据_{timestamp}.xlsx"

            filepath = os.path.join(self.export_folder, filename)

            # 创建DataFrame并导出
            df = pd.DataFrame(data)
            df.to_excel(filepath, index=False, engine='openpyxl')

            logger.info(f"数据已导出到: {filepath}")
            logger.info(f"共导出 {len(data)} 条数据")
            return True

        except Exception as e:
            logger.error(f"导出Excel时出错: {str(e)}")
            return False

    def scrape_daily_rank_data(self, keywords):
        """抓取MCN榜单数据"""
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
                return []

            # 点击榜单按钮（日榜/周榜/月榜）
            if not self.click_daily_rank_button():
                logger.error(f"点击{self.rank_type}按钮失败")
                return []

            all_extracted_data = []

            # 如果没有提供关键词，则只获取一次当前页面数据
            if not keywords:
                logger.info("未提供搜索关键词，获取当前页面数据")
                self.human_delay(2.0, 3.0)
                extracted_data = self.extract_rank_data()
                all_extracted_data.extend(extracted_data)
            else:
                # 循环搜索每个关键词
                for keyword in keywords:
                    logger.info(f"开始处理关键词: {keyword}")

                    # 搜索关键词
                    if not self.search_keyword(keyword):
                        logger.error(f"搜索关键词 {keyword} 失败")
                        continue

                    # 提取数据
                    extracted_data = self.extract_rank_data()
                    all_extracted_data.extend(extracted_data)

                    # 稍作延迟再处理下一个关键词
                    time.sleep(2)

            logger.info(f"所有关键词处理完成，共提取 {len(all_extracted_data)} 条数据")
            return all_extracted_data

        except Exception as e:
            logger.error(f"抓取MCN榜单数据时出错: {str(e)}")
            return []

    def run(self, keywords=None):
        """运行爬虫"""
        try:
            logger.info("开始运行爬虫...")

            # 检查并处理登录
            if not self.check_and_handle_login():
                logger.error("登录失败,程序退出")
                return

            # 抓取数据
            if keywords is None:
                keywords = self.search_keywords

            data = self.scrape_daily_rank_data(keywords)

            # 导出数据
            if data:
                self.export_to_excel(data)
            else:
                logger.warning("未获取到任何数据")

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
    # 显示MCN选择对话框
    logger.info("显示MCN选择对话框...")
    selected_mcns = show_mcn_selection_dialog()

    if not selected_mcns:
        logger.warning("未选择任何MCN机构，程序退出")
        sys.exit(0)

    logger.info(f"用户选择的MCN顺序: {selected_mcns}")

    # 显示榜单类型选择对话框
    logger.info("显示榜单类型选择对话框...")
    rank_type = show_rank_type_dialog()
    logger.info(f"用户选择的榜单类型: {rank_type}")

    # 创建爬虫实例（传入用户选择的榜单类型）
    spider = QianguaMcnDailyRankSpider(rank_type=rank_type)

    # 使用用户选择的MCN列表作为关键词
    spider.run(keywords=selected_mcns)
