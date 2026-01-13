import time
import json
import os
import sys
import configparser
from datetime import datetime
import tkinter as tk
from tkinter import messagebox

from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import random
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

from core.localhost_fp_project import session
from models.models import QgCompanyNoteInfo

"""
    获取千瓜品牌投放大盘数据
"""


def load_last_selection():
    """加载上次选择的时间范围和标签"""
    try:
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        config_file = os.path.join(base_dir, 'last_brand_selection.json')
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('time_range'), data.get('selected_categories', [])
    except Exception as e:
        print(f"加载上次选择失败: {e}")
    return None, []


def save_last_selection(time_range, selected_categories):
    """保存本次选择的时间范围和标签"""
    try:
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        config_file = os.path.join(base_dir, 'last_brand_selection.json')
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump({
                'time_range': time_range,
                'selected_categories': selected_categories
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存选择失败: {e}")


def show_use_last_selection_dialog(time_range, categories):
    """显示是否使用上次选择的对话框"""
    if not time_range or not categories:
        return False

    result = {'use_last': False}

    def on_yes():
        result['use_last'] = True
        root.quit()
        root.destroy()

    def on_no():
        result['use_last'] = False
        root.quit()
        root.destroy()

    def on_close():
        root.quit()
        root.destroy()
        sys.exit(0)

    root = tk.Tk()
    root.title("千瓜品牌投放数据抓取 - 使用上次选择")
    root.geometry("650x520")
    root.configure(bg='#f5f5f5')
    root.protocol("WM_DELETE_WINDOW", on_close)

    # 标题
    title_frame = tk.Frame(root, bg='#2196F3', height=80)
    title_frame.pack(fill='x')
    title_frame.pack_propagate(False)

    title_label = tk.Label(
        title_frame,
        text="💡 使用上次选择?",
        font=("Microsoft YaHei UI", 18, "bold"),
        bg='#2196F3',
        fg='white'
    )
    title_label.pack(pady=20)

    # 说明
    instruction_frame = tk.Frame(root, bg='#f5f5f5')
    instruction_frame.pack(pady=20)

    instruction_label = tk.Label(
        instruction_frame,
        text="检测到上次选择的配置",
        font=("Microsoft YaHei UI", 14),
        bg='#f5f5f5',
        fg='#333'
    )
    instruction_label.pack()

    hint_label = tk.Label(
        instruction_frame,
        text="是否继续使用上次的选择?",
        font=("Microsoft YaHei UI", 12),
        bg='#f5f5f5',
        fg='#666'
    )
    hint_label.pack(pady=10)

    # 显示上次选择
    list_frame = tk.Frame(root, bg='#f5f5f5')
    list_frame.pack(pady=10, fill='x', padx=30)

    list_label = tk.Label(
        list_frame,
        text="📋 上次选择:",
        font=("Microsoft YaHei UI", 11, "bold"),
        bg='#f5f5f5',
        fg='#333'
    )
    list_label.pack(anchor='w', pady=(0, 5))

    list_text = tk.Text(
        list_frame,
        height=6,
        state='normal',
        font=("Microsoft YaHei UI", 10),
        bg='#FFF9C4',
        relief='solid',
        bd=1,
        wrap='word'
    )
    list_text.pack(fill='x')
    content = f"时间范围: {time_range}\n\n标签 (共{len(categories)}个):\n" + ' → '.join(categories)
    list_text.insert(1.0, content)
    list_text.config(state='disabled')

    # 按钮
    button_frame = tk.Frame(root, bg='#f5f5f5')
    button_frame.pack(pady=20)

    yes_btn = tk.Button(
        button_frame,
        text="✓ 是,使用上次选择",
        width=18,
        height=2,
        font=("Microsoft YaHei UI", 11, "bold"),
        bg='#4CAF50',
        fg='white',
        relief='raised',
        bd=0,
        cursor='hand2',
        activebackground='#388E3C',
        command=on_yes
    )
    yes_btn.pack(side='left', padx=15)

    no_btn = tk.Button(
        button_frame,
        text="✗ 否,重新选择",
        width=18,
        height=2,
        font=("Microsoft YaHei UI", 11, "bold"),
        bg='#FF9800',
        fg='white',
        relief='raised',
        bd=0,
        cursor='hand2',
        activebackground='#F57C00',
        command=on_no
    )
    no_btn.pack(side='left', padx=15)

    # 居中
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    root.mainloop()
    return result['use_last']


def show_selection_dialog():
    """显示选择对话框"""
    time_ranges = ['近7天', '近15天', '近30天', '近60天', '近90天']
    all_categories = ['彩妆', '护肤', '个护清洁', '母婴育儿', '美食饮品', '服饰穿搭', '鞋靴箱包', '珠宝配饰', '时尚潮流', '教育',
                    '家居家装','健身减肥', '科技数码', '动漫', '萌宠动物', '影音娱乐', '情感两性', '星座情感', '出行工具', '婚嫁',
                      '美甲', '旅行住宿', '摄影', '医疗养生', '民生资讯', '游戏应用', '赛事', '生活经验', '其他']

    selected_time_range = None
    selected_categories = []
    category_buttons = {}

    def on_time_range_select(time_range, btn):
        nonlocal selected_time_range
        # 取消其他时间范围按钮的选中状态
        for tr, b in time_range_buttons.items():
            b.config(bg='#E3F2FD', fg='black', relief='raised')
        # 选中当前按钮
        selected_time_range = time_range
        btn.config(bg='#2196F3', fg='white', relief='sunken')

    def on_category_click(category, btn):
        if category in selected_categories:
            selected_categories.remove(category)
            btn.config(bg='#E3F2FD', fg='black', relief='raised')
        else:
            selected_categories.append(category)
            btn.config(bg='#4CAF50', fg='white', relief='sunken')
        update_selected_list()

    def update_selected_list():
        selected_text.config(state='normal')
        selected_text.delete(1.0, tk.END)
        if selected_categories:
            selected_text.insert(1.0, f"已选择 {len(selected_categories)} 个标签:\n" + ' → '.join(selected_categories))
        else:
            selected_text.insert(1.0, "尚未选择任何标签")
        selected_text.config(state='disabled')

    def on_confirm():
        if not selected_time_range:
            messagebox.showwarning("提示", "请选择时间范围!")
            return
        if not selected_categories:
            messagebox.showwarning("提示", "请至少选择一个标签!")
            return
        root.quit()
        root.destroy()

    def on_reset():
        nonlocal selected_time_range
        selected_time_range = None
        selected_categories.clear()
        for btn in time_range_buttons.values():
            btn.config(bg='#E3F2FD', fg='black', relief='raised')
        for btn in category_buttons.values():
            btn.config(bg='#E3F2FD', fg='black', relief='raised')
        update_selected_list()

    def on_close():
        root.quit()
        root.destroy()
        sys.exit(0)

    root = tk.Tk()
    root.title("千瓜品牌投放数据抓取 - 选择配置")
    root.geometry("800x700")
    root.configure(bg='#f5f5f5')
    root.protocol("WM_DELETE_WINDOW", on_close)

    # 标题
    title_frame = tk.Frame(root, bg='#2196F3', height=80)
    title_frame.pack(fill='x')
    title_frame.pack_propagate(False)

    title_label = tk.Label(
        title_frame,
        text="📊 千瓜品牌投放数据抓取工具",
        font=("Microsoft YaHei UI", 18, "bold"),
        bg='#2196F3',
        fg='white'
    )
    title_label.pack(pady=20)

    # 时间范围选择
    time_frame = tk.Frame(root, bg='#f5f5f5')
    time_frame.pack(pady=15, fill='x', padx=30)

    time_label = tk.Label(
        time_frame,
        text="⏰ 选择时间范围 (单选):",
        font=("Microsoft YaHei UI", 12, "bold"),
        bg='#f5f5f5',
        fg='#333'
    )
    time_label.pack(anchor='w', pady=(0, 10))

    time_button_frame = tk.Frame(time_frame, bg='#f5f5f5')
    time_button_frame.pack(fill='x')

    time_range_buttons = {}
    for idx, tr in enumerate(time_ranges):
        btn = tk.Button(
            time_button_frame,
            text=tr,
            width=12,
            height=2,
            font=("Microsoft YaHei UI", 10),
            bg='#E3F2FD',
            fg='black',
            relief='raised',
            bd=2,
            cursor='hand2',
            activebackground='#90CAF9'
        )
        btn.config(command=lambda t=tr, b=btn: on_time_range_select(t, b))
        btn.pack(side='left', padx=5)
        time_range_buttons[tr] = btn

    # 标签选择
    category_frame = tk.Frame(root, bg='#f5f5f5')
    category_frame.pack(pady=15, fill='both', expand=True, padx=30)

    category_label = tk.Label(
        category_frame,
        text="🏷️ 选择标签 (多选，点击顺序即为查询顺序):",
        font=("Microsoft YaHei UI", 12, "bold"),
        bg='#f5f5f5',
        fg='#333'
    )
    category_label.pack(anchor='w', pady=(0, 10))

    # 创建带滚动条的容器
    canvas_container = tk.Frame(category_frame, bg='white')
    canvas_container.pack(fill='both', expand=True)

    # 创建Canvas和滚动条
    canvas = tk.Canvas(canvas_container, bg='white', highlightthickness=0)
    scrollbar = tk.Scrollbar(canvas_container, orient='vertical', command=canvas.yview)

    # 创建可滚动的Frame
    category_button_frame = tk.Frame(canvas, bg='white')

    # 配置Canvas
    canvas.configure(yscrollcommand=scrollbar.set)

    # 放置滚动条和Canvas
    scrollbar.pack(side='right', fill='y')
    canvas.pack(side='left', fill='both', expand=True)

    # 在Canvas中创建窗口
    canvas_window = canvas.create_window((0, 0), window=category_button_frame, anchor='nw')

    # 更新滚动区域
    def update_scrollregion(event=None):
        canvas.configure(scrollregion=canvas.bbox('all'))

    category_button_frame.bind('<Configure>', update_scrollregion)

    # 鼠标滚轮滚动支持（只在canvas区域生效）
    def on_mousewheel(event):
        canvas.yview_scroll(int(-1*(event.delta/120)), "units")

    # 绑定到canvas和其内部的widget
    canvas.bind("<MouseWheel>", on_mousewheel)
    category_button_frame.bind("<MouseWheel>", on_mousewheel)

    cols = 4
    for idx, cat in enumerate(all_categories):
        row = idx // cols
        col = idx % cols
        btn = tk.Button(
            category_button_frame,
            text=cat,
            width=14,
            height=2,
            font=("Microsoft YaHei UI", 10),
            bg='#E3F2FD',
            fg='black',
            relief='raised',
            bd=2,
            cursor='hand2',
            activebackground='#90CAF9'
        )
        btn.config(command=lambda c=cat, b=btn: on_category_click(c, b))
        btn.grid(row=row, column=col, padx=8, pady=8)
        category_buttons[cat] = btn

    # 已选择列表
    selected_frame = tk.Frame(root, bg='#f5f5f5')
    selected_frame.pack(pady=10, fill='x', padx=30)

    selected_label = tk.Label(
        selected_frame,
        text="✓ 已选择列表:",
        font=("Microsoft YaHei UI", 10, "bold"),
        bg='#f5f5f5',
        fg='#333'
    )
    selected_label.pack(anchor='w', pady=(0, 5))

    # 创建文本框和滚动条的容器
    text_container = tk.Frame(selected_frame, bg='#FFF9C4')
    text_container.pack(fill='x')

    # 添加垂直滚动条
    scrollbar = tk.Scrollbar(text_container)
    scrollbar.pack(side='right', fill='y')

    selected_text = tk.Text(
        text_container,
        height=3,
        state='disabled',
        font=("Microsoft YaHei UI", 9),
        bg='#FFF9C4',
        relief='solid',
        bd=1,
        wrap='word',
        yscrollcommand=scrollbar.set
    )
    selected_text.pack(side='left', fill='both', expand=True)
    scrollbar.config(command=selected_text.yview)

    update_selected_list()

    # 底部按钮
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

    # 居中
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    root.mainloop()

    return selected_time_range, selected_categories


class QianguaMcnRankSpider:
    def __init__(self, time_range, selected_categories):
        self.setup_logger()
        self.base_url = "https://app.qian-gua.com"
        self.mcn_rank_url = "https://app.qian-gua.com/#/data/brand"
        self.is_logged_in = False
        self.api_data = {}
        self.time_range = time_range  # 用户选择的时间范围
        self.selected_categories = selected_categories  # 用户选择的标签

        # 获取exe文件所在目录或脚本所在目录
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        self.cookie_file = os.path.join(base_dir, 'cookies.json')
        self.setup_browser()

    """设置日志"""

    def setup_logger(self):
        # 获取exe文件所在目录或脚本所在目录
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        log_path = os.path.join(base_dir, 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(os.path.join(log_path, "qiangua_brand_{time}.log"), rotation="1 day", retention="7 days")

    def human_delay(self, min_sec=None, max_sec=None):
        """模拟人工延迟,避免频繁操作"""
        try:
            min_delay = min_sec if min_sec is not None else 0.8
            max_delay = max_sec if max_sec is not None else 1.8
            if max_delay < min_delay:
                min_delay, max_delay = max_delay, min_delay
            delay = random.uniform(min_delay, max_delay)
            time.sleep(delay)
        except Exception as e:
            time.sleep(1)

    """初始化浏览器"""

    def setup_browser(self):
        self.playwright = sync_playwright().start()
        # 使用小红书的chrome_user_data目录，共享登录状态

        # 获取exe文件所在目录或脚本所在目录
        if getattr(sys, 'frozen', False):
            current_dir = os.path.dirname(sys.executable)
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))

        # 获取父目录（command目录）
        parent_dir = os.path.dirname(current_dir)
        user_data_dir = os.path.join(parent_dir, 'xiaohongshu_notes', 'chrome_user_data')
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

            # 等待用户手动完成滑块验证和登录,最多等待60秒
            logger.info("等待用户手动完成滑块验证和登录(最多等待60秒)...")
            wait_time = 0
            max_wait_time = 300

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
                # 拦截关键接口
                api_name = None
                if 'GetBrandDataStat' in url:
                    api_name = 'GetBrandDataStat'
                elif 'GetBrandRankCategorys' in url:
                    api_name = 'GetBrandRankCategorys'

                if api_name and response.status == 200:
                    try:
                        response_data = response.json()
                        timestamp = str(int(time.time() * 1000))

                        # 保存API数据
                        if api_name not in self.api_data:
                            self.api_data[api_name] = []

                        self.api_data[api_name].append({
                            'url': url,
                            'data': response_data,
                            'timestamp': timestamp,
                            'processed': False
                        })

                        logger.info(f"捕获{api_name}接口")

                    except Exception as e:
                        logger.error(f"解析{api_name}接口响应数据时出错: {str(e)}")
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
        """点击商业投放"""
        try:
            logger.info("点击商业投放...")
            clicked = self.page.evaluate('''
                () => {
                    const elements = Array.from(document.querySelectorAll('span'));
                    for (const element of elements) {
                        if (element.textContent.trim() === '商业投放') {
                            element.click();
                            return true;
                        }
                    }
                    return false;
                }
            ''')

            if clicked:
                logger.info("成功点击商业投放")
                self.human_delay(1.5, 2.5)
                self.page.wait_for_load_state('networkidle', timeout=10000)
                return True
            else:
                logger.error("未找到商业投放按钮")
                return False
        except Exception as e:
            logger.error(f"点击商业投放时出错: {str(e)}")
            return False

    def click_time_range(self):
        """点击用户选择的时间范围并获取默认选中标签的数据"""
        try:
            logger.info(f"点击时间范围: {self.time_range}")

            # 提取期望的天数
            import re
            days_match = re.search(r'近(\d+)天', self.time_range)
            expected_days = days_match.group(1) if days_match else None

            # 清空API数据
            self.api_data['GetBrandDataStat'] = []
            self.api_data['GetBrandRankCategorys'] = []

            # 点击时间范围按钮
            clicked = False
            try:
                # 策略1：使用文本内容查找并点击
                element = self.page.get_by_text(self.time_range, exact=True).first
                if element.count() > 0:
                    element.click(force=True)
                    clicked = True
                else:
                    # 策略2：使用XPath
                    xpath = f"//span[contains(text(), '{self.time_range}')]"
                    elements = self.page.locator(xpath)
                    if elements.count() > 0:
                        for i in range(elements.count()):
                            try:
                                elem = elements.nth(i)
                                if elem.is_visible():
                                    elem.click(force=True)
                                    clicked = True
                                    break
                            except:
                                continue

                if not clicked:
                    # 策略3：使用evaluate
                    clicked = self.page.evaluate(f'''
                        () => {{
                            const elements = Array.from(document.querySelectorAll('span'));
                            for (const element of elements) {{
                                if (element.textContent.trim() === '{self.time_range}') {{
                                    const parent = element.closest('.el-radio-button, .el-button, button, [role="button"]');
                                    if (parent) {{
                                        parent.click();
                                        return true;
                                    }}
                                }}
                            }}
                            return false;
                        }}
                    ''')

            except Exception as e:
                logger.error(f"点击过程出错: {str(e)}")
                clicked = False

            if not clicked:
                logger.error(f"未能点击时间范围按钮: {self.time_range}")
                return None

            logger.info(f"成功点击时间范围: {self.time_range}")

            # 使用 page.wait_for_event() 等待API响应
            stat_received = False
            rank_received = False

            try:
                self.page.wait_for_event(
                    'response',
                    timeout=15000,
                    predicate=lambda response: (
                        'GetBrandDataStat' in response.url
                        and response.request.resource_type in ('xhr', 'fetch')
                        and (f'days={expected_days}' in response.url if expected_days else True)
                    )
                )
                stat_received = True
            except PlaywrightTimeoutError:
                logger.warning("等待GetBrandDataStat接口超时")

            try:
                self.page.wait_for_event(
                    'response',
                    timeout=15000,
                    predicate=lambda response: (
                        'GetBrandRankCategorys' in response.url
                        and response.request.resource_type in ('xhr', 'fetch')
                        and (f'days={expected_days}' in response.url if expected_days else True)
                    )
                )
                rank_received = True
            except PlaywrightTimeoutError:
                logger.warning("等待GetBrandRankCategorys接口超时")

            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            self.human_delay(1.0, 2.0)

            # 如果未通过wait_for_event捕获，fallback等待一下
            if not (stat_received and rank_received):
                time.sleep(2)

            # 获取数据
            stat_data = None
            rank_data = None

            if len(self.api_data.get('GetBrandDataStat', [])) > 0:
                # 优先使用匹配days的数据
                if expected_days:
                    matched = [e for e in self.api_data['GetBrandDataStat']
                              if f'days={expected_days}' in e.get('url', '')]
                    if matched:
                        stat_data = matched[-1]
                    else:
                        stat_data = self.api_data['GetBrandDataStat'][-1]
                else:
                    stat_data = self.api_data['GetBrandDataStat'][-1]
            else:
                logger.error("未捕获GetBrandDataStat数据")

            if len(self.api_data.get('GetBrandRankCategorys', [])) > 0:
                # 优先使用匹配days的数据
                if expected_days:
                    matched = [e for e in self.api_data['GetBrandRankCategorys']
                              if f'days={expected_days}' in e.get('url', '')]
                    if matched:
                        rank_data = matched[-1]
                    else:
                        rank_data = self.api_data['GetBrandRankCategorys'][-1]
                else:
                    rank_data = self.api_data['GetBrandRankCategorys'][-1]
            else:
                logger.error("未捕获GetBrandRankCategorys数据")

            if stat_data and rank_data:
                return {'stat': stat_data, 'rank': rank_data}
            else:
                logger.error(f"数据不完整: stat={'有' if stat_data else '无'}, rank={'有' if rank_data else '无'}")
                return None

        except Exception as e:
            logger.error(f"点击时间范围时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def click_category_and_get_data(self, category_name):
        """点击指定标签并获取两个接口的数据"""
        try:
            logger.info(f"点击标签: {category_name}")

            # 清空API数据
            self.api_data['GetBrandDataStat'] = []
            self.api_data['GetBrandRankCategorys'] = []

            # 点击标签
            clicked = self.page.evaluate(f'''
                () => {{
                    const elements = Array.from(document.querySelectorAll('span'));
                    for (const element of elements) {{
                        if (element.textContent.trim() === '{category_name}') {{
                            element.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            ''')

            if not clicked:
                logger.error(f"未找到标签: {category_name}")
                return None

            # 使用 page.wait_for_event() 等待API响应
            stat_received = False
            rank_received = False

            try:
                self.page.wait_for_event(
                    'response',
                    timeout=15000,
                    predicate=lambda response: (
                        'GetBrandDataStat' in response.url
                        and response.request.resource_type in ('xhr', 'fetch')
                    )
                )
                stat_received = True
            except PlaywrightTimeoutError:
                logger.warning(f"{category_name} - 等待GetBrandDataStat接口超时")

            try:
                self.page.wait_for_event(
                    'response',
                    timeout=15000,
                    predicate=lambda response: (
                        'GetBrandRankCategorys' in response.url
                        and response.request.resource_type in ('xhr', 'fetch')
                    )
                )
                rank_received = True
            except PlaywrightTimeoutError:
                logger.warning(f"{category_name} - 等待GetBrandRankCategorys接口超时")

            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            self.human_delay(0.8, 1.5)

            # 如果未通过wait_for_event捕获，fallback等待一下
            if not (stat_received and rank_received):
                time.sleep(2)

            # 获取数据
            stat_data = None
            rank_data = None

            if len(self.api_data.get('GetBrandDataStat', [])) > 0:
                stat_data = self.api_data['GetBrandDataStat'][-1]
            else:
                logger.error(f"{category_name} - 未捕获GetBrandDataStat数据")

            if len(self.api_data.get('GetBrandRankCategorys', [])) > 0:
                rank_data = self.api_data['GetBrandRankCategorys'][-1]
            else:
                logger.error(f"{category_name} - 未捕获GetBrandRankCategorys数据")

            if stat_data and rank_data:
                logger.info(f"成功获取 {category_name} 的数据")
                return {'stat': stat_data, 'rank': rank_data}
            else:
                logger.error(f"{category_name} - 数据不完整")
                return None

        except Exception as e:
            logger.error(f"点击标签 {category_name} 时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def process_all_categories(self, default_category_name, default_data):
        """依次点击所有用户选择的标签并获取数据"""
        try:
            # 存储所有标签数据（使用列表保持顺序）
            all_category_data = {}

            # 按照用户选择的顺序处理每个标签
            for category in self.selected_categories:
                logger.info(f"开始处理标签: {category}")

                # 如果是默认选中的标签，直接使用已获取的数据，不需要再点击
                if category == default_category_name and default_data:
                    all_category_data[category] = default_data
                    logger.info(f"使用默认选中标签 {category} 的数据")
                else:
                    # 其他标签需要点击获取数据
                    data = self.click_category_and_get_data(category)

                    if data:
                        all_category_data[category] = data
                        logger.info(f"成功获取 {category} 的数据")
                    else:
                        logger.warning(f"未能获取 {category} 的数据")

                    # 等待一段时间再点击下一个标签
                    self.human_delay(2, 3)

            logger.info(f"所有标签处理完成,共获取 {len(all_category_data)} 个标签的数据")
            return all_category_data

        except Exception as e:
            logger.error(f"处理所有标签时出错: {str(e)}")
            return {}

    def export_to_excel(self, all_category_data):
        """将数据导出为Excel"""
        try:
            if not all_category_data:
                logger.warning("没有数据可以导出")
                return None

            # 准备数据列表
            data_list = []

            for category, api_responses in all_category_data.items():
                try:
                    # api_responses现在是一个字典: {'stat': ..., 'rank': ...}
                    stat_response = api_responses.get('stat')
                    rank_response = api_responses.get('rank')

                    # 检查是否有有效的stat_response
                    if not stat_response:
                        logger.warning(f"标签 {category} 没有GetBrandDataStat响应数据，跳过")
                        continue

                    # 从GetBrandDataStat接口提取数据
                    stat_data = stat_response.get('data', {})
                    cur_data = stat_data.get('Data', {}).get('CurData', {})

                    if not cur_data:
                        logger.warning(f"标签 {category} 没有GetBrandDataStat的CurData数据")
                        continue

                    # 从GetBrandRankCategorys接口提取数据（根据Name匹配）
                    if not rank_response:
                        logger.warning(f"标签 {category} 没有GetBrandRankCategorys响应数据")
                        rank_data = {}
                        ranks_list = []
                    else:
                        rank_data = rank_response.get('data', {})
                        ranks_list = rank_data.get('Data', {}).get('Ranks', [])

                    # 查找匹配的标签数据
                    matched_rank = None
                    for rank_item in ranks_list:
                        if rank_item.get('Name') == category:
                            matched_rank = rank_item
                            break

                    # 获取商业笔记数和占比
                    rank_count = matched_rank.get('Count', 0) if matched_rank else 0
                    rank_percent = matched_rank.get('Percent', 0) if matched_rank else 0

                    # 按照字段映射提取数据
                    row_data = {
                        '赛道': category,
                        '参投品牌数': cur_data.get('BrandCount', 0),
                        '品牌周增幅(%)': round(cur_data.get('BrandCountChange', 0) * 100, 2),
                        '投放费用总数': cur_data.get('Amount', 0),
                        '投放周增幅(%)': round(cur_data.get('AmountChange', 0) * 100, 2),
                        '商业笔记数(排行)': rank_count,
                        '商业笔记数占比(%)': round(rank_percent, 2),
                        '商业笔记数': cur_data.get('NoteCount', 0),
                        '商业笔记周增幅(%)': round(cur_data.get('NoteCountChange', 0) * 100, 2),
                        '爆文总数': cur_data.get('HotNoteCount', 0),
                        '爆文总数周增幅(%)': round(cur_data.get('HotNoteCountChange', 0) * 100, 2),
                        '平均阅读数': cur_data.get('AvgView', 0),
                        '阅读数周增幅(%)': round(cur_data.get('AvgViewChange', 0) * 100, 2),
                        '平均互动数': cur_data.get('AvgLcc', 0),
                        '互动数周增幅(%)': round(cur_data.get('AvgLccChange', 0) * 100, 2)
                    }

                    data_list.append(row_data)

                except Exception as e:
                    logger.error(f"解析标签 {category} 数据时出错: {str(e)}")
                    continue

            if not data_list:
                logger.warning("没有成功解析的数据")
                return None

            # 创建DataFrame
            df = pd.DataFrame(data_list)

            # 生成文件名(带时间戳)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_filename = f'品牌投放大盘数据_{timestamp}.xlsx'

            # 获取exe文件所在目录或脚本所在目录
            if getattr(sys, 'frozen', False):
                base_dir = os.path.dirname(sys.executable)
            else:
                base_dir = os.path.dirname(os.path.abspath(__file__))

            excel_path = os.path.join(base_dir, excel_filename)

            # 导出为Excel
            df.to_excel(excel_path, index=False, engine='openpyxl')
            logger.info(f"成功导出Excel文件: {excel_path}")
            logger.info(f"共导出 {len(data_list)} 条数据")

            return excel_path

        except Exception as e:
            logger.error(f"导出Excel时出错: {str(e)}")
            return None

    def get_belong_mcn(self):
        """从GetMcnDetail接口数据中获取BelongMcn"""
        try:
            detail_entries = self.api_data.get('GetMcnDetail', [])
            if not detail_entries:
                logger.warning("未找到GetMcnDetail接口数据")
                return None

            # 获取最新的一条数据
            latest_entry = detail_entries[-1]
            response_data = latest_entry.get('data') or {}
            belong_mcn = (response_data.get('Data') or {}).get('BelongMcn')

            if belong_mcn:
                logger.info(f"成功获取BelongMcn: {belong_mcn}")
                return belong_mcn
            else:
                logger.warning("GetMcnDetail接口数据中未找到BelongMcn字段")
                return None
        except Exception as e:
            logger.error(f"获取BelongMcn时出错: {str(e)}")
            return None

    def click_cooperation_brand(self):
        """点击商业合作标签"""
        try:
            logger.info("点击商业合作...")

            # 清空之前的API数据
            if 'GetMcnNoteList' in self.api_data:
                self.api_data['GetMcnNoteList'] = []

            # 使用正确的选择器点击商业合作tab
            clicked = self.page.evaluate('''
                () => {
                    // 查找商业合作tab: div.el-tabs__nav-wrap.is-top -> .el-tabs__nav-scroll -> [role="tablist"] -> #tab-business
                    const brandTab = document.querySelector('.el-tabs__nav-wrap.is-top .el-tabs__nav-scroll [role="tablist"] #tab-business');
                    if (brandTab) {
                        brandTab.click();
                        return true;
                    }
                    return false;
                }
            ''')

            if clicked:
                logger.info("成功点击商业合作")
                self.human_delay(1.5, 2.5)
                self.page.wait_for_load_state('networkidle', timeout=10000)
                return True
            else:
                logger.error("未找到商业合作按钮")
                return False
        except Exception as e:
            logger.error(f"点击商业合作时出错: {str(e)}")
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

                    const panesBusiness = tabsContent.querySelector('#pane-business');
                    if (!panesBusiness) {
                        return {success: false, message: '未找到#pane-business'};
                    }

                    const imgPermissionWrapper = panesBusiness.querySelector('.img-permission-wrapper');
                    if (!imgPermissionWrapper) {
                        return {success: false, message: '未找到.img-permission-wrapper'};
                    }

                    const datePickerWrapper = imgPermissionWrapper.querySelector('.date-picker.range-picker-wrapper');
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
                        self.page.click(
                            '.el-picker-panel__content.el-date-range-picker__content.is-right .el-date-range-picker__header .el-picker-panel__icon-btn.el-icon-arrow-right')
                    else:
                        # 需要向左切换(过去的月份)
                        logger.info("点击左侧箭头切换到上一个月")
                        self.page.click(
                            '.el-picker-panel__content.el-date-range-picker__content.is-left .el-date-range-picker__header .el-picker-panel__icon-btn.el-icon-arrow-left')

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

    def scroll_to_load_notes(self, max_records):
        """滚动加载笔记数据"""
        try:
            logger.info(f"开始滚动加载笔记数据,最多加载 {max_records} 条...")

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

            # 清空之前可能触发的GetMcnNoteList接口数据
            if 'GetMcnNoteList' in self.api_data:
                self.api_data['GetMcnNoteList'] = []

            # 在指定位置持续滚轮滚动，直到GetMcnNoteList接口出现
            logger.info("移动鼠标到笔记列表位置(931, 575)并滚动，等待接口响应...")
            self.page.mouse.move(931, 575)
            time.sleep(0.5)

            max_scroll_attempts = 20  # 最多滚动20次
            scroll_attempt = 0

            while scroll_attempt < max_scroll_attempts:
                # 向下滚动鼠标滚轮
                self.page.mouse.wheel(0, 1000)  # 增大滚动量
                scroll_attempt += 1
                logger.info(f"第 {scroll_attempt} 次鼠标滚轮滚动...")
                time.sleep(1)

                # 检查是否有GetMcnNoteList接口响应
                if 'GetMcnNoteList' in self.api_data and len(self.api_data['GetMcnNoteList']) > 0:
                    logger.info(f"检测到GetMcnNoteList接口响应，停止鼠标滚轮滚动")
                    break

            if scroll_attempt >= max_scroll_attempts:
                logger.warning("鼠标滚轮滚动达到最大次数，但未检测到接口响应")

            # 然后使用鼠标滚轮滚动笔记列表
            logger.info("开始使用鼠标滚轮滚动笔记列表...")

            # 先获取初始已有的数据条数
            initial_count = self.page.evaluate('''
                () => {
                    return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                }
            ''')
            logger.info(f"初始已有 {initial_count} 条笔记数据")

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
                    logger.info(
                        f"当前已加载 {new_count} 条笔记数据 (初始 {initial_count} + 新增 {new_count - initial_count})")
                    prev_count = new_count

                if prev_count >= max_records:
                    logger.info(f"已达到最大记录数 {max_records},停止加载")
                    break

            # 统计本次滚动总共获取的API数据
            total_api_count = len(self.api_data.get('GetMcnNoteList', []))
            logger.info(f"滚动完成,共滚动 {scroll_count} 次,获取 {total_api_count} 次GetMcnNoteList接口数据")

            final_count = self.page.evaluate('''
                () => {
                    return document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length;
                }
            ''')

            logger.info(f"笔记数据加载完成,共 {final_count} 条")
            return final_count
        except Exception as e:
            logger.error(f"滚动加载笔记数据时出错: {str(e)}")
            return 0

    def click_note_original_links(self):
        """点击每条笔记的原文按钮并获取URL - 返回标题到URL的映射字典"""
        try:
            logger.info("开始点击笔记原文按钮...")

            # 获取所有笔记卡片
            note_cards_count = self.page.evaluate('''
                () => {
                    const noteCards = document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom');
                    return noteCards.length;
                }
            ''')

            logger.info(f"共找到 {note_cards_count} 条笔记")

            note_url_map = {}  # 存储标题到URL的映射

            for index in range(note_cards_count):
                try:
                    logger.info(f"处理第 {index + 1}/{note_cards_count} 条笔记")

                    # 先提取笔记标题（使用准确的DOM路径）
                    note_title = self.page.evaluate(f'''
                        () => {{
                            const noteCards = document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom');
                            if (noteCards.length <= {index}) {{
                                console.log('索引超出范围');
                                return null;
                            }}

                            const card = noteCards[{index}];
                            const listRow = card.querySelector('.list-row');
                            if (!listRow) {{
                                console.log('未找到list-row');
                                return null;
                            }}

                            // 第1个div
                            const rowDivs = listRow.querySelectorAll(':scope > div');
                            if (rowDivs.length < 1) {{
                                console.log('rowDivs数量不足1个');
                                return null;
                            }}

                            const firstDiv = rowDivs[0];
                            const colCell = firstDiv.querySelector('.col-cell');
                            if (!colCell) {{
                                console.log('未找到col-cell');
                                return null;
                            }}

                            // 按照准确路径查找标题
                            const noteUserWrap = colCell.querySelector('.common-note-user-wrap');
                            if (!noteUserWrap) {{
                                console.log('未找到common-note-user-wrap');
                                return null;
                            }}

                            const userContainer = noteUserWrap.querySelector('.user-container.note-user-wrapper');
                            if (!userContainer) {{
                                console.log('未找到user-container');
                                return null;
                            }}

                            const userInfo = userContainer.querySelector('.user-info');
                            if (!userInfo) {{
                                console.log('未找到user-info');
                                return null;
                            }}

                            const name = userInfo.querySelector('.name');
                            if (!name) {{
                                console.log('未找到name');
                                return null;
                            }}

                            const noteTitleWrapper = name.querySelector('.note-title-wrapper');
                            if (!noteTitleWrapper) {{
                                console.log('未找到note-title-wrapper');
                                return null;
                            }}

                            const titleSpan = noteTitleWrapper.querySelector('span.title');
                            if (!titleSpan) {{
                                console.log('未找到span.title');
                                return null;
                            }}

                            return titleSpan.textContent.trim();
                        }}
                    ''')

                    if not note_title:
                        logger.warning(f"第 {index + 1} 条笔记未找到标题，跳过")
                        continue

                    # 标准化标题用于匹配
                    normalized_title = self.normalize_title(note_title)
                    logger.info(f"第 {index + 1} 条笔记标题: {note_title[:50]}...")
                    logger.debug(f"标准化后: {normalized_title[:50]}...")

                    # 等待新页面打开（先开始监听popup，再点击）
                    try:
                        # 使用context manager捕获popup
                        with self.page.expect_popup(timeout=15000) as popup_info:
                            # 在监听过程中点击原文按钮
                            clicked = self.page.evaluate(f'''
                                () => {{
                                    const noteCards = document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom');
                                    if (noteCards.length <= {index}) {{
                                        console.log('索引超出范围');
                                        return false;
                                    }}

                                    const card = noteCards[{index}];
                                    const listRow = card.querySelector('.list-row');
                                    if (!listRow) {{
                                        console.log('未找到list-row');
                                        return false;
                                    }}

                                    // 获取所有子div
                                    const rowDivs = listRow.querySelectorAll(':scope > div');
                                    if (rowDivs.length < 5) {{
                                        console.log('rowDivs数量不足5个:', rowDivs.length);
                                        return false;
                                    }}

                                    const fifthDiv = rowDivs[4];  // 第5个div，索引为4
                                    const colCell = fifthDiv.querySelector('.col-cell');
                                    if (!colCell) {{
                                        console.log('未找到col-cell');
                                        return false;
                                    }}

                                    const flexColumn = colCell.querySelector('.flex-column');
                                    if (!flexColumn) {{
                                        console.log('未找到flex-column');
                                        return false;
                                    }}

                                    const spans = flexColumn.querySelectorAll(':scope > span');
                                    if (spans.length < 2) {{
                                        console.log('spans数量不足2个:', spans.length);
                                        return false;
                                    }}

                                    const secondSpan = spans[1];  // 第2个span，索引为1
                                    const referenceWrapper = secondSpan.querySelector('.el-popover__reference-wrapper');
                                    if (!referenceWrapper) {{
                                        console.log('未找到el-popover__reference-wrapper');
                                        return false;
                                    }}

                                    const link = referenceWrapper.querySelector('a.text-link');
                                    if (!link) {{
                                        console.log('未找到原文链接');
                                        return false;
                                    }}

                                    link.click();
                                    console.log('成功点击原文按钮');
                                    return true;
                                }}
                            ''')

                        # 检查是否点击成功
                        if not clicked:
                            logger.warning(f"第 {index + 1} 条笔记未找到原文按钮，跳过")
                            continue

                        logger.info(f"成功点击第 {index + 1} 条笔记的原文按钮，等待页面打开...")
                        new_page = popup_info.value

                        # 等待新页面完全加载（确保获取最终URL）
                        logger.info("等待小红书页面完全加载...")
                        try:
                            new_page.wait_for_load_state('load', timeout=20000)
                            self.human_delay(1, 2)
                            logger.info("页面加载完成")
                        except PlaywrightTimeoutError:
                            logger.warning(f"第 {index + 1} 条笔记页面加载超时，但继续获取URL")
                            # 即使超时，页面也已经打开，URL也应该可用，继续执行
                            self.human_delay(1, 2)

                        # 检查小红书登录状态，如果未登录则先登录
                        if not self.xhs_logged_in:
                            logger.info("检查小红书登录状态...")
                            if not self.check_xiaohongshu_login_status(new_page):
                                logger.info("小红书未登录，开始登录流程...")
                                if not self.xiaohongshu_login(new_page):
                                    logger.error("小红书登录失败，关闭当前页面，跳过此笔记")
                                    new_page.close()
                                    self.human_delay(2, 3)
                                    continue
                                logger.info("小红书登录成功，继续获取笔记URL")
                            else:
                                logger.info("小红书已登录，继续获取笔记URL")
                        else:
                            logger.info("小红书已登录（使用缓存状态），继续获取笔记URL")

                        # 获取新页面的URL（小红书笔记URL，此时已是最终URL）
                        note_url = new_page.url
                        logger.info(f"✓ 获取到小红书笔记URL: {note_url}")

                        # 添加到映射字典中（使用标准化标题作为key）
                        note_url_map[normalized_title] = note_url
                        logger.debug(f"保存映射: {normalized_title[:30]}... -> {note_url}")

                        # 在页面停留10秒左右，模拟真实浏览行为
                        logger.info("在小红书页面停留10秒左右，模拟真实浏览...")
                        self.human_delay(8, 12)

                        # 关闭新页面
                        new_page.close()
                        logger.info(f"已关闭第 {index + 1} 条笔记的原文页面")

                        # 增加延迟时间，避免小红书检测为异常（每20-30秒点击一次）
                        logger.info("等待20-30秒再点击下一条，避免触发小红书异常检测...")
                        self.human_delay(20, 30)

                    except PlaywrightTimeoutError:
                        logger.warning(f"第 {index + 1} 条笔记原文页面打开超时")
                        continue
                    except Exception as e:
                        logger.error(f"处理第 {index + 1} 条笔记原文时出错: {str(e)}")
                        continue

                except Exception as e:
                    logger.error(f"点击第 {index + 1} 条笔记原文按钮时出错: {str(e)}")
                    continue

            logger.info(f"原文链接获取完成，共获取 {len(note_url_map)} 条有效URL")

            # 打印映射表摘要（用于调试）
            if note_url_map:
                logger.info("映射表摘要（前5条）:")
                for i, (title_key, url) in enumerate(list(note_url_map.items())[:5]):
                    logger.info(f"  {i+1}. {title_key[:50]}... -> {url[:60]}...")
            else:
                logger.warning("未获取到任何笔记URL映射！")

            return note_url_map

        except Exception as e:
            logger.error(f"点击笔记原文按钮时出错: {str(e)}")
            return {}

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

            # 获取机构列表数量(最多5条)
            mcn_count = self.page.evaluate('''
                () => {
                    return Math.min(document.querySelectorAll('.list-bd.page-component__scroll .item-border-bottom').length, 5);
                }
            ''')

            logger.info(f"找到 {mcn_count} 个机构,将处理前5个")

            # 循环处理每个机构
            for mcn_index in range(min(mcn_count, 5)):
                logger.info(f"处理第 {mcn_index + 1}/{mcn_count} 个机构")
                if mcn_index != 2:
                    continue

                # 点击机构(会触发GetMcnDetail接口)
                if not self.click_mcn_item(mcn_index):
                    logger.error(f"点击第 {mcn_index + 1} 个机构失败")
                    continue

                # 获取BelongMcn
                belong_mcn = self.get_belong_mcn()
                if not belong_mcn:
                    logger.error(f"未能获取第 {mcn_index + 1} 个机构的BelongMcn，使用搜索名称")
                    belong_mcn = org_name

                # 点击商业合作
                if not self.click_cooperation_brand():
                    logger.error("点击商业合作失败")
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

                    # 滚动加载笔记数据
                    note_count = self.scroll_to_load_notes(self.max_note_records)

                    if note_count == 0:
                        logger.warning(f"{year_month} 没有加载到笔记数据")
                        continue

                    # 点击每条笔记的原文按钮并获取URL（返回标题到URL的映射字典）
                    note_url_map = self.click_note_original_links()

                    # 保存笔记数据到数据库（使用标题匹配URL）
                    self.save_note_data_to_db(belong_mcn, year_month, note_url_map)

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
        """抓取品牌投放大盘数据"""
        try:
            # 访问品牌投放大盘页面
            logger.info("开始访问品牌投放大盘页面...")
            self.page.goto(self.mcn_rank_url)

            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            time.sleep(3)

            # 关闭可能的弹出框
            self.close_popups()

            # 点击商业投放
            if not self.click_business_income_tab():
                logger.error("点击商业投放失败")
                return

            # 点击用户选择的时间范围并获取默认选中标签的数据
            default_data = self.click_time_range()
            if not default_data:
                logger.error(f"点击时间范围 {self.time_range} 失败或未获取到数据")
                return

            # 获取默认选中的标签名称(通过页面元素判断)
            default_category = self.page.evaluate('''
                () => {
                    // 查找被选中的标签(通常有active、selected等class)
                    const activeTab = document.querySelector('.el-radio-button.is-active .el-radio-button__inner');
                    if (activeTab) {
                        return activeTab.textContent.trim();
                    }
                    // 如果没找到，返回默认值
                    return '彩妆';
                }
            ''')

            logger.info(f"默认选中的标签: {default_category}")
            logger.info(f"用户选择的标签列表: {self.selected_categories}")

            # 依次点击用户选择的所有标签并获取数据
            all_category_data = self.process_all_categories(default_category, default_data)

            # 打印获取到的所有标签数据
            logger.info(f"成功获取 {len(all_category_data)} 个标签的数据:")
            for category in all_category_data.keys():
                logger.info(f"  - {category}")

            # 导出为Excel
            excel_path = self.export_to_excel(all_category_data)
            if excel_path:
                logger.info(f"数据已成功导出到: {excel_path}")
            else:
                logger.error("导出Excel失败")

            logger.info("所有标签数据获取完成")
        except Exception as e:
            logger.error(f"抓取数据时出错: {str(e)}")

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
    # 尝试加载上次的选择
    last_time_range, last_categories = load_last_selection()

    # 如果有上次的选择，询问是否使用
    time_range = None
    selected_categories = []

    if last_time_range and last_categories:
        use_last = show_use_last_selection_dialog(last_time_range, last_categories)
        if use_last:
            time_range = last_time_range
            selected_categories = last_categories
            logger.info(f"使用上次选择: 时间范围={time_range}, 标签={selected_categories}")

    # 如果没有使用上次的选择，显示选择对话框
    if not time_range or not selected_categories:
        time_range, selected_categories = show_selection_dialog()
        logger.info(f"新选择: 时间范围={time_range}, 标签={selected_categories}")

        # 保存本次选择
        save_last_selection(time_range, selected_categories)
        logger.info("已保存本次选择")

    # 创建爬虫实例并运行
    spider = QianguaMcnRankSpider(time_range, selected_categories)
    spider.run()

