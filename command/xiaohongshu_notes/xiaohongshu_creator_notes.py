import json
import os
import sys
import time
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from loguru import logger
import requests
import tkinter as tk
from tkinter import messagebox

"""
    小红书创作者平台笔记爬虫
    抓取最近14天内的笔记数据
"""


# 配置文件路径
CONFIG_FILE = 'last_notes_selection.json'


def load_last_selection():
    """加载上次的选择配置"""
    try:
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        config_file = os.path.join(base_dir, CONFIG_FILE)
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {
                    'scroll_days': data.get('scroll_days', 31),
                    'fetch_all': data.get('fetch_all', False)
                }
    except Exception as e:
        print(f"加载上次选择失败: {e}")
    return {'scroll_days': 31, 'fetch_all': False}


def save_selection(scroll_days, fetch_all):
    """保存本次选择配置"""
    try:
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        config_file = os.path.join(base_dir, CONFIG_FILE)
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump({
                'scroll_days': scroll_days,
                'fetch_all': fetch_all
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存选择失败: {e}")


def show_use_last_selection_dialog():
    """显示是否使用上次选择的对话框"""
    last_config = load_last_selection()

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

    result = {'use_last': False}

    root = tk.Tk()
    root.title("小红书创作者笔记 - 使用上次选择")
    root.geometry("600x450")
    root.configure(bg='#f5f5f5')
    root.protocol("WM_DELETE_WINDOW", on_close)

    # 标题
    title_frame = tk.Frame(root, bg='#FF2442', height=80)
    title_frame.pack(fill='x')
    title_frame.pack_propagate(False)

    title_label = tk.Label(
        title_frame,
        text="💡 使用上次选择?",
        font=("Microsoft YaHei UI", 18, "bold"),
        bg='#FF2442',
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
        text="是否继续使用上次的配置?",
        font=("Microsoft YaHei UI", 12),
        bg='#f5f5f5',
        fg='#666'
    )
    hint_label.pack(pady=10)

    # 显示上次配置
    list_frame = tk.Frame(root, bg='#f5f5f5')
    list_frame.pack(pady=10, fill='x', padx=30)

    if last_config['fetch_all']:
        config_text = "📋 上次配置: 抓取所有笔记"
    else:
        config_text = f"📋 上次配置: 抓取最近 {last_config['scroll_days']} 天的笔记"

    list_text = tk.Text(
        list_frame,
        height=3,
        state='normal',
        font=("Microsoft YaHei UI", 11),
        bg='#FFF9C4',
        relief='solid',
        bd=1,
        wrap='word'
    )
    list_text.pack(fill='x')
    list_text.insert(1.0, config_text)
    list_text.config(state='disabled')

    # 按钮
    button_frame = tk.Frame(root, bg='#f5f5f5')
    button_frame.pack(pady=20)

    yes_btn = tk.Button(
        button_frame,
        text="✓ 是,使用上次配置",
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
        text="✗ 否,重新配置",
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

    # 居中显示
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    root.mainloop()

    return last_config if result['use_last'] else None



def show_selection_dialog():
    """显示抓取配置选择对话框"""
    result = {'scroll_days': 31, 'fetch_all': False, 'confirmed': False}

    def on_confirm():
        days_str = days_entry.get().strip()
        if not result['fetch_all']:
            if not days_str:
                messagebox.showwarning("提示", "请输入抓取天数!")
                return
            try:
                days = int(days_str)
                if days < 1:
                    messagebox.showwarning("提示", "天数必须大于0!")
                    return
                result['scroll_days'] = days
            except ValueError:
                messagebox.showwarning("提示", "请输入有效的数字!")
                return
        result['confirmed'] = True
        root.quit()
        root.destroy()

    def on_fetch_all_toggle():
        """切换抓取所有笔记选项"""
        if fetch_all_var.get():
            result['fetch_all'] = True
            days_entry.config(state='disabled')
            days_label.config(fg='#999')
        else:
            result['fetch_all'] = False
            days_entry.config(state='normal')
            days_label.config(fg='#333')

    def on_close():
        root.quit()
        root.destroy()
        sys.exit(0)

    root = tk.Tk()
    root.title("小红书创作者笔记 - 抓取配置")
    root.geometry("550x420")
    root.configure(bg='#f5f5f5')
    root.protocol("WM_DELETE_WINDOW", on_close)

    # 标题
    title_frame = tk.Frame(root, bg='#FF2442', height=80)
    title_frame.pack(fill='x')
    title_frame.pack_propagate(False)

    title_label = tk.Label(
        title_frame,
        text="📝 笔记抓取配置",
        font=("Microsoft YaHei UI", 18, "bold"),
        bg='#FF2442',
        fg='white'
    )
    title_label.pack(pady=20)

    # 说明
    instruction_frame = tk.Frame(root, bg='#f5f5f5')
    instruction_frame.pack(pady=20)

    instruction_label = tk.Label(
        instruction_frame,
        text="请选择要抓取的笔记范围",
        font=("Microsoft YaHei UI", 13),
        bg='#f5f5f5',
        fg='#333'
    )
    instruction_label.pack()

    # 抓取所有笔记选项
    fetch_all_frame = tk.Frame(root, bg='#f5f5f5')
    fetch_all_frame.pack(pady=15)

    fetch_all_var = tk.BooleanVar()
    fetch_all_var.set(False)

    fetch_all_check = tk.Checkbutton(
        fetch_all_frame,
        text="🔄 抓取所有笔记 (无限滚动直到没有笔记)",
        font=("Microsoft YaHei UI", 12, "bold"),
        bg='#f5f5f5',
        fg='#FF2442',
        activebackground='#f5f5f5',
        activeforeground='#FF2442',
        selectcolor='#f5f5f5',
        variable=fetch_all_var,
        command=on_fetch_all_toggle,
        cursor='hand2'
    )
    fetch_all_check.pack()

    # 天数输入
    days_frame = tk.Frame(root, bg='#f5f5f5')
    days_frame.pack(pady=10)

    days_label = tk.Label(
        days_frame,
        text="📅 抓取最近多少天的笔记:",
        font=("Microsoft YaHei UI", 11),
        bg='#f5f5f5',
        fg='#333'
    )
    days_label.grid(row=0, column=0, padx=10, sticky='e')

    days_entry = tk.Entry(
        days_frame,
        width=10,
        font=("Microsoft YaHei UI", 11),
        relief='solid',
        bd=1
    )
    days_entry.grid(row=0, column=1, padx=10)
    days_entry.insert(0, "31")

    days_hint = tk.Label(
        days_frame,
        text="天",
        font=("Microsoft YaHei UI", 11),
        bg='#f5f5f5',
        fg='#666'
    )
    days_hint.grid(row=0, column=2, padx=5, sticky='w')

    # 提示
    hint_frame = tk.Frame(root, bg='#f5f5f5')
    hint_frame.pack(pady=10)

    hint_label = tk.Label(
        hint_frame,
        text="💡 提示: 如果选择抓取所有笔记，将忽略天数限制",
        font=("Microsoft YaHei UI", 9),
        bg='#f5f5f5',
        fg='#999'
    )
    hint_label.pack()

    # 按钮
    button_frame = tk.Frame(root, bg='#f5f5f5')
    button_frame.pack(pady=20)

    confirm_btn = tk.Button(
        button_frame,
        text="✓ 确认开始抓取",
        width=16,
        height=2,
        font=("Microsoft YaHei UI", 11, "bold"),
        bg='#FF2442',
        fg='white',
        relief='raised',
        bd=0,
        cursor='hand2',
        activebackground='#D41D36',
        command=on_confirm
    )
    confirm_btn.pack()

    # 居中显示
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    root.mainloop()

    if result['confirmed']:
        return result
    return None



class XiaohongshuCreatorSpider:
    # 常量
    HOME_URL = "https://creator.xiaohongshu.com/new/home"
    NOTE_MANAGER_URL = "https://creator.xiaohongshu.com/new/note-manager"
    API_URL = "creator/note/user/posted"
    MAX_LOGIN_WAIT = 300  # 最大登录等待时间(秒)
    CHECK_INTERVAL = 5  # 登录检查间隔(秒)

    def __init__(self, scroll_days=31, fetch_all=False):
        self.base_dir = self._get_base_dir()
        self.cookie_file = os.path.join(self.base_dir, 'creator_cookies.json')
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.log_dir = os.path.join(self.base_dir, 'logs')

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

        self.home_url = self.HOME_URL
        self.note_manager_url = self.NOTE_MANAGER_URL
        self.api_url = self.API_URL

        # 抓取配置
        self.scroll_days = scroll_days
        self.fetch_all = fetch_all

        # 保存接口配置
        self.save_api_url = "https://tianji.fangpian999.com/api/admin/blogger/saveCrawlerNotes"
        # self.save_api_url = "http://localhost:5666/api/admin/blogger/saveCrawlerNotes"
        self.save_api_timeout = 60

        self.is_logged_in = False
        self.notes_data = []
        self.api_responses = []
        self.red_num = None
        self.fans_count = 0

        self.setup_logger()
        self.setup_browser()

    @staticmethod
    def _get_base_dir():
        """获取基础目录，支持exe打包"""
        if hasattr(sys, '_MEIPASS'):
            return os.path.dirname(os.path.abspath(sys.argv[0]))
        return os.path.dirname(os.path.abspath(__file__))

    def setup_logger(self):
        """设置日志配置"""
        os.makedirs(self.log_dir, exist_ok=True)
        log_path = self.log_dir

        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="INFO"
        )
        logger.add(
            os.path.join(log_path, "xhs_creator_{time:YYYY-MM-DD}.log"),
            rotation="1 day",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            encoding="utf-8"
        )

    def setup_browser(self):
        """初始化浏览器"""
        playwright_browsers_path = os.path.join(self.base_dir, 'ms-playwright')

        if os.path.exists(playwright_browsers_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = playwright_browsers_path
            logger.info(f"使用自定义浏览器路径: {playwright_browsers_path}")
        else:
            logger.warning(f"未找到自定义浏览器路径: {playwright_browsers_path}，将使用系统默认路径")

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )

        self.context = self.browser.new_context(
            viewport={'width': 1512, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )

        self.page = self.context.new_page()
        if self._load_cookies():
            try:
                self.page.goto(self.home_url, wait_until='domcontentloaded', timeout=30000)
                self._wait_for_home_ready()

                user_avatar = self.page.locator('.user_avatar').first
                if user_avatar.is_visible(timeout=5000):
                    self.is_logged_in = True
                    logger.info("Cookie有效，已自动登录")
                else:
                    logger.info("Cookie已失效，需要重新登录")
                    self.is_logged_in = False
            except Exception as e:
                logger.warning(f"Cookie验证失败: {str(e)}")
                self.is_logged_in = False
        else:
            logger.info("未找到Cookie文件，需要登录")
            self.page.goto(self.home_url, wait_until='domcontentloaded', timeout=30000)
            self._wait_for_home_ready()
            self.is_logged_in = False

        self.page.set_default_timeout(30000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)


    def login(self):
        """手动登录流程"""
        if self.is_logged_in:
            logger.info("已处于登录状态")
            try:
                if self.home_url not in self.page.url:
                    self.page.goto(self.home_url, wait_until='domcontentloaded', timeout=30000)
                self._wait_for_home_ready(timeout_ms=30000)
                self._wait_for_home_identity_ready(timeout_ms=30000)
                if not self._sync_home_identity(max_attempts=4, wait_seconds=2):
                    logger.warning("已登录，但首页身份信息暂未完全加载，抓取阶段会再次重试")
            except Exception as e:
                logger.warning(f"已登录状态下同步首页信息失败: {str(e)}")
            return True

        logger.info(f"开始等待用户手动登录，请在{self.MAX_LOGIN_WAIT//60}分钟内完成登录操作")

        try:
            if self.home_url not in self.page.url:
                self.page.goto(self.home_url, wait_until='domcontentloaded', timeout=30000)
                self._wait_for_home_ready()

            for elapsed in range(0, self.MAX_LOGIN_WAIT, self.CHECK_INTERVAL):
                try:
                    user_avatar = self.page.locator('.user_avatar').first
                    if user_avatar.is_visible(timeout=2000):
                        logger.info("检测到登录成功！")
                        self.is_logged_in = True
                        self._save_cookies()

                        if self.home_url not in self.page.url:
                            logger.info(f"登录后跳回首页: {self.home_url}")
                            self.page.goto(self.home_url, wait_until='domcontentloaded', timeout=30000)
                            self._wait_for_home_ready()
                        else:
                            self._wait_for_home_ready(timeout_ms=30000)

                        self._wait_for_home_identity_ready(timeout_ms=30000)
                        if not self._sync_home_identity(max_attempts=8, wait_seconds=2):
                            logger.warning("登录成功，但首页身份信息暂未完全提取到，抓取阶段会再次重试")

                        return True
                except:
                    pass
                time.sleep(self.CHECK_INTERVAL)

            logger.error(f"等待登录超时（{self.MAX_LOGIN_WAIT//60}分钟）")
            return False

        except Exception as e:
            logger.error(f"登录过程异常: {str(e)}")
            return False

    def _handle_api_response(self, response):
        """处理API响应，捕获目标接口"""
        try:
            url = response.url

            # 检查是否是目标API
            if self.api_url in url and response.request.resource_type in ['fetch', 'xhr']:
                try:
                    if response.status != 200:
                        logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                        return

                    data = response.json()
                    logger.info(f"捕获到接口数据，状态码: {response.status}")

                    try:
                        self.page.evaluate("window.__xhs_notes_api_ready = true")
                    except Exception:
                        pass

                    # 存储API响应
                    self.api_responses.append({
                        'url': url,
                        'data': data,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'status': response.status
                    })

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")

        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    @staticmethod
    def _parse_chinese_count(value):
        """将中文数量文本转为整数（如 1.7万 -> 17000）"""
        if value is None:
            return 0
        text = str(value).strip().replace(",", "")
        if not text:
            return 0
        multiplier = 1
        if text.endswith("万"):
            multiplier = 10000
            text = text[:-1]
        elif text.endswith("亿"):
            multiplier = 100000000
            text = text[:-1]
        try:
            return int(float(text) * multiplier)
        except Exception:
            return 0

    def _wait_for_home_ready(self, timeout_ms=15000):
        """等待首页关键元素渲染完成"""
        try:
            self.page.wait_for_function(
                """() => {
                    const bodyText = document.body && document.body.innerText
                        ? document.body.innerText.trim()
                        : '';
                    return !!document.querySelector('.user_avatar')
                        || !!document.querySelector('.user-name')
                        || !!document.querySelector('.user-desc')
                        || !!document.querySelector('.user-redId')
                        || !!document.querySelector('.user-interactions')
                        || !!document.querySelector('.static.description-text')
                        || bodyText.length > 20;
                }""",
                timeout=timeout_ms
            )
            return True
        except Exception as e:
            logger.warning(f"等待首页关键元素超时: {str(e)}")
            return False

    def _wait_for_home_identity_ready(self, timeout_ms=20000):
        """等待首页身份信息渲染完成，至少出现小红书号或统计块"""
        try:
            self.page.wait_for_function(
                """() => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const redIdEl = document.querySelector('.user-redId');
                    const descEl = document.querySelector('.user-desc');
                    const statsEl = document.querySelector('.static.description-text');
                    const interactionsEl = document.querySelector('.user-interactions');
                    const bodyText = normalize(document.body && document.body.innerText ? document.body.innerText : '');
                    const redIdText = normalize(redIdEl && redIdEl.textContent ? redIdEl.textContent : '');
                    const descText = normalize(descEl && descEl.textContent ? descEl.textContent : '');
                    const statsText = normalize(
                        statsEl && statsEl.textContent
                            ? statsEl.textContent
                            : interactionsEl && interactionsEl.textContent
                                ? interactionsEl.textContent
                                : ''
                    );
                    return !!redIdText
                        || descText.includes('小红书号')
                        || bodyText.includes('小红书号')
                        || (statsText.includes('粉丝') && (statsText.includes('关注') || statsText.includes('获赞') || statsText.includes('收藏')));
                }""",
                timeout=timeout_ms
            )
            return True
        except Exception as e:
            logger.warning(f"等待首页身份信息超时: {str(e)}")
            return False

    def _extract_home_basic_info(self):
        """从首页提取小红书号、粉丝数等基础信息"""
        try:
            script = """() => {
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const stripPrefix = (value, prefixes) => {
                    let text = normalize(value);
                    for (const prefix of prefixes) {
                        text = text.replace(prefix, '').trim();
                    }
                    return text;
                };

                const result = {
                    nickname: '',
                    red_id: '',
                    desc: '',
                    following_text: '',
                    fans_text: '',
                    liked_collected_text: '',
                    stats_ready: false
                };

                const nicknameEl = document.querySelector('.user-name');
                const redIdEl = document.querySelector('.user-redId');
                const descEl = document.querySelector('.user-desc');

                if (nicknameEl) {
                    result.nickname = normalize(nicknameEl.textContent);
                }

                if (descEl) {
                    result.desc = normalize(descEl.textContent);
                }

                if (redIdEl) {
                    result.red_id = stripPrefix(redIdEl.textContent, [
                        '小红书号：',
                        '小红书号:',
                        '小红书账号：',
                        '小红书账号:'
                    ]);
                }

                const applyStat = (label, value) => {
                    const textLabel = normalize(label);
                    const textValue = normalize(value);
                    if (!textLabel) {
                        return;
                    }
                    if (textLabel.includes('关注') && !result.following_text) {
                        result.following_text = textValue;
                    } else if (textLabel.includes('粉丝') && !result.fans_text) {
                        result.fans_text = textValue;
                    } else if ((textLabel.includes('获赞') || textLabel.includes('收藏')) && !result.liked_collected_text) {
                        result.liked_collected_text = textValue;
                    }

                    if (textLabel.includes('关注') || textLabel.includes('粉丝') || textLabel.includes('获赞') || textLabel.includes('收藏')) {
                        result.stats_ready = true;
                    }
                };

                const statBlocks = Array.from(document.querySelectorAll(
                    '.static.description-text > div, .user-interactions > div, [class*="interactions"] > div'
                ));

                for (const block of statBlocks) {
                    const numberEl = block.querySelector('.numerical, .count');
                    const labelEl = block.querySelector('.shows');
                    const numberText = normalize(numberEl ? numberEl.textContent : '');
                    let labelText = normalize(labelEl ? labelEl.textContent : '');

                    if (!labelText) {
                        const spanTexts = Array.from(block.querySelectorAll('span'))
                            .map((el) => normalize(el.textContent))
                            .filter(Boolean);
                        if (spanTexts.length >= 2) {
                            labelText = normalize(spanTexts.slice(1).join(''));
                        } else {
                            const blockText = normalize(block.textContent);
                            labelText = numberText ? normalize(blockText.replace(numberText, '')) : blockText;
                        }
                    }

                    applyStat(labelText, numberText);
                }

                const textSources = [
                    result.desc,
                    normalize(document.body && document.body.innerText ? document.body.innerText : '')
                ].filter(Boolean);

                if (!result.red_id) {
                    const redPatterns = [
                        /小红书号\\s*[:：]\\s*([a-zA-Z0-9_\\-]+)/,
                        /小红书账号\\s*[:：]\\s*([a-zA-Z0-9_\\-]+)/
                    ];
                    for (const text of textSources) {
                        for (const pattern of redPatterns) {
                            const match = text.match(pattern);
                            if (match && match[1]) {
                                result.red_id = normalize(match[1]);
                                break;
                            }
                        }
                        if (result.red_id) {
                            break;
                        }
                    }
                }

                if (!result.fans_text) {
                    const fansPatterns = [
                        /粉丝数\\s*[:：]?\\s*(\\d+(?:\\.\\d+)?(?:万|亿)?)/,
                        /粉丝\\s*[:：]?\\s*(\\d+(?:\\.\\d+)?(?:万|亿)?)/
                    ];
                    for (const text of textSources) {
                        for (const pattern of fansPatterns) {
                            const match = text.match(pattern);
                            if (match && match[1]) {
                                result.fans_text = normalize(match[1]);
                                result.stats_ready = true;
                                break;
                            }
                        }
                        if (result.fans_text) {
                            break;
                        }
                    }
                }

                return result;
            }"""
            info = self.page.evaluate(script)
            return info if isinstance(info, dict) else {}
        except Exception as e:
            logger.warning(f"从首页提取基础信息失败: {str(e)}")
            return {}

    def _sync_home_identity(self, max_attempts=6, wait_seconds=2):
        """循环等待首页身份信息加载完成，并同步到实例字段"""
        last_info = {}

        for attempt in range(1, max_attempts + 1):
            last_info = self._extract_home_basic_info()
            red_id = str(last_info.get('red_id') or '').strip()
            fans_text = str(last_info.get('fans_text') or '').strip()

            if red_id:
                self.red_num = red_id

            if fans_text:
                self.fans_count = self._parse_chinese_count(fans_text)

            if red_id and self.fans_count > 0:
                logger.info(f"✅ 首页信息获取成功: 小红书号={self.red_num}, 粉丝数={fans_text} ({self.fans_count})")
                return True

            logger.info(
                f"首页信息第{attempt}/{max_attempts}次检测: "
                f"小红书号={red_id or '空'}, 粉丝原始值={fans_text or '空'}"
            )

            if attempt < max_attempts:
                try:
                    self.page.wait_for_load_state('domcontentloaded', timeout=3000)
                except Exception:
                    pass
                time.sleep(wait_seconds)

        if self.red_num:
            logger.warning(
                f"首页已获取到小红书号 {self.red_num}，"
                f"但粉丝数仍未稳定，当前按 {self.fans_count} 继续"
            )
            return True

        logger.error(
            f"首页身份信息加载失败: 小红书号={str(last_info.get('red_id') or '').strip() or '空'}, "
            f"粉丝原始值={str(last_info.get('fans_text') or '').strip() or '空'}"
        )
        return False

    def _wait_for_notes_api_ready(self, timeout_ms=20000):
        """等待笔记列表接口至少返回一次"""
        try:
            self.page.wait_for_function(
                "() => window.__xhs_notes_api_ready === true",
                timeout=timeout_ms
            )
            return True
        except Exception as e:
            logger.warning(f"等待笔记接口返回超时: {str(e)}")
            return False

    def _extract_fans_count_from_page(self):
        """从 home 页面提取粉丝数，兼容不同 DOM 结构"""
        info = self._extract_home_basic_info()
        return str(info.get('fans_text') or '').strip()

    def scrape_notes(self):
        """抓取笔记数据"""
        logger.info("开始抓取笔记数据...")

        if self.fetch_all:
            logger.info("🔄 模式: 抓取所有笔记 (无限滚动)")
        else:
            logger.info(f"📅 模式: 抓取最近 {self.scroll_days} 天的笔记")

        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return False

            # 清空之前的响应数据
            self.api_responses.clear()
            self.notes_data.clear()

            logger.info(f"当前页面: {self.page.url}")
            if self.home_url not in self.page.url:
                logger.info(f"当前不在首页，跳转: {self.home_url}")
                self.page.goto(self.home_url, wait_until='domcontentloaded', timeout=30000)
            self._wait_for_home_ready(timeout_ms=30000)
            self._wait_for_home_identity_ready(timeout_ms=30000)

            if not self._sync_home_identity(max_attempts=8, wait_seconds=2):
                logger.error("首页未加载完成，未获取到小红书号，停止进入笔记管理页")
                return False

            # 访问笔记管理页面
            logger.info(f"访问笔记管理页面: {self.note_manager_url}")
            try:
                self.page.evaluate("window.__xhs_notes_api_ready = false")
            except Exception:
                pass
            self.page.goto(self.note_manager_url, wait_until='domcontentloaded', timeout=30000)

            if not self._wait_for_notes_api_ready(timeout_ms=30000):
                logger.error("笔记管理页加载超时，未等到笔记列表接口返回")
                return False

            # 记录上次响应数量，用于检测是否有新数据
            last_response_count = 0
            no_data_scroll_count = 0  # 连续无数据滚动次数
            max_no_data_scroll = 3    # 最大无数据滚动次数

            while True:
                current_response_count = len(self.api_responses)

                # 检查是否有新的API响应
                if current_response_count > last_response_count:
                    # 有新数据，重置计数器
                    last_response_count = current_response_count
                    no_data_scroll_count = 0

                    # 获取最新的一条响应
                    latest_response = self.api_responses[-1]
                    response_data = latest_response.get('data', {})

                    if response_data.get('success') and response_data.get('data'):
                        notes = response_data['data'].get('notes', [])

                        if notes:
                            # 获取最后一条笔记的时间
                            last_note = notes[-1]
                            note_time_str = last_note.get('time', '')

                            # 如果不是抓取所有笔记，则检查时间
                            if not self.fetch_all and note_time_str:
                                try:
                                    # 解析时间
                                    if isinstance(note_time_str, str) and '-' in note_time_str:
                                        if ':' in note_time_str:
                                            note_time = datetime.strptime(note_time_str, '%Y-%m-%d %H:%M')
                                        else:
                                            note_time = datetime.strptime(note_time_str, '%Y-%m-%d')
                                    else:
                                        note_time = datetime.fromtimestamp(int(note_time_str) / 1000)

                                    # 计算截止日期
                                    cutoff_date = datetime.now() - timedelta(days=self.scroll_days)

                                    logger.info(f"最后一条笔记时间: {note_time}, 对比{self.scroll_days}天前: {cutoff_date}")

                                    if note_time < cutoff_date:
                                        logger.info(f"最后一条笔记已超过{self.scroll_days}天，停止滚动")
                                        break
                                    else:
                                        logger.info(f"最后一条笔记在{self.scroll_days}天内，继续滚动")

                                except Exception as e:
                                    logger.warning(f"解析时间失败: {str(e)}")

                            # 继续滚动
                            self._scroll_in_container()
                            time.sleep(2)
                        else:
                            logger.info("当前响应没有笔记数据，继续滚动")
                            self._scroll_in_container()
                            time.sleep(2)
                    else:
                        logger.warning("API响应数据格式异常")
                        break
                else:
                    # 没有新数据
                    no_data_scroll_count += 1
                    logger.info(f"暂无新数据，滚动次数: {no_data_scroll_count}/{max_no_data_scroll}")

                    if no_data_scroll_count >= max_no_data_scroll:
                        logger.info("连续多次滚动无新数据，停止滚动")
                        break

                    self._scroll_in_container()
                    time.sleep(2)

            # 收集所有笔记数据
            self._collect_all_notes()

            logger.info(f"抓取完成，共获取 {len(self.notes_data)} 条笔记数据")
            return True

        except Exception as e:
            logger.error(f"抓取笔记数据时出错: {str(e)}")
            return False

    def _extract_red_num_from_page(self):
        """从 home 页面提取小红书号，兼容页面慢加载与文案变化"""
        info = self._extract_home_basic_info()
        return str(info.get('red_id') or '').strip()

    def _scroll_to_bottom(self):
        """滚动到页面底部"""
        try:
            # 滚动到页面底部
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            logger.debug("滚动到底部")
        except Exception as e:
            logger.warning(f"滚动时出错: {str(e)}")

    def _scroll_in_container(self):
        """使用鼠标滚轮在页面中间滚动"""
        try:
            # 获取页面尺寸
            viewport = self.page.viewport_size
            x = viewport['width'] // 2
            y = viewport['height'] // 2

            # 移动鼠标到页面中间
            self.page.mouse.move(x, y)

            # 模拟滚轮滚动（每次滚动500像素）
            self.page.mouse.wheel(0, 1000)
            time.sleep(2)

            logger.info(f"鼠标滚轮滚动: x={x}, y={y}, delta=1000")
        except Exception as e:
            logger.warning(f"滚轮滚动出错: {str(e)}")

    def _collect_all_notes(self):
        """收集所有API响应中的笔记数据"""
        logger.info("开始收集所有笔记数据...")

        for response in self.api_responses:
            response_data = response.get('data', {})
            if response_data.get('success') and response_data.get('data'):
                notes = response_data['data'].get('notes', [])
                if notes:
                    self.notes_data.extend(notes)
                    logger.info(f"收集到 {len(notes)} 条笔记")

        # 去重（根据note_id或其他唯一标识）
        seen_ids = set()
        unique_notes = []
        for note in self.notes_data:
            note_id = note.get('id', note.get('note_id'))
            if note_id and note_id not in seen_ids:
                seen_ids.add(note_id)
                unique_notes.append(note)

        self.notes_data = unique_notes
        logger.info(f"去重后共 {len(self.notes_data)} 条笔记")


    def format_and_print_notes(self):
        """重新组织数组并打印需要的字段"""
        formatted_notes = []

        for note in self.notes_data:
            # 获取images_list的第一张图片url
            images_list = note.get("images_list", [])
            cover_url = images_list[0].get("url", "") if images_list else ""

            formatted_note = {
                "likes": note.get("likes", 0),
                "shared_count": note.get("shared_count", 0),
                "view_count": note.get("view_count", 0),
                "collected_count": note.get("collected_count", 0),
                "xsec_token": note.get("xsec_token", ""),
                "cover_url": cover_url,
                "id": note.get("id", ""),
                "display_title": note.get("display_title", ""),
                "time": note.get("time", ""),
                "type": note.get("type", ""),
                "permission_code": note.get("permission_code", ""),
                "comments_count": note.get("comments_count", 0)
            }
            formatted_notes.append(formatted_note)

        # 组装最终数据结构
        result = {
            "reid_id": str(self.red_num) if self.red_num else "",
            "fans_count": self.fans_count,
            "notes": formatted_notes
        }

        # 记录格式化后的数据到日志
        logger.info("=" * 80)
        logger.info(f"格式化后的笔记数据 (共 {len(formatted_notes)} 条):")
        logger.info("=" * 80)
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        logger.info("=" * 80)

        # 调用保存接口
        logger.info("开始调用保存接口...")
        self.save_to_server(result)

        return result

    def save_to_server(self, data):
        """调用保存接口将数据发送到服务器"""
        try:
            logger.info(f"发送数据到服务器: {self.save_api_url}")
            logger.info(f"博主ID (reid_id): {data.get('reid_id')}")
            logger.info(f"粉丝数: {data.get('fans_count', 0)}")
            logger.info(f"笔记数量: {len(data.get('notes', []))}")

            if not str(data.get('reid_id') or '').strip():
                logger.error("❌ 未获取到小红书号(reid_id)，跳过保存接口调用")
                return

            headers = {"Content-Type": "application/json"}

            response = requests.post(
                self.save_api_url,
                json=data,
                headers=headers,
                timeout=self.save_api_timeout
            )

            if response.status_code == 200:
                result = response.json()
                logger.info("保存接口调用成功！")
                logger.info(f"响应状态码: {response.status_code}")
                logger.info(f"响应内容: {json.dumps(result, ensure_ascii=False, indent=2)}")

                # 检查业务逻辑是否成功
                if result.get('code') == 200:
                    data_info = result.get('data', {})
                    logger.info("✅ 保存成功统计:")
                    logger.info(f"  - 接收总数: {data_info.get('total', 0)}")
                    logger.info(f"  - 新增笔记: {data_info.get('insert', 0)}")
                    logger.info(f"  - 更新笔记: {data_info.get('update', 0)}")
                else:
                    logger.error(f"❌ 保存失败: {result.get('msg', '未知错误')}")
            else:
                logger.error(f"❌ 接口请求失败，状态码: {response.status_code}")
                logger.error(f"响应内容: {response.text}")

        except requests.exceptions.Timeout:
            logger.error(f"接口请求超时（超过 {self.save_api_timeout} 秒）")
        except requests.exceptions.ConnectionError:
            logger.error("接口连接失败，请检查服务器是否运行")
        except Exception as e:
            logger.error(f"调用保存接口时出错: {str(e)}")

    def save_notes_to_json(self):
        """保存笔记数据到JSON文件"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(self.data_dir, f'notes_{timestamp}.json')

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.notes_data, f, ensure_ascii=False, indent=2)

            logger.info(f"笔记数据已保存到: {filename}")
            return True
        except Exception as e:
            logger.error(f"保存笔记数据时出错: {str(e)}")
            return False

    def _save_cookies(self):
        """保存当前会话的Cookie"""
        try:
            cookies = self.context.cookies()
            cookie_dir = os.path.dirname(self.cookie_file)
            if cookie_dir and not os.path.exists(cookie_dir):
                os.makedirs(cookie_dir, exist_ok=True)

            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logger.info(f"Cookie已保存到: {self.cookie_file}")
        except Exception as e:
            logger.error(f"保存Cookie时出错: {str(e)}")

    def _load_cookies(self):
        """从文件加载保存的Cookie"""
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
            try:
                if os.path.exists(self.cookie_file):
                    os.remove(self.cookie_file)
                    logger.info("已删除损坏的Cookie文件")
            except:
                pass
            return False

    def close(self):
        """关闭浏览器和playwright"""
        try:
            if self.is_logged_in:
                self._save_cookies()

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


def main():
    """主函数"""
    spider = None
    try:
        # 首先检查是否有上次的配置
        last_config = show_use_last_selection_dialog()

        if last_config:
            # 使用上次配置
            scroll_days = last_config['scroll_days']
            fetch_all = last_config['fetch_all']
            logger.info(f"✅ 使用上次配置: {'抓取所有笔记' if fetch_all else f'抓取最近{scroll_days}天笔记'}")
        else:
            # 显示配置选择对话框
            config = show_selection_dialog()
            if not config:
                logger.error("未选择配置，程序退出")
                return False

            scroll_days = config['scroll_days']
            fetch_all = config['fetch_all']

            # 保存本次配置
            save_selection(scroll_days, fetch_all)
            logger.info(f"✅ 保存配置: {'抓取所有笔记' if fetch_all else f'抓取最近{scroll_days}天笔记'}")

        # 初始化爬虫
        spider = XiaohongshuCreatorSpider(scroll_days=scroll_days, fetch_all=fetch_all)
        logger.info("爬虫实例初始化成功")

        # 执行登录
        logger.info("开始登录流程...")
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        logger.info("登录成功，开始抓取数据...")

        # 执行抓取
        scrape_success = spider.scrape_notes()
        if scrape_success:
            # 格式化并打印数据
            spider.format_and_print_notes()
            # 保存数据
            spider.save_notes_to_json()
            logger.info("数据抓取任务完成")
            return True
        else:
            logger.error("数据抓取失败")
            return False

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        return False
    finally:
        if spider:
            try:
                spider.close()
                logger.info("资源清理完成")
            except Exception as e:
                logger.error(f"清理资源时出错: {str(e)}")


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
