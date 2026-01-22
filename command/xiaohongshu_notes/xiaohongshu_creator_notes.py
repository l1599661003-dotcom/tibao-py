import json
import os
import sys
import time
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from loguru import logger
import requests

"""
    小红书创作者平台笔记爬虫
    抓取最近14天内的笔记数据
"""


class XiaohongshuCreatorSpider:
    # 常量
    HOME_URL = "https://creator.xiaohongshu.com/new/home"
    NOTE_MANAGER_URL = "https://creator.xiaohongshu.com/new/note-manager"
    API_URL = "https://edith.xiaohongshu.com/web_api/sns/v5/creator/note/user/posted"
    SCROLL_DAYS = 31  # 抓取天数
    MAX_LOGIN_WAIT = 300  # 最大登录等待时间(秒)
    CHECK_INTERVAL = 5  # 登录检查间隔(秒)

    def __init__(self):
        self.base_dir = self._get_base_dir()
        self.cookie_file = os.path.join(self.base_dir, 'creator_cookies.json')
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.log_dir = os.path.join(self.base_dir, 'logs')

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

        self.home_url = self.HOME_URL
        self.note_manager_url = self.NOTE_MANAGER_URL
        self.api_url = self.API_URL

        # 保存接口配置
        self.save_api_url = "https://tianji.fangpian999.com/api/admin/blogger/saveCrawlerNotes"
        # self.save_api_url = "http://localhost:5666/api/admin/blogger/saveCrawlerNotes"
        self.save_api_timeout = 60

        self.is_logged_in = False
        self.notes_data = []
        self.api_responses = []
        self.red_num = None

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

        # 尝试加载已保存的Cookie
        if self._load_cookies():
            self.page = self.context.new_page()
            try:
                self.page.goto(self.home_url)
                time.sleep(3)

                # 检查是否存在用户头像元素（登录成功的标志）
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
            self.page = self.context.new_page()
            self.is_logged_in = False

        self.page.set_default_timeout(30000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

    def login(self):
        """手动登录流程"""
        if self.is_logged_in:
            logger.info("已处于登录状态")
            return True

        logger.info(f"开始等待用户手动登录，请在{self.MAX_LOGIN_WAIT//60}分钟内完成登录操作")

        try:
            self.page.goto(self.home_url)
            time.sleep(3)

            for elapsed in range(0, self.MAX_LOGIN_WAIT, self.CHECK_INTERVAL):
                try:
                    user_avatar = self.page.locator('.user_avatar').first
                    if user_avatar.is_visible(timeout=2000):
                        logger.info("检测到登录成功！")
                        self.is_logged_in = True
                        self._save_cookies()
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

    def scrape_notes(self):
        """抓取笔记数据"""
        logger.info("开始抓取笔记数据...")

        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return False

            # 清空之前的响应数据
            self.api_responses.clear()
            self.notes_data.clear()

            # 访问 home 页面
            logger.info(f"访问home页面: {self.home_url}")
            self.page.goto(self.home_url)

            # 等待页面加载
            try:
                self.page.wait_for_load_state('domcontentloaded', timeout=10000)
                time.sleep(2)
            except:
                time.sleep(3)

            # 从页面元素获取小红书号
            try:
                red_num = self.page.evaluate('''() => {
                    // 查找包含"小红书账号"的元素
                    const elements = document.querySelectorAll('*');
                    for (const el of elements) {
                        const text = el.textContent;
                        if (text && text.includes('小红书账号')) {
                            const match = text.match(/小红书账号[::：]\\s*(\\d+)/);
                            if (match) return match[1];
                        }
                    }
                    return '';
                }''')

                if red_num:
                    self.red_num = str(red_num)
                    logger.info(f"✅ 从页面获取到小红书号: {self.red_num}")
                else:
                    logger.warning("⚠️ 未能从页面找到小红书账号")
            except Exception as e:
                logger.warning(f"从页面获取小红书号失败: {str(e)}")

            # 访问笔记管理页面
            logger.info(f"访问笔记管理页面: {self.note_manager_url}")
            self.page.goto(self.note_manager_url)

            # 等待页面加载
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except:
                pass

            time.sleep(4)

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

                            if note_time_str:
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
                                    cutoff_date = datetime.now() - timedelta(days=self.SCROLL_DAYS)

                                    logger.info(f"最后一条笔记时间: {note_time}, 对比{self.SCROLL_DAYS}天前: {cutoff_date}")

                                    if note_time < cutoff_date:
                                        logger.info(f"最后一条笔记已超过{self.SCROLL_DAYS}天，停止滚动")
                                        break
                                    else:
                                        logger.info(f"最后一条笔记在{self.SCROLL_DAYS}天内，继续滚动")

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
            self.page.mouse.wheel(0, 500)

            logger.info(f"鼠标滚轮滚动: x={x}, y={y}, delta=500")
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
            images_list = note.get('images_list', [])
            cover_url = images_list[0].get('url', '') if images_list else ''

            formatted_note = {
                'likes': note.get('likes', 0),
                'shared_count': note.get('shared_count', 0),
                'view_count': note.get('view_count', 0),
                'collected_count': note.get('collected_count', 0),
                'xsec_token': note.get('xsec_token', ''),
                'cover_url': cover_url,
                'id': note.get('id', ''),
                'display_title': note.get('display_title', ''),
                'time': note.get('time', ''),
                'type': note.get('type', ''),
                'comments_count': note.get('comments_count', 0)
            }
            formatted_notes.append(formatted_note)

        # 组装最终数据结构
        result = {
            'reid_id': str(self.red_num) if self.red_num else '',
            'notes': formatted_notes
        }

        # 记录格式化后的数据到日志
        logger.info(f"{'='*80}")
        logger.info(f"格式化后的笔记数据 (共 {len(formatted_notes)} 条):")
        logger.info(f"{'='*80}")
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
        logger.info(f"{'='*80}")

        # 调用保存接口
        logger.info("开始调用保存接口...")
        self.save_to_server(result)

        return result

    def save_to_server(self, data):
        """调用保存接口将数据发送到服务器"""
        try:
            logger.info(f"发送数据到服务器: {self.save_api_url}")
            logger.info(f"博主ID (reid_id): {data.get('reid_id')}")
            logger.info(f"笔记数量: {len(data.get('notes', []))}")

            headers = {
                "Content-Type": "application/json"
            }

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
                    logger.info(f"✅ 保存成功统计:")
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
        # 初始化爬虫
        spider = XiaohongshuCreatorSpider()
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
