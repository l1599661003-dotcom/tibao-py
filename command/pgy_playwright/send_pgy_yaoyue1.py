import json
import os
import sys
import configparser
import time
import cv2
import requests
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback

from core.database_onlone_tibao_2 import session
from unitl.common import Common

"""
    蒲公英邀约表单批量发送工具
    功能：从API获取待邀约数据，批量填写并发送邀约表单
    基于Playwright模拟浏览器操作
"""

# API配置
# API_URL = "https://tianji.fangpian999.com/api/admin/creatorInvitation/getPendingByEmployeeName"
API_URL = "http://localhost:5666/api/admin/creatorInvitation/getPendingByEmployeeName"


class PGYSpider:
    def __init__(self):
        # 设置cookie和数据目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            self.cookie_file = os.path.join(exe_dir, 'cookies.json')
            self.data_dir = os.path.join(exe_dir, 'data')
            self.config_file = os.path.join(exe_dir, 'config.ini')
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.cookie_file = os.path.join(current_dir, 'cookies.json')
            self.data_dir = os.path.join(current_dir, 'data')
            self.config_file = os.path.join(current_dir, 'config.ini')

        os.makedirs(self.data_dir, exist_ok=True)

        self.setup_logger()
        self.load_config()

        self.is_logged_in = False
        self.common = Common()

        # 从配置文件读取员工名字
        self.employee_name = self.get_config_value('API_CONFIG', 'employee_name')
        logger.info(f"员工名字: {self.employee_name}")

        # 邀约数据列表
        self.invite_data_list = []

    def setup_logger(self):
        """设置日志配置"""
        if hasattr(sys, '_MEIPASS'):
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            log_path = os.path.join(exe_dir, 'logs')
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            log_path = os.path.join(current_dir, 'logs')

        os.makedirs(log_path, exist_ok=True)

        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="INFO"
        )
        logger.add(
            os.path.join(log_path, "pgy_{time:YYYY-MM-DD}.log"),
            rotation="1 day",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            encoding="utf-8"
        )
        logger.info(f"日志文件保存路径: {log_path}")

    def load_config(self):
        """加载配置文件"""
        try:
            self.config = configparser.ConfigParser()
            self.config.read(self.config_file, encoding='utf-8')
            logger.info(f"成功加载配置文件: {self.config_file}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def get_config_value(self, section, key, default_value=''):
        """获取配置值"""
        try:
            return self.config.get(section, key, fallback='')
        except configparser.NoSectionError:
            return None
        except Exception as e:
            logger.error(f"获取配置值失败: {str(e)}")
            return None

    def fetch_invite_data_from_api(self):
        """从API获取待邀约数据"""
        try:
            logger.info(f"开始从API获取邀约数据，员工名字: {self.employee_name}")

            params = {'employee_name': self.employee_name}
            response = requests.get(API_URL, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()

                if data.get('code') == 200 and data.get('data'):
                    # 数据在 data.list 中
                    raw_list = data.get('data', {}).get('list', [])

                    # 转换字段名
                    for item in raw_list:
                        invite_item = {
                            'blogger_id': item.get('platform_user_id', ''),
                            'coop_type': item.get('cooperation_type_text', ''),
                            'wechat': item.get('wechat', ''),
                            'product_name': item.get('product_name', ''),
                            'coop_content': item.get('cooperation_content', ''),
                            'expect_publish_time': f"{item.get('expected_start_time_text', '')}至{item.get('expected_end_time_text', '')}"
                        }
                        self.invite_data_list.append(invite_item)

                    logger.info(f"成功获取 {len(self.invite_data_list)} 条邀约数据")
                    return True
                else:
                    logger.error(f"API返回错误: {data.get('msg', '未知错误')}")
                    return False
            else:
                logger.error(f"API请求失败: status_code: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"获取邀约数据时出错: {str(e)}")
            return False

    def setup_browser(self):
        """初始化浏览器"""
        try:
            if hasattr(sys, '_MEIPASS'):
                exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
                playwright_browsers_path = os.path.join(exe_dir, 'ms-playwright')
            else:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                playwright_browsers_path = os.path.join(current_dir, 'ms-playwright')

            if os.path.exists(playwright_browsers_path):
                os.environ['PLAYWRIGHT_BROWSERS_PATH'] = playwright_browsers_path
                logger.info(f"使用自定义浏览器路径: {playwright_browsers_path}")
            else:
                logger.warning(f"未找到自定义浏览器路径: {playwright_browsers_path}")

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

            logger.info("成功启动浏览器")

            self.context = self.browser.new_context(
                viewport={'width': 1512, 'height': 768},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            )

            if self._load_cookies():
                self.page = self.context.new_page()
                try:
                    self.page.goto('https://pgy.xiaohongshu.com')
                    self.common.random_sleep(2, 3)

                    if self.page.locator(".home_head_user_info").is_visible(timeout=5000):
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

            self.page.set_default_timeout(20000)

        except Exception as e:
            logger.error(f"启动浏览器失败: {str(e)}")
            raise

    def login(self):
        """等待用户手动登录，最多等待5分钟"""
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

            logger.info("开始等待用户手动登录...")
            logger.info("请在5分钟内完成登录操作，程序将自动检测登录状态")

            self.page.goto('https://pgy.xiaohongshu.com')
            self.common.random_sleep(2, 3)

            max_wait_time = 300
            check_interval = 10
            elapsed_time = 0

            while elapsed_time < max_wait_time:
                try:
                    user_avatar = self.page.locator(".home_head_user_info").first
                    if user_avatar and user_avatar.is_visible():
                        logger.info("检测到登录成功！")
                        self.is_logged_in = True
                        self._save_cookies()
                        return True

                    logger.info(f"等待登录中... ({elapsed_time}/{max_wait_time}秒)")
                    time.sleep(check_interval)
                    elapsed_time += check_interval

                except Exception as e:
                    logger.warning(f"检查登录状态时出错: {str(e)}")
                    time.sleep(check_interval)
                    elapsed_time += check_interval

            logger.error("等待登录超时（5分钟），程序退出")
            return False

        except Exception as e:
            logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def _select_coop_type(self, coop_type):
        """根据合作类型选择图文/视频一口价"""
        try:
            logger.info(f"选择合作类型: {coop_type}")

            if coop_type == '图文':
                image_text_option = self.page.locator("div.note-video-select:has-text('图文笔记一口价')").first
                if image_text_option:
                    logger.info("找到图文笔记一口价选项，点击")
                    image_text_option.click()
                    self.common.random_sleep(1, 2)
                else:
                    logger.warning("未找到图文笔记一口价选项")

            elif coop_type == '视频':
                video_option = self.page.locator("div.note-video-select:has-text('视频笔记一口价')").first
                if video_option:
                    logger.info("找到视频笔记一口价选项，点击")
                    video_option.click()
                    self.common.random_sleep(1, 2)
                else:
                    logger.warning("未找到视频笔记一口价选项")
            else:
                logger.warning(f"未知的合作类型: {coop_type}")

        except Exception as e:
            logger.error(f"选择合作类型时出错: {str(e)}")

    def process_invites(self):
        """批量处理邀约数据"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法发送邀约")
                return False

            if not self.invite_data_list:
                logger.warning("没有待处理的邀约数据")
                return False

            total = len(self.invite_data_list)
            success_count = 0
            failed_count = 0

            logger.info(f"开始批量处理邀约，共 {total} 条")

            for index, invite_data in enumerate(self.invite_data_list):
                try:
                    logger.info(f"处理第 {index + 1}/{total} 条邀约")

                    if self._send_invite_form(invite_data, index + 1):
                        success_count += 1
                    else:
                        failed_count += 1

                    # 添加延迟，避免请求过于频繁
                    self.common.random_sleep(5, 10)

                except Exception as e:
                    logger.error(f"处理第 {index + 1} 条邀约时出错: {str(e)}")
                    failed_count += 1
                    continue

            logger.info(f"邀约处理完成: 成功 {success_count} 条，失败 {failed_count} 条")
            return True

        except Exception as e:
            logger.error(f"批量处理邀约时出错: {str(e)}")
            return False

    def _send_invite_form(self, invite_data, row_number):
        """发送单个邀约表单"""
        try:
            blogger_id = invite_data.get('blogger_id', '')
            logger.info(f'开始发送第 {row_number} 条邀约，博主ID: {blogger_id}')

            page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/invite-form?id={blogger_id}&trackId="
            logger.info(f"访问邀约表单页面: {page_url}")
            self.page.goto(page_url)

            logger.info("等待页面加载完成...")
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.warning(f"等待页面加载完成时出错: {str(e)}")

            logger.info("首次填写，不使用复用功能，直接手动填写表单")

            # 选择合作类型
            coop_type = invite_data.get('coop_type', '')
            if coop_type:
                self._select_coop_type(coop_type)

            # 填写邀约表单
            if not self._fill_invite_form(invite_data):
                logger.error("填写邀约表单失败")
                return False

            # 测试模式：不点击发起邀约按钮
            logger.info("=== 测试模式：表单已填写完成，不发送邀约 ===")

            # 等待30-45秒随机时间后自动继续
            wait_time = self.common.random_sleep(30, 45)
            logger.info(f"等待 {wait_time} 秒后自动继续下一条...")
            time.sleep(wait_time)

            logger.info(f"第 {row_number} 条邀约表单填写完成")
            return True

        except Exception as e:
            logger.error(f"发送第 {row_number} 条邀约时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False

    def _fill_invite_form(self, invite_data):
        """填写邀约表单"""
        try:
            logger.info("开始填写邀约表单...")

            # 填写微信号
            wechat = invite_data.get('wechat', '')
            if wechat:
                try:
                    wechat_input = self.page.wait_for_selector("input[placeholder='请输入']", timeout=3000)
                    if wechat_input:
                        logger.info(f"填写微信号: {wechat}")
                        wechat_input.fill(wechat)
                        self.common.random_sleep(1, 2)
                except Exception as e:
                    logger.warning(f"填写微信号时出错: {str(e)}")

            # 填写产品名称
            product_name = invite_data.get('product_name', '')
            if product_name:
                try:
                    product_name_input = self.page.wait_for_selector("input[placeholder='请输入产品名称']", timeout=3000)
                    if product_name_input:
                        logger.info(f"填写产品名称: {product_name}")
                        product_name_input.fill(product_name)
                        self.common.random_sleep(1, 2)
                except Exception as e:
                    logger.warning(f"填写产品名称时出错: {str(e)}")

            # 填写合作内容介绍
            coop_content = invite_data.get('coop_content', '')
            if coop_content:
                try:
                    selectors = ["textarea.d-text", "textarea[placeholder*='合作内容']", "textarea"]
                    coop_content_input = None
                    for selector in selectors:
                        try:
                            coop_content_input = self.page.wait_for_selector(selector, timeout=2000)
                            if coop_content_input:
                                break
                        except:
                            continue

                    if coop_content_input:
                        logger.info(f"填写合作内容介绍: {coop_content}")
                        coop_content_input.fill(coop_content)
                        self.common.random_sleep(1, 2)
                    else:
                        logger.warning("未找到合作内容介绍输入框")
                except Exception as e:
                    logger.warning(f"填写合作内容介绍时出错: {str(e)}")

            # 选择期望发布时间
            expect_publish_time = invite_data.get('expect_publish_time', '')
            if expect_publish_time:
                logger.info(f"期望发布时间: {expect_publish_time}")
                self._select_date_range(expect_publish_time)

            logger.info("邀约表单填写完成")
            return True

        except Exception as e:
            logger.error(f"填写邀约表单时出错: {str(e)}")
            return False

    def _select_date_range(self, date_range_str):
        """选择日期范围，格式：2026-03-12至2026-04-12"""
        try:
            logger.info(f"开始选择日期范围: {date_range_str}")

            # 解析日期
            parts = date_range_str.split('至')
            if len(parts) != 2:
                logger.error(f"日期格式错误: {date_range_str}")
                return False

            start_date = parts[0].strip()
            end_date = parts[1].strip()

            start_parts = start_date.split('-')
            end_parts = end_date.split('-')

            start_day = int(start_parts[2])
            end_day = int(end_parts[2])

            logger.info(f"开始日期: {start_date}, 结束日期: {end_date}")

            # 点击开始日期输入框，弹出日历
            start_input = self.page.locator("input[placeholder='开始日期']").first
            if start_input:
                logger.info("点击开始日期输入框")
                start_input.click()
                self.common.random_sleep(1, 2)
            else:
                logger.warning("未找到开始日期输入框")
                return False

            # 等待日历出现
            calendars = self.page.locator(".d-datepicker-calendar").all()
            logger.info(f"找到 {len(calendars)} 个日历")

            if len(calendars) < 2:
                logger.warning("日历数量不足")
                return False

            # 在第一个日历中选择开始日期
            self._click_date_in_calendar(calendars[0], start_day, "开始日期")

            self.common.random_sleep(1, 2)

            # 在第二个日历中选择结束日期
            self._click_date_in_calendar(calendars[1], end_day, "结束日期")

            logger.info("日期范围选择完成")
            return True

        except Exception as e:
            logger.error(f"选择日期范围时出错: {str(e)}")
            return False

    def _click_date_in_calendar(self, calendar, day, date_type):
        """在指定日历中点击某一天"""
        try:
            cells = calendar.locator(".d-datepicker-cell.d-clickable:not(.disabled)").all()
            logger.info(f"{date_type}日历中找到 {len(cells)} 个可点击单元格")

            for cell in cells:
                try:
                    day_text = cell.locator("span.d-text").text_content().strip()
                    if day_text == str(day):
                        logger.info(f"找到{date_type}: {day}日，点击")
                        cell.click()
                        return True
                except Exception as e:
                    continue

            logger.warning(f"未找到{date_type}: {day}日")
            return False

        except Exception as e:
            logger.error(f"在日历中点击日期时出错: {str(e)}")
            return False

    def close(self):
        """关闭浏览器和playwright"""
        try:
            if self.is_logged_in:
                self._save_cookies()

            cv2.destroyAllWindows()

            if hasattr(self, 'page'):
                self.page.close()
            if hasattr(self, 'context'):
                self.context.close()
            if hasattr(self, 'browser'):
                self.browser.close()
            if hasattr(self, 'playwright'):
                self.playwright.stop()

            session.close()
            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

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
        """加载保存的Cookie"""
        try:
            if os.path.exists(self.cookie_file):
                logger.info(f"找到Cookie文件: {self.cookie_file}")
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


def run_spider_task():
    """执行爬虫任务"""
    spider = None
    try:
        logger.info("=== 蒲公英邀约表单批量发送程序启动 ===")

        spider = PGYSpider()
        logger.info("爬虫实例初始化成功")

        # 从API获取邀约数据
        logger.info("开始从API获取邀约数据...")
        if not spider.fetch_invite_data_from_api():
            logger.error("获取邀约数据失败，程序退出")
            return False

        if not spider.invite_data_list:
            logger.info("没有待处理的邀约数据，程序退出")
            return True

        # 初始化浏览器
        logger.info("开始初始化浏览器...")
        spider.setup_browser()
        logger.info("浏览器初始化成功")

        # 执行登录
        logger.info("开始登录流程...")
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        logger.info("登录成功，开始批量发送邀约...")

        # 批量处理邀约
        spider.process_invites()

        logger.info("邀约处理完成")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        return False
    finally:
        if spider:
            try:
                spider.close()
                logger.info("资源清理完成")
            except Exception as e:
                logger.error(f"清理资源时出错: {str(e)}")


if __name__ == "__main__":
    run_spider_task()
