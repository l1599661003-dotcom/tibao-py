import json
import os
import time
import re
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import random
from sqlalchemy.exc import SQLAlchemyError

from core.localhost_fp_project import session
from models.models import QgCompanyNoteInfo

"""
    获取小红书笔记封面图ID
    从数据库查询小红书笔记链接，提取xg-poster元素的背景图ID并保存到数据库
"""


class XiaohongshuCoverFetcher:
    def __init__(self):
        self.setup_logger()
        self.base_url = "https://www.xiaohongshu.com"
        self.is_logged_in = False
        self.cookie_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.json')
        self.setup_browser()

    def setup_logger(self):
        """设置日志"""
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(
            os.path.join(log_path, "xhs_cover_fetcher_{time}.log"),
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

    def human_delay(self, min_sec=0.8, max_sec=2.0):
        """模拟人工延迟"""
        try:
            delay = random.uniform(min_sec, max_sec)
            logger.debug(f"模拟人工延时 {delay:.2f} 秒")
            time.sleep(delay)
        except Exception as e:
            logger.debug(f"模拟延时失败: {e}, 使用默认1秒")
            time.sleep(1)

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

    def extract_cover_id(self, note_url):
        """
        访问笔记页面并提取封面图ID

        Args:
            note_url: 小红书笔记链接

        Returns:
            str: 封面图ID，如 "1040g2sg31q54kbfjn2e05n8nisf41p81lmko75o"
        """
        try:
            logger.info(f"访问笔记页面: {note_url}")

            # 访问笔记页面
            self.page.goto(note_url)
            self.human_delay(3, 5)

            # 等待页面加载
            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
            except:
                logger.warning("页面加载超时，但继续尝试提取数据")

            # 使用JavaScript提取xg-poster元素的背景图URL
            cover_id = self.page.evaluate('''
                () => {
                    const poster = document.querySelector('xg-poster.xgplayer-poster');
                    if (!poster) {
                        return null;
                    }

                    const style = poster.getAttribute('style');
                    if (!style) {
                        return null;
                    }

                    // 从style中提取URL
                    // 格式: background-image: url("http://sns-webpic-qc.xhscdn.com/.../1040g2sg31q54kbfjn2e05n8nisf41p81lmko75o!nd_dft_wlteh_webp_3");
                    const urlMatch = style.match(/url\\(&quot;([^&]+)&quot;\\)/);
                    if (!urlMatch || !urlMatch[1]) {
                        return null;
                    }

                    const fullUrl = urlMatch[1];

                    // 从URL中提取ID部分（最后一个/之后，!之前的内容）
                    const idMatch = fullUrl.match(/\\/([^/]+)!/);
                    if (!idMatch || !idMatch[1]) {
                        return null;
                    }

                    return idMatch[1];
                }
            ''')

            if cover_id:
                logger.info(f"✓ 成功提取封面图ID: {cover_id}")
                return cover_id
            else:
                logger.warning("未找到封面图ID")
                return None

        except Exception as e:
            logger.error(f"提取封面图ID时出错: {str(e)}")
            return None

    def process_notes(self):
        """处理所有需要更新封面图ID的笔记"""
        try:
            # 查询所有cover_image为空的笔记
            logger.info("查询需要更新封面图ID的笔记...")
            notes = session.query(QgCompanyNoteInfo).filter(
                (QgCompanyNoteInfo.cover_image == None) |
                (QgCompanyNoteInfo.cover_image == '')
            ).all()

            logger.info(f"共找到 {len(notes)} 条需要更新的笔记")

            if len(notes) == 0:
                logger.info("没有需要更新的笔记")
                return

            # 处理每条笔记
            updated_count = 0
            failed_count = 0

            for index, note in enumerate(notes):
                logger.info(f"处理第 {index + 1}/{len(notes)} 条笔记")
                logger.info(f"笔记ID: {note.note_id}, 标题: {note.title}")

                # 构建笔记URL
                note_url = f"{self.base_url}/explore/{note.note_id}"

                # 提取封面图ID
                cover_id = self.extract_cover_id(note_url)

                if cover_id:
                    try:
                        # 更新数据库
                        note.cover_image = cover_id
                        session.commit()
                        logger.info(f"✓ 已更新笔记 {note.note_id} 的封面图ID: {cover_id}")
                        updated_count += 1
                    except SQLAlchemyError as e:
                        logger.error(f"更新数据库失败: {str(e)}")
                        session.rollback()
                        failed_count += 1
                else:
                    logger.warning(f"✗ 未能获取笔记 {note.note_id} 的封面图ID")
                    failed_count += 1

                # 每处理一条笔记，休息一下
                self.human_delay(2, 4)

            logger.info(f"处理完成: 成功 {updated_count} 条, 失败 {failed_count} 条")

        except Exception as e:
            logger.error(f"处理笔记时出错: {str(e)}")
            session.rollback()

    def run(self):
        """运行脚本"""
        try:
            logger.info("开始运行小红书封面图ID提取器...")

            # 检查并处理登录
            if not self.check_and_handle_login():
                logger.error("登录失败,程序退出")
                return

            cover_id = self.extract_cover_id("")
            # 处理笔记
            # self.process_notes()

            logger.info("所有任务完成")

        except Exception as e:
            logger.error(f"运行过程出错: {str(e)}")
        finally:
            self.close()

    def close(self):
        """关闭资源"""
        try:
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
    fetcher = XiaohongshuCoverFetcher()
    fetcher.run()
