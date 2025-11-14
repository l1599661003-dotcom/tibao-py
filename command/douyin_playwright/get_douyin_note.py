import json
import os
import sys
import time
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright
import traceback

from core.localhost_fp_project import session
from models.models import DouyinKol, DouyinNote
from unitl.common import Common

"""
    更新外采博主账号信息,博主变现，粉丝情况,从蒲公英抓取数据
    重构版本：基于Playwright模拟浏览器操作，无需token
    新增功能：Excel导入和数据填充
"""

def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../..")

def get_resource_path(relative_path):
    """获取资源文件路径，支持exe打包"""
    try:
        # PyInstaller创建临时文件夹并将路径存储在_MEIPASS中
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath("../..")
    return os.path.join(base_path, relative_path)

class DouyinSearchSpider:
    def __init__(self):
        self.setup_logger()

        # 设置cookie和数据目录，支持exe打包
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = 'https://www.douyin.com'
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.common = Common()

        # 浏览器相关属性，但不立即初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # Excel处理相关
        self.excel_data = None
        self.excel_file_path = None

    def search_douyin_user(self):
        """搜索抖音用户"""
        try:
            if not self.page:
                logger.error("页面未初始化")
                return False

            kol_list = session.query(DouyinKol).all()
            for kol in kol_list:
                time.sleep(10)
                # 访问抖音用户主页
                logger.info(f"正在访问用户: {kol.douyin_nickname if hasattr(kol, 'douyin_nickname') else kol.id}")
                self.page.goto(kol.douyin_url)
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception as e:
                    logger.warning(f"等待页面加载完成时出错: {str(e)}")

                if self.api_data:
                    api_data_copy = dict(self.api_data)
                    for api_url, response_data in api_data_copy.items():
                        if 'data' not in response_data:
                            continue

                        api_data = response_data['data']
                        if 'aweme/post' in api_url:
                            aweme_list = api_data.get('aweme_list', [])
                            logger.info(f"获取到 {len(aweme_list)} 条笔记数据")
                            self.parse_search_results(aweme_list, kol)
        except Exception as e:
            logger.error(f"搜索抖音用户时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False

    def parse_search_results(self, api_data, douyin):
        """解析搜索结果并存储到数据库"""
        try:
            for item in api_data:
                # 检查笔记是否已存在
                existing_record = session.query(DouyinNote).filter(
                    DouyinNote.note_id == item['aweme_id']
                ).first()

                # 获取标签信息
                tag = ''
                if 'video_tag' in item and item['video_tag']:
                    tag_list = []
                    for tag_item in item['video_tag']:
                        if 'tag_name' in tag_item:
                            tag_list.append(tag_item['tag_name'])
                    tag = ','.join(tag_list)

                if existing_record:
                    # 更新已存在的笔记
                    session.query(DouyinNote).filter(
                        DouyinNote.note_id == item['aweme_id']
                    ).update({
                        DouyinNote.note_title: item.get('desc', ''),
                        DouyinNote.note_like: item.get('statistics', {}).get('digg_count', 0),
                        DouyinNote.note_collect: item.get('statistics', {}).get('collect_count', 0),
                        DouyinNote.note_comment: item.get('statistics', {}).get('comment_count', 0),
                        DouyinNote.note_share: item.get('statistics', {}).get('share_count', 0),
                        DouyinNote.note_tags: tag,
                    })
                    logger.info(f"更新笔记: {item['aweme_id']}")
                else:
                    # 创建新笔记记录
                    note_data = DouyinNote(
                        douyin_kol_id=douyin.id,
                        note_id=item['aweme_id'],
                        note_link=item.get('share_url', ''),
                        note_title=item.get('desc', ''),
                        note_like=item.get('statistics', {}).get('digg_count', 0),
                        note_collect=item.get('statistics', {}).get('collect_count', 0),
                        note_comment=item.get('statistics', {}).get('comment_count', 0),
                        note_share=item.get('statistics', {}).get('share_count', 0),
                        note_publish_time=item.get('create_time', 0),
                        note_tags=tag,
                    )
                    session.add(note_data)
                    logger.info(f"新增笔记: {item['aweme_id']}")

            session.commit()
            logger.info(f"成功处理 {len(api_data)} 条笔记数据")
            self.api_data = {}
            return True

        except Exception as e:
            logger.error(f"解析搜索结果时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            session.rollback()
            return False

    def setup_logger(self):
        """设置日志配置，支持exe打包"""
        # 设置日志目录
        base_path = get_base_path()
        log_path = os.path.join(base_path, 'logs')
        os.makedirs(log_path, exist_ok=True)

        # 移除默认处理器，避免重复输出
        logger.remove()

        # 添加控制台输出
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="INFO"
        )

        # 添加文件输出
        logger.add(
            os.path.join(log_path, "pgy_{time:YYYY-MM-DD}.log"),
            rotation="1 day",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            encoding="utf-8"
        )

    def setup_browser(self):
        """初始化浏览器"""
        # 如果浏览器已经初始化，直接返回
        if self.browser and self.context and self.page:
            logger.info("浏览器已经初始化，跳过重复初始化")
            return

        # 设置playwright浏览器路径，支持exe打包
        base_path = get_base_path()
        playwright_browsers_path = os.path.join(base_path, 'ms-playwright')

        # 设置环境变量
        if os.path.exists(playwright_browsers_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = playwright_browsers_path
            logger.info(f"使用自定义浏览器路径: {playwright_browsers_path}")
        else:
            logger.warning(f"未找到自定义浏览器路径: {playwright_browsers_path}")

        self.playwright = sync_playwright().start()
        # 配置浏览器选项
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )
        # 创建上下文
        self.context = self.browser.new_context(
            viewport={
                'width': 1512,
                'height': 768
            },
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )

        # 尝试加载已保存的Cookie
        if self._load_cookies():
            # 验证Cookie是否有效
            self.page = self.context.new_page()
            try:
                self.page.goto(self.base_url)
                self.common.random_sleep(2, 3)

                # 检查是否存在用户头像元素
                try:
                    avatar = self.page.locator('[data-e2e="live-avatar"]').first
                    if avatar and avatar.is_visible(timeout=3000):
                        logger.info("找到登录状态元素: data-e2e=live-avatar")
                        self.is_logged_in = True
                        logger.info("Cookie有效，已自动登录")
                    else:
                        # 尝试备用选择器
                        avatar = self.page.locator('.home_head_user_info').first
                        if avatar and avatar.is_visible(timeout=2000):
                            logger.info("找到登录状态元素: home_head_user_info")
                            self.is_logged_in = True
                            logger.info("Cookie有效，已自动登录")
                        else:
                            logger.info("Cookie已失效，需要重新登录")
                            self.is_logged_in = False
                except:
                    logger.info("Cookie已失效，需要重新登录")
                    self.is_logged_in = False
            except Exception as e:
                logger.warning(f"Cookie验证失败: {str(e)}")
                logger.info("将进行重新登录")
                self.is_logged_in = False
        else:
            logger.info("未找到Cookie文件，需要登录")
            self.page = self.context.new_page()
            self.is_logged_in = False

        # 设置页面超时时间
        self.page.set_default_timeout(20000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

        logger.info("浏览器初始化完成")

    def login(self):
        """
        等待用户手动登录，最多等待5分钟
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

            try:
                # 访问首页
                self.page.goto(self.base_url)
                self.common.random_sleep(2, 3)

                # 等待5分钟，每10秒检查一次登录状态
                max_wait_time = 300  # 5分钟 = 300秒
                check_interval = 10  # 每10秒检查一次
                elapsed_time = 0

                while elapsed_time < max_wait_time:
                    try:
                        # 检查是否存在用户头像元素（登录成功的标志）
                        try:
                            avatar = self.page.locator('[data-e2e="live-avatar"]').first
                            if avatar and avatar.is_visible(timeout=2000):
                                logger.info("检测到登录成功！找到头像元素")
                                self.is_logged_in = True
                                # 登录成功后保存Cookie
                                self._save_cookies()
                                return True
                        except:
                            # 如果主要选择器失败，尝试备用选择器
                            try:
                                avatar = self.page.locator('.home_head_user_info').first
                                if avatar and avatar.is_visible(timeout=1000):
                                    logger.info("检测到登录成功！使用备用选择器")
                                    self.is_logged_in = True
                                    self._save_cookies()
                                    return True
                            except:
                                pass
                            
                        time.sleep(check_interval)
                        elapsed_time += check_interval

                    except Exception as e:
                        logger.warning(f"检查登录状态时出错: {str(e)}")
                        time.sleep(check_interval)
                        elapsed_time += check_interval

                # 5分钟超时，仍未登录成功
                logger.error("等待登录超时（5分钟），程序退出")
                return False

            except Exception as e:
                logger.error(f"等待登录过程中出现异常: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def close(self):
        """
        关闭浏览器、playwright和数据库连接
        """
        try:
            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 检查浏览器是否已初始化
            if hasattr(self, 'page') and self.page:
                self.page.close()
            if hasattr(self, 'context') and self.context:
                self.context.close()
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应，捕获指定的API请求"""
        try:
            if not response:
                logger.warning("API响应对象为空")
                return

            url = response.url
            if not url:
                logger.warning("API响应URL为空")
                return

            # 从配置获取需要捕获的API路径，匹配reptile_pgy_data_waicai.py的逻辑
            target_apis = ['aweme/v1/web/aweme/post']

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                try:
                    # 检查响应状态
                    if response.status != 200:
                        logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                        return

                    # 安全解析JSON
                    try:
                        data = response.json()
                    except Exception as json_error:
                        logger.warning(f"JSON解析失败: {str(json_error)}, URL: {url}")
                        return

                    # 验证数据结构
                    if not isinstance(data, dict):
                        logger.warning(f"API响应数据格式异常，期望dict，实际: {type(data)}, URL: {url}")
                        return

                    # 找到匹配的API类型
                    matched_api = None
                    for api in target_apis:
                        if api in url:
                            matched_api = api
                            break

                    if not matched_api:
                        logger.warning(f"未找到匹配的API类型: {url}")
                        return

                    # 存储API数据
                    try:
                        self.api_data[url] = {
                            'url': url,
                            'data': data,
                            'api_type': matched_api,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'status': response.status
                        }
                        logger.debug(f"成功捕获API响应: {matched_api}, URL: {url}")
                    except Exception as e:
                        logger.error(f"存储API数据时出错: {str(e)}")

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
                    logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _save_cookies(self):
        """
        保存当前会话的Cookie到同级目录
        """
        try:
            cookies = self.context.cookies()
            # 确保cookie文件的目录存在
            cookie_dir = os.path.dirname(self.cookie_file)
            if cookie_dir and not os.path.exists(cookie_dir):
                os.makedirs(cookie_dir, exist_ok=True)

            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logger.info(f"Cookie已保存到: {self.cookie_file}")
        except Exception as e:
            logger.error(f"保存Cookie时出错: {str(e)}")

    def _load_cookies(self):
        """
        从同级目录加载保存的Cookie
        :return: 是否成功加载Cookie
        """
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
                logger.info("未找到Cookie文件，需要登录")
                return False
        except Exception as e:
            logger.error(f"加载Cookie时出错: {str(e)}")
            # 如果cookie文件损坏，删除它
            try:
                if os.path.exists(self.cookie_file):
                    os.remove(self.cookie_file)
                    logger.info("已删除损坏的Cookie文件")
            except:
                pass
            return False


def main():
    """
    主函数 - 抖音搜索自动化程序
    """
    spider = None
    try:
        logger.info("=== 抖音搜索自动化程序启动 ===")
        spider = DouyinSearchSpider()

        # 3. 初始化浏览器和登录
        spider.setup_browser()
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        search = spider.search_douyin_user()
        if not search:
            logger.error("查询失败")
            return False

        logger.info("抖音搜索程序执行完成")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        return False
    finally:
        # 确保资源被正确释放
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