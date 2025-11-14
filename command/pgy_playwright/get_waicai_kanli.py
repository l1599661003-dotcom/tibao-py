import json
import os
import sys
import time
import configparser
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright
import traceback
import requests
from requests.exceptions import RequestException

from unitl.common import Common

"""
    更新外采博主账号信息,博主变现，粉丝情况,从蒲公英抓取数据
    重构版本：基于Playwright模拟浏览器操作，无需token
    新版本：从API获取博主列表，数据保存到API
"""


def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(
            os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../../WeekAccountUpdate")


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        'command/pgy_playwright/config.ini',
        'config.ini',
        os.path.join(get_base_path(), 'config.ini')
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"成功加载配置文件: {config_path}")
            break

    if not config_loaded:
        logger.warning("未找到配置文件，使用默认配置")

    # 解析配置
    return {
        'PGY_LOGIN_CONFIG': {
            'page': config.get('PGY_LOGIN', 'page', fallback='1'),
            'pageSize': config.get('PGY_LOGIN', 'pageSize', fallback='10')
        }
    }


class WaicaiPGYSpider:
    def __init__(self):
        self.setup_logger()

        # 加载配置
        try:
            self.config = load_config()
        except Exception as e:
            logger.warning(f"加载配置文件失败: {str(e)}，使用默认配置")
            self.config = {
                'PGY_LOGIN_CONFIG': {
                    'page': '1',
                    'pageSize': '10'
                }
            }

        # 设置cookie和数据目录，支持exe打包
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')
        self.processed_ids_file = os.path.join(base_path, 'processed_ids.json')

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = 'https://pgy.xiaohongshu.com'
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.common = Common()

        # 浏览器相关属性，但不立即初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 初始化payload结构
        self.payload = {
            "apis": [
                {"tb_name": "blogger_info", "tb_data": []},
                {"tb_name": "blogger_note_rate", "tb_data": []},
                {"tb_name": "blogger_data_summary", "tb_data": []},
                {"tb_name": "blogger_note_detail", "tb_data": []},
                {"tb_name": "blogger_fans_summary", "tb_data": []},
                {"tb_name": "blogger_fans_profile", "tb_data": []},
                {"tb_name": "blogger_fans_history", "tb_data": []},
            ],
            "client_id": 1
        }

        logger.info("外采博主数据采集器初始化完成")

    def get_blogger_list_from_api(self):
        """从API获取博主列表"""
        try:
            api_url = f"https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator?type=3&page={self.config['PGY_LOGIN_CONFIG']['page']}&pageSize={self.config['PGY_LOGIN_CONFIG']['pageSize']}"
            # 生产环境URL：
            # f"https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator?type=3&page={self.config['PGY_LOGIN_CONFIG']['page']}&pageSize={self.config['PGY_LOGIN_CONFIG']['pageSize']}"

            headers = {"Content-Type": "application/json"}
            logger.info(f"正在请求API获取博主列表: {api_url}")

            response = requests.post(api_url, headers=headers, timeout=30)
            response_data = response.json()['data']

            logger.info(f"获取到 {len(response_data) if isinstance(response_data, list) else 0} 个博主数据")

            # 检查响应数据格式
            if not isinstance(response_data, list):
                logger.error(f"API返回数据格式错误: {response_data}")
                return []

            return response_data

        except Exception as e:
            logger.error(f"从API获取博主列表失败: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return []

    def process_blogger_data(self):
        """处理博主数据，抓取博主信息"""
        try:
            # 从API获取博主列表
            blogger_list = self.get_blogger_list_from_api()
            if not blogger_list:
                logger.error("未获取到博主数据")
                return False

            # 加载已处理的ID列表
            processed_ids = self._load_processed_ids()
            logger.info(f"已处理的博主数量: {len(processed_ids)}")

            # 过滤掉已处理的博主
            new_blogger_list = []
            for blogger in blogger_list:
                platform_user_id = blogger.get('platform_user_id')
                if platform_user_id and platform_user_id not in processed_ids:
                    new_blogger_list.append(blogger)
                else:
                    logger.info(f"跳过已处理的博主: {blogger.get('creator_nickname', 'Unknown')} (ID: {platform_user_id})")

            if not new_blogger_list:
                logger.info("所有博主都已处理完成，没有新数据需要处理")
                return True

            logger.info(f"待处理的新博主数量: {len(new_blogger_list)}/{len(blogger_list)}")

            # 遍历每个博主数据
            for idx, blogger in enumerate(new_blogger_list, 1):
                try:
                    platform_user_id = blogger.get('platform_user_id')

                    pgy_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/blogger-detail/{platform_user_id}"


                    if not pgy_url:
                        logger.info(f"[{idx}/{len(new_blogger_list)}] 博主蒲公英链接为空，跳过")
                        continue

                    # 清空之前的数据
                    self.api_data.clear()

                    # 重置payload
                    self.payload = {
                        "apis": [
                            {"tb_name": "blogger_info", "tb_data": []},
                            {"tb_name": "blogger_note_rate", "tb_data": []},
                            {"tb_name": "blogger_data_summary", "tb_data": []},
                            {"tb_name": "blogger_note_detail", "tb_data": []},
                            {"tb_name": "blogger_fans_summary", "tb_data": []},
                            {"tb_name": "blogger_fans_profile", "tb_data": []},
                            {"tb_name": "blogger_fans_history", "tb_data": []},
                        ],
                        "client_id": 1
                    }

                    logger.info(
                        f"[{idx}/{len(new_blogger_list)}] 正在处理博主: {blogger.get('creator_nickname', 'Unknown')}")
                    logger.info(f"访问页面: {pgy_url}")

                    try:
                        # 访问页面
                        self.page.goto(pgy_url)

                        try:
                            self.page.wait_for_load_state('networkidle', timeout=5000)
                            logger.info("页面网络请求完成")
                        except Exception as net_error:
                            logger.warning(f"等待网络空闲超时: {str(net_error)}")

                        self.common.random_sleep(20, 30)

                    except Exception as e:
                        logger.error(f"访问页面失败: {str(e)}")
                        continue

                    # 检查API数据
                    if self.api_data:
                        # 处理API数据
                        api_data_copy = dict(self.api_data)

                        # 处理各种API数据
                        for api_url, response_data in api_data_copy.items():
                            try:
                                if not response_data or not isinstance(response_data, dict):
                                    continue

                                if 'data' not in response_data:
                                    continue

                                api_data = response_data.get('data', {})
                                if not api_data or not isinstance(api_data, dict):
                                    continue

                                # 根据不同的API进行不同的处理
                                if 'blogger' in api_url:
                                    logger.info(f"处理博主API: {api_url}")

                                    # 获取博主数据
                                    if 'data' in api_data and isinstance(api_data['data'], dict):
                                        blogger_data = api_data['data']
                                    else:
                                        blogger_data = api_data

                                    # 检查博主数据是否有效
                                    if blogger_data and isinstance(blogger_data, dict):
                                        if any(key in blogger_data for key in
                                               ['name', 'redId', 'fansCount', 'picturePrice']):
                                            self._process_blogger_info(blogger_data, blogger)

                                elif 'fans_profile' in api_url:
                                    self._process_fans_profile(api_data, blogger)

                                elif 'notes_rate' in api_url:
                                    self._process_notes_rate(api_data, blogger, 0, 3, 1, 1)

                                elif 'fans_summary' in api_url:
                                    self._process_fans_summary(api_data, blogger)

                                elif 'notes_detail' in api_url:
                                    self._process_notes_detail(api_data, blogger)

                            except Exception as api_error:
                                logger.error(f"处理API {api_url} 数据时出错: {str(api_error)}")
                                continue

                        # 处理数据摘要
                        try:
                            self.api_data.clear()

                            once = self.page.locator("button:has-text('下一步')").first
                            if once:
                                once.click()

                            # 点击"按成本"按钮
                            dropdown_container = self.page.locator('.d-spinner-container')
                            switch_button = dropdown_container.locator('button:has-text("按成本")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                            # 等待页面加载完成
                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(8, 12)

                            # 处理数据摘要API
                            data_summary_copy = dict(self.api_data)
                            for api_url, response_data in data_summary_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'data_summary' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            self._process_data_summary(api_data, blogger)
                                            break
                                except Exception as e:
                                    logger.warning(f"处理数据摘要API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理数据摘要步骤时出错: {str(e)}")

                        # 点击"合作笔记"按钮
                        try:
                            self.api_data.clear()
                            dropdown_container = self.page.locator('.d-spinner-nested-loading')
                            switch_button = dropdown_container.locator('button:has-text("合作笔记")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(8, 12)

                            # 处理合作笔记API
                            notes_rate_copy = dict(self.api_data)
                            for api_url, response_data in notes_rate_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'notes_rate' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            self._process_notes_rate(api_data, blogger, 1, 3, 1, 1)
                                            break
                                except Exception as e:
                                    logger.warning(f"处理合作笔记API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理合作笔记步骤时出错: {str(e)}")

                        # 调用同步接口保存数据
                        sync_result = self.sync_single_record_to_api(self.payload)
                        if sync_result:
                            logger.info(f"✓ 成功同步博主 {blogger.get('creator_nickname', 'Unknown')} 的数据到API")
                            # 同步成功后，保存已处理的ID
                            self._save_processed_id(platform_user_id)
                        else:
                            logger.warning(f"✗ 同步博主 {blogger.get('creator_nickname', 'Unknown')} 的数据到API失败")

                    else:
                        logger.warning(f"[{idx}/{len(new_blogger_list)}] 未获取到API数据")

                except Exception as e:
                    logger.error(f"处理第 {idx} 个博主时出错: {str(e)}")
                    logger.error(f"错误详情: {traceback.format_exc()}")
                    continue

            logger.info("所有博主数据处理完成")
            return True

        except Exception as e:
            logger.error(f"处理博主数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False

    def sync_single_record_to_api(self, payload):
        """同步单条记录到API"""
        try:
            url = "http://47.104.76.46:19000/api/v1/sync/spider/data"
            headers = {"Content-Type": "application/json"}

            try:
                # 发送payload数据
                response = requests.post(url, json=payload, headers=headers, timeout=30)
            except RequestException as sync_error:
                logger.error(f"单条数据同步请求失败: {str(sync_error)}")
                return False

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    if response_data.get('code') == 200:
                        logger.debug(f"同步成功: {response_data}")
                        return True
                    else:
                        logger.warning(f"同步失败，API返回错误: {response_data}")
                        return False
                except ValueError:
                    logger.error(f"同步请求返回非JSON响应，无法解析: {response.text}")
                    return False
            else:
                logger.warning(f"同步请求失败，HTTP 状态码: {response.status_code}, 响应: {response.text}")
                return False
        except Exception as e:
            logger.warning(f"单条数据同步异常: {str(e)}")
            logger.warning(f"错误详情: {traceback.format_exc()}")
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
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            level="INFO"
        )

        # 添加文件输出
        log_file = os.path.join(log_path, f"waicai_pgy_{datetime.now().strftime('%Y-%m-%d')}.log")
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="DEBUG",
            rotation="00:00",
            retention="30 days",
            encoding="utf-8"
        )

    def setup_browser(self):
        """初始化浏览器"""
        # 设置playwright浏览器路径，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，使用exe文件所在目录的ms-playwright（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            playwright_browsers_path = os.path.join(exe_dir, 'ms-playwright')
        else:
            # 开发环境下，使用当前目录同级的ms-playwright
            current_dir = os.path.dirname(os.path.abspath(__file__))
            playwright_browsers_path = os.path.join(current_dir, 'ms-playwright')

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
                userSide = self.page.locator(".home_head_user_info").all()
                logger.info(f"找到用户头像元素数量: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 0 or self.page.locator(".home_head_user_info").is_visible(timeout=5000):
                    self.is_logged_in = True
                    logger.info("Cookie有效，已自动登录")
                else:
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

    def login(self):
        """等待用户手动登录，最多等待5分钟"""
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
                        user_avatar = self.page.locator(".home_head_user_info").first
                        if user_avatar and user_avatar.is_visible():
                            logger.info("检测到登录成功！")
                            self.is_logged_in = True

                            # 登录成功后保存Cookie
                            self._save_cookies()

                            return True
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
        """关闭浏览器和playwright"""
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

            # 从配置获取需要捕获的API路径
            target_apis = ['user/blogger', 'fans_profile', 'notes_rate', 'fans_summary', 'notes_detail',
                           'data_summary']

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (
                    response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
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
        """保存当前会话的Cookie到同级目录"""
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
        """从同级目录加载保存的Cookie"""
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

    def _load_processed_ids(self):
        """从文件加载已处理的platform_user_id列表"""
        try:
            if os.path.exists(self.processed_ids_file):
                with open(self.processed_ids_file, 'r', encoding='utf-8') as f:
                    processed_ids = json.load(f)

                if isinstance(processed_ids, list):
                    logger.info(f"已加载 {len(processed_ids)} 个已处理的ID")
                    return set(processed_ids)  # 转换为set以便快速查询
                else:
                    logger.warning("processed_ids.json 格式错误，应该是数组格式")
                    return set()
            else:
                logger.info("未找到已处理ID文件，将创建新文件")
                return set()
        except Exception as e:
            logger.error(f"加载已处理ID时出错: {str(e)}")
            return set()

    def _save_processed_id(self, platform_user_id):
        """追加保存单个已处理的platform_user_id到文件"""
        try:
            # 先加载现有的ID列表
            processed_ids = list(self._load_processed_ids())

            # 如果ID不在列表中，则添加
            if platform_user_id not in processed_ids:
                processed_ids.append(platform_user_id)

                # 保存到文件
                with open(self.processed_ids_file, 'w', encoding='utf-8') as f:
                    json.dump(processed_ids, f, indent=2, ensure_ascii=False)

                logger.debug(f"已保存已处理ID: {platform_user_id}")
                return True
            else:
                logger.debug(f"ID {platform_user_id} 已存在于记录中")
                return False
        except Exception as e:
            logger.error(f"保存已处理ID时出错: {str(e)}")
            return False

    def _process_blogger_info(self, data, blogger):
        """处理博主基本信息"""
        try:
            # 验证输入数据
            if not data or not isinstance(data, dict):
                logger.warning("博主数据为空或格式错误")
                return

            # 将数据添加到payload
            blogger_info_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_info"), None)
            if blogger_info_index is not None:
                # 克隆数据并添加博主ID
                payload_data = dict(data)
                payload_data['platform_user_id'] = blogger['platform_user_id']
                self.payload["apis"][blogger_info_index]["tb_data"] = [payload_data]

        except Exception as e:
            logger.error(f"处理博主基本信息时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_fans_profile(self, api_data, blogger):
        """处理粉丝画像数据"""
        try:
            if not self._validate_api_data(api_data):
                logger.warning("粉丝画像API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝画像数据为空或格式错误")
                return

            # 将数据添加到payload
            fans_profile_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_profile"),
                None)
            if fans_profile_index is not None:
                payload_data = dict(data)
                payload_data['platform_user_id'] = blogger.get('platform_user_id')
                self.payload["apis"][fans_profile_index]["tb_data"] = [payload_data]

            logger.info("粉丝画像数据处理完成")

        except Exception as e:
            logger.error(f"处理粉丝画像数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_data_summary(self, api_data, blogger):
        """处理数据摘要"""
        try:
            if not self._validate_api_data(api_data):
                logger.warning("数据摘要API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("数据摘要数据为空或格式错误")
                return

            # 将数据添加到payload
            data_summary_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_data_summary"),
                None)
            if data_summary_index is not None:
                payload_data = dict(data)
                payload_data['platform_user_id'] = blogger.get('platform_user_id')
                self.payload["apis"][data_summary_index]["tb_data"] = [payload_data]

            logger.info("数据摘要处理完成")

        except Exception as e:
            logger.error(f"处理数据摘要时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_notes_rate(self, api_data, url, business, note_type, date_type, advertise_switch):
        """处理数据摘要"""
        try:
            if not self._validate_api_data(api_data):
                logger.warning(f"{note_type}笔记率API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning(f"{note_type}笔记率数据为空或格式错误")
                return
            # 将数据添加到payload中
            note_rate_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_note_rate"), None)
            if note_rate_index is not None:
                # 克隆数据并添加平台用户ID和额外信息
                payload_data = dict(data)
                payload_data['platform_user_id'] = url['platform_user_id']
                payload_data['business'] = business
                payload_data['note_type'] = note_type
                payload_data['date_type'] = date_type
                payload_data['advertise_switch'] = advertise_switch

                # 检查是否已存在相同的数据，避免重复添加
                existing_data = self.payload["apis"][note_rate_index]["tb_data"]
                is_duplicate = any(
                    item.get('platform_user_id') == payload_data['platform_user_id'] and
                    item.get('business') == payload_data['business'] and
                    item.get('note_type') == payload_data['note_type'] and
                    item.get('date_type') == payload_data['date_type'] and
                    item.get('advertise_switch') == payload_data['advertise_switch']
                    for item in existing_data
                )

                if not is_duplicate:
                    # 只有不重复的数据才追加
                    self.payload["apis"][note_rate_index]["tb_data"].append(payload_data)
                    logger.debug(
                        f"添加笔记率数据: business={business}, note_type={note_type}, date_type={date_type}, advertise_switch={advertise_switch}")
                else:
                    logger.debug(
                        f"跳过重复的笔记率数据: business={business}, note_type={note_type}, date_type={date_type}, advertise_switch={advertise_switch}")

        except Exception as e:
            logger.error(f"处理笔记率数据时出错: {str(e)}")

    def _process_fans_summary(self, api_data, blogger):
        """处理粉丝概要数据"""
        try:
            if not self._validate_api_data(api_data):
                logger.warning("粉丝概要API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝概要数据为空或格式错误")
                return

            # 将数据添加到payload
            fans_summary_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_fans_summary"),
                None)
            if fans_summary_index is not None:
                payload_data = dict(data)
                payload_data['platform_user_id'] = blogger.get('platform_user_id')
                self.payload["apis"][fans_summary_index]["tb_data"] = [payload_data]

            logger.info("粉丝概要数据处理完成")

        except Exception as e:
            logger.error(f"处理粉丝概要数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_notes_detail(self, api_data, blogger):
        """处理笔记详情数据"""
        try:
            if not self._validate_api_data(api_data):
                logger.warning("笔记详情API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("笔记详情数据为空或格式错误")
                return

            note_list = data.get('list', [])
            if not isinstance(note_list, list):
                logger.warning("笔记列表不是数组格式")
                return

            # 将数据添加到payload
            note_detail_index = next(
                (i for i, item in enumerate(self.payload["apis"]) if item["tb_name"] == "blogger_note_detail"),
                None)
            if note_detail_index is not None:
                # 为每个笔记添加platform_user_id
                for item in note_list:
                    item['platform_user_id'] = blogger.get('platform_user_id')
                self.payload["apis"][note_detail_index]["tb_data"] = note_list

            logger.info(f"笔记详情数据处理完成: {len(note_list)} 条笔记")

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _validate_api_data(self, api_data, required_fields=None):
        """验证API数据的有效性"""
        try:
            if not isinstance(api_data, dict):
                logger.warning("API数据不是字典格式")
                return False

            if api_data.get('code') != 0:
                logger.warning(f"API响应码异常: {api_data.get('code')}")
                return False

            if 'data' not in api_data:
                logger.warning("API数据缺少data字段")
                return False

            if required_fields:
                data = api_data.get('data', {})
                missing_fields = [field for field in required_fields if field not in data]
                if missing_fields:
                    logger.warning(f"API数据缺少必需字段: {missing_fields}")
                    return False

            return True

        except Exception as e:
            logger.error(f"验证API数据时出错: {str(e)}")
            return False

    def _safe_get_nested(self, data, keys, default=""):
        """安全获取嵌套字典中的值"""
        try:
            current = data
            for key in keys:
                if current is None or not isinstance(current, dict):
                    return default
                current = current.get(key)
                if current is None:
                    return default
            return current if current is not None else default
        except Exception:
            return default

    def run(self):
        """运行爬虫主程序"""
        try:
            logger.info("=" * 50)
            logger.info("外采博主数据采集程序启动")
            logger.info("=" * 50)

            self.process_blogger_data()

            logger.info("=" * 50)
            logger.info("程序执行完成")
            logger.info("=" * 50)
        except KeyboardInterrupt:
            logger.info("用户中断程序")
        except Exception as e:
            logger.error(f"程序异常: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise


def main():
    """主函数"""
    spider = None
    try:
        logger.info("=== 蒲公英数据抓取程序启动 ===")
        spider = WaicaiPGYSpider()

        # 初始化浏览器和登录
        spider.setup_browser()
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        # 运行主程序
        spider.run()

        logger.info("程序执行完成")
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
