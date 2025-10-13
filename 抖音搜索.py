import configparser
import json
import os
import sys
import time
from datetime import datetime
from tkinter import filedialog, messagebox, Tk

import pandas as pd
from loguru import logger
from playwright.sync_api import sync_playwright
import traceback

from core.database_text_tibao_2 import session
from models.models_tibao import DouyinUserList
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
        return os.path.abspath(".")

def get_resource_path(relative_path):
    """获取资源文件路径，支持exe打包"""
    try:
        # PyInstaller创建临时文件夹并将路径存储在_MEIPASS中
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        get_resource_path('command/pgy_playwright/config.ini'),
        get_resource_path('config.ini'),
        'WeekAccountUpdate/config.ini',
        'config.ini'
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            logger.info(f"已加载配置文件: {config_path}")
            break

    if not config_loaded:
        logger.error("未找到配置文件")
        raise FileNotFoundError("配置文件不存在")

    # 解析配置
    return {
        'USERNAME': {
            'username': config.get('USERNAME', 'username', fallback='未知用户'),
        }
    }

class DouyinSearchSpider:
    def __init__(self):
        self.setup_logger()
        self.config = load_config()

        # 设置cookie和数据目录，支持exe打包
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = 'https://www.douyin.com/?recommend=1'
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
        self.required_columns = [
            '达人昵称'
        ]

    def search_douyin_user(self, keyword):
        """搜索抖音用户"""
        try:
            if not self.page:
                logger.error("页面未初始化")
                return False
            
            # 访问抖音首页
            self.page.goto(self.base_url)
            self.common.random_sleep(5, 10)
            
            # 等待搜索框出现 - 使用多种选择器提高稳定性
            search_input = None
            search_selectors = [
                'input[data-e2e="searchbar-input"]',
                'input[placeholder*="搜索你感兴趣的内容"]',
                'input[type="text"][placeholder*="搜索"]',
                '.search-input input',
                'input.search-input'
            ]
            
            for selector in search_selectors:
                try:
                    search_input = self.page.locator(selector).first
                    search_input.wait_for(state='visible', timeout=5000)
                    logger.info(f"搜索框已出现，使用选择器: {selector}")
                    break
                except Exception as e:
                    logger.debug(f"选择器 {selector} 失败: {str(e)}")
                    continue
            
            if not search_input:
                logger.error("所有搜索框选择器都失败，未找到搜索框")
                return False
            
            # 清空搜索框并输入关键词
            search_input.clear()
            search_input.fill(keyword)
            self.common.random_sleep(1, 2)
            
            # 按回车键搜索或点击搜索按钮
            try:
                search_input.press('Enter')
            except:
                # 如果回车键失败，尝试点击搜索按钮
                search_button = self.page.locator('button[data-e2e="searchbar-button"]').first
                if search_button.is_visible():
                    search_button.click()
                else:
                    search_input.press('Enter')
            self.common.random_sleep(3, 5)
            
            # 等待搜索结果加载
            logger.info(f"正在搜索: {keyword}")
            
            # 查找用户标签 - 使用多种选择器提高稳定性
            user_tab = None
            selectors = [
                "span:has-text('用户')",
                "[data-e2e*='user']",
                ".tab-item:has-text('用户')",
                "div[role='tab']:has-text('用户')"
            ]
            
            for selector in selectors:
                try:
                    user_tab = self.page.locator(selector).first
                    if user_tab.is_visible(timeout=3000):
                        logger.info(f"找到用户标签，使用选择器: {selector}")
                        break
                except:
                    continue
            
            if user_tab and user_tab.is_visible():
                # 清空之前的API数据
                self.api_data.clear()
                # 点击用户标签前暂停5-10秒
                logger.info("点击用户标签前暂停...")
                self.common.random_sleep(5, 10)
                user_tab.click()
            else:
                logger.error("未找到用户标签")
                return False

            # 等待页面加载完成
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.warning(f"等待页面加载完成时出错: {str(e)}")
            
            # 额外等待API数据加载
            logger.info("等待API数据加载...")
            self.common.random_sleep(3, 5)
            
            # 检查是否获取到API数据
            if self.api_data:
                logger.info(f"搜索 {keyword} 完成，获取到 {len(self.api_data)} 个API响应")
                return True
            else:
                logger.warning(f"搜索 {keyword} 未获取到API数据")
                return False
            
        except Exception as e:
            logger.error(f"搜索用户 {keyword} 时出错: {str(e)}")
            return False

    def parse_search_results(self, keyword):
        """解析搜索结果并存储到数据库"""
        try:
            if not self.api_data:
                logger.warning(f"搜索 {keyword} 无API数据")
                return False
            
            # 查找包含user_list的API响应
            user_list_data = None
            for api_url, response_data in self.api_data.items():
                if 'web/discover/search' in api_url:
                    data = response_data.get('data', {})
                    if 'user_list' in data:
                        user_list_data = data['user_list']
                        break
            
            if not user_list_data:
                logger.warning(f"搜索 {keyword} 未找到user_list数据")
                return False
            
            logger.info(f"搜索 {keyword} 找到 {len(user_list_data)} 个用户")
            
            # 解析每个用户数据
            saved_count = 0
            skipped_count = 0
            for user_data in user_list_data:
                try:
                    # 提取基本信息
                    user_info = user_data.get('user_info', {})
                    if not user_info or not user_info.get('uid'):
                        logger.warning("用户数据缺少uid，跳过")
                        skipped_count += 1
                        continue
                    
                    uid = user_info.get('uid', '')
                    
                    # 检查uid是否已存在
                    existing_record = session.query(DouyinUserList).filter(DouyinUserList.uid == uid).first()
                    if existing_record:
                        # 更新上传人名字
                        old_nick_name = existing_record.nick_name
                        existing_record.nick_name = self.config['USERNAME']['username']
                        existing_record.updated_at = datetime.now()
                        
                        try:
                            session.commit()
                            logger.info(f"用户UID {uid} 已存在，已更新上传人: {old_nick_name} -> {self.config['USERNAME']['username']}")
                            saved_count += 1
                        except Exception as db_error:
                            logger.error(f"更新用户 {uid} 上传人失败: {str(db_error)}")
                            session.rollback()
                        continue
                    
                    # 创建数据库记录
                    db_record = DouyinUserList()
                    db_record.uid = uid
                    db_record.nick_name = self.config['USERNAME']['username']
                    db_record.created_at = datetime.now()
                    db_record.updated_at = datetime.now()
                    db_record.raw_response = json.dumps(user_data, ensure_ascii=False) if user_data else None
                    
                    # 提取基本信息
                    user_info = user_data.get('user_info', {})
                    if user_info:
                        db_record.user_info = json.dumps(user_info, ensure_ascii=False) if user_info else None
                    
                    # 提取其他字段，将对象/数组转换为JSON字符串
                    db_record.user_service_info = json.dumps(user_data.get('user_service_info'), ensure_ascii=False) if user_data.get('user_service_info') else None
                    db_record.baikes = json.dumps(user_data.get('baikes'), ensure_ascii=False) if user_data.get('baikes') else None
                    db_record.challenges = json.dumps(user_data.get('challenges'), ensure_ascii=False) if user_data.get('challenges') else None
                    db_record.effects = json.dumps(user_data.get('effects'), ensure_ascii=False) if user_data.get('effects') else None
                    db_record.items = json.dumps(user_data.get('items'), ensure_ascii=False) if user_data.get('items') else None
                    db_record.mix_list = json.dumps(user_data.get('mix_list'), ensure_ascii=False) if user_data.get('mix_list') else None
                    db_record.musics = json.dumps(user_data.get('musics'), ensure_ascii=False) if user_data.get('musics') else None
                    db_record.position = json.dumps(user_data.get('position'), ensure_ascii=False) if user_data.get('position') else None
                    db_record.product_info = json.dumps(user_data.get('product_info'), ensure_ascii=False) if user_data.get('product_info') else None
                    db_record.product_list = json.dumps(user_data.get('product_list'), ensure_ascii=False) if user_data.get('product_list') else None
                    db_record.shop_product_info = json.dumps(user_data.get('shop_product_info'), ensure_ascii=False) if user_data.get('shop_product_info') else None
                    db_record.uniqid_position = json.dumps(user_data.get('uniqid_position'), ensure_ascii=False) if user_data.get('uniqid_position') else None
                    db_record.userSubLightApp = json.dumps(user_data.get('userSubLightApp'), ensure_ascii=False) if user_data.get('userSubLightApp') else None
                    db_record.is_red_uniqueid = bool(user_data.get('is_red_uniqueid', False))
                    
                    # 保存到数据库
                    try:
                        session.add(db_record)
                        session.commit()
                        saved_count += 1
                        logger.info(f"保存用户: (UID: {uid})")
                    except Exception as db_error:
                        logger.error(f"保存用户 {uid} 到数据库失败: {str(db_error)}")
                        session.rollback()
                        continue
                    
                except Exception as e:
                    logger.error(f"解析用户数据时出错: {str(e)}")
                    session.rollback()
                    continue
            
            logger.info(f"搜索 {keyword} 完成，成功保存 {saved_count} 个用户，跳过 {skipped_count} 个重复用户")
            return True
            
        except Exception as e:
            logger.error(f"解析搜索结果时出错: {str(e)}")
            session.rollback()
            return False

    def select_excel_file(self):
        """选择Excel文件"""
        try:
            # 创建隐藏的根窗口
            root = Tk()
            root.withdraw()

            # 显示提示信息
            messagebox.showinfo("Excel导入", "请选择包含蒲公英链接的Excel文件\n\n文件应包含以下列："
                                             "达人昵称")

            # 打开文件选择对话框
            file_path = filedialog.askopenfilename(
                title="选择Excel文件",
                filetypes=[("Excel文件", "*.xlsx *.xls"), ("所有文件", "*.*")]
            )

            root.destroy()

            if file_path:
                self.excel_file_path = file_path
                logger.info(f"已选择Excel文件: {file_path}")
                return True
            else:
                logger.warning("未选择文件")
                return False

        except Exception as e:
            logger.error(f"选择Excel文件时出错: {str(e)}")
            return False

    def load_excel_data(self):
        """加载Excel数据"""
        try:
            if not self.excel_file_path:
                logger.error("未选择Excel文件")
                return False

            # 读取Excel文件
            self.excel_data = pd.read_excel(self.excel_file_path)
            logger.info(f"成功加载Excel数据，共 {len(self.excel_data)} 行")

            # 检查必需列
            missing_columns = [col for col in self.required_columns if col not in self.excel_data.columns]
            if missing_columns:
                logger.warning(f"Excel文件缺少以下列: {missing_columns}")
                # 添加缺失的列，确保数据类型为字符串
                for col in missing_columns:
                    self.excel_data[col] = ''
                    # 确保列的数据类型为字符串
                    self.excel_data[col] = self.excel_data[col].astype(str)

            # 显示表头信息
            logger.info(f"Excel表头: {list(self.excel_data.columns)}")

            # 确保所有必需列都是字符串类型，避免数据类型不兼容问题
            for col in self.required_columns:
                if col in self.excel_data.columns:
                    self.excel_data[col] = self.excel_data[col].astype(str)

            return True

        except Exception as e:
            logger.error(f"加载Excel数据时出错: {str(e)}")
            return False

    def process_excel_data(self):
        """处理Excel数据，搜索抖音用户"""
        try:
            if self.excel_data is None:
                logger.error("Excel数据未加载")
                return False

            total_rows = len(self.excel_data)
            logger.info(f"开始处理Excel数据，共 {total_rows} 行")
            logger.info("=" * 50)

            # 统计变量
            success_count = 0
            failed_count = 0
            skipped_count = 0

            # 遍历每一行数据
            for index, row in self.excel_data.iterrows():
                try:
                    keyword = row.get('达人昵称', '')
                    if not keyword or pd.isna(keyword) or keyword.strip() == '':
                        logger.info(f"第 {index + 1} 行：达人昵称为空，跳过")
                        continue

                    keyword = keyword.strip()
                    logger.info(f"第 {index + 1}/{total_rows} 行：开始搜索 '{keyword}'")
                    logger.info("-" * 30)

                    # 执行搜索
                    search_success = self.search_douyin_user(keyword)
                    if not search_success:
                        logger.warning(f"第 {index + 1} 行：搜索 '{keyword}' 失败")
                        failed_count += 1
                        continue

                    # 解析搜索结果并保存到数据库
                    parse_success = self.parse_search_results(keyword)
                    if not parse_success:
                        logger.warning(f"第 {index + 1} 行：解析 '{keyword}' 搜索结果失败")
                        failed_count += 1
                        continue

                    logger.info(f"第 {index + 1} 行：'{keyword}' 处理完成")
                    success_count += 1
                    
                    # 添加延迟，避免请求过于频繁（30-35秒）
                    if index < total_rows - 1:  # 不是最后一行才延迟
                        logger.info("等待30-35秒后处理下一个博主...")
                        self.common.random_sleep(30, 35)

                except Exception as e:
                    logger.error(f"处理第 {index + 1} 行时出错: {str(e)}")
                    continue

            logger.info("=" * 50)
            logger.info(f"Excel数据处理完成！")
            logger.info(f"总行数: {total_rows}")
            logger.info(f"成功处理: {success_count}")
            logger.info(f"处理失败: {failed_count}")
            logger.info(f"跳过空行: {skipped_count}")
            logger.info("=" * 50)
            return True

        except Exception as e:
            logger.error(f"处理Excel数据时出错: {str(e)}")
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
            target_apis = ['web/discover/search']

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

    def _get_numeric_value(self, row, column_name, default=0):
        """从Excel行数据中安全获取数值"""
        try:
            value = row.get(column_name, default)
            if pd.isna(value) or value is None or value == '':
                return default
            return float(value)
        except (ValueError, TypeError):
            return default


def main():
    """
    主函数 - 抖音搜索自动化程序
    """
    spider = None
    try:
        logger.info("=== 抖音搜索自动化程序启动 ===")
        spider = DouyinSearchSpider()

        # 1. 选择Excel文件
        if not spider.select_excel_file():
            logger.error("未选择Excel文件，程序退出")
            return False

        # 2. 加载Excel数据
        if not spider.load_excel_data():
            logger.error("加载Excel数据失败，程序退出")
            return False

        # 3. 初始化浏览器和登录
        spider.setup_browser()
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        # 4. 处理Excel数据（搜索抖音用户）
        if not spider.process_excel_data():
            logger.error("处理Excel数据失败")
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