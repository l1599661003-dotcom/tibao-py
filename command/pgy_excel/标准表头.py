import json
import os
import sys
import configparser
import time
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from urllib.parse import urlparse

from unitl.common import Common

"""
    更新外采博主账号信息,博主变现，粉丝情况,从蒲公英抓取数据
    重构版本：基于Playwright模拟浏览器操作，无需token
    新增功能：Excel导入和数据填充
"""


def get_resource_path(relative_path):
    """获取资源文件路径，支持exe打包"""
    try:
        # PyInstaller创建临时文件夹并将路径存储在_MEIPASS中
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath("../../WeekAccountUpdate")
    return os.path.join(base_path, relative_path)


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        get_resource_path('WeekAccountUpdate/config.ini'),
        get_resource_path('../pgy_playwright/config.ini'),
        'WeekAccountUpdate/config.ini',
        'config.ini'
    ]

    config_loaded = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            config.read(config_path, encoding='utf-8')
            config_loaded = True
            break

    if not config_loaded:
        logger.error("未找到配置文件")
        raise FileNotFoundError("配置文件不存在")


class PGYSpider:
    def __init__(self):
        # 加载配置
        self.config = load_config()
        self.setup_logger()

        # 设置cookie和数据目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，使用exe文件所在目录（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            self.cookie_file = os.path.join(exe_dir, 'cookies.json')
            self.data_dir = os.path.join(exe_dir, 'data')
        else:
            # 开发环境下，使用当前文件同级目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.cookie_file = os.path.join(current_dir, 'cookies.json')
            self.data_dir = os.path.join(current_dir, 'data')

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
        
        # Excel处理相关
        self.excel_data = None
        self.excel_file_path = None
        self.required_columns = [
            '蒲公英链接', '小红书链接', '达人昵称', '小红书ID', 
            '粉丝量', '标签', '图文价格', '视频价格', '赞藏', '所在地区', '健康等级'
        ]

    def select_excel_file(self):
        """选择Excel文件"""
        try:
            # 创建隐藏的根窗口
            root = tk.Tk()
            root.withdraw()  # 隐藏主窗口
            
            # 显示提示信息
            messagebox.showinfo("Excel导入", "请选择包含蒲公英链接的Excel文件\n\n文件应包含以下列：\n• 蒲公英链接（必填）\n• 达人昵称\n• 小红书ID\n• 粉丝量\n• 标签\n• 图文价格\n• 视频价格\n• 赞藏\n• 所在地区\n• 健康等级")
            
            # 打开文件选择对话框
            file_path = filedialog.askopenfilename(
                title="选择Excel文件",
                filetypes=[
                    ("Excel文件", "*.xlsx *.xls"),
                    ("所有文件", "*.*")
                ]
            )
            
            root.destroy()  # 销毁根窗口
            
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
        """处理Excel数据，抓取博主信息"""
        try:
            if self.excel_data is None:
                logger.error("Excel数据未加载")
                return False
                
            total_rows = len(self.excel_data)
            
            # 遍历每一行数据
            for index, row in self.excel_data.iterrows():
                try:
                    
                    pgy_url = row.get('蒲公英链接', '')
                    if not pgy_url or pd.isna(pgy_url):
                        logger.info(f"第 {index + 1} 行：蒲公英链接为空，跳过")
                        continue
                    
                    # 清空之前的数据
                    self.api_data.clear()
                    
                    # 访问博主页面
                    logger.info(f"访问页面: {pgy_url}")
                    
                    try:
                        # 访问页面
                        self.page.goto(pgy_url)
                            
                        try:
                            self.page.wait_for_load_state('networkidle', timeout=5000)
                            logger.info("页面网络请求完成")
                        except Exception as net_error:
                            logger.warning(f"等待网络空闲超时: {str(net_error)}")

                        self.common.random_sleep(20, 40)
                            
                    except Exception as e:
                        logger.error(f"访问页面失败: {str(e)}")
                        continue
                    
                    # 检查API数据
                    if self.api_data:
                        # 处理API数据
                        api_data_copy = dict(self.api_data)
                        
                        # 检查是否有有效的API响应
                        has_valid_api = False
                        
                        for api_url, response_data in api_data_copy.items():
                            if 'data' not in response_data:
                                logger.warning(f"API响应缺少data字段: {api_url}")
                                continue
                            
                            if 'code' not in response_data['data']:
                                logger.warning(f"API响应data缺少code字段: {api_url}")
                                continue
                                
                            if response_data['data']['code'] != 0:
                                logger.warning(f"API响应code不为0: {response_data['data']['code']}")
                                continue
                                
                            # 检查是否包含博主基本信息
                            if 'blogger' in api_url or 'user' in api_url or 'cooperator' in api_url:
                                has_valid_api = True
                                break
                        
                        # 更新Excel数据
                        self._update_excel_row(index, row, api_data_copy)

                    else:
                        logger.warning(f"第 {index + 1} 行：未获取到API数据")
                        # 即使没有API数据，也尝试从URL提取基本信息
                        self._update_excel_row(index, row, {})
                        
                        # 记录处理结果
                        logger.info(f"第 {index + 1} 行：仅从URL提取信息")
                        
                except Exception as e:
                    logger.error(f"处理第 {index + 1} 行时出错: {str(e)}")
                    continue
            
            # 处理完成，开始保存文件
            if self._save_excel_data_to_original():
                logger.info("所有数据处理完成，文件已保存")
                return True
            else:
                logger.error("保存文件失败")
                return False
            
        except Exception as e:
            logger.error(f"处理Excel数据时出错: {str(e)}")
            return False

    def _update_excel_row(self, index, row, api_data_dict):
        """更新Excel行数据"""
        try:
            # 数据类型转换函数，确保所有数据都是字符串类型
            def safe_convert_to_str(value):
                if pd.isna(value) or value is None:
                    return ''
                return str(value)
            
            # 从蒲公英链接中提取ID作为小红书ID
            pgy_url = row.get('蒲公英链接', '')
            if pgy_url and not pd.isna(pgy_url):
                parsed = urlparse(pgy_url)
                path_parts = parsed.path.split('/')
                for part in path_parts:
                    if part and len(part) > 10 and not part.startswith('blogger-detail'):
                        # 确保数据类型兼容性
                        self.excel_data.at[index, '小红书ID'] = safe_convert_to_str(part)
                        self.excel_data.at[index, '小红书链接'] = f"https://www.xiaohongshu.com/user/profile/{part}"
                        break
            
            # 处理API数据，直接更新到Excel
            if api_data_dict:
                for api_url, response_data in api_data_dict.items():
                    if 'data' not in response_data:
                        continue
                        
                    api_data = response_data['data']
                    if 'code' not in api_data or api_data['code'] != 0:
                        continue
                        
                    # 检查数据结构
                    if 'data' in api_data and isinstance(api_data['data'], dict):
                        data = api_data['data']
                    elif 'name' in api_data and 'userId' in api_data:
                        data = api_data
                    else:
                        continue
                    
                    # 安全获取数据的辅助函数
                    def safe_get(obj, key, default=None):
                        if not obj or not isinstance(obj, dict):
                            return default
                        return obj.get(key, default)
                    
                    # 直接更新Excel数据
                    # 更新达人昵称
                    nickname = safe_get(data, 'name', '')
                    if nickname:
                        self.excel_data.at[index, '达人昵称'] = safe_convert_to_str(nickname)
                    
                    # 更新小红书ID
                    red_id = safe_get(data, 'redId', '')
                    if red_id:
                        self.excel_data.at[index, '小红书ID'] = safe_convert_to_str(red_id)
                    
                    # 更新粉丝量
                    fans_count = safe_get(data, 'fansCount', 0)
                    if fans_count:
                        self.excel_data.at[index, '粉丝量'] = safe_convert_to_str(fans_count)
                    
                    # 更新标签 - 组合多个标签源
                    tags_list = []
                    
                    # 处理contentTags
                    content_tags = safe_get(data, 'contentTags', [])
                    if content_tags and isinstance(content_tags, list):
                        for content_tag in content_tags:
                            if not isinstance(content_tag, dict):
                                continue
                            if safe_get(content_tag, 'taxonomy1Tag'):
                                tags_list.append(safe_convert_to_str(content_tag['taxonomy1Tag']))
                            taxonomy2_tags = safe_get(content_tag, 'taxonomy2Tags', [])
                            if taxonomy2_tags and isinstance(taxonomy2_tags, list):
                                tags_list.extend([safe_convert_to_str(tag) for tag in taxonomy2_tags])
                    
                    # 处理featureTags
                    feature_tags = safe_get(data, 'featureTags', [])
                    if feature_tags and isinstance(feature_tags, list):
                        tags_list.extend([safe_convert_to_str(tag) for tag in feature_tags])
                    
                    # 组合标签
                    combined_tags = ', '.join(tags_list) if tags_list else ''
                    if combined_tags:
                        self.excel_data.at[index, '标签'] = combined_tags
                    
                    # 更新图文价格
                    picture_price = safe_get(data, 'picturePrice', 0)
                    if picture_price:
                        self.excel_data.at[index, '图文价格'] = safe_convert_to_str(picture_price)
                    
                    # 更新视频价格
                    video_price = safe_get(data, 'videoPrice', 0)
                    if video_price:
                        self.excel_data.at[index, '视频价格'] = safe_convert_to_str(video_price)
                    
                    # 更新赞藏
                    like_count = safe_get(data, 'likeCollectCountInfo', '')
                    if like_count:
                        self.excel_data.at[index, '赞藏'] = safe_convert_to_str(like_count)

                    # 更新赞藏
                    like_count = safe_get(data, 'currentLevel', 2)
                    if like_count:
                        currentLevel = ''
                        if like_count == 2:
                            currentLevel = '健康'
                        elif like_count == 1:
                            currentLevel = '普通'
                        elif like_count == 0:
                            currentLevel = '异常'
                        self.excel_data.at[index, '健康等级'] = safe_convert_to_str(currentLevel)
                    
                    # 更新所在地区
                    location = safe_get(data, 'location', '')
                    if location:
                        self.excel_data.at[index, '所在地区'] = safe_convert_to_str(location)
                    
                    # 构建小红书链接
                    user_id = safe_get(data, 'userId', '')
                    if user_id:
                        xhs_url = f"https://www.xiaohongshu.com/user/profile/{safe_convert_to_str(user_id)}"
                        self.excel_data.at[index, '小红书链接'] = xhs_url
                    
                    # 只处理第一个有效的API响应
                    break
        except Exception as e:
            logger.error(f"更新Excel行数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _save_excel_data_to_original(self):
        """直接保存到原Excel文件"""
        try:
            if not self.excel_file_path:
                logger.error("未选择Excel文件")
                return False
                
            # 检查文件是否被占用
            try:
                # 尝试以写入模式打开文件，检查是否被占用
                with open(self.excel_file_path, 'r+b') as f:
                    pass
            except PermissionError:
                logger.error(f"文件被占用，无法保存: {self.excel_file_path}")
                logger.error("请关闭Excel文件后重试")
                return False
            
            # 直接保存到原文件
            try:
                self.excel_data.to_excel(self.excel_file_path, index=False)
                logger.info(f"数据已保存到原Excel文件: {self.excel_file_path}")
                return True
            except Exception as save_error:
                logger.error(f"保存Excel文件时出错: {str(save_error)}")
                return False
            
        except PermissionError as e:
            logger.error(f"文件权限错误: {str(e)}")
            logger.info("尝试保存到新文件...")
            
            # 尝试保存到新文件
            try:
                # 生成新的文件名
                file_dir = os.path.dirname(self.excel_file_path)
                file_name = os.path.basename(self.excel_file_path)
                name, ext = os.path.splitext(file_name)
                new_file_path = os.path.join(file_dir, f"{name}_已填充{ext}")
                
                # 保存到新文件
                self.excel_data.to_excel(new_file_path, index=False)
                logger.info(f"数据已保存到新文件: {new_file_path}")
                return True
                
            except Exception as save_error:
                logger.error(f"保存到新文件也失败: {str(save_error)}")
                return False
                
        except Exception as e:
            logger.error(f"保存Excel数据时出错: {str(e)}")
            return False

    def _save_excel_data(self):
        """保存更新后的Excel数据（保留原方法作为备用）"""
        try:
            if not self.excel_file_path:
                logger.error("未选择Excel文件")
                return False
                
            # 生成新的文件名
            file_dir = os.path.dirname(self.excel_file_path)
            file_name = os.path.basename(self.excel_file_path)
            name, ext = os.path.splitext(file_name)
            new_file_path = os.path.join(file_dir, f"{name}_已填充{ext}")
            
            # 保存文件
            self.excel_data.to_excel(new_file_path, index=False)
            logger.info(f"Excel数据已保存到: {new_file_path}")
            return True
            
        except PermissionError as e:
            logger.error(f"文件权限错误: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"保存Excel数据时出错: {str(e)}")
            return False
    def setup_logger(self):
        """设置日志配置，支持exe打包"""
        # 设置日志目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，在exe文件所在目录创建logs文件夹（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            log_path = os.path.join(exe_dir, 'logs')
        else:
            # 开发环境下，在WeekAccountUpdate同级目录创建logs文件夹
            current_dir = os.path.dirname(os.path.abspath(__file__))
            log_path = os.path.join(current_dir, 'logs')

        # 确保logs目录存在
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

    def scrape_user_notes(self):
        """抓取博主信息 - 单次执行模式"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return None

            logger.info("单次执行模式：请使用Excel导入模式来抓取博主数据")
            logger.info("或者选择创建Excel模板来准备数据文件")
            return True

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise  # 重新抛出异常，让上层处理


    def close(self):
        """
        关闭浏览器和playwright
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
        """处理API响应，只捕获指定的API请求"""
        try:
            url = response.url
            # 设置更广泛的目标API关键词
            target_apis = ['user/blogger']
            
            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)

            if is_target_api and (response.request.resource_type == 'fetch' or response.request.resource_type == 'xhr'):
                try:
                    
                    # 检查响应状态
                    if response.status != 200:
                        logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                        return

                    try:
                        data = response.json()
                        
                        # 找到匹配的API类型
                        matched_api = None
                        for api in target_apis:
                            if api in url:
                                matched_api = api
                                break
                        
                        # 存储有效的API数据
                        if matched_api:
                            self.api_data[url] = {
                                'url': url,
                                'data': data,
                                'api_type': matched_api,
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'status': response.status
                            }
                    except ValueError:
                        logger.warning(f"无效的JSON响应: {url}")

                except Exception as e:
                    logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

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
    主函数 - 直接进入Excel导入模式
    """
    try:
        logger.info("=== 蒲公英数据抓取程序启动 ===")
        spider = PGYSpider()
        
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

        # 4. 处理Excel数据
        if not spider.process_excel_data():
            logger.error("处理Excel数据失败")
            return False

        logger.info("Excel数据处理完成")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"Excel模式运行出错: {str(e)}")
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