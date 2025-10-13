import json
import os
import sys
import time
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright
import traceback
import pandas as pd
from tkinter import filedialog, messagebox, Tk
from urllib.parse import urlparse

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
        return os.path.abspath("../../WeekAccountUpdate")


class PGYSpider:
    def __init__(self):
        self.setup_logger()

        # 设置cookie和数据目录，支持exe打包
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')

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
            '粉丝量', '标签', '图文价格', '视频价格', '赞藏', '所在地区',
            '蒲公英状态', '所属机构', '最新笔记更新时间', '前两页视频笔记占比',
            '前两页笔记点赞中位数', '邀约48小时回复率',
            '合作行业', '图文预估cpm', '视频预估cpm', '图文预估cpc', '视频预估cpc',
            '图文预估cpe', '视频预估cpe', '日常曝光中位数', '日常阅读中位数', '日常互动中位数',
            '合作曝光中位数', '合作阅读中位数', '合作互动中位数', '日常图文+视频cpm', '合作图文+视频cpm',
            '日常图文+视频cpc', '合作图文+视频cpc', '日常图文+视频cpe', '合作图文+视频cpe', '女性粉丝占比',
            '活跃粉丝占比', '年龄＜18', '年龄18-24', '年龄25-34', '年龄35-44', '年龄＞44', '地域分布', '用户兴趣',
            '设备苹果华为', '博主已合作品牌', '博主已合作品牌及合作日期', '博主人设', '百赞比例', '千赞比例'
        ]

    def select_excel_file(self):
        """选择Excel文件"""
        try:
            # 创建隐藏的根窗口
            root = Tk()
            root.withdraw()

            # 显示提示信息
            messagebox.showinfo("Excel导入", "请选择包含蒲公英链接的Excel文件\n\n文件应包含以下列："
                                             "\n• 蒲公英链接（必填）\n• 达人昵称\n• 小红书ID\n• 粉丝量\n• 标签\n• "
                                             "图文价格\n• 视频价格\n• 赞藏\n• 所在地区")

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
        """处理Excel数据，抓取博主信息"""
        try:
            if self.excel_data is None:
                logger.error("Excel数据未加载")
                return False

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
                                    logger.info(f"API数据结构: {list(api_data.keys()) if isinstance(api_data, dict) else 'Not a dict'}")
                                    
                                    # 获取博主数据
                                    if 'data' in api_data and isinstance(api_data['data'], dict):
                                        blogger_data = api_data['data']
                                        logger.info(f"从api_data['data']获取博主数据")
                                    else:
                                        blogger_data = api_data
                                        logger.info(f"直接使用api_data作为博主数据")
                                    
                                    # 检查博主数据是否有效
                                    if blogger_data and isinstance(blogger_data, dict):
                                        if any(key in blogger_data for key in ['name', 'redId', 'fansCount', 'picturePrice']):
                                            logger.info(f"博主数据有效，包含字段: {[k for k in blogger_data.keys() if k in ['name', 'redId', 'fansCount', 'picturePrice', 'videoPrice']]}")
                                            self._process_blogger_info(blogger_data, index)
                                        else:
                                            logger.warning(f"博主数据无效，缺少必要字段")
                                    else:
                                        logger.warning(f"博主数据格式错误")

                                elif 'fans_profile' in api_url:
                                    self._process_fans_profile(api_data, index)

                                elif 'notes_rate' in api_url:
                                    # 确保价格数据已获取
                                    graphic_price = self._get_numeric_value(row, '图文价格', 0)
                                    video_price = self._get_numeric_value(row, '视频价格', 0)
                                    self._process_notes_rate(api_data, graphic_price, video_price, 'daily', index)

                                elif 'fans_summary' in api_url:
                                    self._process_fans_summary(api_data, index)

                                elif 'notes_detail' in api_url:
                                    self._process_notes_detail(api_data, index)

                            except Exception as api_error:
                                logger.error(f"处理API {api_url} 数据时出错: {str(api_error)}")
                                continue
                        
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

                            self.common.random_sleep(10, 15)

                            # 处理数据摘要API
                            data_summary_copy = dict(self.api_data)
                            for api_url, response_data in data_summary_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'data_summary' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            self._process_data_summary(api_data, index)
                                            break
                                except Exception as e:
                                    logger.warning(f"处理数据摘要API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理数据摘要步骤时出错: {str(e)}")

                        # 点击"合作笔记"按钮，获取合作笔记数据
                        try:
                            self.api_data.clear()
                            dropdown_container = self.page.locator('.d-spinner-nested-loading')
                            switch_button = dropdown_container.locator('button:has-text("合作笔记")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                            # 等待页面加载完成
                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(10, 15)

                            # 处理合作笔记API
                            notes_rate_copy = dict(self.api_data)
                            for api_url, response_data in notes_rate_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'notes_rate' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            # 确保价格数据已获取
                                            graphic_price = self._get_numeric_value(row, '图文价格', 0)
                                            video_price = self._get_numeric_value(row, '视频价格', 0)
                                            self._process_notes_rate(api_data, graphic_price, video_price, 'coop',
                                                                     index)
                                            break
                                except Exception as e:
                                    logger.warning(f"处理合作笔记API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理合作笔记步骤时出错: {str(e)}")

                        # 点击"图文＋视频"按钮，获取图文+视频数据
                        try:
                            self.api_data.clear()
                            dropdown_container = self.page.locator('.d-spinner-nested-loading')
                            switch_button = dropdown_container.locator('button:has-text("图文＋视频")').first
                            if switch_button.is_visible(timeout=5000):
                                switch_button.click()

                            # 等待页面加载完成
                            try:
                                self.page.wait_for_load_state('networkidle', timeout=5000)
                            except Exception as e:
                                logger.warning(f"等待页面加载完成时出错: {str(e)}")

                            self.common.random_sleep(10, 15)

                            # 处理图文+视频API
                            notes_rate_copy = dict(self.api_data)
                            for api_url, response_data in notes_rate_copy.items():
                                try:
                                    if not response_data or not isinstance(response_data, dict):
                                        continue

                                    if 'notes_rate' in api_url and 'data' in response_data:
                                        api_data = response_data.get('data', {})
                                        if api_data and isinstance(api_data, dict):
                                            # 确保价格数据已获取
                                            graphic_price = self._get_numeric_value(row, '图文价格', 0)
                                            video_price = self._get_numeric_value(row, '视频价格', 0)
                                            self._process_notes_rate(api_data, graphic_price, video_price, 'pic_video',
                                                                     index)
                                            break
                                except Exception as e:
                                    logger.warning(f"处理图文+视频API时出错: {str(e)}")
                                    continue

                        except Exception as e:
                            logger.error(f"处理图文+视频步骤时出错: {str(e)}")

                        # 更新Excel数据 - 现在由各个处理函数直接更新
                        logger.info(f"第 {index + 1} 行：API数据处理完成")

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
                        self.excel_data.at[
                            index, '小红书链接'] = f"https://www.xiaohongshu.com/solar/pre-trade/blogger-detail/{part}"
                        break

            # 设置蒲公英链接
            if pgy_url and not pd.isna(pgy_url):
                self.excel_data.at[index, '蒲公英链接'] = safe_convert_to_str(pgy_url)
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
            target_apis = ['user/blogger', 'fans_profile', 'notes_rate', 'fans_summary', 'notes_detail', 'data_summary']

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

    def _process_blogger_info(self, data, row_index):
        """处理博主基本信息"""
        try:
            # 验证输入数据
            if not data or not isinstance(data, dict):
                logger.warning("博主数据为空或格式错误")
                return

            personal_string = ''
            personal_list = []
            content_tags = data.get("contentTags", [])
            featureTags = data.get("featureTags", [])
            personal_tags = data.get("personalTags", [])

            # 安全处理个人标签
            if personal_tags and isinstance(personal_tags, list):
                for tag in personal_tags:
                    if tag and str(tag).strip():
                        personal_list.append(str(tag).strip())
            personal_string = "、".join(personal_list)

            tags = set()
            # 安全处理内容标签
            if content_tags and isinstance(content_tags, list):
                for tag in content_tags:
                    if isinstance(tag, dict):
                        taxonomy1 = tag.get("taxonomy1Tag")
                        taxonomy2 = tag.get("taxonomy2Tag")
                        if taxonomy1 and str(taxonomy1).strip():
                            tags.add(str(taxonomy1).strip())
                        if taxonomy2 and str(taxonomy2).strip():
                            tags.add(str(taxonomy2).strip())

            # 安全处理特征标签
            if featureTags and isinstance(featureTags, list):
                for tag in featureTags:
                    if tag and str(tag).strip():
                        tags.add(str(tag).strip())

            level_map = {
                0: "异常",
                1: "普通",
                2: "优秀"
            }
            currentLevel = data.get("currentLevel", 0)
            current_level = level_map.get(currentLevel, "")

            tag_string = "、".join(tags)

            # 安全处理价格信息
            picture_price = 0
            video_price = 0
            video_price_raw = data.get('videoPrice')
            picture_price_raw = data.get('picturePrice')

            if video_price_raw is not None:
                try:
                    video_price = float(video_price_raw)
                except (ValueError, TypeError):
                    logger.warning(f"视频价格转换失败: {video_price_raw}")
                    video_price = 0

            if picture_price_raw is not None:
                try:
                    picture_price = float(picture_price_raw)
                except (ValueError, TypeError):
                    logger.warning(f"图文价格转换失败: {picture_price_raw}")
                    picture_price = 0

            # 安全获取博主机构信息
            agency_name = self._safe_get_nested(data, ['noteSign', 'name'], "")

            # 安全获取其他字段
            nickname = data.get('name', '')
            userId = data.get('userId', '')
            xhs_id = data.get('redId', '')
            fans_count = data.get('fansCount', 0)
            like_collect_count = data.get('likeCollectCountInfo', '')
            region = data.get('location', '')

            # 验证粉丝数量
            if fans_count is not None:
                try:
                    fans_count = int(fans_count)
                except (ValueError, TypeError):
                    logger.warning(f"粉丝数量转换失败: {fans_count}")
                    fans_count = 0

            # 更新Excel数据
            logger.info(f"开始更新第 {row_index + 1} 行数据...")
            
            # 基本信息
            self.excel_data.at[row_index, '达人昵称'] = str(nickname) if nickname else ''
            
            # 小红书链接 - 优先使用redId，如果没有则使用userId
            if userId:
                self.excel_data.at[row_index, '小红书链接'] = f"https://www.xiaohongshu.com/user/profile/{userId}"
            else:
                self.excel_data.at[row_index, '小红书链接'] = ''
            
            self.excel_data.at[row_index, '小红书ID'] = str(xhs_id) if xhs_id else ''
            self.excel_data.at[row_index, '粉丝量'] = str(fans_count) if fans_count else '0'
            self.excel_data.at[row_index, '图文价格'] = str(picture_price) if picture_price else '0'
            self.excel_data.at[row_index, '视频价格'] = str(video_price) if video_price else '0'
            self.excel_data.at[row_index, '赞藏'] = str(like_collect_count) if like_collect_count else ''
            self.excel_data.at[row_index, '所在地区'] = str(region) if region else ''
            self.excel_data.at[row_index, '所属机构'] = str(agency_name) if agency_name else ''
            self.excel_data.at[row_index, '蒲公英状态'] = str(current_level) if current_level else ''
            self.excel_data.at[row_index, '标签'] = str(tag_string) if tag_string else ''
            self.excel_data.at[row_index, '博主人设'] = str(personal_string) if personal_string else ''
            
            # 验证更新结果
            logger.info(f"数据更新完成:")
            logger.info(f"  达人昵称: {self.excel_data.at[row_index, '达人昵称']}")
            logger.info(f"  小红书链接: {self.excel_data.at[row_index, '小红书链接']}")
            logger.info(f"  小红书ID: {self.excel_data.at[row_index, '小红书ID']}")
            logger.info(f"  粉丝量: {self.excel_data.at[row_index, '粉丝量']}")
            logger.info(f"  图文价格: {self.excel_data.at[row_index, '图文价格']}")
            logger.info(f"  视频价格: {self.excel_data.at[row_index, '视频价格']}")

        except Exception as e:
            logger.error(f"处理博主基本信息时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_fans_profile(self, api_data, row_index):
        """处理粉丝画像数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("粉丝画像API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝画像数据为空或格式错误")
                return

            # 安全获取数组元素的辅助函数
            def safe_get_array_item(arr, index, default_name="未知", default_percent=0.0):
                """安全获取数组元素，避免索引越界"""
                try:
                    if arr and isinstance(arr, list) and len(arr) > index:
                        item = arr[index]
                        if item and isinstance(item, dict):
                            name = item.get('name', default_name)
                            percent = item.get('percent', default_percent)
                            return str(name) if name else default_name, float(
                                percent) if percent is not None else default_percent
                    return default_name, default_percent
                except (ValueError, TypeError, IndexError):
                    return default_name, default_percent

            # 处理年龄分布（确保有5个年龄段）
            ages = data.get('ages', [])
            if not isinstance(ages, list):
                ages = []

            # 处理性别分布
            gender = data.get('gender', {})
            if not isinstance(gender, dict):
                gender = {}

            # 处理兴趣 top5
            interests = data.get('interests', [])
            if not isinstance(interests, list):
                interests = []

            interest_data = ""
            try:
                valid_interests = []
                for p in interests[:5]:
                    if p and isinstance(p, dict):
                        name = p.get('name')
                        percent = p.get('percent')
                        if name and percent is not None:
                            try:
                                percent_val = float(percent)
                                valid_interests.append(f"{str(name)} ({round(percent_val * 100, 1)}%)")
                            except (ValueError, TypeError):
                                continue
                interest_data = " 、".join(valid_interests)
            except Exception as e:
                logger.warning(f"处理兴趣数据时出错: {str(e)}")
                interest_data = ""

            # 处理城市 top3
            cities = data.get('cities', [])
            if not isinstance(cities, list):
                cities = []

            city_data = ""
            try:
                valid_cities = []
                for p in cities[:3]:
                    if p and isinstance(p, dict):
                        name = p.get('name')
                        percent = p.get('percent')
                        if name and percent is not None:
                            try:
                                percent_val = float(percent)
                                valid_cities.append(f"{str(name)} ({round(percent_val * 100, 1)}%)")
                            except (ValueError, TypeError):
                                continue
                city_data = " 、".join(valid_cities)
            except Exception as e:
                logger.warning(f"处理城市数据时出错: {str(e)}")
                city_data = ""

            # 处理设备数据
            devices = data.get('devices', [])
            if not isinstance(devices, list):
                devices = []

            apple_percent = 0
            huawei_percent = 0
            try:
                for d in devices:
                    if d and isinstance(d, dict):
                        name = d.get("name", "")
                        percent = d.get("percent", 0)
                        if isinstance(name, str) and percent is not None:
                            try:
                                if "apple" in name.lower():
                                    apple_percent = float(percent)
                                elif "huawei" in name.lower():
                                    huawei_percent = float(percent)
                            except (ValueError, TypeError):
                                continue
            except Exception as e:
                logger.warning(f"处理设备数据时出错: {str(e)}")

            apple_percent_str = f"苹果:{apple_percent * 100:.2f}%,华为:{huawei_percent * 100:.2f}%"

            # 安全处理年龄数据，避免索引越界
            def safe_age_percent(ages_list, index, default="0.00"):
                """安全获取年龄百分比"""
                try:
                    if (ages_list and isinstance(ages_list, list) and
                            len(ages_list) > index and
                            ages_list[index] and
                            isinstance(ages_list[index], dict)):
                        percent = ages_list[index].get('percent', 0)
                        if percent is not None:
                            try:
                                return f"{float(percent):.2f}"
                            except (ValueError, TypeError):
                                return default
                    return default
                except (ValueError, TypeError, IndexError):
                    return default

            # 安全获取性别比例
            female_ratio = "0.00"
            try:
                if gender and isinstance(gender, dict):
                    female_val = gender.get('female')
                    if female_val is not None:
                        female_ratio = f"{float(female_val):.2f}"
            except (ValueError, TypeError):
                female_ratio = "0.00"

            # 更新Excel数据
            self.excel_data.at[row_index, "年龄＜18"] = safe_age_percent(ages, 0)
            self.excel_data.at[row_index, "年龄18-24"] = safe_age_percent(ages, 1)
            self.excel_data.at[row_index, "年龄25-34"] = safe_age_percent(ages, 2)
            self.excel_data.at[row_index, "年龄35-44"] = safe_age_percent(ages, 3)
            self.excel_data.at[row_index, "年龄＞44"] = safe_age_percent(ages, 4)
            self.excel_data.at[row_index, "设备苹果华为"] = apple_percent_str
            self.excel_data.at[row_index, "地域分布"] = city_data
            self.excel_data.at[row_index, "女性粉丝占比"] = female_ratio
            self.excel_data.at[row_index, "用户兴趣"] = interest_data

        except Exception as e:
            logger.error(f"处理粉丝画像数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_data_summary(self, api_data, row_index):
        """处理数据摘要"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("数据摘要API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("数据摘要数据为空或格式错误")
                return

            # 处理行业名称
            trade_names = data.get('tradeNames', [])
            trade_name = ""
            try:
                if isinstance(trade_names, list) and trade_names:
                    # 过滤掉空值和None，确保数据质量
                    valid_trade_names = []
                    for name in trade_names:
                        if name and str(name).strip():
                            valid_trade_names.append(str(name).strip())
                    trade_name = ", ".join(valid_trade_names)
            except Exception as e:
                logger.warning(f"处理行业名称时出错: {str(e)}")
                trade_name = ""

            # 安全获取数值，避免类型错误
            def safe_numeric_value(value, default=0):
                """安全获取数值，处理None和类型转换"""
                try:
                    if value is None:
                        return default
                    if value == '':
                        return default
                    return float(value)
                except (ValueError, TypeError):
                    return default

            # 安全获取各个字段值
            try:
                self.excel_data.at[row_index, '邀约48小时回复率'] = str(safe_numeric_value(data.get('responseRate')))
                self.excel_data.at[row_index, '合作行业'] = trade_name
                self.excel_data.at[row_index, '图文预估cpm'] = str(safe_numeric_value(data.get('estimatePictureCpm')))
                self.excel_data.at[row_index, '视频预估cpm'] = str(safe_numeric_value(data.get('estimateVideoCpm')))
                self.excel_data.at[row_index, '图文预估cpc'] = str(safe_numeric_value(data.get('picReadCost')))
                self.excel_data.at[row_index, '视频预估cpc'] = str(safe_numeric_value(data.get('videoReadCostV2')))
                self.excel_data.at[row_index, '图文预估cpe'] = str(
                    safe_numeric_value(data.get('estimatePictureEngageCost')))
                self.excel_data.at[row_index, '视频预估cpe'] = str(
                    safe_numeric_value(data.get('estimateVideoEngageCost')))
            except Exception as e:
                logger.error(f"更新数据摘要字段时出错: {str(e)}")

        except Exception as e:
            logger.error(f"处理数据摘要时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_notes_rate(self, api_data, graphic_price, video_price, type, row_index):
        """处理笔记率数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning(f"{type}笔记率API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning(f"{type}笔记率数据为空或格式错误")
                return

            # 安全获取数值，避免除零错误
            def safe_get_numeric(key, default=0):
                """安全获取数值"""
                try:
                    value = data.get(key, default)
                    if value is None:
                        return default
                    return float(value) if value != '' else default
                except (ValueError, TypeError):
                    return default

            imp_median = safe_get_numeric('impMedian', 0)
            read_median = safe_get_numeric('readMedian', 0)
            engage_median = safe_get_numeric('mEngagementNum', 0)

            # 确保价格不为零，避免除零错误
            safe_graphic_price = graphic_price if graphic_price and graphic_price > 0 else 1
            safe_video_price = video_price if video_price and video_price > 0 else 1

            # 计算CPM、CPC、CPE，添加安全检查
            def safe_division(numerator, denominator, multiplier=1):
                """安全除法，避免除零错误，保留两位小数"""
                try:
                    if denominator and denominator > 0:
                        return round((numerator / denominator) * multiplier, 2)
                    return 0
                except (ZeroDivisionError, TypeError):
                    return 0

            # 计算各项指标
            cpm_value = min(
                safe_division(safe_graphic_price, imp_median, 1000),
                safe_division(safe_video_price, imp_median, 1000)
            )

            cpc_value = min(
                safe_division(safe_graphic_price, read_median),
                safe_division(safe_video_price, read_median)
            )

            cpe_value = min(
                safe_division(safe_graphic_price, engage_median),
                safe_division(safe_video_price, engage_median)
            )

            # 根据类型更新不同的字段
            if type == 'daily':
                self.excel_data.at[row_index, '日常曝光中位数'] = str(imp_median)
                self.excel_data.at[row_index, '日常阅读中位数'] = str(read_median)
                self.excel_data.at[row_index, '日常互动中位数'] = str(engage_median)
                self.excel_data.at[row_index, '日常图文+视频cpm'] = str(cpm_value)
                self.excel_data.at[row_index, '日常图文+视频cpc'] = str(cpc_value)
                self.excel_data.at[row_index, '日常图文+视频cpe'] = str(cpe_value)

                # 处理日常数据的额外字段
                try:
                    hundred_like = safe_get_numeric('hundredLikePercent', 0)
                    thousand_like = safe_get_numeric('thousandLikePercent', 0)
                    self.excel_data.at[row_index, '百赞比例'] = str(hundred_like)
                    self.excel_data.at[row_index, '千赞比例'] = str(thousand_like)
                except Exception as e:
                    logger.warning(f"处理日常数据额外字段时出错: {str(e)}")
                    self.excel_data.at[row_index, '百赞比例'] = '0'
                    self.excel_data.at[row_index, '千赞比例'] = '0'

            elif type == 'coop':
                self.excel_data.at[row_index, '合作曝光中位数'] = str(imp_median)
                self.excel_data.at[row_index, '合作阅读中位数'] = str(read_median)
                self.excel_data.at[row_index, '合作互动中位数'] = str(engage_median)
                self.excel_data.at[row_index, '合作图文+视频cpm'] = str(cpm_value)
                self.excel_data.at[row_index, '合作图文+视频cpc'] = str(cpc_value)
                self.excel_data.at[row_index, '合作图文+视频cpe'] = str(cpe_value)

            elif type == 'pic_video':
                # 图文+视频的混合数据
                self.excel_data.at[row_index, '日常图文+视频cpm'] = str(cpm_value)
                self.excel_data.at[row_index, '日常图文+视频cpc'] = str(cpc_value)
                self.excel_data.at[row_index, '日常图文+视频cpe'] = str(cpe_value)

        except Exception as e:
            logger.error(f"处理{type}笔记率数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_fans_summary(self, api_data, row_index):
        """处理粉丝概要数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("粉丝概要API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("粉丝概要数据为空或格式错误")
                return

            # 安全获取活跃粉丝率
            active_fans_rate = data.get('activeFansRate')
            active_fans_ratio = 0.0

            try:
                if active_fans_rate is not None:
                    # 确保activeFansRate是有效的数值
                    rate_value = float(active_fans_rate)
                    if 0 <= rate_value <= 100:  # 验证百分比范围
                        active_fans_ratio = rate_value / 100
                    else:
                        logger.warning(f"activeFansRate值超出范围: {rate_value}%")
                        active_fans_ratio = 0.0
                else:
                    logger.warning("未找到activeFansRate字段")
                    active_fans_ratio = 0.0

            except (ValueError, TypeError) as e:
                logger.warning(f"activeFansRate值转换失败: {active_fans_rate}, 错误: {str(e)}")
                active_fans_ratio = 0.0
            except Exception as e:
                logger.warning(f"处理活跃粉丝率时出现未知错误: {str(e)}")
                active_fans_ratio = 0.0

            # 更新Excel数据
            try:
                self.excel_data.at[row_index, '活跃粉丝占比'] = str(active_fans_ratio)
            except Exception as e:
                logger.error(f"更新活跃粉丝率字段时出错: {str(e)}")

        except Exception as e:
            logger.error(f"处理粉丝概要数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_notes_detail(self, api_data, row_index):
        """处理笔记详情数据"""
        try:
            # 验证API数据
            if not self._validate_api_data(api_data):
                logger.warning("笔记详情API数据验证失败")
                return

            data = api_data.get('data', {})
            if not data or not isinstance(data, dict):
                logger.warning("笔记详情数据为空或格式错误")
                return

            # 安全获取笔记列表
            note_list = data.get('list', [])
            if not isinstance(note_list, list):
                logger.warning("笔记列表不是数组格式")
                return

            if not note_list:
                logger.info("笔记列表为空")
                return

            brand_set = set()
            brand_date_map = set()
            last_publish_date = None
            likeNum = 0
            video_num = 0

            # 处理每个笔记项
            for item in note_list:
                if not isinstance(item, dict):
                    continue

                try:
                    # 安全获取点赞数
                    item_like_num = item.get("likeNum", 0)
                    if item_like_num is not None:
                        try:
                            like_val = float(item_like_num)
                            if like_val > 0:
                                likeNum += like_val
                        except (ValueError, TypeError):
                            logger.warning(f"点赞数转换失败: {item_like_num}")

                    # 检查是否为视频
                    isVideo = item.get("isVideo", False)
                    if isVideo:
                        video_num += 1

                    # 处理品牌信息
                    brandName = item.get("brandName")
                    if brandName and str(brandName).strip():
                        brand_set.add(str(brandName).strip())

                        # 处理品牌合作日期
                        date_str = item.get("date")
                        if date_str and str(date_str).strip():
                            try:
                                date_obj = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
                                date_str_formatted = f"{date_obj.month}月{date_obj.day}日"
                                brandName_str = f"{str(brandName).strip()}:{date_str_formatted}"
                                brand_date_map.add(brandName_str)
                            except Exception as e:
                                logger.warning(f"解析品牌合作日期失败: {date_str}, 错误: {str(e)}")

                    # 处理笔记发布日期
                    date_str = item.get('date')
                    if date_str and str(date_str).strip():
                        try:
                            note_date = datetime.strptime(str(date_str).strip(), "%Y-%m-%d").date()
                            if not last_publish_date or note_date > last_publish_date:
                                last_publish_date = note_date
                        except Exception as e:
                            logger.warning(f"解析笔记日期失败: {date_str}, 错误: {str(e)}")
                            continue

                except Exception as e:
                    logger.warning(f"处理笔记项时出错: {str(e)}")
                    continue

            # 构造"已合作品牌"格式
            brand_list = sorted(list(brand_set))  # 排序确保一致性
            brand_names_str = "、".join(brand_list) if brand_list else ""

            # 构造"品牌合作日期"格式
            brand_dates_list = sorted(list(brand_date_map))  # 排序确保一致性
            brand_dates_str = "、".join(brand_dates_list) if brand_dates_list else ""

            # 安全计算比例，避免除零错误
            video_note_ratio = 0.0
            note_like_median = 0.0

            try:
                if len(note_list) > 0:
                    video_note_ratio = round(video_num / len(note_list), 4)
                    note_like_median = round(likeNum / len(note_list), 2)
            except (ZeroDivisionError, TypeError) as e:
                logger.warning(f"计算比例时出错: {str(e)}")
                video_note_ratio = 0.0
                note_like_median = 0.0

            # 更新Excel数据
            try:
                self.excel_data.at[row_index, '前两页视频笔记占比'] = str(video_note_ratio)
                self.excel_data.at[row_index, '前两页笔记点赞中位数'] = str(note_like_median)
                self.excel_data.at[row_index, '博主已合作品牌'] = brand_names_str
                self.excel_data.at[row_index, '博主已合作品牌及合作日期'] = brand_dates_str
                if last_publish_date:
                    self.excel_data.at[row_index, '最新笔记更新时间'] = last_publish_date.strftime('%Y-%m-%d')
            except Exception as e:
                logger.error(f"更新笔记详情字段时出错: {str(e)}")

        except Exception as e:
            logger.error(f"处理笔记详情数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _validate_api_data(self, api_data, required_fields=None):
        """
        验证API数据的有效性

        Args:
            api_data: API响应数据
            required_fields: 必需字段列表

        Returns:
            bool: 数据是否有效
        """
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
        """
        安全获取嵌套字典中的值

        Args:
            data: 数据字典
            keys: 键的列表，如 ['noteSign', 'name']
            default: 默认值

        Returns:
            获取到的值或默认值
        """
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