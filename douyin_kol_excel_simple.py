import json
import os
import time
import random
from datetime import datetime, timedelta
import sys
from typing import Optional, Dict, Any
import traceback
import pandas as pd
from tkinter import filedialog, messagebox, Tk
import re
import calendar

import playwright
from loguru import logger
from playwright.sync_api import sync_playwright
from unitl.common import Common

"""
    抖音KOL数据抓取程序 - 简化Excel版本
    功能：
    1. 从Excel导入星图链接
    2. 抓取博主名、报价信息
    3. 统计每月商单数和营收
    4. 直接输出到Excel，不涉及数据库
"""


# 配置常量
def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(
            os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../..")


class DouYinSpiderExcelSimple:
    def __init__(self):
        self.setup_logger()
        # 设置logger属性
        self.logger = logger
        # 设置cookie和数据目录，支持exe打包
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.xingtu.cn/ad/creator/index"
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.common = Common()
        
        # 存储用户笔记数据
        self.note_data = []
        
        # Excel文件路径
        self.excel_file_path = None
        self.excel_data = None
        
        # 当前正在处理的KOL信息
        self.current_kol = None

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # 必需列
        self.required_columns = ['星图链接']

    def select_excel_file(self):
        """选择Excel文件"""
        try:
            # 创建隐藏的根窗口
            root = Tk()
            root.withdraw()

            # 显示提示信息
            messagebox.showinfo("Excel导入", "请选择包含星图链接的Excel文件\n\n文件应包含以下列：\n• 星图链接（必填）\n\n程序将自动填充以下字段：\n• 博主名\n• 20秒报价\n• 20-60秒报价\n• 60秒+报价\n• 每月商单数统计\n• 每月营收统计")

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
                logger.error(f"Excel文件缺少以下必需列: {missing_columns}")
                return False

            # 添加需要的列（如果不存在）
            required_new_columns = [
                '博主名', '20秒报价', '20-60秒报价', '60秒+报价',
                '总商单数', '总营收'
            ]
            
            for col in required_new_columns:
                if col not in self.excel_data.columns:
                    self.excel_data[col] = ''
                    logger.info(f"添加新列: {col}")
            
            # 预先添加可能的月度字段（1-12月）
            # 这样确保所有月度字段都能正确添加
            for month in range(1, 13):
                month_key_orders = f'{month}月商单数'
                month_key_revenue = f'{month}月营收'
                
                if month_key_orders not in self.excel_data.columns:
                    self.excel_data[month_key_orders] = ''
                    logger.debug(f"预添加月度列: {month_key_orders}")
                
                if month_key_revenue not in self.excel_data.columns:
                    self.excel_data[month_key_revenue] = ''
                    logger.debug(f"预添加月度列: {month_key_revenue}")
            
            # 将所有新添加的列设置为object类型，避免数据类型不兼容警告
            all_new_columns = required_new_columns + [
                f'{month}月商单数' for month in range(1, 13)
            ] + [
                f'{month}月营收' for month in range(1, 13)
            ]
            
            # 统一设置所有列的数据类型为object
            for col in all_new_columns:
                if col in self.excel_data.columns:
                    self.excel_data[col] = self.excel_data[col].astype('object')
                    logger.debug(f"设置列 {col} 为object类型")
            
            # 显示表头信息
            logger.info(f"Excel表头: {list(self.excel_data.columns)}")
            
            # 显示数据类型信息用于调试
            logger.info("列数据类型:")
            for col in all_new_columns:
                if col in self.excel_data.columns:
                    logger.info(f"  {col}: {self.excel_data[col].dtype}")

            return True

        except Exception as e:
            logger.error(f"加载Excel数据时出错: {str(e)}")
            return False

    def process_excel_data(self):
        """处理Excel数据，抓取KOL信息"""
        try:
            if self.excel_data is None:
                logger.error("Excel数据未加载")
                return False

            # 遍历每一行数据
            for index, row in self.excel_data.iterrows():
                try:
                    xingtu_url = row.get('星图链接', '')
                    if not xingtu_url or pd.isna(xingtu_url) or xingtu_url.strip() == '':
                        logger.info(f"第 {index + 1} 行：星图链接为空，跳过")
                        continue

                    # 从星图链接中提取user_id
                    user_id = self._extract_user_id_from_url(xingtu_url)
                    if not user_id:
                        logger.warning(f"第 {index + 1} 行：无法从星图链接提取用户ID，跳过")
                        continue

                    logger.info(f"第 {index + 1} 行：开始处理KOL，用户ID: {user_id}")

                    # 清空之前的数据
                    self.api_data.clear()
                    self.note_data = []

                    # 执行抓取
                    result = self.scrape_user_notes(user_id, xingtu_url)
                    
                    # 更新Excel数据
                    self._update_excel_row(index, row, user_id)
                    
                    if result == 1:
                        logger.info(f"第 {index + 1} 行：KOL {user_id} 处理成功")
                    elif result == 2:
                        logger.info(f"第 {index + 1} 行：KOL {user_id} 没有创作能力数据")
                    else:
                        logger.warning(f"第 {index + 1} 行：KOL {user_id} 处理失败")

                    # 每个KOL之间等待一段时间，避免请求过于频繁
                    if index < len(self.excel_data) - 1:  # 最后一个不需要等待
                        wait_time = random.randint(10, 15)
                        logger.info(f"等待 {wait_time} 秒后处理下一个KOL...")
                        time.sleep(wait_time)

                except Exception as e:
                    logger.error(f"处理第 {index + 1} 行时出错: {str(e)}")
                    continue

            # 处理完成，保存Excel文件
            if self._save_excel_data_to_original():
                logger.info("所有KOL数据处理完成，Excel文件已保存")
                return True
            else:
                logger.error("保存Excel文件失败")
                return False

        except Exception as e:
            logger.error(f"处理Excel数据时出错: {str(e)}")
            return False

    def _update_excel_row(self, index, row, user_id):
        """更新Excel行数据"""
        try:
            logger.info(f"开始更新第 {index + 1} 行数据")

            # 数据类型转换函数
            def safe_convert_to_str(value):
                if pd.isna(value) or value is None or value == 'nan' or value == 'None':
                    return ''
                # 确保返回的是字符串类型
                result = str(value).strip()
                # 如果原值是数字，尝试保持数字格式但转换为字符串
                try:
                    if isinstance(value, (int, float)) and result != '':
                        # 对于数字，直接返回字符串形式
                        return result
                except:
                    pass
                return result

            # 更新博主名
            if self.current_kol:
                self.excel_data.at[index, '博主名'] = safe_convert_to_str(self.current_kol['name'])
                self.excel_data.at[index, '20秒报价'] = safe_convert_to_str(self.current_kol['1-20'])
                self.excel_data.at[index, '20-60秒报价'] = safe_convert_to_str(self.current_kol['20-60'])
                self.excel_data.at[index, '60秒+报价'] = safe_convert_to_str(self.current_kol['60+'])

            # 统计每月商单数和营收
            monthly_stats = self._calculate_monthly_stats()
            
            if monthly_stats:
                months_with_data = monthly_stats.get('months', set())
                
                # 只对有数据的月份动态添加月度字段
                if months_with_data:
                    self._add_monthly_columns(months_with_data)
                    
                    # 按月份顺序更新商单数和营收
                    for month in sorted(months_with_data):
                        month_key_orders = f'{month}月商单数'
                        month_key_revenue = f'{month}月营收'
                        
                        # 更新商单数
                        if month_key_orders in self.excel_data.columns:
                            orders_count = monthly_stats.get(f'month_{month}_orders', 0)
                            self.excel_data.at[index, month_key_orders] = safe_convert_to_str(orders_count)
                        
                        # 更新营收
                        if month_key_revenue in self.excel_data.columns:
                            revenue_amount = monthly_stats.get(f'month_{month}_revenue', 0)
                            self.excel_data.at[index, month_key_revenue] = safe_convert_to_str(revenue_amount)

                # 更新总商单数和总营收
                total_orders = monthly_stats.get('total_orders', 0)
                total_revenue = monthly_stats.get('total_revenue', 0)
                self.excel_data.at[index, '总商单数'] = safe_convert_to_str(total_orders)
                self.excel_data.at[index, '总营收'] = safe_convert_to_str(total_revenue)
                
                logger.info(f"月度统计完成 - 涉及月份: {sorted(months_with_data)}, 总商单数: {total_orders}, 总营收: {total_revenue}")
            else:
                logger.info("没有找到月度商单数据")

            logger.info(f"第 {index + 1} 行：已更新Excel数据")
            
        except Exception as e:
            logger.error(f"更新Excel行数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _calculate_monthly_stats(self):
        """计算每月商单数和营收统计"""
        try:
            monthly_stats = {}
            total_orders = 0
            total_revenue = 0
            months_with_data = set()  # 记录有数据的月份
            
            # 从笔记数据中统计
            if not self.note_data:
                logger.info("没有笔记数据，无法计算月度统计")
                return monthly_stats
                
            # 获取20-60秒的报价作为营收计算基准
            price_20_60 = 0
            if self.current_kol and '20-60' in self.current_kol:
                try:
                    price_20_60 = int(self.current_kol['20-60'])
                    logger.info(f"使用20-60秒报价计算营收: {price_20_60}")
                except (ValueError, TypeError):
                    logger.warning("无法解析20-60秒报价，营收计算可能不准确")
            else:
                logger.warning("未找到20-60秒报价，无法计算营收")
            
            # 分析笔记数据
            
            # 统计每个月的商单数
            for i, note in enumerate(self.note_data):
                try:
                    # 获取视频创建时间
                    create_timestamp = note.get('create_timestamp')
                    
                    if create_timestamp:
                        # 转换为datetime对象
                        create_date = datetime.fromtimestamp(create_timestamp)
                        month = create_date.month
                        
                        # 记录有数据的月份
                        months_with_data.add(month)
                        
                        # 初始化该月的数据（如果还没有）
                        if f'month_{month}_orders' not in monthly_stats:
                            monthly_stats[f'month_{month}_orders'] = 0
                            monthly_stats[f'month_{month}_revenue'] = 0
                        
                        # 增加该月的商单数（统计所有笔记）
                        monthly_stats[f'month_{month}_orders'] += 1
                        total_orders += 1
                        
                        # 计算营收
                        if price_20_60 > 0:
                            monthly_revenue = price_20_60
                            monthly_stats[f'month_{month}_revenue'] += monthly_revenue
                            total_revenue += monthly_revenue
                                
                except Exception as e:
                    logger.warning(f"处理第{i+1}条笔记数据时出错: {str(e)}")
                    continue
            
            monthly_stats['total_orders'] = total_orders
            monthly_stats['total_revenue'] = total_revenue
            monthly_stats['months'] = months_with_data  # 添加有数据的月份集合
            
            if months_with_data:
                logger.info(f"商单统计完成：总商单数 {total_orders}，总营收 {total_revenue}")
                logger.info(f"涉及月份: {sorted(months_with_data)}")
                for month in sorted(months_with_data):
                    orders = monthly_stats.get(f'month_{month}_orders', 0)
                    revenue = monthly_stats.get(f'month_{month}_revenue', 0)
                    logger.info(f"  {month}月: {orders}个商单, {revenue}营收")
            else:
                logger.info("没有找到符合条件的商单数据")
                
            return monthly_stats
            
        except Exception as e:
            logger.error(f"计算每月统计时出错: {str(e)}")
            return {}

    def _add_monthly_columns(self, months):
        """确认月度字段存在（已在load_excel_data中预添加）"""
        try:
            if not months:
                return
                
            # 将月份转换为排序后的列表
            sorted_months = sorted(list(months))
            logger.info(f"确认月度字段存在: {sorted_months}")
            
            # 检查月度字段是否都已存在
            missing_columns = []
            for month in sorted_months:
                month_key_orders = f'{month}月商单数'
                month_key_revenue = f'{month}月营收'
                
                if month_key_orders not in self.excel_data.columns:
                    missing_columns.append(month_key_orders)
                if month_key_revenue not in self.excel_data.columns:
                    missing_columns.append(month_key_revenue)
            
            if missing_columns:
                logger.warning(f"缺少月度字段: {missing_columns}")
                # 动态添加缺少的字段
                for col in missing_columns:
                    self.excel_data[col] = ''
                    # 确保新添加的列是object类型
                    self.excel_data[col] = self.excel_data[col].astype('object')
                    logger.info(f"动态添加缺少的列: {col} (object类型)")
            else:
                logger.info("所有月度字段都已存在")
                
            # 确保所有月度字段都是object类型
            for month in sorted_months:
                month_key_orders = f'{month}月商单数'
                month_key_revenue = f'{month}月营收'
                
                if month_key_orders in self.excel_data.columns:
                    self.excel_data[month_key_orders] = self.excel_data[month_key_orders].astype('object')
                if month_key_revenue in self.excel_data.columns:
                    self.excel_data[month_key_revenue] = self.excel_data[month_key_revenue].astype('object')
                    
        except Exception as e:
            logger.error(f"确认月度字段时出错: {str(e)}")

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

    def _extract_user_id_from_url(self, url):
        """从星图链接中提取user_id"""
        try:
            # 星图链接格式: https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{user_id}
            if 'author-homepage/douyin-video/' in url:
                user_id = url.split('author-homepage/douyin-video/')[-1]
                return user_id.split('?')[0]  # 去掉可能的查询参数
            return None
        except Exception as e:
            logger.error(f"提取用户ID时出错: {str(e)}")
            return None

    def scrape_user_notes(self, user_id: str, xingtu_url: str) -> int:
        """抓取指定KOL的笔记信息
        返回值：
        - 1: 处理成功
        - 2: 没有创作能力按钮（该KOL没有创作能力数据）
        - 0: 处理失败
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            self.current_kol = {'user_id': user_id, 'name': '', 'url': xingtu_url}
            self.api_data.clear()
            self.note_data = []

            self.page.goto(xingtu_url, timeout=30000)
            self.logger.info(f"成功访问页面: {xingtu_url}")

            # 等待页面加载完成
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

            self.common.random_sleep(3, 4)
            
            # 点击创作能力标签
            creative_tab = self.page.locator("div.el-tabs__nav >> div:has-text('创作能力')")
            if creative_tab and creative_tab.is_visible():
                # 点击前等待一下确保元素稳定
                time.sleep(0.5)
                creative_tab.click()
                self.logger.info("成功点击创作能力标签")

                # 等待点击生效
                try:
                    # 等待页面有变化
                    self.page.wait_for_timeout(1000)  # 等待1秒

                    # 等待API请求完成
                    self.logger.info("等待API请求完成...")
                    wait_time = random.randint(8, 12)
                    self.logger.info(f"等待 {wait_time} 秒，确保所有API请求完成...")
                    time.sleep(wait_time)

                except Exception as e:
                    self.logger.warning(f"检查点击效果时出错: {str(e)}")

            else:
                self.logger.warning(f"未找到创作能力标签，KOL {user_id} 可能没有创作能力数据")
                return 2  # 返回2表示没有创作能力按钮

            # 等待API数据
            try:
                # 简单等待一小段时间让API响应处理完成
                time.sleep(3)

                # 检查是否已经获取到API响应数据
                if self.api_data:
                    self.logger.info("✅ 成功获取到API响应数据")
                else:
                    self.logger.info("ℹ️ API响应处理完成，继续执行")

                return 1  # 返回1表示处理成功

            except Exception as e:
                self.logger.warning(f"等待API数据时出错: {str(e)}")
                return 1  # 即使出错也继续执行

        except Exception as e:
            self.logger.error(f"抓取KOL {user_id} 笔记时出错: {str(e)}")
            raise

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
            os.path.join(log_path, "douyin_kol_excel_simple_{time:YYYY-MM-DD}.log"),
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
            self.logger.info("浏览器已经初始化，跳过重复初始化")
            return

        # 设置playwright浏览器路径，支持exe打包
        base_path = get_base_path()
        playwright_browsers_path = os.path.join(base_path, 'ms-playwright')

        # 设置环境变量
        if os.path.exists(playwright_browsers_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = playwright_browsers_path
            self.logger.info(f"使用自定义浏览器路径: {playwright_browsers_path}")
        else:
            self.logger.warning(f"未找到自定义浏览器路径: {playwright_browsers_path}")

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
                self.logger.info("验证Cookie是否有效...")
                
                login_detected = False

                try:
                    element = self.page.locator(".user-avatar")
                    count = element.count()
                    self.logger.info(f"选择器 '{".user-avatar"}' 找到 {count} 个元素")

                    if count > 0:
                        if element.first.is_visible(timeout=3000):
                            self.logger.info(f"✅ 通过选择器 '{".user-avatar"}' 检测到Cookie有效")
                            login_detected = True
                except Exception as e:
                    self.logger.debug(f"选择器 '{".user-avatar"}' 检查出错: {str(e)}")
                
                # 更新登录状态
                if login_detected:
                    self.is_logged_in = True
                    self.logger.info("Cookie有效，已自动登录")
                else:
                    self.logger.info("Cookie已失效，需要重新登录")
                    self.is_logged_in = False
            except Exception as e:
                self.logger.warning(f"Cookie验证失败: {str(e)}")
                self.logger.info("将进行重新登录")
                self.is_logged_in = False
        else:
            self.page = self.context.new_page()
            self.is_logged_in = False

        # 设置页面超时时间
        self.page.set_default_timeout(20000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

        self.logger.info("浏览器初始化完成")

    def login(self):
        """
        等待用户手动登录，最多等待5分钟
        """
        try:
            if self.is_logged_in:
                self.logger.info("已处于登录状态")
                return True

            try:
                # 访问首页
                self.page.goto(self.base_url)
                self.logger.info("等待登录成功标识出现...")
                self.common.random_sleep(20, 30)
                # 尝试多个可能的选择器
                selectors = [
                    ".text-avatar",           # 抖音头像
                    ".user-avatar",           # 通用头像
                ]
                
                # 设置最大等待时间(5分钟)
                max_wait_time = 300  # 秒
                start_time = time.time()
                login_detected = False
                
                # 循环检查直到找到元素或超时
                while time.time() - start_time < max_wait_time:
                    # 每30秒提示一次等待状态
                    elapsed_time = int(time.time() - start_time)
                    if elapsed_time % 30 == 0 and elapsed_time > 0:
                        self.logger.info(f"⏳ 等待登录中... 已等待 {elapsed_time} 秒")
                    
                    # 尝试每个选择器
                    for selector in selectors:
                        try:
                            # 检查元素是否可见，设置较短的超时时间
                            element = self.page.locator(selector)
                            if element.count() > 0:
                                if element.first.is_visible(timeout=2000):
                                    self.logger.info(f"✅ 通过选择器 '{selector}' 检测到登录成功！")
                                    login_detected = True
                                    break
                        except Exception as e:
                            # 忽略错误，继续尝试下一个选择器
                            pass
                    
                    # 如果找到登录标识，退出循环
                    if login_detected:
                        break
                    
                    # 等待一小段时间再检查
                    time.sleep(2)
                
                # 检查是否登录成功
                if login_detected:
                    self.is_logged_in = True
                    
                    # 登录成功后保存Cookie
                    self._save_cookies()
                    
                    self.logger.info("🎉 登录成功！已保存Cookie")
                    return True
                else:
                    # 超时未检测到登录
                    self.logger.error("❌ 等待登录超时（5分钟），程序退出")
                    return False

            except Exception as e:
                self.logger.error(f"等待登录过程中出现异常: {str(e)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")
                return False

        except Exception as e:
            self.logger.error(f"登录过程出现异常: {str(e)}")
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

            self.logger.info("浏览器和playwright已关闭")
        except Exception as e:
            self.logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应 - 只处理指定的API接口"""
        try:
            url = response.url

            # 定义需要处理的目标API列表
            target_apis = [
                '/api/author/get_author_marketing_info',
                '/api/author/get_author_show_items_v2',
                '/api/author/get_author_base_info',
            ]

            # 检查是否是目标API
            matched_api = None
            for api in target_apis:
                if api in url:
                    matched_api = api
                    break

            # 如果不是目标API，直接返回
            if not matched_api:
                return

            # 验证当前是否有正在处理的用户
            if not self.current_kol or not self.current_kol.get('user_id'):
                self.logger.warning(f"没有正在处理的用户，跳过API响应: {url}")
                return

            current_user_id = self.current_kol.get('user_id')

            # 只处理XHR或fetch请求
            if response.request.resource_type not in ['xhr', 'fetch']:
                return

            # 检查响应状态
            if response.status != 200:
                self.logger.warning(f"API响应状态码异常: {response.status}, URL: {url}")
                return

            # 检查浏览器是否仍然有效
            if not hasattr(self, 'page') or not self.page or self.page.is_closed():
                self.logger.info(f"页面已关闭，跳过API数据处理: {url}")
                return

            try:
                response_data = response.json()
            except playwright._impl._errors.Error as pe:
                if "Protocol error (Network.getResponseBody)" in str(pe):
                    self.logger.warning("无法获取响应体，可能是临时性问题，将在下次请求时重试")
                    return
                raise
            except ValueError as e:
                self.logger.error(f"解析JSON时出错: {str(e)}, URL: {url}")
                return

            if not response_data or not isinstance(response_data, dict):
                self.logger.warning(f"API响应数据格式不正确: {url}")
                return

            # 存储API数据
            self.api_data[url] = response_data

            # 根据不同的API类型进行处理
            if '/api/author/get_author_marketing_info' in url:
                self.logger.info(f"捕获到营销信息API: {url}")
                self._process_marketing_info(response_data)

            if '/api/author/get_author_base_info' in url:
                self.logger.info(f"捕获到个人API: {url}")
                self._process_base_info(response_data)

            elif '/api/author/get_author_show_items_v2' in url:
                self.logger.info(f"捕获到用户笔记API: {url}")
                self._process_user_notes_data(response_data, current_user_id)

        except Exception as e:
            # 如果是浏览器关闭错误，不记录为错误
            if "Target page, context or browser has been closed" in str(e):
                self.logger.info(f"浏览器已关闭，跳过API数据处理: {url}")
            else:
                self.logger.error(f"处理API响应时出错: {str(e)}, URL: {url}")

    def _process_marketing_info(self, response_data: Dict[str, Any]):
        """处理营销信息数据，提取博主名和价格信息"""
        try:
            if not response_data:
                return

            # 提取博主名
            data = response_data
            prices = data.get('price_info')
            for price in prices:
                if price.get('video_type') == 1:
                    self.current_kol['1-20'] = price.get('price')
                elif price.get('video_type') == 2:
                    self.current_kol['20-60'] = price.get('price')
                elif price.get('video_type') == 71:
                    self.current_kol['60+'] = price.get('price')

        except Exception as e:
            self.logger.error(f"处理营销信息时出错: {str(e)}")

    def _process_base_info(self, response_data: Dict[str, Any]):
        """处理营销信息数据，提取博主名和价格信息"""
        try:
            if not response_data:
                return

            # 提取博主名
            data = response_data
            name = data.get('nick_name')
            if name:
                self.current_kol['name'] = name
                self.logger.info(f"获取到博主名: {name}")

        except Exception as e:
            self.logger.error(f"处理营销信息时出错: {str(e)}")

    def _process_user_notes_data(self, response_data: Dict[str, Any], user_id: str):
        """处理用户笔记数据"""
        try:
            if not response_data:
                self.logger.error("用户笔记API响应数据为空")
                return

            # 处理latest_star_item_info数据
            if 'latest_star_item_info' in response_data:
                notes_data = response_data.get('latest_star_item_info', [])
                if notes_data:
                    self.logger.info(f"开始处理星图笔记数据，共 {len(notes_data)} 条")
                    for note in notes_data:
                        self.note_data.append(note)

            # 处理latest_item_info数据
            if 'latest_item_info' in response_data:
                items_data = response_data.get('latest_item_info', [])
                if items_data:
                    self.logger.info(f"开始处理普通笔记数据，共 {len(items_data)} 条")
                    for item in items_data:
                        self.note_data.append(item)

            self.logger.info(f"总共收集到 {len(self.note_data)} 条笔记数据")

        except Exception as e:
            self.logger.error(f"处理用户笔记数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

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
    主函数 - 抖音KOL数据抓取程序（简化Excel版本）
    """
    spider = None
    try:
        print("=== 抖音KOL数据抓取程序启动（简化Excel版本）===")

        # 1. 选择Excel文件
        spider = DouYinSpiderExcelSimple()
        if not spider.select_excel_file():
            print("未选择Excel文件，程序退出")
            return False

        # 2. 加载Excel数据
        if not spider.load_excel_data():
            print("加载Excel数据失败，程序退出")
            return False

        # 3. 初始化爬虫
        spider.setup_browser()

        # 4. 登录
        login_success = spider.login()
        if not login_success:
            print("登录失败，程序退出")
            return False

        # 5. 处理Excel数据
        if not spider.process_excel_data():
            print("处理Excel数据失败")
            return False

        print("所有KOL数据处理完成")
        return True

    except KeyboardInterrupt:
        print("⚠️ 用户手动中断程序")
        return False
    except Exception as e:
        print(f"❌ 程序运行出错: {str(e)}")
        print(f"错误详情: {traceback.format_exc()}")
        return False
    finally:
        # 确保资源被正确释放
        if spider:
            try:
                spider.close()
                print("资源清理完成")
            except Exception as e:
                print(f"清理资源时出错: {str(e)}")


if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("程序执行成功")
            sys.exit(0)
        else:
            print("程序执行失败")
            sys.exit(1)
    except Exception as e:
        print(f"程序启动失败: {str(e)}")
        sys.exit(1)
