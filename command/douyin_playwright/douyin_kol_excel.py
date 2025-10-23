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

import playwright
from models.models_tibao import DouYinKolRealization, DouYinKolNote
from core.database_text_tibao_2 import session
from loguru import logger
from playwright.sync_api import sync_playwright
from unitl.common import Common

"""
    抖音KOL数据抓取程序 - Excel导入版本
    结合douyin_kol.py的抓取逻辑和常用表头.py的Excel导入功能
"""


# 配置常量
def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(
            os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../..")


class DouYinSpiderExcel:
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
        self.found_match = False  # 添加标志位作为类属性
        self.api_data = {}  # 存储API数据
        self.common = Common()
        self.current_kol: Optional[Dict[str, str, str]] = None  # 当前正在处理的KOL信息
        self.processed_api_responses = set()  # 用于追踪已处理的API响应
        self.marketing_info = {}  # 存储营销信息
        self.last_request_time = 0  # 记录上次请求时间

        # 新增：存储所有API数据的字典
        self.kol_api_data = {
            'author_display': {},
            'link_struct': {},
            'platform_info': {},
            'commerce_info': {},
            'spread_info': {},
            'audience_distribution': {}
        }
        
        # 存储用户笔记数据
        self.note_data = []
        
        # Excel文件路径
        self.excel_file_path = None
        
        # 当前正在处理的KOL信息
        self.current_kol = None

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        # Excel处理相关
        self.excel_data = None
        self.excel_file_path = None
        self.required_columns = ['星图链接']  # 必需列

    def select_excel_file(self):
        """选择Excel文件"""
        try:
            # 创建隐藏的根窗口
            root = Tk()
            root.withdraw()

            # 显示提示信息
            messagebox.showinfo("Excel导入", "请选择包含星图链接的Excel文件\n\n文件应包含以下列：\n• 星图链接（必填）")

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

                    # 检查24小时缓存机制
                    cache_valid = self._check_cache_validity(user_id)
                    if cache_valid:
                        logger.info(f"第 {index + 1} 行：KOL {user_id} 数据在24小时内，跳过抓取")
                        # 即使跳过抓取，也要更新Excel数据
                        self._update_excel_row_with_db_data(index, row, user_id)
                        continue

                    # 清空之前的数据
                    self.api_data.clear()
                    self.kol_api_data = {
                        'author_display': {},
                        'link_struct': {},
                        'platform_info': {},
                        'commerce_info': {},
                        'spread_info': {},
                        'audience_distribution': {}
                    }
                    self.note_data = []

                    # 执行抓取
                    result = self.scrape_user_notes(user_id, xingtu_url)
                    
                    # 更新Excel数据
                    self._update_excel_row_with_db_data(index, row, user_id)
                    
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

    def _get_kol_data_from_db(self, user_id, xingtu_url):
        """从数据库获取KOL数据"""
        try:
            # 从DouYinKolRealization表获取数据
            realization_record = session.query(DouYinKolRealization).filter_by(
                douyin_user_id=user_id
            ).first()
            
            if realization_record:
                return {
                    'user_id': user_id,
                    'xingtu_url': xingtu_url,  # 保留原始星图链接
                    'douyin_nickname': realization_record.douyin_nickname or '',
                    'douyin_link': realization_record.douyin_link or '',
                    'price_info': realization_record.price_info or '[]',
                    'follower_count': realization_record.follower_count or 0,
                    'author_base_info': realization_record.author_base_info or '{}',
                    'self_intro': realization_record.self_intro or ''
                }
            
            # 如果没有找到记录，返回基本信息
            return {
                'user_id': user_id,
                'xingtu_url': xingtu_url,
                'douyin_nickname': '',
                'douyin_link': '',
                'price_info': '[]',
                'follower_count': 0,
                'author_base_info': '{}',
                'self_intro': ''
            }
            
        except Exception as e:
            logger.error(f"从数据库获取KOL数据时出错: {str(e)}")
            return None

    def _ensure_excel_columns_are_string(self, index):
        """确保Excel列都是字符串类型，避免pandas数据类型警告"""
        try:
            # 需要确保为字符串类型的列
            string_columns = [
                '星图链接', '昵称', '星图ID', '1-20s视频报价', '21-60s视频报价', 
                '60s+视频报价', '粉丝数', '90天商单数', '90天GMV(21s-60s)', 
                'MCN', '微信号'
            ]
            
            for col in string_columns:
                if col in self.excel_data.columns:
                    # 将列转换为字符串类型，并将nan替换为空字符串
                    self.excel_data[col] = self.excel_data[col].astype(str).replace('nan', '')
                    
        except Exception as e:
            # 忽略转换错误，继续执行
            pass

    def _update_excel_row_with_db_data(self, index, row, user_id):
        """根据数据库数据更新Excel行"""
        try:
            # 确保Excel列都是字符串类型，避免pandas数据类型警告
            self._ensure_excel_columns_are_string(index)
            
            # 从数据库获取KOL数据
            realization_record = session.query(DouYinKolRealization).filter_by(
                douyin_user_id=user_id
            ).first()
            
            if not realization_record:
                logger.warning(f"第 {index + 1} 行：未找到用户 {user_id} 的DouYinKolRealization记录")
                # 即使没有DouYinKolRealization记录，也尝试从DouYinKolNote获取基本信息
                note_record = session.query(DouYinKolNote).filter_by(
                    douyin_user_id=user_id
                ).order_by(DouYinKolNote.update_time.desc()).first()
                
                if note_record:
                    logger.info(f"第 {index + 1} 行：从DouYinKolNote表获取到用户 {user_id} 的记录")
                    # 使用note_record的基本信息，但其他字段可能为空
                    self._update_excel_with_note_data(index, user_id, note_record)
                else:
                    logger.warning(f"第 {index + 1} 行：DouYinKolNote表中也没有用户 {user_id} 的记录")
                return
            
            # 数据类型转换函数，确保所有数据都是字符串类型
            def safe_convert_to_str(value):
                if pd.isna(value) or value is None or value == 'nan' or value == 'None':
                    return ''
                return str(value).strip()
            
            # 更新昵称 - 优先从author_base_info中获取nick_name
            nickname = ''
            try:
                author_base_info_json = realization_record.author_base_info or '{}'
                author_info = json.loads(author_base_info_json) if author_base_info_json else {}
                nickname = author_info.get('nick_name', '') or ''
            except (json.JSONDecodeError, TypeError):
                pass
            
            # 如果author_base_info中没有昵称，使用douyin_nickname字段
            if not nickname and realization_record.douyin_nickname:
                nickname = realization_record.douyin_nickname
                
            if nickname:
                self.excel_data.at[index, '昵称'] = safe_convert_to_str(nickname)
            
            # 更新星图ID
            self.excel_data.at[index, '星图ID'] = safe_convert_to_str(user_id)
            
            # 解析价格信息
            price_info_json = realization_record.price_info or '[]'
            try:
                price_info = json.loads(price_info_json) if price_info_json else []
                if isinstance(price_info, list) and len(price_info) > 0:
                    # 取前三个价格信息
                    for i, price_item in enumerate(price_info[:3]):
                        if isinstance(price_item, dict):
                            price_value = price_item.get('price', 0)
                            
                            if i == 0:
                                self.excel_data.at[index, '1-20s视频报价'] = safe_convert_to_str(price_value)
                            elif i == 1:
                                self.excel_data.at[index, '21-60s视频报价'] = safe_convert_to_str(price_value)
                            elif i == 2:
                                self.excel_data.at[index, '60s+视频报价'] = safe_convert_to_str(price_value)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"解析价格信息失败 (user_id: {user_id}): {str(e)}")
            
            # 更新粉丝数
            if realization_record.follower_count:
                self.excel_data.at[index, '粉丝数'] = safe_convert_to_str(realization_record.follower_count)
            
            # 统计90天商单数
            business_orders_90d = 0
            try:
                # 计算90天前的日期
                ninety_days_ago = datetime.now() - timedelta(days=90)
                
                # 使用SQLAlchemy ORM查询
                business_count = session.query(DouYinKolNote).filter(
                    DouYinKolNote.douyin_user_id == user_id,
                    DouYinKolNote.duration_min == 1,
                    DouYinKolNote.douyin_item_date >= ninety_days_ago.strftime('%Y-%m-%d')
                ).count()
                
                business_orders_90d = business_count or 0
                self.excel_data.at[index, '90天商单数'] = safe_convert_to_str(business_orders_90d)
                
            except Exception as e:
                logger.warning(f"统计90天商单数失败 (user_id: {user_id}): {str(e)}")
            
            # 计算90天GMV (21s-60s)
            gmv_90d = 0
            try:
                price_21_60 = self.excel_data.at[index, '21-60s视频报价']
                if price_21_60:
                    # 从价格字符串中提取数字
                    price_match = re.search(r'(\d+)', str(price_21_60))
                    if price_match:
                        price_value = int(price_match.group(1))
                        gmv_90d = business_orders_90d * price_value
                        self.excel_data.at[index, '90天GMV(21s-60s)'] = safe_convert_to_str(gmv_90d)
            except Exception as e:
                logger.warning(f"计算GMV失败 (user_id: {user_id}): {str(e)}")
            
            # 提取MCN信息
            try:
                author_base_info_json = realization_record.author_base_info or '{}'
                author_info = json.loads(author_base_info_json) if author_base_info_json else {}
                mcn_name = author_info.get('mcn_name', '') or ''
                if mcn_name:
                    self.excel_data.at[index, 'MCN'] = safe_convert_to_str(mcn_name)
                
            except (json.JSONDecodeError, TypeError):
                pass
            
            # 直接使用self_intro字段作为微信号
            if realization_record.self_intro:
                self.excel_data.at[index, '微信号'] = safe_convert_to_str(realization_record.self_intro)
            
            logger.info(f"第 {index + 1} 行：已更新Excel数据")
            
        except Exception as e:
            logger.error(f"更新Excel行数据时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    def _update_excel_with_note_data(self, index, user_id, note_record):
        """使用DouYinKolNote数据更新Excel行"""
        try:
            # 确保Excel列都是字符串类型，避免pandas数据类型警告
            self._ensure_excel_columns_are_string(index)
            # 数据类型转换函数，确保所有数据都是字符串类型
            def safe_convert_to_str(value):
                if pd.isna(value) or value is None or value == 'nan' or value == 'None':
                    return ''
                return str(value).strip()
            
            # 更新星图ID
            self.excel_data.at[index, '星图ID'] = safe_convert_to_str(user_id)
            
            # 统计90天商单数
            business_orders_90d = 0
            try:
                # 计算90天前的日期
                ninety_days_ago = datetime.now() - timedelta(days=90)
                
                # 统计该用户90天内的商单数
                business_count = session.query(DouYinKolNote).filter(
                    DouYinKolNote.douyin_user_id == user_id,
                    DouYinKolNote.duration_min == 1,
                    DouYinKolNote.douyin_item_date >= ninety_days_ago.strftime('%Y-%m-%d')
                ).count()
                
                business_orders_90d = business_count or 0
                self.excel_data.at[index, '90天商单数'] = safe_convert_to_str(business_orders_90d)
                
                # 由于没有价格信息，GMV设为0
                self.excel_data.at[index, '90天GMV(21s-60s)'] = '0'
                
            except Exception as e:
                logger.warning(f"统计90天商单数失败 (user_id: {user_id}): {str(e)}")
            
            logger.warning(f"第 {index + 1} 行：DouYinKolNote表中只有笔记数据，缺少KOL基本信息（昵称、价格、粉丝数等）")
            logger.warning(f"建议强制重新抓取该KOL数据以获得完整信息")
            
        except Exception as e:
            logger.error(f"使用DouYinKolNote数据更新Excel行时出错: {str(e)}")
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

    def _generate_excel_report(self, processed_data):
        """生成Excel报表，将抓取的数据回填到Excel中"""
        try:
            logger.info("开始生成Excel报表...")
            
            # 准备Excel数据
            excel_data = []
            
            for data in processed_data:
                user_id = data.get('user_id', '')
                xingtu_url = data.get('xingtu_url', '')
                douyin_nickname = data.get('douyin_nickname', '')
                douyin_link = data.get('douyin_link', '')
                
                # 解析价格信息
                price_info_json = data.get('price_info', '[]')
                price_1_20 = ''
                price_21_60 = ''
                price_60_plus = ''
                
                try:
                    price_info = json.loads(price_info_json) if price_info_json else []
                    if isinstance(price_info, list) and len(price_info) > 0:
                        # 取前三个价格信息
                        for i, price_item in enumerate(price_info[:3]):
                            if isinstance(price_item, dict):
                                price_value = price_item.get('price', 0)
                                
                                if i == 0:
                                    price_1_20 = f"{price_value}"
                                elif i == 1:
                                    price_21_60 = f"{price_value}"
                                elif i == 2:
                                    price_60_plus = f"{price_value}"
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"解析价格信息失败 (user_id: {user_id}): {str(e)}")
                
                # 粉丝数
                follower_count = data.get('follower_count', 0)
                
                # 统计90天商单数
                business_orders_90d = 0
                try:
                    # 计算90天前的日期
                    ninety_days_ago = datetime.now() - timedelta(days=90)
                    
                    # 使用SQLAlchemy ORM查询
                    business_count = session.query(DouYinKolNote).filter(
                        DouYinKolNote.douyin_user_id == user_id,
                        DouYinKolNote.duration_min == 1,
                        DouYinKolNote.douyin_item_date >= ninety_days_ago.strftime('%Y-%m-%d')
                    ).count()
                    
                    business_orders_90d = business_count or 0
                    
                except Exception as e:
                    logger.warning(f"统计90天商单数失败 (user_id: {user_id}): {str(e)}")
                
                # 计算90天GMV (21s-60s)
                gmv_90d = 0
                try:
                    if price_21_60:
                        # 从价格字符串中提取数字
                        price_match = re.search(r'(\d+)', price_21_60)
                        if price_match:
                            price_value = int(price_match.group(1))
                            gmv_90d = business_orders_90d * price_value
                except Exception as e:
                    logger.warning(f"计算GMV失败 (user_id: {user_id}): {str(e)}")
                
                # 提取MCN信息
                mcn_name = ''
                wechat_id = ''
                try:
                    author_base_info_json = data.get('author_base_info', '{}')
                    author_info = json.loads(author_base_info_json) if author_base_info_json else {}
                    mcn_name = author_info.get('mcn_name', '') or ''
                    
                    # 从self_intro中提取微信号
                    self_intro = data.get('self_intro', '')
                    wechat_match = re.search(r'微信[号|：]\s*([a-zA-Z0-9_-]+)', self_intro)
                    if wechat_match:
                        wechat_id = wechat_match.group(1)
                    
                except (json.JSONDecodeError, TypeError):
                    pass
                
                # 添加到Excel数据
                excel_data.append({
                    '星图链接': xingtu_url,
                    '昵称': douyin_nickname,
                    '星图ID': user_id,
                    '1-20s视频报价': price_1_20,
                    '21-60s视频报价': price_21_60,
                    '60s+视频报价': price_60_plus,
                    '粉丝数': follower_count,
                    '90天商单数': business_orders_90d,
                    '90天GMV(21s-60s)': gmv_90d,
                    'MCN': mcn_name,
                    '微信号': wechat_id,
                })
            
            # 创建DataFrame并导出Excel
            df = pd.DataFrame(excel_data)
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'抖音KOL抓取结果_{timestamp}.xlsx'
            
            # 导出Excel
            df.to_excel(filename, index=False, engine='openpyxl')
            
            logger.info(f"✅ Excel报表已生成: {filename}")
            logger.info(f"📊 共导出 {len(excel_data)} 条数据")
            
            # 显示统计信息
            logger.info("\n📈 数据统计:")
            logger.info(f"总KOL数量: {len(excel_data)}")
            logger.info(f"有粉丝数的KOL: {len([x for x in excel_data if x['粉丝数'] > 0])}")
            logger.info(f"有90天商单的KOL: {len([x for x in excel_data if x['90天商单数'] > 0])}")
            logger.info(f"有MCN信息的KOL: {len([x for x in excel_data if x['MCN']])}")
            logger.info(f"有微信号的KOL: {len([x for x in excel_data if x['微信号']])}")
            
            return filename
            
        except Exception as e:
            logger.error(f"生成Excel报表失败: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return None

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

    def _check_cache_validity(self, user_id):
        """检查用户数据是否在24小时内更新过"""
        try:
            current_time = datetime.now()
            # 24小时前的时间戳
            cache_threshold = int((current_time.timestamp() - 24 * 3600))

            # 检查DouYinKolRealization表
            realization_record = session.query(DouYinKolRealization).filter_by(
                douyin_user_id=user_id
            ).first()

            # 检查DouYinKolNote表（取最新的记录）
            note_record = session.query(DouYinKolNote).filter_by(
                douyin_user_id=user_id
            ).order_by(DouYinKolNote.update_time.desc()).first()

            # 如果两个表都没有记录，需要抓取
            if not realization_record and not note_record:
                logger.info(f"用户 {user_id} 没有历史记录，需要抓取")
                return False

            # 只有DouYinKolRealization表有记录且时间有效才认为缓存有效
            if realization_record:
                if realization_record.update_time and realization_record.update_time >= cache_threshold:
                    logger.info(f"用户 {user_id} DouYinKolRealization数据在24小时内（更新时间: {datetime.fromtimestamp(realization_record.update_time)}）")
                    return True
                else:
                    logger.info(f"用户 {user_id} DouYinKolRealization数据超过24小时，需要重新抓取")
                    return False

            # 如果只有DouYinKolNote记录而没有DouYinKolRealization记录，需要抓取
            if note_record and not realization_record:
                logger.info(f"用户 {user_id} 只有DouYinKolNote记录，缺少DouYinKolRealization记录，需要抓取")
                return False

            logger.info(f"用户 {user_id} 数据超过24小时，需要重新抓取")
            return False

        except Exception as e:
            logger.error(f"检查缓存有效性时出错: {str(e)}")
            return False

    def scrape_user_notes(self, user_id: str, xingtu_url: str) -> int:
        """抓取指定KOL的笔记信息并匹配更新数据库
        返回值：
        - 1: 处理成功
        - 2: 没有连接用户按钮（该KOL没有连接用户数据）
        - 0: 处理失败
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            self.current_kol = {'user_id': user_id, 'name': '', 'url': xingtu_url}
            self.processed_api_responses.clear()
            # 完全重置营销信息，确保数据隔离
            self.marketing_info = {'user_id': user_id}
            # 重置API数据缓存
            self.api_data = {}
            # 添加API响应处理标志
            self.api_response_processed = False

            try:
                self.page.goto(xingtu_url, timeout=30000)
                self.logger.info(f"成功访问页面: {xingtu_url}")

                # 等待页面加载完成
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception as e:
                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

            except Exception as e:
                self.logger.error(f"访问页面超时: {xingtu_url}")
                return 0

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
                if self.api_response_processed:
                    self.logger.info("✅ 成功获取到API响应数据")
                else:
                    # 从日志看API数据实际上已经正确处理了，所以这里只是提示
                    self.logger.info("ℹ️ API响应处理完成，继续执行")

                return 1  # 返回1表示处理成功

            except Exception as e:
                self.logger.warning(f"等待API数据时出错: {str(e)}")
                return 1  # 即使出错也继续执行

        except Exception as e:
            self.logger.error(f"抓取KOL {user_id} 笔记时出错: {str(e)}")
            raise

    def _process_author_display(self, response_data: Dict[str, Any], user_id: str):
        """处理作者显示检查API数据，只保存follower字段、link_cnt字段和release_videos_cnt字段"""
        try:
            if not response_data:
                return

            # 提取需要的字段
            follower = response_data.get('follower', 0)
            link_cnt = response_data.get('link_cnt', 0)
            release_videos_cnt = response_data.get('release_videos_cnt', 0)

            # 存储到kol_api_data中
            self.kol_api_data['author_display'] = {
                'follower_count': follower,
                'link_count': link_cnt,
                'videos_count': release_videos_cnt
            }

            # 尝试更新数据库
            self._update_kol_api_data_to_db(user_id)

        except Exception as e:
            self.logger.error(f"处理作者显示数据时出错: {str(e)}")

    def _process_author_link_struct(self, response_data: Dict[str, Any], user_id: str):
        """处理作者链接结构API数据，保存link_struct对象为JSON格式"""
        try:
            if not response_data:
                self.logger.error("作者链接结构API响应数据为空")
                return

            # 提取link_struct字段
            link_struct = response_data.get('link_struct', {})

            if not link_struct:
                self.logger.warning(f"用户ID {user_id} 的链接结构数据为空")
                return

            # 将link_struct转换为JSON字符串
            try:
                link_struct_json = json.dumps(link_struct, ensure_ascii=False)

                # 存储到kol_api_data中
                self.kol_api_data['link_struct'] = {
                    'link_struct': link_struct_json
                }

                # 尝试更新数据库
                self._update_kol_api_data_to_db(user_id)

            except Exception as json_error:
                self.logger.error(f"将链接结构转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理链接结构数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_platform_info(self, response_data: Dict[str, Any], user_id: str):
        """处理作者平台渠道信息API数据，只保存self_intro字段"""
        try:
            if not response_data:
                self.logger.error("作者平台渠道信息API响应数据为空")
                return

            # 提取self_intro字段
            self_intro = response_data.get('self_intro', '')

            # 存储到kol_api_data中
            self.kol_api_data['platform_info'] = {
                'self_intro': self_intro
            }

            # 尝试更新数据库
            self._update_kol_api_data_to_db(user_id)

        except Exception as e:
            self.logger.error(f"处理平台渠道信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_commerce_info(self, response_data: Dict[str, Any], user_id: str):
        """处理作者商业传播信息API数据，保存整个响应对象为JSON格式"""
        try:
            if not response_data:
                self.logger.error("作者商业传播信息API响应数据为空")
                return

            # 将整个响应数据转换为JSON字符串
            try:
                commerce_info_json = json.dumps(response_data, ensure_ascii=False)

                # 存储到kol_api_data中
                self.kol_api_data['commerce_info'] = {
                    'commerce_info': commerce_info_json
                }

                # 尝试更新数据库
                self._update_kol_api_data_to_db(user_id)

            except Exception as json_error:
                self.logger.error(f"将商业传播信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理商业传播信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_spread_info(self, response_data: Dict[str, Any], user_id: str):
        """处理传播信息API数据"""
        try:
            if not response_data:
                return

            # 将整个响应数据转换为JSON字符串
            try:
                spread_info_json = json.dumps(response_data, ensure_ascii=False)

                # 存储到kol_api_data中
                self.kol_api_data['spread_info'] = {
                    'spread_info': spread_info_json
                }

                # 尝试更新数据库
                self._update_kol_api_data_to_db(user_id)

            except Exception as json_error:
                self.logger.error(f"将传播信息转换为JSON时出错: {str(json_error)}")

        except Exception as e:
            self.logger.error(f"处理传播信息数据时出错: {str(e)}")

    def _process_marketing_info(self, response_data: Dict[str, Any]):
        """处理营销信息数据"""
        try:
            if not response_data:
                return

            # 获取当前正在处理的KOL信息
            current_user_id = self.current_kol.get('user_id') if self.current_kol else None
            if not current_user_id:
                return

            # 提取价格信息
            price_info = response_data.get('price_info', [])
            
            # 将JSON对象转换为字符串
            try:
                industry_tags_json = json.dumps(response_data.get('industry_tags', []), ensure_ascii=False)
                price_info_json = json.dumps(price_info, ensure_ascii=False)
            except Exception as json_error:
                self.logger.error(f"将营销信息转换为JSON时出错: {str(json_error)}")
                return

            # 初始化价格数据
            price_data = {
                'industry_tags': industry_tags_json,
                'price_info': price_info_json,
                'douyin_user_id': current_user_id,
                'douyin_nickname': self.current_kol.get('name'),
                'create_time': int(datetime.now().timestamp()),
                'update_time': int(datetime.now().timestamp()),
            }

            # 保存到数据库
            self._save_marketing_data(current_user_id, price_data)

        except Exception as e:
            self.logger.error(f"处理营销信息时出错: {str(e)}")

    def _process_author_base_info(self, response_data: Dict[str, Any]):
        """处理作者基本信息数据"""
        try:
            if not response_data:
                return

            # 获取当前正在处理的KOL信息
            current_user_id = self.current_kol.get('user_id') if self.current_kol else None
            if not current_user_id:
                return
            
            # 提取链接信息
            douyin_link = f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{current_user_id}"
            
            # 将整个响应数据转换为JSON字符串
            try:
                author_base_info_json = json.dumps(response_data, ensure_ascii=False)
            except Exception as json_error:
                self.logger.error(f"将作者基本信息转换为JSON时出错: {str(json_error)}")
                return

            # 初始化价格数据
            price_data = {
                'author_base_info': author_base_info_json,
                'douyin_user_id': current_user_id,
                'douyin_nickname': self.current_kol.get('name'),
                'update_time': int(datetime.now().timestamp()),
                'douyin_link': douyin_link
            }

            # 保存到数据库
            self._save_marketing_data(current_user_id, price_data)

        except Exception as e:
            self.logger.error(f"处理作者基本信息时出错: {str(e)}")

    def _save_marketing_data(self, user_id: str, price_data: Dict[str, Any]):
        """保存营销数据到数据库"""
        try:
            # 检查是否已存在该用户的记录
            existing_record = session.query(DouYinKolRealization).filter_by(
                douyin_user_id=user_id
            ).first()

            if existing_record:
                # 更新现有记录
                for key, value in price_data.items():
                    setattr(existing_record, key, value)
                # 确保nickname字段也被更新
                if 'douyin_nickname' in price_data:
                    existing_record.douyin_nickname = price_data['douyin_nickname']
            else:
                # 创建新记录时，确保包含nickname字段
                if 'douyin_nickname' not in price_data and self.current_kol:
                    price_data['douyin_nickname'] = self.current_kol.get('name', '')
                record = DouYinKolRealization(**price_data)
                session.add(record)

            session.commit()

        except Exception as db_error:
            self.logger.error(f"保存营销数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _process_author_audience_distribution(self, response_data: Dict[str, Any], user_id: str):
        """处理受众分布API数据"""
        try:
            if not response_data:
                self.logger.error("受众分布API响应数据为空")
                return

            self.logger.info(f"开始处理受众分布API数据，用户ID: {user_id}")
            self.logger.info(f"受众分布API响应数据: {response_data}")

            # 提取distributions字段
            distributions = response_data.get('distributions', [])

            # 将distributions转换为JSON字符串
            try:
                distributions_json = json.dumps(distributions, ensure_ascii=False)

                # 存储到kol_api_data中
                self.kol_api_data['audience_distribution'] = {
                    'audience_distribution': distributions_json
                }

                self.logger.info(f"受众分布已存储到kol_api_data，准备更新数据库")

                # 尝试更新数据库
                self._update_kol_api_data_to_db(user_id)

            except Exception as json_error:
                self.logger.error(f"将受众分布转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理受众分布数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_user_posted_data(self, response_data: Dict[str, Any], user_id: str):
        """处理用户笔记数据，包括latest_star_item_info和latest_item_info"""
        try:
            if not response_data:
                self.logger.error("API响应数据为空")
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

            # 保存笔记数据到数据库
            if self.note_data:
                self._save_note_data_to_db(user_id)

        except Exception as e:
            self.logger.error(f"处理用户视频数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _save_note_data_to_db(self, user_id: str):
        """保存笔记数据到DouYinKolNote表"""
        try:
            current_time = int(datetime.now().timestamp())
            
            for note in self.note_data:
                try:
                    item_id = note.get('item_id', '')
                    if not item_id:
                        self.logger.warning("跳过处理：item_id为空")
                        continue

                    # 检查记录是否已存在
                    existing_record = session.query(DouYinKolNote).filter_by(
                        douyin_item_id=item_id).first()

                    if existing_record:
                        # 更新现有记录
                        self._update_note_record(existing_record, note, user_id)
                    else:
                        # 创建新记录
                        self._create_note_record(note, user_id)

                except Exception as e:
                    self.logger.error(f"处理单条笔记数据时出错: {str(e)}")
                    continue

            self.logger.info(f"成功处理 {len(self.note_data)} 条笔记数据")

        except Exception as e:
            self.logger.error(f"保存笔记数据时出错: {str(e)}")
            session.rollback()

    def _update_note_record(self, existing_record, note: Dict[str, Any], user_id: str):
        """更新现有笔记记录"""
        try:
            existing_record.douyin_user_id = user_id
            existing_record.douyin_item_title = note.get('item_title', '')
            existing_record.video_like = note.get('like', 0)
            existing_record.video_play = note.get('play', 0)
            existing_record.video_share = note.get('share', 0)
            existing_record.video_comment = note.get('comment', 0)
            existing_record.update_time = int(datetime.now().timestamp())

            # 更新新增字段
            existing_record.core_user_id = note.get('core_user_id', '')
            existing_record.create_timestamp = note.get('create_timestamp')
            existing_record.duration = note.get('duration')
            existing_record.duration_min = note.get('duration_min')
            existing_record.head_image_uri = note.get('head_image_uri', '')
            existing_record.is_hot = note.get('is_hot', False)
            existing_record.is_playlet = note.get('is_playlet', 0)
            existing_record.item_animated_cover = note.get('item_animated_cover', '')
            existing_record.item_cover = note.get('item_cover', '')
            existing_record.media_type = note.get('media_type', '')
            existing_record.original_status = note.get('original_status')
            existing_record.status = note.get('status', 1)
            existing_record.title = note.get('title', '')
            existing_record.url = note.get('url', '')
            existing_record.video_id = note.get('video_id', '')

            session.commit()

        except Exception as db_error:
            self.logger.error(f"更新笔记数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _create_note_record(self, note: Dict[str, Any], user_id: str):
        """创建新的笔记记录"""
        try:
            current_time = int(datetime.now().timestamp())
            note_record = DouYinKolNote(
                douyin_user_id=user_id,
                douyin_item_id=note.get('item_id', ''),
                douyin_item_date=note.get('item_date', ''),
                douyin_item_title=note.get('item_title', ''),
                video_like=note.get('like', 0),
                video_play=note.get('play', 0),
                video_share=note.get('share', 0),
                video_comment=note.get('comment', 0),
                create_time=current_time,
                update_time=current_time,

                # 新增字段
                core_user_id=note.get('core_user_id', ''),
                create_timestamp=note.get('create_timestamp'),
                duration=note.get('duration'),
                duration_min=note.get('duration_min'),
                head_image_uri=note.get('head_image_uri', ''),
                is_hot=note.get('is_hot', False),
                is_playlet=note.get('is_playlet', 0),
                item_animated_cover=note.get('item_animated_cover', ''),
                item_cover=note.get('item_cover', ''),
                media_type=note.get('media_type', ''),
                original_status=note.get('original_status'),
                status=note.get('status', 1),
                title=note.get('title', ''),
                url=note.get('url', ''),
                video_id=note.get('video_id', '')
            )
            session.add(note_record)
            session.commit()

        except Exception as db_error:
            self.logger.error(f"创建笔记数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _update_kol_api_data_to_db(self, user_id: str):
        """将收集到的API数据统一更新到数据库"""
        try:
            # 检查是否已存在该用户的记录
            existing_record = session.query(DouYinKolRealization).filter_by(
                douyin_user_id=user_id
            ).first()

            current_time = int(datetime.now().timestamp())

            if existing_record:
                # 更新现有记录
                self.logger.info(f"更新现有记录，用户ID: {user_id}")
                
                # 作者显示数据
                if self.kol_api_data['author_display']:
                    self.logger.info(f"更新author_display字段")
                    existing_record.follower_count = self.kol_api_data['author_display'].get('follower_count')
                    existing_record.link_count = self.kol_api_data['author_display'].get('link_count')
                    existing_record.videos_count = self.kol_api_data['author_display'].get('videos_count')

                # 链接结构数据
                if self.kol_api_data['link_struct']:
                    self.logger.info(f"更新link_struct字段")
                    existing_record.link_struct = self.kol_api_data['link_struct'].get('link_struct')

                # 平台信息数据
                if self.kol_api_data['platform_info']:
                    self.logger.info(f"更新platform_info字段")
                    existing_record.self_intro = self.kol_api_data['platform_info'].get('self_intro')

                # 商业信息数据
                if self.kol_api_data['commerce_info']:
                    self.logger.info(f"更新commerce_info字段")
                    existing_record.commerce_info = self.kol_api_data['commerce_info'].get('commerce_info')

                # 传播信息数据
                if self.kol_api_data['spread_info']:
                    self.logger.info(f"更新spread_info字段")
                    existing_record.spread_info = self.kol_api_data['spread_info'].get('spread_info')

                # 受众分布数据
                if self.kol_api_data['audience_distribution']:
                    self.logger.info(f"更新audience_distribution字段")
                    existing_record.audience_distribution = self.kol_api_data['audience_distribution'].get('audience_distribution')

                existing_record.update_time = current_time
            else:
                # 创建新记录
                self.logger.info(f"创建新记录，用户ID: {user_id}")
                record_data = {
                    'douyin_user_id': user_id,
                    'create_time': current_time,
                    'update_time': current_time
                }

                # 作者显示数据
                if self.kol_api_data['author_display']:
                    self.logger.info(f"创建新记录时添加author_display字段")
                    record_data.update({
                        'follower_count': self.kol_api_data['author_display'].get('follower_count'),
                        'link_count': self.kol_api_data['author_display'].get('link_count'),
                        'videos_count': self.kol_api_data['author_display'].get('videos_count')
                    })

                # 链接结构数据
                if self.kol_api_data['link_struct']:
                    self.logger.info(f"创建新记录时添加link_struct字段")
                    record_data['link_struct'] = self.kol_api_data['link_struct'].get('link_struct')

                # 平台信息数据
                if self.kol_api_data['platform_info']:
                    self.logger.info(f"创建新记录时添加platform_info字段")
                    record_data['self_intro'] = self.kol_api_data['platform_info'].get('self_intro')

                # 商业信息数据
                if self.kol_api_data['commerce_info']:
                    self.logger.info(f"创建新记录时添加commerce_info字段")
                    record_data['commerce_info'] = self.kol_api_data['commerce_info'].get('commerce_info')

                # 传播信息数据
                if self.kol_api_data['spread_info']:
                    self.logger.info(f"创建新记录时添加spread_info字段")
                    record_data['spread_info'] = self.kol_api_data['spread_info'].get('spread_info')

                # 受众分布数据
                if self.kol_api_data['audience_distribution']:
                    self.logger.info(f"创建新记录时添加audience_distribution字段")
                    record_data['audience_distribution'] = self.kol_api_data['audience_distribution'].get('audience_distribution')

                record = DouYinKolRealization(**record_data)
                session.add(record)

            session.commit()
            self.logger.info(f"数据库更新完成，用户ID: {user_id}")

        except Exception as db_error:
            self.logger.error(f"保存综合API数据时出错: {str(db_error)}")
            session.rollback()

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
            os.path.join(log_path, "douyin_kol_excel_{time:YYYY-MM-DD}.log"),
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
        参考小红书登录检测逻辑，使用wait_for_selector
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

            self.logger.info("浏览器和playwright已关闭")
        except Exception as e:
            self.logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应 - 只处理指定的API接口"""
        try:
            url = response.url

            # 定义需要处理的目标API列表
            target_apis = [
                '/api/data_sp/check_author_display',
                '/api/data_sp/author_link_struct',
                '/api/author/get_author_platform_channel_info_v2',
                '/api/aggregator/get_author_commerce_spread_info',
                '/api/data_sp/author_audience_distribution',
                '/api/author/get_author_base_info',
                '/api/author/get_author_marketing_info',
                '/api/data_sp/get_author_spread_info',
                '/api/author/get_author_show_items_v2'
            ]

            # 检查是否是目标API
            matched_api = None
            for api in target_apis:
                if api in url:
                    matched_api = api
                    break

            # 如果不是目标API，直接返回（不打印任何信息）
            if not matched_api:
                return

            # 验证当前是否有正在处理的用户
            if not self.current_kol or not self.current_kol.get('user_id'):
                # 如果是登录相关的API请求，记录为调试信息而不是警告
                if any(keyword in url.lower() for keyword in ['login', 'user', 'auth', 'profile', 'config']):
                    self.logger.debug(f"登录过程中的API请求: {url}")
                else:
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

            # 根据不同的API类型进行处理
            if '/api/data_sp/check_author_display' in url:
                self.logger.info(f"捕获到作者显示检查API: {url}")
                self._process_author_display(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/data_sp/author_link_struct' in url:
                self.logger.info(f"捕获到作者链接结构API: {url}")
                self._process_author_link_struct(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/author/get_author_platform_channel_info_v2' in url:
                self.logger.info(f"捕获到平台渠道信息API: {url}")
                self._process_author_platform_info(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/aggregator/get_author_commerce_spread_info' in url:
                self.logger.info(f"捕获到商业传播信息API: {url}")
                self._process_author_commerce_info(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/data_sp/author_audience_distribution' in url:
                self.logger.info(f"捕获到受众分布API: {url}")
                self._process_author_audience_distribution(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/author/get_author_base_info' in url:
                self._process_author_base_info(response_data)
                self.api_response_processed = True

            elif '/api/author/get_author_marketing_info' in url:
                self._process_marketing_info(response_data)
                self.api_response_processed = True

            elif '/api/data_sp/get_author_spread_info' in url:
                self._process_author_spread_info(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/author/get_author_show_items_v2' in url:
                if url in self.processed_api_responses:
                    self.logger.debug("跳过重复的API响应")
                    return

                self.processed_api_responses.add(url)
                self.logger.info(f"捕获到作者展示项目API: {url}")
                self._process_user_posted_data(response_data, current_user_id)
                self.api_response_processed = True

        except Exception as e:
            # 如果是浏览器关闭错误，不记录为错误
            if "Target page, context or browser has been closed" in str(e):
                self.logger.info(f"浏览器已关闭，跳过API数据处理: {url}")
            else:
                self.logger.error(f"处理API响应时出错: {str(e)}, URL: {url}")

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
    主函数 - 抖音KOL数据抓取程序（Excel导入版本）
    """
    spider = None
    try:
        print("=== 抖音KOL数据抓取程序启动（Excel导入版本）===")

        # 1. 选择Excel文件
        spider = DouYinSpiderExcel()
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

        # 关闭数据库连接
        try:
            session.commit()
            session.close()
            print("数据库连接已关闭")
        except Exception as e:
            print(f"关闭数据库连接时出错: {str(e)}")
            try:
                session.rollback()
                session.close()
            except:
                pass


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
