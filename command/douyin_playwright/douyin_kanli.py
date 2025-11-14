import configparser
import json
import os
import time
import random
from datetime import datetime
import sys
from typing import Optional, Dict, Any, List
import traceback

import playwright
import requests

# 不再使用数据库，改为API接口
# from models.models_tibao import DouYinKolRealization
# from core.localhost_fp_project import session
import pandas as pd
import schedule
from loguru import logger
from playwright.sync_api import sync_playwright
from unitl.common import Common

"""
    获取抖音博主的月总营收
"""


# 配置常量
def get_base_path():
    """获取基础路径，支持exe打包"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(
            os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../..")

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
        get_resource_path('config.ini'),
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

    # 解析配置
    return {
        'SCHEDULER_CONFIG': {
            'enable_scheduler': config.getboolean('SCHEDULER', 'enable_scheduler'),
            'daily_time': config.get('SCHEDULER', 'daily_time'),
            'run_once': config.getboolean('SCHEDULER', 'run_once'),
            'check_interval': config.getint('SCHEDULER', 'check_interval')
        }
    }

class DouYinSpider:
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
            'audience_distribution': {},
            'avg_a3_incr_cnt': {},
            'marketing_info': {},
            'author_base_info': {}
        }

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def scrape_user_notes(self, kol_name: str, kol_url: str, star_id: str) -> int:
        """抓取指定KOL的笔记信息并匹配更新数据库
        返回值：
        - 1: 处理成功（无论是否有连接用户按钮，都会尝试获取所有可用的数据）
        - 0: 处理失败
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            self.current_kol = {'name': kol_name, 'url': kol_url, 'user_id':star_id}
            self.processed_api_responses.clear()
            # 完全重置营销信息，确保数据隔离
            self.marketing_info = {'user_id': star_id}
            # 重置API数据缓存
            self.api_data = {}
            # 添加API响应处理标志
            self.api_response_processed = False

            # 不再在开始时创建记录，统一在最后保存所有数据
            self.page.goto(kol_url, timeout=30000)
            self.logger.info(f"成功访问页面: {kol_url}")

            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

            self.common.random_sleep(3, 4)
            # 点击连接用户标签
            creative_tab = self.page.locator("div.el-tabs__nav >> div:has-text('连接用户')")
            if creative_tab and creative_tab.is_visible():
                # 点击前等待一下确保元素稳定
                time.sleep(0.5)
                creative_tab.click()
                self.logger.info("成功点击连接用户标签")

                # 等待点击生效
                try:
                    # 等待页面有变化（比如URL变化或者元素状态变化）
                    self.page.wait_for_timeout(1000)  # 等待1秒

                    # 检查是否点击成功（可以检查URL变化或者特定元素出现）
                    current_url = self.page.url
                    if 'creative' in current_url.lower() or '连接' in current_url.lower():
                        self.logger.info("检测到页面已切换到连接用户页面")
                    else:
                        self.logger.info("页面切换状态未知，继续执行")
                    
                    # 等待页面加载完成
                    time.sleep(2)

                except Exception as e:
                    self.logger.warning(f"检查点击效果时出错: {str(e)}")
                    # 即使检查失败也继续执行
            else:
                self.logger.warning(f"未找到连接用户标签，KOL {kol_name} 可能没有连接用户数据，但继续尝试获取其他数据")
                # 不直接返回，继续尝试获取其他可用的API数据

            # 等待页面有变化（比如URL变化或者元素状态变化）
            self.page.wait_for_timeout(1000)  # 等待1秒
            
            # 鼠标滚轮向下滚动几下，确保页面完全加载
            self.logger.info("向下滚动页面确保内容完全加载...")
            try:
                # 向下滚动3次，每次滚动500像素
                for i in range(3):
                    self.page.mouse.wheel(0, 500)
                    time.sleep(0.5)  # 每次滚动后等待0.5秒
                self.logger.info("页面滚动完成")
            except Exception as e:
                self.logger.warning(f"页面滚动时出错: {str(e)}")
            
            # 尝试点击粉丝画像按钮
            self.logger.info("开始点击粉丝画像按钮...")
            
            # 等待页面完全加载，确保所有元素都已渲染
            self.logger.info("等待页面元素完全加载...")
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                self.logger.warning(f"等待页面网络空闲时出错: {str(e)}")
            
            # 额外等待一段时间确保动态内容加载完成
            time.sleep(2)
            
            # 尝试多种选择器来查找粉丝画像按钮
            fan_portrait_selectors = [
                "text=粉丝画像",
                "span:has-text('粉丝画像')",
                "label:has-text('粉丝画像')",
                "div:has-text('粉丝画像')",
                "button:has-text('粉丝画像')",
                ".el-checkbox-button__inner:has-text('粉丝画像')",
                "[class*='checkbox']:has-text('粉丝画像')",
                "[class*='button']:has-text('粉丝画像')"
            ]
            
            fan_portrait_button = None
            for i, selector in enumerate(fan_portrait_selectors):
                try:
                    self.logger.info(f"尝试选择器 {i+1}/{len(fan_portrait_selectors)}: {selector}")
                    
                    # 检查元素是否存在
                    element = self.page.locator(selector).first
                    count = element.count()
                    self.logger.info(f"选择器 '{selector}' 找到 {count} 个元素")
                    
                    if count > 0:
                        # 检查元素是否可见
                        if element.is_visible(timeout=3000):
                            fan_portrait_button = element
                            self.logger.info(f"✅ 找到粉丝画像按钮，使用选择器: {selector}")
                            break
                        else:
                            self.logger.info(f"元素存在但不可见，选择器: {selector}")
                    else:
                        self.logger.info(f"未找到元素，选择器: {selector}")
                        
                except Exception as e:
                    self.logger.info(f"选择器 {selector} 检查出错: {str(e)}")
                    continue
            
            # 如果还是没找到，尝试更通用的方法
            if not fan_portrait_button:
                self.logger.info("尝试更通用的查找方法...")
                try:
                    # 查找所有包含"粉丝画像"文本的元素
                    all_elements = self.page.locator("text=粉丝画像").all()
                    self.logger.info(f"找到 {len(all_elements)} 个包含'粉丝画像'文本的元素")
                    
                    for i, element in enumerate(all_elements):
                        try:
                            if element.is_visible(timeout=1000):
                                fan_portrait_button = element
                                self.logger.info(f"✅ 通过通用方法找到粉丝画像按钮，元素 {i+1}")
                                break
                        except Exception as e:
                            self.logger.info(f"元素 {i+1} 不可见: {str(e)}")
                            continue
                            
                except Exception as e:
                    self.logger.warning(f"通用查找方法出错: {str(e)}")
            
            if fan_portrait_button:
                # 点击前等待一下确保元素稳定
                time.sleep(0.5)
                fan_portrait_button.click()
                self.logger.info("成功点击粉丝画像按钮")

                # 等待点击生效和API请求完成
                try:
                    # 等待页面有变化
                    self.page.wait_for_timeout(1000)  # 等待1秒
                    
                    # 等待API请求完成 - 增加等待时间
                    self.logger.info("等待粉丝画像API请求完成...")

                except Exception as e:
                    self.logger.warning(f"检查粉丝画像点击效果时出错: {str(e)}")
                    # 即使检查失败也继续执行
            else:
                self.logger.warning(f"未找到粉丝画像按钮，KOL {kol_name} 可能没有粉丝画像数据，但继续尝试获取其他数据")
                
                # 保存页面截图用于调试
                try:
                    screenshot_path = os.path.join(self.data_dir, f"fan_portrait_not_found_{kol_name}_{int(time.time())}.png")
                    self.page.screenshot(path=screenshot_path)
                    self.logger.info(f"已保存页面截图用于调试: {screenshot_path}")
                except Exception as e:
                    self.logger.warning(f"保存页面截图失败: {str(e)}")
                
                # 即使没有粉丝画像按钮，也继续处理其他API数据

            # 等待API数据 - 简化检测方式
            try:
                # 等待更长时间让API响应处理完成，即使没有点击按钮也可能有基础数据
                wait_time = random.randint(5, 8)
                self.logger.info(f"等待 {wait_time} 秒让API响应处理完成...")
                time.sleep(wait_time)

                # 检查是否已经获取到API响应数据
                if self.api_response_processed:
                    self.logger.info("✅ 成功获取到API响应数据")
                else:
                    # 即使没有API响应，也尝试保存基础信息
                    self.logger.info("ℹ️ 未检测到API响应，但仍会保存基础KOL信息")

                # 统一保存所有收集到的API数据到远程接口
                if self.current_kol and self.current_kol.get('user_id'):
                    self.logger.info("开始统一保存所有API数据到远程接口")
                    self._save_all_kol_data_to_api(self.current_kol.get('user_id'))
                    self.logger.info("✅ 所有API数据已统一保存到远程接口")

                return 1  # 返回1表示处理成功

            except Exception as e:
                self.logger.warning(f"等待API数据时出错: {str(e)}")
                return 1  # 即使出错也继续执行

        except Exception as e:
            self.logger.error(f"抓取KOL {kol_name} 笔记时出错: {str(e)}")
            raise



    def _save_all_kol_data_to_api(self, user_id: str):
        """统一保存所有收集到的API数据到远程接口"""
        try:
            self.logger.info(f"开始统一保存所有API数据到远程接口，用户ID: {user_id}")
            self.logger.info(f"当前kol_api_data内容: {self.kol_api_data}")
            
            # 构建请求数据
            current_timestamp = int(time.time())
            douyin_data = {
                "douyin_user_id": user_id,
                "douyin_nickname": self.current_kol.get('name', '') if self.current_kol else '',
                "douyin_link": f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{user_id}",
                "create_time": current_timestamp,
                "update_time": current_timestamp
            }

            # 作者显示数据
            if self.kol_api_data.get('author_display'):
                douyin_data.update({
                    "follower_count": self.kol_api_data['author_display'].get('follower_count'),
                    "link_count": self.kol_api_data['author_display'].get('link_count'),
                    "videos_count": self.kol_api_data['author_display'].get('videos_count')
                })

            # 链接结构数据
            if self.kol_api_data.get('link_struct'):
                try:
                    link_struct_data = json.loads(self.kol_api_data['link_struct'].get('link_struct', '{}'))
                    douyin_data['link_struct'] = link_struct_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['link_struct'] = {}

            # 平台信息数据
            if self.kol_api_data.get('platform_info'):
                douyin_data['self_intro'] = self.kol_api_data['platform_info'].get('self_intro', '')

            # 商业信息数据
            if self.kol_api_data.get('commerce_info'):
                try:
                    commerce_info_data = json.loads(self.kol_api_data['commerce_info'].get('commerce_info', '{}'))
                    douyin_data['commerce_info'] = commerce_info_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['commerce_info'] = {}

            # 传播信息数据
            if self.kol_api_data.get('spread_info'):
                self.logger.info(f"添加spread_info字段")
                try:
                    spread_info_data = json.loads(self.kol_api_data['spread_info'].get('spread_info', '{}'))
                    douyin_data['spread_info'] = spread_info_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['spread_info'] = {}

            # 受众分布数据
            if self.kol_api_data.get('audience_distribution'):
                self.logger.info(f"添加audience_distribution字段")
                try:
                    audience_distribution_data = json.loads(self.kol_api_data['audience_distribution'].get('audience_distribution', '[]'))
                    douyin_data['audience_distribution'] = audience_distribution_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['audience_distribution'] = []

            # 商业种子基础信息数据
            if self.kol_api_data.get('avg_a3_incr_cnt'):
                self.logger.info(f"添加avg_a3_incr_cnt字段")
                avg_a3_incr_cnt_value = self.kol_api_data.get('avg_a3_incr_cnt', '')
                douyin_data['avg_a3_incr_cnt'] = str(avg_a3_incr_cnt_value)
                self.logger.info(f"avg_a3_incr_cnt值: {avg_a3_incr_cnt_value}")
            else:
                self.logger.warning(f"未找到avg_a3_incr_cnt数据，使用默认值")
                self.logger.info(f"当前kol_api_data中avg_a3_incr_cnt: {self.kol_api_data.get('avg_a3_incr_cnt')}")
                # 即使没有数据也发送默认值，避免服务器报错
                douyin_data['avg_a3_incr_cnt'] = ''

            # 营销信息数据
            if self.kol_api_data.get('marketing_info'):
                self.logger.info(f"添加营销信息字段")
                try:
                    industry_tags_data = json.loads(self.kol_api_data['marketing_info'].get('industry_tags', '[]'))
                    douyin_data['industry_tags'] = industry_tags_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['industry_tags'] = []

                try:
                    price_info_data = json.loads(self.kol_api_data['marketing_info'].get('price_info', '{}'))
                    douyin_data['price_info'] = price_info_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['price_info'] = {}

            # 作者基本信息数据
            if self.kol_api_data.get('author_base_info'):
                self.logger.info(f"添加作者基本信息字段")
                try:
                    author_base_info_data = json.loads(self.kol_api_data['author_base_info'].get('author_base_info', '{}'))
                    douyin_data['author_base_info'] = author_base_info_data
                except (json.JSONDecodeError, TypeError):
                    douyin_data['author_base_info'] = {}

                # 更新douyin_link如果从author_base_info中有更准确的信息
                if self.kol_api_data['author_base_info'].get('douyin_link'):
                    douyin_data['douyin_link'] = self.kol_api_data['author_base_info'].get('douyin_link')

            # 构建最终的请求体
            request_data = {
                "douyin_data": douyin_data
            }

            self.logger.info(f"准备发送数据到API接口: {request_data}")

            # 发送POST请求到API接口
            # 正确的URL应该是
            api_url = "https://tianji.fangpian999.com/api/admin/creatorSign/recordDouyinKolData"
            headers = {
                "Content-Type": "application/json"
            }

            response = requests.post(api_url, json=request_data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                response_data = response.json()
                self.logger.info(f"✅ 数据成功发送到API接口，响应: {response_data}")
            else:
                self.logger.error(f"❌ API接口请求失败，状态码: {response.status_code}")
                self.logger.error(f"响应内容: {response.text}")
                raise Exception(f"API接口请求失败，状态码: {response.status_code}")

        except Exception as api_error:
            self.logger.error(f"统一保存API数据到远程接口时出错: {str(api_error)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            raise

    def _process_marketing_info(self, response_data: Dict[str, Any]):
        """处理营销信息数据"""
        try:
            if not response_data:
                self.logger.error("营销信息API响应数据为空")
                return

            # 获取当前正在处理的KOL名称
            current_user_id = self.current_kol.get('user_id') if self.current_kol else None
            if not current_user_id:
                self.logger.error("无法获取当前KOL名称")
                return

            # 验证数据是否属于当前用户
            if self.marketing_info.get('user_id') != current_user_id:
                self.logger.warning(f"数据不匹配：期望 {current_user_id}，实际 {self.marketing_info.get('user_id')}")
                return
            
            # 提取价格信息
            price_info = response_data.get('price_info', [])
            
            # 将JSON对象转换为字符串
            try:
                industry_tags_json = json.dumps(response_data.get('industry_tags', []), ensure_ascii=False)
                price_info_json = json.dumps(price_info, ensure_ascii=False)
            except Exception as json_error:
                self.logger.error(f"将营销信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")
                return

            # 存储到kol_api_data中，等待统一保存
            self.kol_api_data['marketing_info'] = {
                'industry_tags': industry_tags_json,
                'price_info': price_info_json
            }

        except Exception as e:
            self.logger.error(f"处理营销信息时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_base_info(self, response_data: Dict[str, Any]):
        """处理作者基本信息数据"""
        try:
            if not response_data:
                self.logger.error("作者基本信息API响应数据为空")
                return

            # 获取当前正在处理的KOL名称
            current_user_id = self.current_kol.get('user_id') if self.current_kol else None
            if not current_user_id:
                self.logger.error("无法获取当前KOL名称")
                return

            # 验证数据是否属于当前用户
            if self.marketing_info.get('user_id') != current_user_id:
                self.logger.warning(f"数据不匹配：期望 {current_user_id}，实际 {self.marketing_info.get('user_id')}")
                return
            
            # 提取链接信息
            douyin_link = f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{current_user_id}"
            
            # 将整个响应数据转换为JSON字符串
            try:
                author_base_info_json = json.dumps(response_data, ensure_ascii=False)
            except Exception as json_error:
                self.logger.error(f"将作者基本信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")
                return

            # 存储到kol_api_data中，等待统一保存
            self.kol_api_data['author_base_info'] = {
                'author_base_info': author_base_info_json,
                'douyin_link': douyin_link
            }

        except Exception as e:
            self.logger.error(f"处理作者基本信息时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_display(self, response_data: Dict[str, Any], user_id: str):
        """处理作者显示检查API数据，只保存follower字段、link_cnt字段和release_videos_cnt字段"""
        try:
            if not response_data:
                self.logger.error("作者显示检查API响应数据为空")
                return

            # 提取需要的字段
            follower = response_data.get('follower', 0)
            link_cnt = response_data.get('link_cnt', 0)
            release_videos_cnt = response_data.get('release_videos_cnt', 0)

            # 存储到kol_api_data中，等待统一保存
            self.kol_api_data['author_display'] = {
                'follower_count': follower,
                'link_count': link_cnt,
                'videos_count': release_videos_cnt
            }

        except Exception as e:
            self.logger.error(f"处理作者显示数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

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

                # 存储到kol_api_data中，等待统一保存
                self.kol_api_data['link_struct'] = {
                    'link_struct': link_struct_json
                }

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

            # 存储到kol_api_data中，等待统一保存
            self.kol_api_data['platform_info'] = {
                'self_intro': self_intro
            }

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
                try:
                    commerce_info_json = json.dumps(response_data, ensure_ascii=False)
                except Exception as json_error:
                    self.logger.error(f"将作者基本信息转换为JSON时出错: {str(json_error)}")
                    self.logger.error(f"错误详情: {traceback.format_exc()}")
                    return

                # 存储到kol_api_data中，等待统一保存
                self.kol_api_data['commerce_info'] = {
                    'commerce_info': commerce_info_json
                }

            except Exception as json_error:
                self.logger.error(f"将商业传播信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理商业传播信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_get_author_spread_info(self, response_data: Dict[str, Any], user_id: str):
        """处理作者商业传播信息API数据，保存整个响应对象为JSON格式"""
        try:
            if not response_data:
                self.logger.error("作者商业传播信息API响应数据为空")
                return

            # 将整个响应数据转换为JSON字符串
            try:
                try:
                    commerce_info_json = json.dumps(response_data, ensure_ascii=False)
                except Exception as json_error:
                    self.logger.error(f"将作者基本信息转换为JSON时出错: {str(json_error)}")
                    self.logger.error(f"错误详情: {traceback.format_exc()}")
                    return

                # 存储到kol_api_data中
                self.kol_api_data['commerce_info'] = {
                    'commerce_info': commerce_info_json
                }

                # 不再立即更新数据库，等待统一保存

            except Exception as json_error:
                self.logger.error(f"将商业传播信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理商业传播信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")


    def _process_author_spread_info(self, response_data: Dict[str, Any], user_id: str):
        """处理作者传播信息API数据，保存整个响应对象为JSON格式"""
        try:
            self.logger.info(f"开始处理传播信息API数据，用户ID: {user_id}")
            
            if not response_data:
                self.logger.error("作者传播信息API响应数据为空")
                return

            self.logger.info(f"传播信息API响应数据: {response_data}")

            # 将整个响应数据转换为JSON字符串
            try:
                spread_info_json = json.dumps(response_data, ensure_ascii=False)
                
                # 存储到kol_api_data中，等待统一保存
                self.kol_api_data['spread_info'] = {
                    'spread_info': spread_info_json
                }

                self.logger.info(f"传播信息已存储到kol_api_data，等待统一保存")

            except Exception as json_error:
                self.logger.error(f"将传播信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理传播信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_audience_distribution(self, response_data: Dict[str, Any], user_id: str):
        """处理作者受众分布API数据，保存distributions字段为JSON格式"""
        try:
            self.logger.info(f"开始处理受众分布API数据，用户ID: {user_id}")
            
            if not response_data:
                self.logger.error("作者受众分布API响应数据为空")
                return

            self.logger.info(f"受众分布API响应数据: {response_data}")

            # 提取distributions字段
            distributions = response_data.get('distributions', [])

            self.logger.info(f"提取到的distributions数据: {distributions}")

            # 即使distributions为空也要保存，因为空数据也是有效的数据状态
            # 将distributions转换为JSON字符串
            try:
                distributions_json = json.dumps(distributions, ensure_ascii=False)
                
                # 存储到kol_api_data中，等待统一保存
                self.kol_api_data['audience_distribution'] = {
                    'audience_distribution': distributions_json
                }

                self.logger.info(f"受众分布已存储到kol_api_data，等待统一保存")

            except Exception as json_error:
                self.logger.error(f"将受众分布转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理受众分布数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_commerce_seed_base_info(self, response_data: Dict[str, Any], user_id: str):
        """处理作者商业种子基础信息API数据，保存avg_a3_incr_cnt字段"""
        try:
            self.logger.info(f"开始处理商业种子基础信息API数据，用户ID: {user_id}")
            
            if not response_data:
                self.logger.error("作者商业种子基础信息API响应数据为空")
                return

            self.logger.info(f"商业种子基础信息API响应数据: {response_data}")

            # 提取avg_a3_incr_cnt字段
            avg_a3_incr_cnt = response_data.get('avg_a3_incr_cnt', 0)

            self.logger.info(f"提取到的avg_a3_incr_cnt数据: {avg_a3_incr_cnt}")

            # 存储到kol_api_data中，等待统一保存
            self.kol_api_data['avg_a3_incr_cnt'] = avg_a3_incr_cnt

            self.logger.info(f"商业种子基础信息已存储到kol_api_data，等待统一保存")
            self.logger.info(f"存储后的kol_api_data['avg_a3_incr_cnt']: {self.kol_api_data['avg_a3_incr_cnt']}")

        except Exception as e:
            self.logger.error(f"处理商业种子基础信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")



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
                        # 检查所有元素，只要有一个可见就认为登录成功
                        self.logger.info(f"开始检查 {count} 个 .user-avatar 元素的可见性...")
                        all_elements = element.all()
                        for i, elem in enumerate(all_elements):
                            try:
                                if elem.is_visible(timeout=1000):
                                    self.logger.info(f"第 {i+1} 个 .user-avatar 元素可见，Cookie有效")
                                    login_detected = True
                                    break
                                else:
                                    self.logger.debug(f"第 {i+1} 个 .user-avatar 元素不可见")
                            except Exception as elem_error:
                                self.logger.debug(f"第 {i+1} 个 .user-avatar 元素检查出错: {str(elem_error)}")
                                continue

                        if not login_detected:
                            self.logger.warning(f"找到 {count} 个 .user-avatar 元素，但都不可见")
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

            if self.page is None:
                self.logger.info("浏览器未初始化，开始初始化...")
                self.setup_browser()

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

    def update_monitor_status(self, **kwargs):
        """更新监控状态"""
        self.logger.debug(f"更新监控状态: {kwargs}")
        if kwargs.get('completed_count'):
            self.monitor_data['completed_count'] = kwargs.get('completed_count')
        if kwargs.get('fail_count'):
            self.monitor_data['fail_count'] = kwargs.get('fail_count')

    def save_data(self, user_id: str, data: List[Dict[str, Any]]):
        """
        保存抓取的数据到CSV文件
        """
        try:
            filename = os.path.join(self.data_dir, f'user_{user_id}_{datetime.now().strftime("%Y%m%d")}.csv')
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
            self.logger.info(f"数据已保存到 {filename}")
        except Exception as e:
            self.logger.error(f"保存数据时出错: {str(e)}")

    def _check_api_response_status(self, response_data: Dict[str, Any], url: str) -> bool:
        """检查API响应状态，返回True表示状态异常需要跳过处理"""
        try:
            if response_data and 'base_resp' in response_data:
                base_resp = response_data.get('base_resp', {})
                status_code = base_resp.get('status_code')
                status_message = base_resp.get('status_message', '')
                
                if status_code == 10005:
                    self.logger.warning(f"API返回登录失效: {status_message}, URL: {url}")
                    return True
                elif status_code != 0 and status_code is not None:
                    self.logger.warning(f"API返回错误状态: {status_code} - {status_message}, URL: {url}")
                    return True
            
            return False  # 状态正常，可以继续处理
        except Exception as e:
            self.logger.error(f"检查API响应状态时出错: {str(e)}")
            return False

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
                '/api/aggregator/get_author_commerce_seed_base_info'
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

            # 通用检查API响应状态
            if self._check_api_response_status(response_data, url):
                return  # 如果状态异常，直接返回

            # 根据不同的API类型进行处理
            if '/api/data_sp/check_author_display' in url:
                self._process_author_display(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/data_sp/author_link_struct' in url:
                self._process_author_link_struct(response_data, current_user_id)
                self.api_response_processed = True

            # 1
            elif '/api/author/get_author_platform_channel_info_v2' in url:
                self._process_author_platform_info(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/aggregator/get_author_commerce_spread_info' in url:
                self._process_author_commerce_info(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/data_sp/author_audience_distribution' in url:
                if url in self.processed_api_responses:
                    self.logger.debug("跳过重复的API响应")
                    return

                self.processed_api_responses.add(url)
                self._process_author_audience_distribution(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/author/get_author_base_info' in url:
                self._process_author_base_info(response_data)
                self.api_response_processed = True

            elif '/api/author/get_author_marketing_info' in url:
                self._process_marketing_info(response_data)
                self.api_response_processed = True

            elif '/api/data_sp/get_author_spread_info' in url:
                self.logger.info(f"捕获到传播信息API: {url}")
                self.logger.info(f"传播信息API响应数据: {response_data}")
                self._process_author_spread_info(response_data, current_user_id)
                self.api_response_processed = True

            elif '/api/aggregator/get_author_commerce_seed_base_info' in url:
                self.logger.info(f"✅ 捕获到商业种子基础信息API: {url}")
                self.logger.info(f"商业种子基础信息API响应数据: {response_data}")
                self._process_author_commerce_seed_base_info(response_data, current_user_id)
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


def get_pending_kols() -> List[Dict[str, Any]]:
    """获取需要处理的KOL列表"""
    try:
        api_url = f"https://tianji.fangpian999.com/api/admin/creatorBusiness/getNewerCreator?type=1&platform_id=2"
        headers = {"Content-Type": "application/json"}

        response = requests.post(api_url, headers=headers, timeout=30, verify=False)
        creator_data = response.json()['data']
        print(f"从数据库获取到 {len(creator_data)} 个待处理的KOL")
        return creator_data
    except Exception as e:
        print(f"获取KOL列表时出错: {str(e)}")
        raise


def process_kol(spider: DouYinSpider, kol: Dict[str, Any]):
    """处理单个KOL"""
    # 从attribute_datas中提取KOL名称和构建链接
    try:
        kol_name = kol['creator_nickname']
        star_id = kol['platform_user_id']
        douyin_link = f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{star_id}"

        if not kol_name:
            spider.logger.warning(f"无法从attribute_datas中获取KOL名称，使用star_id: {star_id}")
            kol_name = f"KOL_{star_id}"
    except (json.JSONDecodeError, AttributeError) as e:
        spider.logger.error(f"解析attribute_datas失败: {str(e)}")
        return False

    try:
        spider.logger.info(f"开始处理KOL: {kol_name}")

        # 验证必要的字段
        if not star_id:
            spider.logger.warning(f"KOL {kol_name} 缺少star_id，跳过处理")
            return False

        # 执行抓取
        result = spider.scrape_user_notes(kol_name, douyin_link, star_id)

        if result == 1:
            spider.logger.info(f"✅ KOL {kol_name} 处理成功")
            return True
        else:
            # 处理失败
            spider.logger.warning(f"⚠️ KOL {kol_name} 处理失败")
            return False

    except Exception as e:
        spider.logger.error(f"❌ 处理KOL {kol_name} 时出错: {str(e)}")
        spider.logger.error(f"错误详情: {traceback.format_exc()}")
        return False


def run_spider_task():
    """
    执行爬虫任务 - 单次执行版本
    """
    spider = None
    try:
        print("=== 抖音KOL数据抓取程序启动 ===")

        # 1. 获取待处理的KOL列表
        kols = get_pending_kols()
        if not kols:
            print("没有找到需要处理的KOL数据，等待下次查询...")
            return True

        # 2. 初始化爬虫
        spider = DouYinSpider()
        spider.setup_browser()

        # 3. 登录
        login_success = spider.login()
        if not login_success:
            print("登录失败，等待下次重试...")
            return False

        # 4. 批处理KOL
        processed_count = 0
        failed_count = 0

        for i, kol in enumerate(kols, 1):
            # 不再检查数据库中的记录，直接处理所有KOL
            print(f"进度: {i}/{len(kols)} ({(i / len(kols)) * 100:.1f}%)")
            if i <= 55:
                continue

            try:
                result = process_kol(spider, kol)
                if result:
                    processed_count += 1
                    # 注意：这里不再检查status字段，因为kol现在是字典类型
                else:
                    failed_count += 1

                # 每个KOL之间等待一段时间，避免请求过于频繁
                if i < len(kols):  # 最后一个KOL不需要等待
                    wait_time = random.randint(15, 20)
                    print(f"等待 {wait_time} 秒后处理下一个KOL...")
                    time.sleep(wait_time)

            except KeyboardInterrupt:
                print("用户中断程序")
                break
            except Exception as e:
                print(f"批处理过程中出现未预期的错误: {str(e)}")
                failed_count += 1
                continue

        # 5. 输出处理结果统计
        print("=" * 60)
        print("📊 处理结果统计:")
        print(f"总数量: {len(kols)}")
        print(f"成功处理: {processed_count}")
        print(f"处理失败: {failed_count}")
        print(f"成功率: {(processed_count / len(kols) * 100):.1f}%")
        print("=" * 60)

        return failed_count == 0  # 如果没有失败的则返回True

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

        # 不再使用数据库连接
        # try:
        #     session.commit()
        #     session.close()
        #     print("数据库连接已关闭")
        # except Exception as e:
        #     print(f"关闭数据库连接时出错: {str(e)}")
        #     try:
        #         session.rollback()
        #         session.close()
        #     except:
        #         pass


def main():
    """
    主函数 - 只在异常时重启版本
    """
    try:
        # 加载配置
        config = load_config()
        scheduler_config = config['SCHEDULER_CONFIG']

        logger.info("=== 蒲公英数据抓取程序启动 ===")
        logger.info(f"执行时间: 每天 {scheduler_config['daily_time']}")

        if scheduler_config['run_once']:
            success = run_spider_task()
            if not success:
                logger.info("程序异常停止，将在1小时后重启...")
                time.sleep(3600)
                return main()  # 递归重启
            return success

        elif scheduler_config['enable_scheduler']:
            # 注册定时任务
            schedule.every().day.at(scheduler_config['daily_time']).do(run_spider_task)

            # 运行调度器
            logger.info("调度器开始运行...")
            while True:
                try:
                    schedule.run_pending()
                    time.sleep(scheduler_config['check_interval'])
                except Exception as e:
                    logger.error(f"调度器运行出错: {str(e)}")
                    logger.info("调度器异常停止，将在1小时后重启...")
                    time.sleep(3600)
                    return main()  # 重启整个程序

        else:
            # 调度器未启用，直接执行一次
            success = run_spider_task()
            if not success:
                logger.info("程序异常停止，将在1小时后重启...")
                time.sleep(3600)
                return main()  # 递归重启
            return success

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        return True
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        logger.info("程序启动异常，将在1小时后重启...")
        time.sleep(3600)
        return main()  # 递归重启


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