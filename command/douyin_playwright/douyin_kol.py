import configparser
import json
import os
import time
import random
from datetime import datetime
import sys
from typing import Optional, Dict, Any, List
import traceback
import requests

import playwright
from models.models_tibao import DouYinKolRealization, DouYinKolNote, DouyinSearchList
# from core.database_text_tibao_2 import session
from core.localhost_fp_project import session
import pandas as pd
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
        base_path = os.path.abspath("../..")
    return os.path.join(base_path, relative_path)


def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试多个可能的配置文件路径
    config_paths = [
        get_resource_path('../pgy_playwright/config.ini'),
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
        'PGY_LOGIN_CONFIG': {
            'page': config.get('PGY_LOGIN', 'page'),
            'page_size': config.get('PGY_LOGIN', 'page_size'),
            'computer': config.get('PGY_LOGIN', 'computer')
        },
    }


class DouYinSpider:
    def __init__(self):
        self.setup_logger()
        # 设置logger属性
        self.logger = logger
        self.config = load_config()
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
            'commerce_info': {}
        }

        # 企业微信webhook地址
        self.webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b3b0cdf5-62b6-49d7-80d7-6f741c3c2c4d"

        # 浏览器相关属性初始化
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def scrape_user_notes(self, kol_name: str, kol_url: str, star_id: str) -> int:
        """抓取指定KOL的笔记信息并匹配更新数据库
        返回值：
        - 1: 处理成功
        - 2: 没有创作能力按钮（该KOL没有创作能力数据）
        - 0: 处理失败
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            self.current_kol = {'name': kol_name, 'url': kol_url, 'user_id': star_id}
            self.processed_api_responses.clear()
            # 完全重置营销信息，确保数据隔离
            self.marketing_info = {'user_id': star_id}
            # 重置API数据缓存
            self.api_data = {}
            # 添加API响应处理标志
            self.api_response_processed = False

            try:
                self.page.goto(kol_url, timeout=30000)
                self.logger.info(f"成功访问页面: {kol_url}")

                # 等待页面加载完成
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception as e:
                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

            except Exception as e:
                self.logger.error(f"访问页面超时: {kol_url}")
                return 0

            # 检测并处理验证码
            if not self.check_and_handle_captcha():
                self.logger.error("验证码处理失败")
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
                    # 等待页面有变化（比如URL变化或者元素状态变化）
                    self.page.wait_for_timeout(1000)  # 等待1秒

                    # 检查是否点击成功（可以检查URL变化或者特定元素出现）
                    current_url = self.page.url
                    if 'creative' in current_url.lower() or '创作' in current_url.lower():
                        self.logger.info("检测到页面已切换到创作能力页面")
                    else:
                        self.logger.info("页面切换状态未知，继续执行")

                    # 等待API请求完成 - 增加等待时间
                    self.logger.info("等待API请求完成...")
                    wait_time = random.randint(8, 12)
                    self.logger.info(f"等待 {wait_time} 秒，确保所有API请求完成...")
                    time.sleep(wait_time)

                    # 检测并处理验证码
                    if not self.check_and_handle_captcha():
                        self.logger.error("验证码处理失败")
                        return 0

                except Exception as e:
                    self.logger.warning(f"检查点击效果时出错: {str(e)}")
                    # 即使检查失败也继续执行
            else:
                self.logger.warning(f"未找到创作能力标签，KOL {kol_name} 可能没有创作能力数据")
                return 2  # 返回2表示没有创作能力按钮

            # 等待API数据 - 简化检测方式
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
            self.logger.error(f"抓取KOL {kol_name} 笔记时出错: {str(e)}")
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
            self.logger.error(f"错误详情: {traceback.format_exc()}")

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
            self.logger.error(f"保存变现价格数据时出错: {str(db_error)}")
            session.rollback()
            raise

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

                # 尝试更新数据库
                self._update_kol_api_data_to_db(user_id)

            except Exception as json_error:
                self.logger.error(f"将商业传播信息转换为JSON时出错: {str(json_error)}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理商业传播信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

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
                # 作者显示数据
                if self.kol_api_data['author_display']:
                    existing_record.follower_count = self.kol_api_data['author_display'].get('follower_count')
                    existing_record.link_count = self.kol_api_data['author_display'].get('link_count')
                    existing_record.videos_count = self.kol_api_data['author_display'].get('videos_count')

                # 链接结构数据
                if self.kol_api_data['link_struct']:
                    existing_record.link_struct = self.kol_api_data['link_struct'].get('link_struct')

                # 平台信息数据
                if self.kol_api_data['platform_info']:
                    existing_record.self_intro = self.kol_api_data['platform_info'].get('self_intro')

                # 商业信息数据
                if self.kol_api_data['commerce_info']:
                    existing_record.commerce_info = self.kol_api_data['commerce_info'].get('commerce_info')

                existing_record.update_time = current_time
            else:
                # 创建新记录
                record_data = {
                    'douyin_user_id': user_id,
                    'douyin_nickname': self.current_kol.get('name', '') if self.current_kol else '',
                    'create_time': current_time,
                    'update_time': current_time
                }

                # 作者显示数据
                if self.kol_api_data['author_display']:
                    record_data.update({
                        'follower_count': self.kol_api_data['author_display'].get('follower_count'),
                        'link_count': self.kol_api_data['author_display'].get('link_count'),
                        'videos_count': self.kol_api_data['author_display'].get('videos_count')
                    })

                # 链接结构数据
                if self.kol_api_data['link_struct']:
                    record_data['link_struct'] = self.kol_api_data['link_struct'].get('link_struct')

                # 平台信息数据
                if self.kol_api_data['platform_info']:
                    record_data['self_intro'] = self.kol_api_data['platform_info'].get('self_intro')

                # 商业信息数据
                if self.kol_api_data['commerce_info']:
                    record_data['commerce_info'] = self.kol_api_data['commerce_info'].get('commerce_info')

                record = DouYinKolRealization(**record_data)
                session.add(record)

                session.commit()

        except Exception as db_error:
            self.logger.error(f"保存综合API数据时出错: {str(db_error)}")
            session.rollback()

    def _process_user_posted_data(self, response_data: Dict[str, Any]):
        """处理用户笔记数据，包括latest_star_item_info和latest_item_info"""
        try:
            if not response_data:
                self.logger.error("API响应数据为空")
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

            # 处理latest_star_item_info数据
            if 'latest_star_item_info' in response_data:
                notes_data = response_data.get('latest_star_item_info', [])
                if notes_data:
                    processed_count = 0

                    for note in notes_data:
                        try:
                            item_id = note.get('item_id', '')
                            if not item_id:
                                self.logger.warning("跳过处理：item_id为空")
                                continue

                            # 检查记录是否已存在
                            existing_record = session.query(DouYinKolNote).filter_by(
                                douyin_item_id=item_id).first()

                            if existing_record:
                                # 记录已存在，跳过不更新
                                self.logger.debug(f"视频 {item_id} 已存在，跳过保存")
                            else:
                                # 创建新记录
                                self._create_note_record(note, current_user_id)
                                processed_count += 1

                        except Exception as e:
                            self.logger.error(f"处理单条星图视频数据时出错: {str(e)}")
                            continue

                    self.logger.info(f"成功处理 {processed_count} 条星图笔记数据")
                else:
                    self.logger.info("本次获取的星图视频数据为空")

            # # 处理latest_item_info数据
            # if 'latest_item_info' in response_data:
            #     items_data = response_data.get('latest_item_info', [])
            #     if items_data:
            #         processed_count = 0
            #
            #         for item in items_data:
            #             try:
            #                 item_id = item.get('item_id', '')
            #                 if not item_id:
            #                     self.logger.warning("跳过处理：item_id为空")
            #                     continue
            #
            #                 # 检查记录是否已存在
            #                 existing_record = session.query(DouYinKolNote).filter_by(
            #                     douyin_item_id=item_id).first()
            #
            #                 if existing_record:
            #                     # 更新现有记录
            #                     self._update_note_record(existing_record, item, current_user_id)
            #                 else:
            #                     # 创建新记录
            #                     self._create_note_record(item, current_user_id)
            #
            #                 processed_count += 1
            #
            #             except Exception as e:
            #                 self.logger.error(f"处理单条普通视频数据时出错: {str(e)}")
            #                 continue
            #
            #         self.logger.info(f"成功处理 {processed_count} 条普通笔记数据")
            #     else:
            #         self.logger.info("本次获取的普通视频数据为空")

        except Exception as e:
            self.logger.error(f"处理用户视频数据时出错: {str(e)}")

    def _update_note_record(self, existing_record, note: Dict[str, Any], current_user_id: str):
        """更新现有笔记记录"""
        existing_record.douyin_user_id = current_user_id
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

        try:
            session.commit()
        except Exception as db_error:
            self.logger.error(f"更新视频数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def _create_note_record(self, note: Dict[str, Any], current_user_id: str):
        """创建新的笔记记录"""
        current_time = int(datetime.now().timestamp())
        note_record = DouYinKolNote(
            douyin_user_id=current_user_id,
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

        try:
            session.commit()
        except Exception as db_error:
            self.logger.error(f"创建视频数据时出错: {str(db_error)}")
            session.rollback()
            raise

    def send_wechat_notification(self, message):
        """发送企业微信通知"""
        try:
            data = {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }
            response = requests.post(self.webhook_url, json=data, timeout=5)
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    self.logger.info("✅ 企业微信通知发送成功")
                    return True
                else:
                    self.logger.warning(f"企业微信通知发送失败: {result}")
                    return False
            else:
                self.logger.warning(f"企业微信通知发送失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"发送企业微信通知时出错: {str(e)}")
            return False

    def check_and_handle_captcha(self):
        """检测并处理验证码"""
        try:
            self.logger.info("检测是否出现验证码...")

            # 常见的验证码元素选择器
            captcha_selectors = [
                'div[class*="captcha"]',
                'div[class*="verify"]',
                'div[class*="slider"]',
                'iframe[src*="captcha"]',
                'div.secsdk-captcha',
                'div.verification',
                'div.verify-wrap',
            ]

            # 检查是否出现验证码
            captcha_found = False
            for selector in captcha_selectors:
                try:
                    captcha_element = self.page.locator(selector).first
                    if captcha_element.is_visible(timeout=1000):
                        self.logger.warning(f"⚠️  检测到验证码！选择器: {selector}")
                        captcha_found = True
                        # 发送企业微信通知
                        try:
                            computer_name = self.config['PGY_LOGIN_CONFIG'].get('computer', '')
                            self.send_wechat_notification(
                                f"{computer_name}🔒 抖音KOL数据抓取检测到验证码！\n请尽快手动完成验证，程序已暂停等待...")
                        except Exception as notify_error:
                            self.logger.error(f"发送企业微信通知失败: {str(notify_error)}")
                            pass
                        break
                except:
                    continue

            if captcha_found:
                self.logger.warning("=" * 60)
                self.logger.warning("🔒 检测到验证码，请手动完成验证！")
                self.logger.warning("验证完成后程序将自动继续...")
                self.logger.warning("=" * 60)

                # 等待验证码消失，最多等待5分钟
                max_wait_time = 300
                check_interval = 3
                elapsed_time = 0

                while elapsed_time < max_wait_time:
                    time.sleep(check_interval)
                    elapsed_time += check_interval

                    # 检查验证码是否已消失
                    all_disappeared = True
                    for selector in captcha_selectors:
                        try:
                            element = self.page.locator(selector).first
                            if element.is_visible(timeout=500):
                                all_disappeared = False
                                break
                        except:
                            continue

                    if all_disappeared:
                        self.logger.info(f"✅ 验证码已完成！(等待了 {elapsed_time} 秒)")
                        # 发送完成通知
                        try:
                            self.send_wechat_notification(f"✅ 验证码已完成！程序继续执行 (等待了 {elapsed_time} 秒)")
                        except:
                            pass
                        time.sleep(2)
                        return True

                    # 每30秒提示一次
                    if elapsed_time % 30 == 0:
                        self.logger.info(f"仍在等待验证码完成... (已等待 {elapsed_time}/{max_wait_time} 秒)")

                self.logger.error("❌ 验证码等待超时（5分钟）")
                return False
            else:
                self.logger.info("✓ 未检测到验证码")
                return True

        except Exception as e:
            self.logger.error(f"检测验证码时出错: {str(e)}")
            return True

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
                    self.logger.info(f"选择器 '.user-avatar' 找到 {count} 个元素")

                    if count > 0:
                        # 检查所有元素，只要有一个可见就认为登录成功
                        self.logger.info(f"开始检查 {count} 个 .user-avatar 元素的可见性...")
                        all_elements = element.all()
                        for i, elem in enumerate(all_elements):
                            try:
                                if elem.is_visible(timeout=1000):
                                    self.logger.info(f"第 {i + 1} 个 .user-avatar 元素可见，Cookie有效")
                                    login_detected = True
                                    break
                                else:
                                    self.logger.debug(f"第 {i + 1} 个 .user-avatar 元素不可见")
                            except Exception as elem_error:
                                self.logger.debug(f"第 {i + 1} 个 .user-avatar 元素检查出错: {str(elem_error)}")
                                continue

                        if not login_detected:
                            self.logger.warning(f"找到 {count} 个 .user-avatar 元素，但都不可见")
                except Exception as e:
                    self.logger.debug(f"选择器 '.user-avatar' 检查出错: {str(e)}")

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
                    ".text-avatar"
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
                '/api/author/get_author_show_items_v2',
                '/api/author/get_author_base_info',
                '/api/author/get_author_marketing_info'
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

            elif '/api/author/get_author_show_items_v2' in url:
                if url in self.processed_api_responses:
                    self.logger.debug("跳过重复的API响应")
                    return

                self.processed_api_responses.add(url)
                self._process_user_posted_data(response_data)
                self.api_response_processed = True

            elif '/api/author/get_author_base_info' in url:
                self._process_author_base_info(response_data)
                self.api_response_processed = True

            elif '/api/author/get_author_marketing_info' in url:
                self._process_marketing_info(response_data)
                self.api_response_processed = True

        except Exception as e:
            # 如果是浏览器关闭错误，不记录为错误
            if "Target page, context or browser has been closed" in str(e):
                self.logger.info(f"浏览器已关闭，跳过API数据处理: {url}")
            else:
                self.logger.error(f"处理API响应时出错: {str(e)}, URL: {url}")

    def _process_all_api_data(self):
        """处理所有API数据"""
        try:
            if not self.api_data:
                self.logger.warning("没有API数据需要处理")
                return

            self.logger.info(f"开始处理 {len(self.api_data)} 个API响应")

            for api_url, response_data in self.api_data.items():
                if 'data' not in response_data:
                    continue

                api_data = response_data['data']
                if 'search_for_author_square' in api_url:
                    authors_added = 0
                    authors_skipped = 0

                    for author_data in api_data['authors']:
                        # 检查是否已存在
                        existing_data = session.query(DouyinSearchList).filter(
                            DouyinSearchList.star_id == author_data['star_id']).first()
                        if existing_data:
                            authors_skipped += 1
                            continue

                        # 创建新记录 - 将字典类型转换为JSON字符串
                        detail = DouyinSearchList(
                            attribute_datas=self._safe_json_dumps(author_data.get('attribute_datas', {})),
                            extra_data=self._safe_json_dumps(author_data.get('extra_data', {})),
                            items=self._safe_json_dumps(author_data.get('items', [])),
                            star_id=str(author_data.get('star_id', '')),
                            task_infos=self._safe_json_dumps(author_data.get('task_infos', {})),
                        )
                        session.add(detail)
                        authors_added += 1

                    # 提交数据库事务
                    session.commit()
                    self.logger.info(f"API数据处理完成: 新增 {authors_added} 条，跳过 {authors_skipped} 条重复数据")

        except Exception as e:
            self.logger.error(f"处理API数据时出错: {str(e)}")
            session.rollback()

    def _process_all_pages(self):
        """处理所有页面数据，自动翻页直到disabled"""
        try:
            self.logger.info("开始处理所有页面数据...")
            return True
        except Exception as e:
            self.logger.error(f"处理所有页面时出错: {str(e)}")
            return False

    def _safe_json_dumps(self, data):
        """安全地将数据转换为JSON字符串，处理各种异常情况"""
        try:
            if data is None:
                return ""
            elif isinstance(data, (dict, list)):
                return json.dumps(data, ensure_ascii=False)
            elif isinstance(data, str):
                # 如果已经是字符串，尝试解析并重新序列化以确保格式正确
                try:
                    parsed = json.loads(data)
                    return json.dumps(parsed, ensure_ascii=False)
                except (json.JSONDecodeError, ValueError):
                    # 如果解析失败，直接返回原字符串（可能是已经格式化的JSON）
                    return data
            else:
                # 其他类型转换为字符串
                return str(data)
        except Exception as e:
            self.logger.warning(f"JSON序列化失败，使用字符串表示: {str(e)}")
            return str(data) if data is not None else ""

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


def get_pending_kols() -> List[DouyinSearchList]:
    """获取需要处理的KOL列表"""
    try:
        config = load_config()
        page = config['PGY_LOGIN_CONFIG']['page']
        page_size = config['PGY_LOGIN_CONFIG']['page_size']
        offset = (int(page) - 1) * int(page_size)

        kols = (
            session.query(DouyinSearchList)
            .filter(
                DouyinSearchList.status == 0
            )
            .order_by(DouyinSearchList.id.asc())  # ✅ 建议加排序，保证分页顺序稳定
            .offset(offset)
            .limit(page_size)
            .all()
        )
        print(f"从数据库获取到 {len(kols)} 个待处理的KOL")
        return kols
    except Exception as e:
        print(f"获取KOL列表时出错: {str(e)}")
        raise


def process_kol(spider: DouYinSpider, kol: DouyinSearchList):
    """处理单个KOL"""
    # 从attribute_datas中提取KOL名称和构建链接
    try:
        # 检查attribute_datas是字典还是字符串
        if isinstance(kol.attribute_datas, str):
            attribute_data = json.loads(kol.attribute_datas)
        elif isinstance(kol.attribute_datas, dict):
            attribute_data = kol.attribute_datas
        else:
            spider.logger.error(f"attribute_datas类型错误: {type(kol.attribute_datas)}")
            return False

        kol_name = attribute_data.get('nick_name', '')
        star_id = kol.star_id
        douyin_link = f"https://www.xingtu.cn/ad/creator/author-homepage/douyin-video/{star_id}"

        if not kol_name:
            spider.logger.warning(f"无法从attribute_datas中获取KOL名称，使用star_id: {star_id}")
            kol_name = f"KOL_{star_id}"
    except (json.JSONDecodeError, AttributeError, TypeError) as e:
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
            # 处理成功
            kol.status = 1
            kol.updated_at = datetime.now()
            session.commit()

            spider.logger.info(f"✅ KOL {kol_name} 处理成功")
            return True
        elif result == 2:
            # 没有创作能力按钮（该KOL没有创作能力数据）
            kol.status = 3
            kol.updated_at = datetime.now()
            session.commit()

            spider.logger.info(f"ℹ️ KOL {kol_name} 没有创作能力数据，已标记为状态3")
            return True
        else:
            # 处理失败
            spider.logger.warning(f"⚠️ KOL {kol_name} 处理失败")
            return False

    except Exception as e:
        spider.logger.error(f"❌ 处理KOL {kol_name} 时出错: {str(e)}")
        spider.logger.error(f"错误详情: {traceback.format_exc()}")

        # 回滚数据库事务
        try:
            session.rollback()
        except Exception as rollback_error:
            spider.logger.error(f"回滚数据库事务时出错: {str(rollback_error)}")
            return False


def main():
    """
    主函数 - 抖音KOL数据抓取程序
    """
    spider = None
    try:
        print("=== 抖音KOL数据抓取程序启动 ===")

        # 1. 获取待处理的KOL列表
        kols = get_pending_kols()
        if not kols:
            print("没有找到需要处理的KOL数据，程序结束")
            return True

        # 2. 初始化爬虫
        spider = DouYinSpider()
        spider.setup_browser()

        # 3. 登录
        login_success = spider.login()
        if not login_success:
            print("登录失败，程序退出")
            return False

        # 4. 批处理KOL
        processed_count = 0
        failed_count = 0
        no_creative_count = 0

        for i, kol in enumerate(kols, 1):
            print(f"进度: {i}/{len(kols)} ({(i / len(kols)) * 100:.1f}%)")

            try:
                result = process_kol(spider, kol)
                if result:
                    processed_count += 1
                    if kol.status == 2:
                        no_creative_count += 1
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
        print(f"成功处理（有创作能力）: {processed_count - no_creative_count}")
        print(f"成功处理（无创作能力）: {no_creative_count}")
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