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
import urllib3

import pandas as pd
import schedule
from loguru import logger
from playwright.sync_api import sync_playwright
from unitl.common import Common

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        self.current_video_type = None  # 当前视频类型：'personal' 或 'xingtu'

        # 新增：存储所有API数据的字典 - 空对象
        self.kol_api_data = {}
        self.other_api_data = {}
        self.yingxiao_api_data = []  # 营销传播数据数组

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
        - 1: 处理成功（无论是否有连接用户按钮，都会尝试获取所有可用的数据）
        - 0: 处理失败
        """
        try:
            if not self.is_logged_in:
                self.logger.error("未登录状态，无法抓取数据")
                return 0

            user_id = star_id  # 定义 user_id 供后续使用
            self.current_kol = {'name': kol_name, 'url': kol_url, 'user_id':star_id}
            self.processed_api_responses.clear()
            # 完全重置营销信息，确保数据隔离
            self.marketing_info = {'user_id': star_id}
            # 重置API数据缓存
            self.api_data = {}
            # 重新初始化KOL数据结构 - 空对象，只填充基本信息
            self.kol_api_data = {}
            self.other_api_data = {}
            self.yingxiao_api_data = []  # 重置营销传播数组
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

            api_data_copy = dict(self.api_data)
            for api_url, response_info in api_data_copy.items():
                if 'data' not in response_info:
                    continue
                response_data = response_info['data']  # 提取实际的响应数据
                if '/api/author/get_author_base_info' in api_url:
                    self._process_author_base_info(response_data)
                elif '/api/data_sp/check_author_display' in api_url:
                    self._process_author_display(response_data)
                elif '/api/author/get_author_marketing_info' in api_url:
                    self._process_marketing_info(response_data)
                elif '/api/author/get_author_platform_channel_info_v2' in api_url:
                    self._process_author_platform_channel_info_v2(response_data)
                elif '/api/aggregator/get_author_commerce_spread_info' in api_url:
                    self._process_author_commerce_info(response_data)



            # ===== 新增：点击商业能力并处理视频类型 =====
            try:
                self.logger.info("开始处理商业能力的视频类型...")

                # 点击商业能力标签
                business_ability_tab = self.page.locator("div.el-tabs__nav >> div:has-text('商业能力')")
                if business_ability_tab and business_ability_tab.is_visible():
                    # 点击前等待页面完全响应
                    try:
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception as e:
                        self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                    self.api_data = {}
                    time.sleep(0.5)
                    business_ability_tab.click()
                    self.logger.info("成功点击商业能力标签")

                    # 等待页面加载完成并增加延迟
                    try:
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception as e:
                        self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                    self.common.random_sleep(3, 4)  # 增加3-4秒延迟，避免操作过快

                    # 检测并处理验证码
                    if not self.check_and_handle_captcha():
                        self.logger.error("验证码处理失败")
                        return 0

                    # 查找两个label标签
                    try:
                        # 找到所有el-checkbox-button标签
                        checkbox_buttons = self.page.locator("label.el-checkbox-button.xt-checkbox-button")
                        button_count = checkbox_buttons.count()

                        # 找到个人视频和星图视频按钮
                        personal_video_btn = None
                        xingtu_video_btn = None

                        for i in range(button_count):
                            btn = checkbox_buttons.nth(i)
                            btn_text = btn.inner_text()

                            if '个人视频' in btn_text:
                                personal_video_btn = btn
                            elif '星图视频' in btn_text:
                                xingtu_video_btn = btn

                        # 检查星图视频是否被禁用
                        xingtu_disabled = False
                        if xingtu_video_btn:
                            xingtu_class = xingtu_video_btn.get_attribute('class')
                            xingtu_disabled = 'is-disabled' in xingtu_class

                        if not xingtu_disabled and xingtu_video_btn:
                            # 星图视频未禁用，默认选中的是星图视频
                            self.logger.info("星图视频未禁用，获取星图视频数据 (business=1)...")

                            # 确保星图视频被选中
                            xingtu_class = xingtu_video_btn.get_attribute('class')
                            if 'is-checked' not in xingtu_class:
                                # 点击前等待页面完全响应
                                try:
                                    self.page.wait_for_load_state('networkidle', timeout=5000)
                                except Exception as e:
                                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                xingtu_video_btn.click()
                                self.logger.info("点击星图视频按钮")

                                # 等待页面加载完成并增加延迟
                                try:
                                    self.page.wait_for_load_state('networkidle', timeout=5000)
                                except Exception as e:
                                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                self.common.random_sleep(3, 4)  # 增加3-4秒延迟

                            # 等待并标记为获取星图视频数据
                            self.current_video_type = 'xingtu'  # 标记当前视频类型

                            # 【关键修复】主动等待星图视频 spread_info API 出现
                            self.logger.info("等待星图视频API数据加载...")
                            max_wait_time = 15  # 最多等待15秒
                            poll_interval = 0.5  # 每0.5秒检查一次
                            waited_time = 0
                            api_found = False

                            self.logger.info(f"开始轮询等待星图视频 spread_info API (最多等待{max_wait_time}秒)...")

                            while waited_time < max_wait_time:
                                # 检查是否已经有星图视频的 spread_info API
                                xingtu_spread_count = sum(
                                    1 for url in self.api_data.keys()
                                    if '/api/data_sp/get_author_spread_info' in url
                                    and ('type=2' in url or 'only_assign=true' in url)
                                )

                                if xingtu_spread_count > 0:
                                    self.logger.info(f"✅ 检测到星图视频 spread_info API！等待时间: {waited_time:.1f}秒")
                                    api_found = True
                                    break

                                # 【关键】使用 page.wait_for_timeout 而不是 time.sleep
                                # 这样可以让 playwright 事件循环处理响应
                                self.page.wait_for_timeout(int(poll_interval * 1000))
                                waited_time += poll_interval

                            if not api_found:
                                self.logger.warning(f"⏰ 等待超时({max_wait_time}秒)，未检测到星图视频 spread_info API")

                            # 处理商业能力页面的所有API数据
                            self.logger.info("处理商业能力页面的API数据...")

                            # 调试：打印所有捕获到的API
                            self.logger.info(f"📊 当前 api_data 中有 {len(self.api_data)} 个API")
                            for api_url in self.api_data.keys():
                                self.logger.info(f"  - {api_url}")

                            # 1. 处理星图视频的 spread_info (type=2, only_assign=true)
                            xingtu_spread_apis = []
                            for api_url, response_info in self.api_data.items():
                                if 'data' not in response_info:
                                    continue
                                # 判断条件改为：包含 type=2 或 only_assign=true
                                if '/api/data_sp/get_author_spread_info' in api_url and ('type=2' in api_url or 'only_assign=true' in api_url):
                                    xingtu_spread_apis.append((api_url, response_info))

                            self.logger.info(f"找到 {len(xingtu_spread_apis)} 个星图 spread_info API")
                            if xingtu_spread_apis:
                                _, last_response_info = xingtu_spread_apis[-1]
                                response_data = last_response_info['data']
                                self.logger.info(f"使用最后一个星图 spread_info API")
                                self._process_author_spread_info(response_data, user_id)
                            else:
                                self.logger.warning("⚠️ 没有找到星图 spread_info API (type=2)")

                            # 2. 处理种草价值 (commerce_seed_base_info)
                            for api_url, response_info in self.api_data.items():
                                if 'data' not in response_info:
                                    continue
                                if '/api/aggregator/get_author_commerce_seed_base_info' in api_url:
                                    response_data = response_info['data']
                                    self.logger.info("处理种草价值数据...")
                                    self._process_author_commerce_seed_base_info(response_data, user_id)
                                    break

                            # 3. 处理转化价值 (convert_ability)
                            for api_url, response_info in self.api_data.items():
                                if 'data' not in response_info:
                                    continue
                                if '/api/data_sp/get_author_convert_ability' in api_url:
                                    response_data = response_info['data']
                                    self.logger.info("处理转化价值数据...")
                                    self._process_author_convert_ability(response_data, user_id)
                                    break

                            # 处理完星图数据后，先检查是否已经有个人视频的API（可能一起加载了）
                            self.logger.info("检查是否已经捕获到个人视频的 spread_info API...")

                            # 查找个人视频的 spread_info (type=1 或 only_assign=false)
                            personal_spread_in_current = []
                            for api_url, response_info in self.api_data.items():
                                if 'data' not in response_info:
                                    continue
                                if '/api/data_sp/get_author_spread_info' in api_url and ('type=1' in api_url or 'only_assign=false' in api_url):
                                    personal_spread_in_current.append((api_url, response_info))

                            if personal_spread_in_current:
                                # 如果已经有个人视频数据，直接处理，不需要点击
                                self.logger.info(f"✅ 已经捕获到 {len(personal_spread_in_current)} 个个人 spread_info API，无需点击")
                                self.current_video_type = 'personal'
                                _, last_response_info = personal_spread_in_current[-1]
                                response_data = last_response_info['data']
                                self.logger.info(f"使用已捕获的个人 spread_info API")
                                self._process_author_spread_info(response_data, user_id)
                                self.logger.info("✅ 已获取个人视频数据 (type=1)")
                            elif personal_video_btn:
                                # 没有个人视频数据，需要点击切换
                                self.logger.info("未找到个人视频API，准备点击个人视频按钮")

                                # 【重要】先设置视频类型，再清空，再点击
                                # 这样 handler 可以正确识别新的API
                                self.current_video_type = 'personal'
                                self.logger.info("已设置 current_video_type = 'personal'")

                                # 清空后再点击个人视频
                                self.api_data = {}
                                self.logger.info("已清空 api_data")

                                # 点击前等待页面完全响应
                                try:
                                    self.page.wait_for_load_state('networkidle', timeout=5000)
                                except Exception as e:
                                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                self.logger.info("点击个人视频按钮...")
                                personal_video_btn.click()

                                # 等待页面加载完成并增加延迟
                                try:
                                    self.page.wait_for_load_state('networkidle', timeout=5000)
                                    self.logger.info("页面网络空闲")
                                except Exception as e:
                                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                self.common.random_sleep(3, 4)  # 增加3-4秒延迟

                                self.logger.info("等待个人视频API数据加载...")

                                # 【关键修复】主动等待 spread_info API 出现，而不是盲目等待固定时间
                                max_wait_time = 15  # 最多等待15秒
                                poll_interval = 0.5  # 每0.5秒检查一次
                                waited_time = 0
                                api_found = False

                                while waited_time < max_wait_time:
                                    # 检查是否已经有个人视频的 spread_info API
                                    personal_spread_count = sum(
                                        1 for url in self.api_data.keys()
                                        if '/api/data_sp/get_author_spread_info' in url
                                        and ('type=1' in url or 'only_assign=false' in url)
                                    )

                                    if personal_spread_count > 0:
                                        api_found = True
                                        break

                                    # 【关键】使用 page.wait_for_timeout 而不是 time.sleep
                                    # 这样可以让 playwright 事件循环处理响应
                                    self.page.wait_for_timeout(int(poll_interval * 1000))
                                    waited_time += poll_interval

                                if not api_found:
                                    self.logger.warning(f"⏰ 等待超时({max_wait_time}秒)，未检测到个人视频 spread_info API")

                                # 调试：打印所有捕获到的API
                                for api_url in self.api_data.keys():
                                    self.logger.info(f"  - {api_url}")

                                # 处理个人视频的 spread_info (type=1, only_assign=false)
                                personal_spread_apis = []
                                for api_url, response_info in self.api_data.items():
                                    if 'data' not in response_info:
                                        continue
                                    # 判断条件改为：包含 type=1 或 only_assign=false
                                    if '/api/data_sp/get_author_spread_info' in api_url and ('type=1' in api_url or 'only_assign=false' in api_url):
                                        personal_spread_apis.append((api_url, response_info))

                                self.logger.info(f"找到 {len(personal_spread_apis)} 个个人 spread_info API")
                                if personal_spread_apis:
                                    _, last_response_info = personal_spread_apis[-1]
                                    response_data = last_response_info['data']
                                    self.logger.info(f"使用最后一个个人 spread_info API")
                                    self._process_author_spread_info(response_data, user_id)
                                else:
                                    self.logger.warning("⚠️ 没有找到个人 spread_info API (type=1)")

                                self.logger.info("✅ 已获取个人视频数据 (type=1)")
                        else:
                            # 星图视频被禁用，默认是个人视频
                            self.logger.info("星图视频已禁用，只获取个人视频数据 (type=1)...")

                            # 确保个人视频被选中
                            if personal_video_btn:
                                personal_class = personal_video_btn.get_attribute('class')
                                if 'is-checked' not in personal_class:
                                    # 点击前等待页面完全响应
                                    try:
                                        self.page.wait_for_load_state('networkidle', timeout=5000)
                                    except Exception as e:
                                        self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                    personal_video_btn.click()
                                    self.logger.info("点击个人视频按钮")

                                    # 等待页面加载完成并增加延迟
                                    try:
                                        self.page.wait_for_load_state('networkidle', timeout=5000)
                                    except Exception as e:
                                        self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                                    self.common.random_sleep(3, 4)  # 增加3-4秒延迟

                            # 标记为获取个人视频数据
                            self.current_video_type = 'personal'

                            # 【关键修复】主动等待个人视频 spread_info API 出现
                            self.logger.info("等待个人视频API数据加载...")
                            max_wait_time = 15  # 最多等待15秒
                            poll_interval = 0.5  # 每0.5秒检查一次
                            waited_time = 0
                            api_found = False

                            self.logger.info(f"开始轮询等待个人视频 spread_info API (最多等待{max_wait_time}秒)...")

                            while waited_time < max_wait_time:
                                # 检查是否已经有个人视频的 spread_info API
                                personal_spread_count = sum(
                                    1 for url in self.api_data.keys()
                                    if '/api/data_sp/get_author_spread_info' in url
                                    and ('type=1' in url or 'only_assign=false' in url)
                                )

                                if personal_spread_count > 0:
                                    self.logger.info(f"✅ 检测到个人视频 spread_info API！等待时间: {waited_time:.1f}秒")
                                    api_found = True
                                    break

                                # 【关键】使用 page.wait_for_timeout 而不是 time.sleep
                                # 这样可以让 playwright 事件循环处理响应
                                self.page.wait_for_timeout(int(poll_interval * 1000))
                                waited_time += poll_interval

                            if not api_found:
                                self.logger.warning(f"⏰ 等待超时({max_wait_time}秒)，未检测到个人视频 spread_info API")

                            # 处理个人视频的 spread_info (type=1 或 only_assign=false)
                            personal_spread_apis = []
                            for api_url, response_info in self.api_data.items():
                                if 'data' not in response_info:
                                    continue
                                if '/api/data_sp/get_author_spread_info' in api_url and ('type=1' in api_url or 'only_assign=false' in api_url):
                                    personal_spread_apis.append((api_url, response_info))

                            if personal_spread_apis:
                                # 使用最后一个 API 响应（第一个可能没有数据）
                                _, last_response_info = personal_spread_apis[-1]
                                response_data = last_response_info['data']
                                self.logger.info(f"找到 {len(personal_spread_apis)} 个个人 spread_info API，使用最后一个")
                                self._process_author_spread_info(response_data, user_id)

                    except Exception as btn_error:
                        self.logger.warning(f"处理视频类型按钮时出错: {str(btn_error)}")

                else:
                    # 未找到商业能力标签，使用首页默认数据
                    self.logger.info("未找到商业能力标签，使用首页默认的传播信息数据")

                    # 首页默认是个人视频数据
                    self.current_video_type = 'personal'

                    # 处理首页的 spread_info - 选择最后一个（第一个可能没有数据）
                    homepage_spread_apis = []
                    for api_url, response_info in api_data_copy.items():
                        if 'data' not in response_info:
                            continue
                        if '/api/data_sp/get_author_spread_info' in api_url:
                            homepage_spread_apis.append((api_url, response_info))

                    if homepage_spread_apis:
                        # 使用最后一个 API 响应（首页加载时第一个可能没有数据）
                        _, last_response_info = homepage_spread_apis[-1]
                        response_data = last_response_info['data']
                        self.logger.info(f"首页找到 {len(homepage_spread_apis)} 个 spread_info API，使用最后一个")
                        self._process_author_spread_info(response_data, user_id)

            except Exception as ability_error:
                self.logger.warning(f"处理商业能力时出错: {str(ability_error)}")

            # ===== 商业能力处理结束 =====

            # 点击连接用户标签
            creative_tab = self.page.locator("div.el-tabs__nav >> div:has-text('连接用户')")
            if creative_tab and creative_tab.is_visible():
                # 点击前等待页面完全响应
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception as e:
                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                time.sleep(0.5)
                creative_tab.click()
                self.logger.info("成功点击连接用户标签")

                # 等待页面加载完成并增加延迟
                try:
                    self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception as e:
                    self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                self.common.random_sleep(3, 4)  # 增加3-4秒延迟，避免操作过快

                # 检测并处理验证码
                if not self.check_and_handle_captcha():
                    self.logger.error("验证码处理失败")
                    return 0

                # 【关键】主动等待连接用户API出现
                self.logger.info("等待连接用户API数据加载...")
                max_wait_time = 15  # 最多等待15秒
                poll_interval = 0.5  # 每0.5秒检查一次
                waited_time = 0
                api_found = False

                self.logger.info(f"开始轮询等待 author_link_card API (最多等待{max_wait_time}秒)...")

                while waited_time < max_wait_time:
                    # 检查是否已经有连接用户的 API
                    link_card_api_count = sum(
                        1 for url in self.api_data.keys()
                        if '/api/data_sp/author_link_card' in url
                    )

                    if link_card_api_count > 0:
                        self.logger.info(f"✅ 检测到 author_link_card API！等待时间: {waited_time:.1f}秒")
                        api_found = True
                        break

                    # 【关键】使用 page.wait_for_timeout 而不是 time.sleep
                    # 这样可以让 playwright 事件循环处理响应
                    self.page.wait_for_timeout(int(poll_interval * 1000))
                    waited_time += poll_interval

                if not api_found:
                    self.logger.warning(f"⏰ 等待超时({max_wait_time}秒)，未检测到 author_link_card API")

                self.logger.info("处理连接用户页面的API数据...")

                # 1. 处理连接用户分布 (link_card)
                for api_url, response_info in self.api_data.items():
                    if 'data' not in response_info:
                        continue
                    if '/api/data_sp/author_link_card' in api_url:
                        response_data = response_info['data']
                        self.logger.info("处理连接用户分布数据...")
                        self._process_author_link_card(response_data, user_id)
                        break

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

                self.common.random_sleep(3, 4)  # 增加3-4秒延迟，确保页面完全加载

                fan_portrait_button = self.page.locator("text=粉丝画像")
                if fan_portrait_button and fan_portrait_button.is_visible():
                    # 【重要】点击前清空 api_data，确保只捕获粉丝画像相关的API
                    self.api_data = {}
                    self.logger.info("已清空 api_data，准备点击粉丝画像按钮")

                    # 点击前等待页面完全响应
                    try:
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception as e:
                        self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                    time.sleep(0.5)
                    fan_portrait_button.click()
                    self.logger.info("成功点击粉丝画像按钮")

                    # 等待页面加载完成并增加延迟
                    try:
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                    except Exception as e:
                        self.logger.warning(f"等待页面加载完成时出错: {str(e)}")

                    self.common.random_sleep(3, 4)  # 增加3-4秒延迟，避免操作过快

                    # 检测并处理验证码
                    if not self.check_and_handle_captcha():
                        self.logger.error("验证码处理失败")
                        return 0

                    # 【关键】主动等待粉丝分布API出现
                    max_wait_time = 15  # 最多等待15秒
                    poll_interval = 0.5  # 每0.5秒检查一次
                    waited_time = 0
                    api_found = False

                    self.logger.info(f"开始轮询等待粉丝分布 API (最多等待{max_wait_time}秒)...")

                    while waited_time < max_wait_time:
                        # 检查是否已经有粉丝分布的 API
                        fans_api_count = sum(
                            1 for url in self.api_data.keys()
                            if '/api/data_sp/get_author_fans_distribution' in url
                        )

                        if fans_api_count > 0:
                            self.logger.info(f"✅ 检测到粉丝分布 API！等待时间: {waited_time:.1f}秒")
                            api_found = True
                            break

                        # 【关键】使用 page.wait_for_timeout 而不是 time.sleep
                        # 这样可以让 playwright 事件循环处理响应
                        self.page.wait_for_timeout(int(poll_interval * 1000))
                        waited_time += poll_interval

                    if not api_found:
                        self.logger.warning(f"⏰ 等待超时({max_wait_time}秒)，未检测到粉丝分布 API")

                    # 调试：打印所有捕获到的API
                    self.logger.info(f"📊 点击粉丝画像后，api_data 中有 {len(self.api_data)} 个API")
                    for api_url in self.api_data.keys():
                        self.logger.info(f"  - {api_url}")

                    # 2. 处理粉丝数据 (fans_distribution)
                    self.logger.info(f"开始查找粉丝分布API，当前api_data有 {len(self.api_data)} 个API")
                    fans_api_found = False
                    for api_url, response_info in self.api_data.items():
                        if 'data' not in response_info:
                            continue
                        if '/api/data_sp/get_author_fans_distribution' in api_url:
                            fans_api_found = True
                            response_data = response_info['data']
                            self.logger.info(f"✅ 找到粉丝分布API: {api_url}")
                            self.logger.info("处理粉丝数据分布...")
                            self._process_author_fans_distribution(response_data, user_id)
                            break

                    if not fans_api_found:
                        self.logger.warning("⚠️ 未找到粉丝分布API数据")
                else:
                    self.logger.warning("未找到粉丝画像按钮，跳过粉丝数据获取")

            # 统一保存所有收集到的API数据到远程接口
            if self.current_kol and self.current_kol.get('user_id'):
                self.logger.info("开始统一保存所有API数据到远程接口")
                self._save_all_kol_data_to_api(self.current_kol.get('user_id'))
                self.logger.info("✅ 所有API数据已统一保存到远程接口")

            return 1  # 返回1表示处理成功

        except Exception as e:
            self.logger.error(f"抓取KOL {kol_name} 笔记时出错: {str(e)}")
            raise



    def _save_all_kol_data_to_api(self, user_id: str):
        """统一保存所有收集到的API数据到远程接口"""
        try:
            self.logger.info(f"开始统一保存所有API数据到远程接口，用户ID: {user_id}")
            print("=" * 60)
            print("kol_api_data:")
            print(self.kol_api_data)
            print("=" * 60)
            print("other_api_data:")
            print(self.other_api_data)
            print("=" * 60)
            print("yingxiao_api_data:")
            print(self.yingxiao_api_data)
            print("=" * 60)

            # 构建payload，参考get_pgy_intro.py的格式
            payload = {
                "apis": [
                    {"tb_name": "blogger_info", "tb_data": [self.kol_api_data]},
                    {"tb_name": "blogger_note_rate", "tb_data": []},
                    {"tb_name": "blogger_data_summary", "tb_data": []},
                    {"tb_name": "blogger_note_detail", "tb_data": []},
                    {"tb_name": "blogger_fans_summary", "tb_data": []},
                    {"tb_name": "blogger_fans_profile", "tb_data": []},
                    {"tb_name": "blogger_fans_history", "tb_data": []},
                    {"tb_name": "douyin_kol_profile", "tb_data": [self.other_api_data]},
                    {"tb_name": "douyin_kol_marketing_stats", "tb_data": self.yingxiao_api_data},
                ],
                "client_id": 1
            }

            self.logger.info(f"准备发送数据到API接口")

            # 发送POST请求到API接口
            api_url = "http://47.104.76.46:19000/api/v1/sync/spider/data"
            headers = {
                "Content-Type": "application/json"
            }

            response = requests.post(api_url, json=payload, headers=headers, timeout=30, verify=False)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    if response_data.get('code') == 200:
                        self.logger.info(f"✅ 数据成功发送到API接口，响应: {response_data}")
                    else:
                        self.logger.error(f"❌ API接口请求失败，API返回错误: {response_data}")
                        raise Exception(f"API接口请求失败: {response_data}")
                except ValueError:
                    self.logger.error(f"API返回非JSON响应: {response.text[:200]}")
                    raise Exception(f"API返回非JSON响应")
            else:
                self.logger.error(f"❌ API接口请求失败，状态码: {response.status_code}")
                self.logger.error(f"响应内容: {response.text}")
                raise Exception(f"API接口请求失败，状态码: {response.status_code}")

        except Exception as api_error:
            self.logger.error(f"统一保存API数据到远程接口时出错: {str(api_error)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            raise

    def _process_marketing_info(self, response_data: Dict[str, Any]):
        """处理营销信息数据 - 根据新表结构调整字段名"""
        try:
            if not response_data:
                self.logger.error("营销信息API响应数据为空")
                return

            # 报价信息
            if 'price_info' in response_data:
                price_list = response_data['price_info']
                for price in price_list:
                    video_type = price.get('video_type')
                    price_value = price.get('price', 0)
                    if video_type == 1:
                        self.other_api_data['price_1_20s'] = price_value  # 1-20秒报价
                    elif video_type == 2:
                        self.other_api_data['price_20_60s'] = price_value  # 20-60秒报价
                        self.kol_api_data['picturePrice'] = price_value
                        self.kol_api_data['videoPrice'] = price_value
                    elif video_type == 71:
                        self.other_api_data['price_60s_plus'] = price_value  # 60秒以上报价
                    elif video_type == 150:
                        self.other_api_data['price_platform_raw'] = price_value  # 短直种草平台裸价

        except Exception as e:
            self.logger.error(f"处理营销信息时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_base_info(self, response_data: Dict[str, Any]):
        """处理作者基本信息数据 - 参考get_douyin_guakao.py第226-247行"""
        try:
            if not response_data:
                self.logger.error("作者基本信息API响应数据为空")
                return

            # 性别转换
            gender = response_data.get('gender', '')
            if gender == 1:
                gender = 2
            elif gender == 2:
                gender = 1
            # 1. 基本信息
            self.kol_api_data['name'] = response_data.get('nick_name', '')
            self.kol_api_data['platform_user_id'] = response_data.get('id')
            self.kol_api_data['location'] = response_data.get('city')
            self.kol_api_data['redId'] = response_data.get('short_id')
            self.kol_api_data['headPhoto'] = response_data.get('avatar_uri')
            self.kol_api_data['gender'] = gender
            tags_relation = response_data.get('tags_relation', {})
            if tags_relation:
                content_field = []
                for k, v in tags_relation.items():
                    content_field.append({
                        "taxonomy1Tag": k,  # 一级标签
                        "taxonomy2Tags": v or []  # 二级标签数组
                    })
                self.kol_api_data['contentTags'] = content_field
            else:
                self.kol_api_data['contentTags'] = []

            self.other_api_data['douyin_sec_uid'] = response_data.get('sec_uid', '')
            self.other_api_data['platform_user_id'] = response_data.get('id')

        except Exception as e:
            self.logger.error(f"处理作者基本信息时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_display(self, response_data: Dict[str, Any]):
        """处理作者显示检查API数据 - 参考get_douyin_guakao.py第249-253行"""
        try:
            if not response_data:
                self.logger.error("作者显示检查API响应数据为空")
                return

            # 2. 粉丝数赞藏
            self.kol_api_data['fansNum'] = response_data.get('follower', 0)
            self.kol_api_data['likeCollectCountInfo'] = response_data.get('link_cnt', 0)

        except Exception as e:
            self.logger.error(f"处理作者显示数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_platform_channel_info_v2(self, response_data: Dict[str, Any]):
        """处理作者链接结构API数据，保存link_struct对象为JSON格式"""
        try:
            if not response_data:
                self.logger.error("作者链接结构API响应数据为空")
                return

            self.kol_api_data['creator_intro'] = response_data.get('self_intro', {})

        except Exception as e:
            self.logger.error(f"处理链接结构数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_commerce_info(self, response_data: Dict[str, Any]):
        """处理作者商业传播信息API数据 - 参考get_douyin_guakao.py第349-359行"""
        try:
            if not response_data:
                self.logger.error("作者商业传播信息API响应数据为空")
                return

            # 6. 预估CPE/CPM
            self.other_api_data['expect_cpe'] = response_data.get('cpe_20_60', '')
            self.other_api_data['expect_cpm'] = response_data.get('cpm_20_60', '')
            self.other_api_data['platform_hot_rate'] = response_data.get('platform_hot_rate', '')
            self.other_api_data['expect_read'] = response_data.get('vv', '')

        except Exception as e:
            self.logger.error(f"处理商业传播信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_spread_info(self, response_data: Dict[str, Any], user_id: str):
        """处理作者传播信息API数据 - 存入yingxiao_api_data数组"""
        try:
            self.logger.info(f"开始处理传播信息API数据，用户ID: {user_id}，视频类型: {self.current_video_type}")

            if not response_data:
                self.logger.error("作者传播信息API响应数据为空")
                return

            # 提取基础数据
            play_mid = response_data.get('play_mid', '')
            like_avg = response_data.get('like_avg', 0)
            share_avg = response_data.get('share_avg', 0)
            comment_avg = response_data.get('comment_avg', 0)
            interact_total = int(like_avg) + int(share_avg) + int(comment_avg)
            avg_duration = response_data.get('avg_duration', '')

            # 完播率和互动率
            play_over_rate = response_data.get('play_over_rate', {})
            play_over_rate_value = play_over_rate.get('value', '') if isinstance(play_over_rate, dict) else ''

            interact_rate = response_data.get('interact_rate', {})
            interact_rate_value = interact_rate.get('value', '') if isinstance(interact_rate, dict) else ''

            # 计算CPE和CPC
            price_20_60 = self.kol_api_data.get('videoPrice', 0)
            cpe_value = None
            cpc_value = None

            if price_20_60 and interact_total:
                try:
                    cpe_value = round(float(price_20_60) / float(interact_total), 2)
                except:
                    pass

            if price_20_60 and play_mid:
                try:
                    cpc_value = round(float(price_20_60) / float(play_mid), 2)
                except:
                    pass

            # 根据当前视频类型创建数据对象并添加到yingxiao_api_data数组
            if self.current_video_type == 'xingtu':
                # 星图视频数据 (douyin_business=1)
                xingtu_data = {
                    'platform_user_id': user_id,
                    'douyin_business': 1,
                    'play_median': play_mid,
                    'interaction_volume': interact_total,
                    'avg_duration': avg_duration,
                    'completion_rate': play_over_rate_value,
                    'interaction_rate': interact_rate_value,
                    'douyin_likes': like_avg,
                    'douyin_shares': share_avg,
                    'douyin_comments': comment_avg,
                    'douyin_cpe': cpe_value,
                    'douyin_cpc': cpc_value
                }
                self.yingxiao_api_data.append(xingtu_data)
                self.logger.info(f"✅ 星图视频传播信息已添加到yingxiao_api_data (douyin_business=1)")

            elif self.current_video_type == 'personal':
                # 个人视频数据 (douyin_business=0)
                personal_data = {
                    'platform_user_id': user_id,
                    'douyin_business': 0,
                    'play_median': play_mid,
                    'interaction_volume': interact_total,
                    'avg_duration': avg_duration,
                    'completion_rate': play_over_rate_value,
                    'interaction_rate': interact_rate_value,
                    'douyin_likes': like_avg,
                    'douyin_shares': share_avg,
                    'douyin_comments': comment_avg,
                    'douyin_cpe': cpe_value,
                    'douyin_cpc': cpc_value
                }
                self.yingxiao_api_data.append(personal_data)
                self.logger.info(f"✅ 个人视频传播信息已添加到yingxiao_api_data (douyin_business=0)")
            else:
                self.logger.warning(f"未知的视频类型: {self.current_video_type}，跳过保存")

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
        """处理作者商业种子基础信息API数据 - 参考get_douyin_guakao.py第361-368行"""
        try:
            self.logger.info(f"开始处理商业种子基础信息API数据，用户ID: {user_id}")

            if not response_data:
                self.logger.error("作者商业种子基础信息API响应数据为空")
                return

            # 7. 种草价值
            self.other_api_data['search_after_view_count'] = response_data.get('avg_search_after_view_cnt', '')
            self.other_api_data['search_after_view_rate'] = response_data.get('avg_search_after_view_rate', '')
            self.other_api_data['a3_increase_count'] = response_data.get('avg_a3_incr_cnt', '')
            self.other_api_data['store_entry_cost'] = response_data.get('shop_cost', '')

            self.logger.info(f"✅ 种草价值信息处理完成：A3增长数 {self.other_api_data.get('a3_increase_count', '')}")

        except Exception as e:
            self.logger.error(f"处理商业种子基础信息数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_convert_ability(self, response_data: Dict[str, Any], user_id: str):
        """处理作者转化能力API数据 - 参考get_douyin_guakao.py第370-380行"""
        try:
            self.logger.info(f"开始处理转化能力API数据，用户ID: {user_id}")

            if not response_data:
                self.logger.error("作者转化能力API响应数据为空")
                return

            # 8. 转化价值
            video_vv_median = response_data.get('video_vv_median', {})
            if isinstance(video_vv_median, dict):
                self.other_api_data['business_play_median'] = video_vv_median.get('value', '')

            self.other_api_data['component_click_volume'] = response_data.get('component_click_cnt_range', '')
            self.other_api_data['component_click_rate'] = response_data.get('component_click_rate_range', '')
            self.other_api_data['conversion_cpc'] = response_data.get('related_cpc_range', '')

            self.logger.info(f"✅ 转化价值信息处理完成")

        except Exception as e:
            self.logger.error(f"处理转化能力数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_link_card(self, response_data: Dict[str, Any], user_id: str):
        """处理连接用户分布API数据 - 参考get_douyin_guakao.py第382-391行"""
        try:
            self.logger.info(f"开始处理连接用户分布API数据，用户ID: {user_id}")

            if not response_data or 'link_struct' not in response_data:
                self.logger.warning("连接用户分布API响应数据为空或缺少link_struct字段")
                return
            print(response_data)
            # 9. 连接用户分布
            link_struct = response_data['link_struct']
            if isinstance(link_struct, dict):
                self.other_api_data['aware_user_count'] = link_struct.get('1', {}).get('value', '')
                self.other_api_data['interest_user_cost'] = link_struct.get('2', {}).get('value', '')
                self.other_api_data['like_user_count'] = link_struct.get('3', {}).get('value', '')
                self.other_api_data['connected_user_count'] = link_struct.get('5', {}).get('value', '')

            self.logger.info(f"✅ 连接用户分布处理完成")

        except Exception as e:
            self.logger.error(f"处理连接用户分布数据时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

    def _process_author_fans_distribution(self, response_data: Dict[str, Any], user_id: str):
        """处理粉丝数据分布API数据 - 解析成JSON格式"""
        try:
            self.logger.info(f"开始处理粉丝数据分布API数据，用户ID: {user_id}")

            if not response_data or 'distributions' not in response_data:
                self.logger.warning("粉丝数据分布API响应数据为空或缺少distributions字段")
                return

            distributions = response_data['distributions']

            # 初始化JSON对象
            age_distribution = []  # 年龄分布（数组）
            region_distribution = []  # 地域分布（数组）
            crowd_distribution = []  # 八大人群分布（数组）

            for dist in distributions:
                dist_type = dist.get('type')
                distribution_list = dist.get('distribution_list', [])

                # 性别分布 type=1
                if dist_type == 1:
                    # 确保转换为数字类型
                    total = sum([float(item.get('distribution_value', 0)) for item in distribution_list])
                    for item in distribution_list:
                        key = item.get('distribution_key')
                        value = float(item.get('distribution_value', 0))
                        if key == 'male' and total > 0:
                            self.other_api_data['male_fan_ratio'] = f"{round(value / total * 100, 2)}%"
                        elif key == 'female' and total > 0:
                            self.other_api_data['female_fan_ratio'] = f"{round(value / total * 100, 2)}%"

                # 年龄分布 type=2 - 转成数组格式
                elif dist_type == 2:
                    total = sum([float(item.get('distribution_value', 0)) for item in distribution_list])
                    if total > 0:
                        for item in distribution_list:
                            age_range = item.get('distribution_key', '')
                            value = float(item.get('distribution_value', 0))
                            if age_range:
                                percentage = round(value / total * 100, 2)
                                age_distribution.append({
                                    'age_range': age_range,
                                    'percentage': percentage
                                })

                # 地域分布 type=4 - 转成JSON数组
                elif dist_type == 4:
                    total = sum([float(item.get('distribution_value', 0)) for item in distribution_list])
                    if total > 0:
                        for item in distribution_list:
                            region_name = item.get('distribution_key', '')
                            value = float(item.get('distribution_value', 0))
                            if region_name:
                                percentage = round(value / total * 100, 2)
                                region_distribution.append({
                                    'region': region_name,
                                    'percentage': percentage
                                })
                        # 按占比降序排序
                        region_distribution.sort(key=lambda x: x['percentage'], reverse=True)

                # 八大人群分布 type=1024 - 转成数组格式
                elif dist_type == 1024:
                    total = sum([float(item.get('distribution_value', 0)) for item in distribution_list])
                    if total > 0:
                        for item in distribution_list:
                            crowd_name = item.get('distribution_key', '')
                            value = float(item.get('distribution_value', 0))
                            if crowd_name:
                                percentage = round(value / total * 100, 2)
                                crowd_distribution.append({
                                    'crowd_type': crowd_name,
                                    'percentage': percentage
                                })

            # 将JSON数组转为字符串存储
            if age_distribution:
                self.other_api_data['old_ratio'] = json.dumps(age_distribution, ensure_ascii=False)

            if region_distribution:
                self.other_api_data['region_distribution'] = json.dumps(region_distribution, ensure_ascii=False)

            if crowd_distribution:
                self.other_api_data['below_ratio'] = json.dumps(crowd_distribution, ensure_ascii=False)

        except Exception as e:
            self.logger.error(f"处理粉丝数据分布时出错: {str(e)}")
            self.logger.error(f"错误详情: {traceback.format_exc()}")

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
                            self.send_wechat_notification(f"🔒 抖音刊例数据抓取检测到验证码！\n请尽快手动完成验证，程序已暂停等待...")
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
                '/api/author/get_author_base_info', #详细信息
                '/api/data_sp/check_author_display', #粉丝赞藏数
                '/api/author/get_author_marketing_info', #报价
                '/api/author/get_author_platform_channel_info_v2', #报价
                '/api/aggregator/get_author_commerce_spread_info',  # 预估CPE/CPM
                '/api/data_sp/get_author_spread_info',  # 传播价值
                '/api/aggregator/get_author_commerce_seed_base_info',  # 种草价值
                '/api/data_sp/get_author_convert_ability',  # 转化价值
                '/api/data_sp/author_link_card',  # 连接用户分布
                '/api/data_sp/get_author_fans_distribution',  # 粉丝数据
            ]

            # 检查是否是目标API
            is_target_api = any(api in url for api in target_apis)
            if not is_target_api:
                return

            # 只处理XHR或fetch请求
            if response.request.resource_type not in ['xhr', 'fetch']:
                if '/api/data_sp/get_author_spread_info' in url:
                    self.logger.warning(f"❌ spread_info API被过滤：资源类型 = {response.request.resource_type}")
                return

            try:
                # 检查页面状态
                if self.page.is_closed():
                    if '/api/data_sp/get_author_spread_info' in url:
                        self.logger.warning(f"❌ spread_info API被过滤：页面已关闭")
                    return

                # 检查响应状态
                if response.status != 200:
                    self.logger.warning(f"API响应状态异常: {response.status}, URL: {url}")
                    return

                # 解析响应数据
                response_data = response.json()

                # 检查数据有效性
                if not response_data or not isinstance(response_data, dict):
                    self.logger.warning(f"API响应数据格式不正确: {url}")
                    return

                # 检查API响应状态
                if self._check_api_response_status(response_data, url):
                    if '/api/data_sp/get_author_spread_info' in url:
                        self.logger.warning(f"❌ spread_info API被过滤：响应状态异常")
                    return  # 如果状态异常，直接返回

                # 确定匹配的API类型
                matched_api = None
                for api in target_apis:
                    if api in url:
                        matched_api = api
                        break

                # 存储API数据（用于首页加载时的批处理）
                self.api_data[url] = {
                    'url': url,
                    'data': response_data,
                    'api_type': matched_api,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': response.status
                }

                # 验证当前是否有正在处理的用户
                if not self.current_kol or not self.current_kol.get('user_id'):
                    return

            except Exception as e:
                if '/api/data_sp/get_author_spread_info' in url:
                    self.logger.error(f"❌ 处理 spread_info API时出错: {str(e)}")
                self.logger.error(f"处理API数据时出错: {str(e)}, URL: {url}")
                self.logger.error(f"错误详情: {traceback.format_exc()}")

        except Exception as e:
            self.logger.error(f"处理API响应时出错: {str(e)}")
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