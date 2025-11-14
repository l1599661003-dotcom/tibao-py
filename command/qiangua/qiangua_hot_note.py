import time
import json
import os
from datetime import datetime
import signal
import sys

import schedule
from loguru import logger
from playwright.sync_api import sync_playwright
import random  # 添加到文件顶部的导入部分

from core.database_text_fangpian import session
from models.models_tibao import SpiderQianguaHotNote

"""
    获取千瓜实时-热点榜，低粉-爆文榜，-商业笔记榜数据
"""


class QianguaSpider:
    def __init__(self):
        self.setup_logger()
        self.base_url = "https://app.qian-gua.com"
        self.rank_url = "https://app.qian-gua.com/#/material/rank"
        self.is_logged_in = False
        self.api_data = {}
        self.cookie_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookies.json')
        self.setup_browser()

    """设置日志"""

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.add(os.path.join(log_path, "qiangua_{time}.log"), rotation="1 day", retention="7 days")

    """初始化浏览器"""

    def setup_browser(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.context = self.browser.new_context(
            viewport={'width': 1512, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )
        self.page = self.context.new_page()
        self.page.set_default_timeout(20000)
        self.page.on("response", self._handle_api_response)

    """关闭所有弹出框"""

    def close_popups(self):
        try:
            # 使用JavaScript关闭所有可能的弹出框
            self.page.evaluate('''
                (() => {
                    const closeButtons = document.querySelectorAll('.el-icon-close, .lei-icon-close, [class*="close-btn"]');
                    closeButtons.forEach(btn => {
                        if (btn.offsetParent !== null) {
                            btn.click();
                        }
                    });
                })()
            ''')
            confirm_button = self.page.locator('button:has-text("确认")')

            if confirm_button.is_visible(timeout=1000):  # 最多等1秒
                confirm_button.click()
            time.sleep(1)
        except Exception as e:
            logger.error(f"关闭弹出框时出错: {str(e)}")

    """检查登录状态"""

    def check_login_status(self):
        try:
            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            # 打印页面内容用于调试
            html_content = self.page.content()
            if 'avatar' in html_content or 'user-container' in html_content:
                self.is_logged_in = True
                logger.info("通过页面内容检测到用户相关元素，已登录")
                return True

            self.is_logged_in = False
            logger.info("未检测到用户头像或登录状态")
            return False

        except Exception as e:
            self.is_logged_in = False
            logger.error(f"检查登录状态失败: {str(e)}")
            return False

    def login(self):
        """执行登录操作"""
        try:
            logger.info("开始登录...")
            self.page.click("text=登录/注册")
            time.sleep(3)

            self.page.click("text=手机登录")
            time.sleep(3)

            # 输入账号密码
            self.page.fill("input[placeholder='请输入手机号']", '13151572333')
            time.sleep(2)
            self.page.fill("input[placeholder='请输入登录密码']", '12345678abc')
            time.sleep(2)

            # 勾选协议
            self.page.click('.el-checkbox__inner')
            time.sleep(1)

            # 点击登录按钮
            self.page.click('button[class="el-button el-button--primary"][style="width: 200px;"]')

            # 增加登录后的等待时间，确保页面完全加载
            time.sleep(5)

            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            time.sleep(5)

            # 多次检查登录状态
            for _ in range(3):
                if self.check_login_status():
                    logger.info("登录成功")
                    return True
                time.sleep(2)

            logger.error("登录失败，多次检查均未检测到用户头像")
            return False
        except Exception as e:
            logger.error(f"登录过程出错: {str(e)}")
            return False

    def save_data_to_db(self, note_data, category_name, rank_type):
        """保存数据到数据库，如果记录已存在则更新"""
        try:
            # 获取当天0点的时间戳
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            hot_date = int(today.timestamp())
            current_time = int(time.time())

            # 提取图片ID
            cover_image = note_data.get('CoverImage', '')
            image_id = ''
            if cover_image:
                parts = cover_image.split('/')[-1]
                image_id = parts.split('-')[0]

            blogger_image = note_data.get('BloggerSmallAvatar', '')
            kol_image_id = ''
            if blogger_image:
                parts = blogger_image.split('/')[-1]
                kol_image_id = parts.split('-')[0]

            # 处理标签
            note_keywords = note_data.get('NoteKeyWords', [])
            if note_keywords is None:
                note_keywords = []
            elif isinstance(note_keywords, str):
                note_keywords = [note_keywords]

            # 构建数据字典
            note_data_dict = {
                'kol_name': note_data.get('BloggerNickName'),
                'note_title': note_data.get('Title'),
                'kol_img': blogger_image,
                'kol_image_id': kol_image_id,
                'kol_type': note_data.get('BloggerProp'),
                'note_like': note_data.get('LikedCount', 0),
                'note_collect': note_data.get('CollectedCount', 0),
                'note_issue_time': note_data.get('PublishTime'),
                'note_comment': note_data.get('CommentsCount', 0),
                'note_share': note_data.get('ShareCount', 0),
                'note_read': note_data.get('ViewCount', 0),
                'note_interact': note_data.get('Lcc', 0),
                'note_classify': note_data.get('Tag'),
                'note_image_id': image_id,
                'note_tags': ','.join(str(tag) for tag in note_keywords),
                'note_tag_classify': category_name,
                'hot_note_24h': note_data.get('UpdateTime'),
                'note_type': 0 if rank_type == 'hot' else (1 if rank_type == 'low_fans' else 2),
                'hot_date': hot_date,
                'update_time': current_time
            }

            # 查找是否存在记录
            existing_record = session.query(SpiderQianguaHotNote).filter(
                SpiderQianguaHotNote.note_image_id == image_id
            ).first()

            if existing_record:
                # 更新现有记录
                for key, value in note_data_dict.items():
                    if key != 'create_time':  # 不更新创建时间
                        setattr(existing_record, key, value)
                return True
            else:
                # 创建新记录
                note_data_dict['create_time'] = current_time  # 只在创建时设置create_time
                note_record = SpiderQianguaHotNote(**note_data_dict)
                session.add(note_record)
                return True

        except Exception as e:
            logger.error(f"保存数据时出错: {str(e)}")
            session.rollback()
            return False

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if response.request.resource_type in ['fetch', 'xhr'] and 'GetBusinessNoteRankList' in url:
                # 提取时间戳作为唯一标识
                try:
                    timestamp = url.split('_=')[1].split('&')[0]  # 确保提取正确的时间戳部分
                except:
                    # 如果无法从URL中提取时间戳，使用当前时间的毫秒级时间戳
                    timestamp = str(int(time.time() * 1000))

                if response.status == 200:
                    try:
                        response_data = response.json()
                        if 'Data' in response_data and 'ItemList' in response_data['Data']:
                            items_count = len(response_data['Data']['ItemList'])
                            logger.info(
                                f"收到API响应: timestamp={timestamp}, URL={url[:100]}..., 数据条数={items_count}")

                            self.api_data[timestamp] = {
                                'url': url,
                                'data': response_data,
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'processed': False,
                                'items_count': items_count
                            }
                            logger.info(f"成功获取API数据: timestamp={timestamp}, 数据条数: {items_count}")
                        else:
                            logger.warning(f"API响应数据结构不符合预期: {str(response_data)[:200]}")
                    except Exception as e:
                        logger.error(f"解析API响应数据时出错: {str(e)}")
                else:
                    logger.warning(f"API请求状态码异常: {response.status}")
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def wait_for_api_response(self, timeout=10, check_processed=False):
        """等待API响应数据"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.api_data:
                if check_processed:
                    # 检查是否有未处理的API数据
                    for timestamp, data in self.api_data.items():
                        if not data.get('processed', True):  # 如果数据未处理
                            logger.info(f"找到未处理的API数据: timestamp={timestamp}")
                            return timestamp
                else:
                    # 只要有数据就返回最新的timestamp
                    latest_timestamp = max(self.api_data.keys())
                    logger.info(f"找到API数据: timestamp={latest_timestamp}")
                    return latest_timestamp
            time.sleep(0.5)
        return None

    def process_category_data(self, category, rank_type):
        """处理分类数据"""
        try:
            # 记录点击前的API数据时间戳
            last_timestamp = max(self.api_data.keys()) if self.api_data else None

            # 使用JavaScript点击分类标签
            clicked = self.page.evaluate(f'''
                () => {{
                    let clicked = false;
                    const elements = Array.from(document.querySelectorAll('span'));
                    for (const element of elements) {{
                        if (element.textContent.trim() === '{category}') {{
                            const rect = element.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {{
                                element.click();
                                clicked = true;
                                break;
                            }}
                        }}
                    }}
                    return clicked;
                }}
            ''')

            if not clicked:
                logger.error(f"未找到{category}分类按钮")
                return False

            # 等待页面加载和网络请求完成
            self.page.wait_for_load_state('networkidle', timeout=5000)
            time.sleep(2)  # 给予一些额外的等待时间

            # 等待新的API响应
            wait_start = time.time()
            max_wait_time = 15  # 最长等待15秒

            while time.time() - wait_start < max_wait_time:
                # 检查是否有新的API响应
                current_timestamp = max(self.api_data.keys()) if self.api_data else None

                if current_timestamp and (not last_timestamp or current_timestamp > last_timestamp):
                    data = self.api_data[current_timestamp]
                    if not data.get('processed', False):
                        try:
                            api_data = data['data']
                            if 'Data' in api_data and 'ItemList' in api_data['Data']:
                                notes = api_data['Data']['ItemList']
                                if notes:
                                    new_records = 0
                                    logger.info(f"处理{category}分类数据, 共{len(notes)}条")

                                    # 保存数据到数据库
                                    for note in notes:
                                        if self.save_data_to_db(note, category, rank_type):
                                            new_records += 1

                                    session.commit()
                                    logger.info(f"{category}分类新增{new_records}条数据")
                                    data['processed'] = True
                                    return True
                        except Exception as e:
                            logger.error(f"处理{category}分类数据时出错: {str(e)}")
                            session.rollback()

                # 如果还没有收到新的API响应，检查页面状态
                try:
                    # 检查页面是否仍在加载
                    loading_indicator = self.page.locator('.loading-indicator').is_visible()
                    if loading_indicator:
                        logger.info(f"页面正在加载中，继续等待...")
                        time.sleep(1)
                        continue

                    # 检查是否有错误提示
                    error_message = self.page.locator('.error-message').is_visible()
                    if error_message:
                        logger.warning(f"页面显示错误信息，准备重试...")
                        break
                except:
                    pass

                time.sleep(0.5)

            # 如果等待超时，记录日志但不立即重试
            logger.warning(f"{category}分类数据获取超时，等待时间：{time.time() - wait_start}秒")
            return False

        except Exception as e:
            logger.error(f"处理{category}分类时出错: {str(e)}")
            session.rollback()
            return False

    def scrape_rank_data(self):
        """抓取排行榜数据"""
        try:
            # 访问排行榜页面
            logger.info("开始访问排行榜页面...")
            self.api_data = {}  # 初始化为字典
            self.page.goto(self.rank_url)

            # 等待页面加载完成
            self.page.wait_for_load_state('networkidle', timeout=10000)
            time.sleep(3)

            # 关闭可能的弹出框
            self.close_popups()

            # 定义榜单和分类
            rank_tabs = [
                {"name": "实时-热门笔记榜", "type": "hot"},
                {"name": "实时-低粉爆文榜", "type": "low_fans"},
                {"name": "商业笔记榜", "type": "business"}
            ]
            categories = ["彩妆", "护肤", "洗护香氛", "母婴育儿", "家居家装", "科技数码", "情感两性", "星座情感",
                          "生活经验"]

            # 清空API数据，准备获取新数据
            self.api_data.clear()
            logger.info("准备获取初始数据")

            # 切换到48小时视图
            try:
                logger.info("准备切换到48小时视图...")
                self.page.click("text=近48小时")
                logger.info("已切换到48小时视图")

                # 等待48小时数据加载
                self.page.wait_for_load_state('networkidle', timeout=10000)
                time.sleep(3)  # 增加等待时间确保数据加载完成
            except Exception as e:
                logger.error(f"切换到48小时视图时出错: {str(e)}")
                return

            # 点击展开更多分类
            try:
                logger.info("点击展开更多分类...")
                self.page.click("text=更多")
                time.sleep(2)
            except Exception as e:
                logger.error(f"点击更多分类按钮时出错: {str(e)}")

            # 处理每个榜单
            for rank_index, rank in enumerate(rank_tabs):
                logger.info(f"开始处理{rank['name']}...")

                # 如果不是第一个榜单，需要点击切换
                if rank_index > 0:
                    try:
                        logger.info(f"切换到{rank['name']}...")
                        # 清空API数据，准备获取新榜单的数据
                        self.api_data.clear()

                        self.page.click(f"text={rank['name']}")
                        time.sleep(3)
                        self.page.wait_for_load_state('networkidle', timeout=10000)
                    except Exception as e:
                        logger.error(f"切换到{rank['name']}失败: {str(e)}")
                        continue

                # 每个榜单首先处理"全部"分类的数据
                logger.info(f"等待{rank['name']}的全部分类数据...")
                wait_start = time.time()
                while time.time() - wait_start < 15:  # 等待15秒
                    if self.api_data:
                        latest_timestamp = max(self.api_data.keys())
                        data = self.api_data[latest_timestamp]
                        if not data.get('processed', False):
                            logger.info(f"获取到{rank['name']}全部分类数据，timestamp={latest_timestamp}")
                            if self.process_all_category_data(latest_timestamp, rank['type']):
                                logger.info(f"{rank['name']}全部分类数据处理完成")
                                data['processed'] = True
                                break
                    time.sleep(0.5)

                # 处理其他分类
                for category in categories:
                    logger.info(f"处理{rank['name']}-{category}分类...")

                    # 清空之前的API数据
                    self.api_data.clear()

                    # 添加随机延迟，避免请求过于频繁
                    time.sleep(random.uniform(3, 5))

                    # 最多尝试3次
                    max_retries = 3
                    for retry in range(max_retries):
                        if retry > 0:
                            logger.info(f"第{retry + 1}次尝试处理{category}分类...")
                            time.sleep(random.uniform(2, 4))

                        if self.process_category_data(category, rank['type']):
                            logger.info(f"{rank['name']}-{category}分类数据处理完成")
                            break
                        elif retry == max_retries - 1:
                            logger.warning(f"{rank['name']}-{category}分类数据处理失败")

        except Exception as e:
            logger.error(f"抓取排行榜数据时出错: {str(e)}")
            session.rollback()
        finally:
            try:
                session.commit()
            except:
                session.rollback()
            finally:
                session.close()

    def process_all_category_data(self, timestamp, rank_type):
        """处理全部分类数据"""
        try:
            if timestamp not in self.api_data:
                logger.error(f"未找到timestamp={timestamp}的数据")
                return False

            data = self.api_data[timestamp]['data']
            if 'Data' not in data or 'ItemList' not in data['Data']:
                logger.error("API数据结构不正确")
                return False

            notes = data['Data']['ItemList']
            if not notes:
                logger.warning("没有找到笔记数据")
                return False

            new_records = 0
            logger.info(f"开始处理全部分类数据, 共{len(notes)}条")

            # 保存数据到数据库
            for note in notes:
                try:
                    if self.save_data_to_db(note, "全部", rank_type):
                        new_records += 1
                except Exception as e:
                    logger.error(f"保存单条数据时出错: {str(e)}")
                    continue

            try:
                session.commit()
                logger.info(f"全部分类成功保存{new_records}条数据")
                self.api_data[timestamp]['processed'] = True
                return True
            except Exception as e:
                logger.error(f"提交数据库事务时出错: {str(e)}")
                session.rollback()
                return False

        except Exception as e:
            logger.error(f"处理全部分类数据时出错: {str(e)}")
            session.rollback()
            return False

    def save_cookies(self):
        """保存cookies到文件"""
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w') as f:
                json.dump(cookies, f)
            logger.info("Cookies已保存到文件")
        except Exception as e:
            logger.error(f"保存cookies时出错: {str(e)}")

    def load_cookies(self):
        """从文件加载cookies"""
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r') as f:
                    cookies = json.load(f)
                self.context.add_cookies(cookies)
                logger.info("已从文件加载cookies")
                return True
            return False
        except Exception as e:
            logger.error(f"加载cookies时出错: {str(e)}")
            return False

    def check_and_handle_login(self):
        """检查并处理登录状态"""
        try:
            # 首先访问首页
            self.page.goto(self.base_url)
            time.sleep(2)

            # 关闭弹出框
            self.close_popups()

            # 尝试加载已保存的cookies
            if self.load_cookies():
                # 重新访问首页使cookies生效
                self.page.goto(self.base_url)
                time.sleep(2)

                # 再次关闭可能的弹出框
                self.close_popups()

                # 检查登录状态
                if self.check_login_status():
                    logger.info("使用已保存的cookies登录成功")
                    return True
                else:
                    logger.info("已保存的cookies已过期，需要重新登录")
                    # 清除旧的cookies文件
                    if os.path.exists(self.cookie_file):
                        os.remove(self.cookie_file)
                        logger.info("已删除过期的cookies文件")

            # 如果没有cookies或cookies已过期，执行登录操作
            if self.login():
                # 登录成功后保存cookies
                self.save_cookies()
                return True

            return False
        except Exception as e:
            logger.error(f"检查并处理登录状态时出错: {str(e)}")
            return False

    def run(self):
        """运行爬虫"""
        try:
            logger.info("开始运行爬虫...")

            # 检查并处理登录
            if not self.check_and_handle_login():
                logger.error("登录失败，程序退出")
                return

            # 抓取数据
            self.scrape_rank_data()

        except Exception as e:
            logger.error(f"运行过程出错: {str(e)}")
            try:
                session.commit()
            except:
                session.rollback()
        finally:
            try:
                session.commit()
            except:
                session.rollback()
            finally:
                session.close()
                self.close()

    def close(self):
        """关闭资源"""
        try:
            # 移除事件监听器
            if hasattr(self, 'page') and self.page:
                self.page.remove_listener("response", self._handle_api_response)
                self.page.close()

            if hasattr(self, 'context') and self.context:
                self.context.close()

            if hasattr(self, 'browser') and self.browser:
                self.browser.close()

            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()

            logger.info("所有资源已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")


def scheduled_job():
    """定时任务，只在执行时才创建爬虫实例"""
    spider = None
    try:
        spider = QianguaSpider()
        spider.run()
    except Exception as e:
        logger.error(f"执行爬虫任务时出错: {str(e)}")
    finally:
        if spider:
            try:
                spider.close()
                logger.info("爬虫资源已清理")
            except Exception as e:
                logger.error(f"清理爬虫资源时出错: {str(e)}")
        try:
            session.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {str(e)}")


if __name__ == '__main__':
    spider = QianguaSpider()
    spider.run()
    # 设置定时任务，注意这里传递的是函数引用而不是执行结果
    schedule.every().day.at('06:00').do(scheduled_job)

    logger.info("调度器已启动，将在每天06:00运行爬虫任务")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        logger.info("收到退出信号，程序退出")
    except Exception as e:
        logger.error(f"调度器运行出错: {str(e)}")