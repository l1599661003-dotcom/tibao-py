import json
import os
import cv2
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import requests
from datetime import datetime
from sqlalchemy import create_engine, text, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
"""
    获取小红书的评论
"""
# 数据库连接配置
DATABASE_URL = 'mysql+pymysql://fpdev:fpdev@47.104.13.93:3306/qiangua'

# 创建数据库引擎
engine = create_engine(DATABASE_URL, isolation_level="READ UNCOMMITTED")

# 创建会话工厂
Session = sessionmaker(bind=engine)
ScopedSession = scoped_session(Session)

# 创建会话
session = Session()

# 创建基类 - 使用新的导入方式
Base = declarative_base()

# 定义小红书评论模型
class XiaohongshuComment(Base):
    __tablename__ = 'xiaohongshu_comment'

    id = Column(Integer, primary_key=True)
    comment_content = Column(Text)
    comment_id = Column(String(255))
    comment_time = Column(String(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class XiaoHongShuSpider:
    def __init__(self):
        self.setup_logger()
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.base_url = "https://www.xiaohongshu.com"
        self.is_logged_in = False
        self.api_data = {}  # 存储API数据
        self.cookie_file = os.path.join(self.data_dir, 'cookies.json')
        self.progress_file = os.path.join(self.data_dir, 'scraping_progress.json')

        # 初始化headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Cookie': '',  # 将在登录后更新
            'Origin': 'https://www.xiaohongshu.com',
            'Referer': 'https://www.xiaohongshu.com/',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Content-Type': 'application/json;charset=UTF-8',
            'X-t': ''  # 将在每次请求时更新
        }

        self.setup_browser()
        self.notes = []
        self.stopScroll = False
        self.monitor_data = {
            'fail_count': 0,
            'total_count': 0,
            'completed_count': 0,
            'process': 0
        }
        self.comments_data = []  # Store comments data

    def setup_logger(self):
        log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        logger.add(os.path.join(log_path, "qiangua_{time}.log"), rotation="1 day", retention="7 days")

    def setup_browser(self):
        """初始化浏览器"""
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
            viewport={'width': 1924, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        )

        # 尝试加载已保存的Cookie
        if self._load_cookies():
            # 验证Cookie是否有效
            self.page = self.context.new_page()
            self.page.goto(self.base_url)
            time.sleep(2)
            try:
                # 检查是否存在用户头像元素
                userSide = self.page.locator(".user.side-bar-component").all()
                logger.info(f"user: {len(userSide)}")
                user_len = len(userSide)
                if user_len > 1 or self.page.locator(".user.side-bar-component").is_visible(timeout=50000):
                    self.is_logged_in = True
                    logger.info("Cookie有效，已自动登录")
                else:
                    logger.info("Cookie已失效，需要重新登录")
            except:
                logger.info("Cookie已失效，需要重新登录")
        else:
            self.page = self.context.new_page()

        # 设置页面超时时间
        self.page.set_default_timeout(20000)
        # 设置响应监听
        self.page.on("response", self._handle_api_response)

    def login(self):
        """
        使用扫码方式登录小红书
        :return: 是否登录成功
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

            logger.info("开始登录流程...")
            # 访问首页
            self.page.goto(self.base_url)
            time.sleep(2)

            # 等待并点击登录按钮
            logger.info("等待登录按钮出现...")
            login_button = self.page.wait_for_selector("text=登录", timeout=10000)
            if not login_button:
                logger.error("未找到登录按钮")
                return False
            login_button.click()
            time.sleep(2)

            # 等待登录成功
            logger.info("等待扫码登录...")
            try:
                # 等待个人头像出现，表示登录成功
                avatar = self.page.wait_for_selector(".channel-list >> .user", timeout=60000)  # 给用户60秒扫码时间
                if avatar:
                    logger.info("登录成功！")
                    self.is_logged_in = True

                    # 登录成功后保存Cookie并更新headers
                    self._save_cookies()
                    self.headers['Cookie'] = '; '.join([
                        f"{cookie['name']}={cookie['value']}"
                        for cookie in self.context.cookies()
                    ])

                    return True
                else:
                    logger.error("登录失败，未检测到登录成功状态")
                    return False
            except PlaywrightTimeoutError:
                logger.error("登录超时，请重试")
                return False

        except Exception as e:
            logger.error(f"登录过程出现异常: {str(e)}")
            return False

    def scroll_for_more_videos(self, max_scrolls=10):
        """Scroll the page to load more videos"""
        try:
            previous_height = 0
            scroll_count = 0
            no_change_count = 0

            # 等待视频容器加载
            self.page.wait_for_selector('.note-item', timeout=10000)

            while scroll_count < max_scrolls and no_change_count < 3:
                # 获取当前页面高度
                current_height = self.page.evaluate('document.documentElement.scrollHeight')

                # 获取当前视频数量（使用新的选择器）
                video_count = len(self.page.query_selector_all('.note-item'))

                if current_height == previous_height:
                    no_change_count += 1
                else:
                    no_change_count = 0

                # 滚动到底部
                self.page.evaluate('window.scrollTo(0, document.documentElement.scrollHeight)')
                time.sleep(2)   # 等待内容加载

                previous_height = current_height
                scroll_count += 1


        except Exception as e:
            logger.error(f"Error while scrolling: {str(e)}")

    def scrape_user_notes(self):
        """抓取指定用户的笔记信息并获取评论"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法抓取数据")
                return None

            # 添加统计变量
            total_comments_processed = 0
            total_notes_processed = 0
            
            logger.info('开始抓取 小红书数据')
            self.page.goto("https://www.xiaohongshu.com/user/profile/67cae2ca000000000d00a9de")
            time.sleep(2)

            # Scroll to load more videos
            self.scroll_for_more_videos()

            # 等待视频元素加载
            self.page.wait_for_selector('.note-item', timeout=10000)

            # 获取所有视频元素并倒序处理
            video_elements = list(reversed(self.page.query_selector_all('.note-item')))  # 转换为列表并倒序
            total_videos = len(video_elements)
            logger.info(f'找到 {total_videos} 个视频，将从最后一个开始处理')

            # 遍历倒序后的视频元素
            for idx, video in enumerate(video_elements):
                try:
                    logger.info(f'正在处理第 {total_videos - idx}/{total_videos} 个视频（倒序）')
                    
                    # 点击视频打开详情页
                    video.click()
                    time.sleep(2)

                    # 从URL中获取视频ID
                    current_url = self.page.url
                    logger.info(f"当前URL: {current_url}")

                    # 从URL中提取note_id
                    note_id = None
                    if '/explore/' in current_url:
                        note_id = current_url.split('/explore/')[1].split('?')[0]

                    if not note_id:
                        logger.error(f"无法从URL提取note_id: {current_url}")
                        continue

                    logger.info(f"找到note_id: {note_id}")

                    # 等待评论区加载
                    try:
                        # 等待笔记详情弹出框加载
                        self.page.wait_for_selector('.note-content', timeout=10000)
                        
                        # 等待评论区容器加载
                        self.page.wait_for_selector('.note-scroller', timeout=10000)
                        logger.info("评论区容器已加载")

                        # 获取总评论数
                        total_comments = self.page.evaluate('''() => {
                            const commentCountEl = document.querySelector('.comment-count');
                            return commentCountEl ? parseInt(commentCountEl.textContent) : 0;
                        }''')
                        logger.info(f"该笔记总评论数: {total_comments}")

                        # 初始化计数器
                        last_comments_count = 0
                        no_new_comments_count = 0
                        scroll_attempts = 0
                        max_scroll_attempts = 50  # 增加最大滚动次数
                        scroll_step = 3000  # 滚动步长

                        while scroll_attempts < max_scroll_attempts:
                            try:
                                # 获取当前已加载的评论数
                                current_comments = self.page.evaluate('''() => {
                                    return document.querySelectorAll('.comment-item').length;
                                }''')
                                
                                # 执行滚动操作
                                scroll_result = self.page.evaluate('''() => {
                                    const container = document.querySelector('.note-scroller');
                                    if (!container) return { success: false, message: 'Container not found' };
                                    
                                    const previousScrollTop = container.scrollTop;
                                    container.scrollTop += 3000;  // 滚动步长
                                    
                                    return {
                                        success: true,
                                        scrollTop: container.scrollTop,
                                        scrollHeight: container.scrollHeight,
                                        clientHeight: container.clientHeight,
                                        previousScrollTop: previousScrollTop,
                                        currentComments: document.querySelectorAll('.comment-item').length
                                    };
                                }''')

                                if not scroll_result.get('success'):
                                    logger.error(f"滚动失败: {scroll_result.get('message')}")
                                    break

                                # 增加滚动后的等待时间
                                time.sleep(3)  # 固定等待3秒

                                # 检查是否有新评论加载
                                if current_comments == last_comments_count:
                                    no_new_comments_count += 1
                                else:
                                    no_new_comments_count = 0
                                    logger.info(f"已加载评论数: {current_comments}/{total_comments}")

                                # 如果连续5次没有新评论，且已经到达底部，则退出
                                if no_new_comments_count >= 5:
                                    is_at_bottom = self.page.evaluate('''() => {
                                        const container = document.querySelector('.note-scroller');
                                        if (!container) return true;
                                        return Math.abs(container.scrollHeight - container.scrollTop - container.clientHeight) < 50;
                                    }''')
                                    if is_at_bottom:
                                        logger.info(f"已到达评论区底部，共加载 {current_comments}/{total_comments} 条评论")
                                        break

                                last_comments_count = current_comments
                                scroll_attempts += 1

                            except Exception as e:
                                logger.error(f"滚动评论区时出错: {str(e)}")
                                break

                        # 处理评论数据
                        self.process_comments(note_id)
                        
                        # 更新统计数据
                        total_comments_processed += last_comments_count
                        total_notes_processed += 1

                    except Exception as e:
                        logger.error(f"处理评论区时出错: {str(e)}")

                    # 关闭视频详情页
                    try:
                        self.page.keyboard.press('Escape')
                        time.sleep(2)
                    except Exception as e:
                        logger.error(f"关闭视频详情页时出错: {str(e)}")

                except Exception as e:
                    logger.error(f"处理第 {total_videos - idx} 个视频时出错: {str(e)}")
                    continue

            # 输出最终统计信息
            logger.info("=== 爬取统计信息 ===")
            logger.info(f"总处理笔记数: {total_notes_processed}/{total_videos}")
            logger.info(f"总处理评论数: {total_comments_processed}")
            logger.info(f"平均每个笔记评论数: {total_comments_processed/total_notes_processed if total_notes_processed > 0 else 0:.2f}")
            logger.info("==================")

            return True

        except Exception as e:
            logger.error(f"抓取用户笔记时出错: {str(e)}")
            return None

    def process_comments(self, note_id):
        """处理评论数据并保存到数据库"""
        try:
            def check_comment_exists(comment_id, reply_id=None):
                """检查评论或回复是否已存在"""
                try:
                    if reply_id:
                        # 检查回复是否存在
                        exists = session.query(XiaohongshuComment).filter(
                            XiaohongshuComment.comment_id == comment_id,
                            XiaohongshuComment.reply_id == reply_id
                        ).first() is not None
                    else:
                        # 检查主评论是否存在
                        exists = session.query(XiaohongshuComment).filter(
                            XiaohongshuComment.comment_id == comment_id,
                            XiaohongshuComment.reply_id.is_(None)
                        ).first() is not None
                    return exists
                except Exception as e:
                    logger.error(f"检查评论是否存在时出错: {str(e)}")
                    return False

            def save_comment_to_db(comment_data, is_reply=False, parent_comment=None):
                try:
                    # 检查评论是否已存在
                    if is_reply:
                        if check_comment_exists(parent_comment.get('id'), comment_data.get('id')):
                            logger.info(f"回复已存在，跳过保存: {comment_data.get('id')}")
                            return True
                    else:
                        if check_comment_exists(comment_data.get('id')):
                            logger.info(f"评论已存在，跳过保存: {comment_data.get('id')}")
                            return True

                    comment = XiaohongshuComment(
                        comment_content=comment_data.get('content', ''),
                        comment_id=comment_data.get('id', ''),
                        comment_time=datetime.fromtimestamp(comment_data.get('create_time', 0)/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )

                    if is_reply and parent_comment:
                        comment.reply_content = comment_data.get('content', '')
                        comment.reply_id = comment_data.get('id', '')
                        comment.reply_time = datetime.fromtimestamp(comment_data.get('create_time', 0)/1000).strftime('%Y-%m-%d %H:%M:%S')
                        comment.comment_content = parent_comment.get('content', '')
                        comment.comment_id = parent_comment.get('id', '')
                        comment.comment_time = datetime.fromtimestamp(parent_comment.get('create_time', 0)/1000).strftime('%Y-%m-%d %H:%M:%S')

                    session.add(comment)
                    session.commit()
                    logger.info(f"{'回复' if is_reply else '评论'}保存成功: {comment_data.get('id')}")
                    return True
                except Exception as e:
                    session.rollback()
                    logger.error(f"保存评论数据时出错: {str(e)}")
                    return False

            def handle_api_response(response):
                try:
                    if 'api/sns/web/v2/comment/page' in response.url:
                        data = response.json()
                        if data.get('success'):
                            comments = data['data'].get('comments', [])
                            saved_count = 0
                            for comment in comments:
                                # 保存主评论
                                if save_comment_to_db(comment):
                                    saved_count += 1
                                
                                # 处理子评论
                                if comment.get('sub_comments'):
                                    for reply in comment['sub_comments']:
                                        if save_comment_to_db(reply, is_reply=True, parent_comment=comment):
                                            saved_count += 1
                                
                                # 每处理完一条评论及其回复后暂停一下
                                time.sleep(0.5)
                            
                            logger.info(f"本次成功保存 {saved_count} 条评论及回复")
                        else:
                            logger.error("API返回失败")
                except Exception as e:
                    logger.error(f"处理API响应时出错: {str(e)}")

            # 添加响应监听器
            self.page.on('response', handle_api_response)

            # 等待评论加载完成
            time.sleep(5)

            # 移除响应监听器
            self.page.remove_listener('response', handle_api_response)

        except Exception as e:
            logger.error(f"处理评论数据时出错: {str(e)}")

    def save_comments_data(self):
        """Save the collected comments data to a JSON file"""
        try:
            output_file = os.path.join(self.data_dir, 'comments_data.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.comments_data, f, ensure_ascii=False, indent=2)
            logger.info(f'Comments data saved to {output_file}')
        except Exception as e:
            logger.error(f"Error saving comments data: {str(e)}")

    def update_monitor_status(self, **kwargs):
        """更新监控状态"""
        logger.info(kwargs)
        if kwargs.get('completed_count'):
            self.monitor_data['completed_count'] = kwargs.get('completed_count')
        if kwargs.get('fail_count'):
            self.monitor_data['fail_count'] = kwargs.get('fail_count')

    def close(self):
        """
        关闭浏览器和playwright
        """
        try:
            # 保存Cookie
            if self.is_logged_in:
                self._save_cookies()

            # 关闭所有OpenCV窗口
            cv2.destroyAllWindows()

            if hasattr(self, 'page'):
                self.page.close()
            if hasattr(self, 'context'):
                self.context.close()
            if hasattr(self, 'browser'):
                self.browser.close()
            if hasattr(self, 'playwright'):
                self.playwright.stop()

            # 确保关闭数据库会话
            session.close()

            logger.info("浏览器和playwright已关闭")
        except Exception as e:
            logger.error(f"关闭资源时出错: {str(e)}")

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if 'api/sns/web/v2/comment/page' in url:
                try:
                    data = response.json()
                    if data.get('success'):
                        comments = data['data'].get('comments', [])
                        logger.info(f"获取到 {len(comments)} 条评论")
                        
                        # 收集所有评论ID
                        comment_ids = [str(comment.get('id', ''))[:32] for comment in comments]
                        
                        # 先查询数据库中已存在的评论ID
                        existing_comments = session.query(XiaohongshuComment.comment_id).filter(
                            XiaohongshuComment.comment_id.in_(comment_ids)
                        ).all()
                        existing_ids = {item[0] for item in existing_comments}
                        
                        # 过滤出需要新增的评论
                        new_comments = [
                            comment for comment in comments 
                            if str(comment.get('id', ''))[:32] not in existing_ids
                        ]
                        
                        if new_comments:
                            try:
                                # 批量创建评论对象
                                comment_objects = []
                                for comment in new_comments:
                                    comment_id = str(comment.get('id', ''))[:32]
                                    comment_obj = XiaohongshuComment(
                                        comment_content=str(comment.get('content', ''))[:500],
                                        comment_id=comment_id,
                                        comment_time=datetime.fromtimestamp(comment.get('create_time', 0)/1000).strftime('%Y-%m-%d %H:%M:%S'),
                                        created_at=datetime.now(),
                                        updated_at=datetime.now()
                                    )
                                    comment_objects.append(comment_obj)
                                
                                # 批量插入数据
                                session.bulk_save_objects(comment_objects)
                                session.commit()
                                logger.info(f"成功保存 {len(comment_objects)} 条新评论")
                                
                            except Exception as e:
                                session.rollback()
                                logger.error(f"批量保存评论时出错: {str(e)}")
                        else:
                            logger.info("没有新的评论需要保存")
                            
                    else:
                        logger.error("API返回失败")
                    
                except Exception as e:
                    logger.error(f"解析API响应时出错: {str(e)}")
                
        except Exception as e:
            logger.error(f"处理API响应时出错: {str(e)}")

    def _save_cookies(self):
        """
        保存当前会话的Cookie
        """
        try:
            cookies = self.context.cookies()
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f)
            logger.info("Cookie数据已保存")
        except Exception as e:
            logger.error(f"保存Cookie时出错: {str(e)}")

    def _load_cookies(self):
        """
        加载保存的Cookie
        :return: 是否成功加载Cookie
        """
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                self.context.add_cookies(cookies)
                logger.info("已加载保存的Cookie")
                return True
            return False
        except Exception as e:
            logger.error(f"加载Cookie时出错: {str(e)}")
            return False

    def get_element_text(self, element, selector, default=""):
        try:
            found_element = element.query_selector(selector)
            if found_element:
                return found_element.inner_text().strip()
            return default
        except Exception:
            return default

    def get_comments_api(self, note_id, cursor="", top_comment_id=""):
        """获取评论API数据"""
        try:
            url = f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/page"
            params = {
                'note_id': note_id,
                'cursor': cursor,
                'top_comment_id': top_comment_id,
                'image_formats': 'jpg,webp,avif'
            }
            # 更新时间戳
            self.headers['X-t'] = str(int(time.time() * 1000))
            response = requests.get(url, headers=self.headers, params=params)
            return response.json()
        except Exception as e:
            logger.error(f"获取评论数据失败: {str(e)}")
            return None

    def get_comment_replies_api(self, note_id, root_comment_id, cursor="", num=10):
        """获取评论回复API数据"""
        try:
            url = f"https://edith.xiaohongshu.com/api/sns/web/v2/comment/sub/page"
            params = {
                'note_id': note_id,
                'root_comment_id': root_comment_id,
                'cursor': cursor,
                'num': num,
                'image_formats': 'jpg,webp,avif'
            }
            # 更新时间戳
            self.headers['X-t'] = str(int(time.time() * 1000))
            response = requests.get(url, headers=self.headers, params=params)
            return response.json()
        except Exception as e:
            logger.error(f"获取评论回复数据失败: {str(e)}")
            return None

    def process_all_comments(self, note_id):
        """处理视频的所有评论和回复"""
        try:
            logger.info(f"开始处理视频 {note_id} 的评论")

            # 等待评论区加载
            self.page.wait_for_selector('.comment-item', timeout=10000)

            # 监听评论接口的响应
            comments_data = []

            def handle_comment_response(response):
                if 'api/sns/web/v2/comment/page' in response.url:
                    try:
                        data = response.json()
                        if data.get('success') and data.get('data'):
                            comments = data['data'].get('comments', [])
                            comments_data.extend(comments)
                            logger.info(f"获取到 {len(comments)} 条新评论")
                    except:
                        pass

            # 添加响应监听器
            self.page.on('response', handle_comment_response)

            # 滚动评论区以触发加载更多评论
            while True:
                try:
                    # 获取当前评论数量
                    current_comments = len(self.page.query_selector_all('.comment-item'))
                    logger.info(f"当前页面评论数量: {current_comments}")

                    # 滚动评论区
                    self.page.evaluate('''
                        const commentList = document.querySelector('.comment-list');
                        if (commentList) {
                            commentList.scrollTo(0, commentList.scrollHeight);
                        }
                    ''')

                    time.sleep(2)

                    # 检查是否有新评论加载
                    new_comments = len(self.page.query_selector_all('.comment-item'))
                    if new_comments == current_comments:
                        # 检查是否到达底部
                        is_bottom = self.page.evaluate('''
                            const commentList = document.querySelector('.comment-list');
                            return commentList ? 
                                Math.abs(commentList.scrollHeight - commentList.scrollTop - commentList.clientHeight) < 1 
                                : true;
                        ''')
                        if is_bottom:
                            break

                    logger.info(f"已加载评论数: {new_comments}")

                except Exception as e:
                    logger.error(f"滚动评论区时出错: {str(e)}")
                    break

            # 处理所有评论及其回复
            all_comments = []
            comment_elements = self.page.query_selector_all('.comment-item')
            total_comments = len(comment_elements)

            for idx, comment in enumerate(comment_elements):
                try:
                    comment_info = self.extract_comment_info(comment)

                    # 检查是否有展开回复按钮
                    show_more = comment.query_selector('.show-more')
                    if show_more:
                        logger.info(f"展开第 {idx + 1}/{total_comments} 条评论的回复")

                        # 点击展开回复
                        show_more.click()
                        time.sleep(2)

                        # 持续点击加载更多回复
                        while True:
                            try:
                                # 检查是否还有更多回复按钮
                                more_replies = comment.query_selector('.show-more:not([style*="display: none"])')
                                if not more_replies:
                                    break

                                more_replies.click()
                                time.sleep(2)

                            except Exception as e:
                                logger.error(f"加载更多回复时出错: {str(e)}")
                                break

                        # 获取所有回复
                        comment_info['replies'] = self.extract_replies(comment)

                    all_comments.append(comment_info)
                    logger.info(f"已处理 {idx + 1}/{total_comments} 条评论")

                except Exception as e:
                    logger.error(f"处理第 {idx + 1} 条评论时出错: {str(e)}")
                    continue

            return all_comments

        except Exception as e:
            logger.error(f"处理评论时出错: {str(e)}")
            return []

    def extract_replies(self, comment_element):
        """提取评论的所有回复"""
        replies = []
        try:
            # 获取当前评论下的所有回复
            reply_elements = comment_element.query_selector_all('.reply-item')
            for reply in reply_elements:
                reply_info = {
                    'user': self.get_element_text(reply, '.user-name'),
                    'content': self.get_element_text(reply, '.content'),
                    'likes': self.get_element_text(reply, '.like-count', "0"),
                    'time': self.get_element_text(reply, '.time'),
                    'location': self.get_element_text(reply, '.ip-location')
                }
                replies.append(reply_info)

            logger.info(f"获取到 {len(replies)} 条回复")
            return replies

        except Exception as e:
            logger.error(f"提取回复信息时出错: {str(e)}")
            return []

    def scroll_comments(self):
        """滚动加载所有评论"""
        try:
            previous_comments = 0
            no_change_count = 0

            while no_change_count < 3:
                # 获取当前评论数量
                current_comments = len(self.page.query_selector_all('.comment-item'))
                logger.info(f"当前评论数量: {current_comments}")

                # 使用新的选择器滚动评论区
                self.page.evaluate('''
                    const commentList = document.querySelector('di[class="note-scrollernote-scroller"]');
                    if (commentList) {
                        commentList.scrollTo({
                            top: commentList.scrollHeight,
                            behavior: 'smooth'
                        });
                    }
                ''')

                time.sleep(2) 

                # 检查是否有新评论加载
                new_comments = len(self.page.query_selector_all('.comment-item'))

                if new_comments == current_comments:
                    no_change_count += 1
                else:
                    no_change_count = 0
                    logger.info(f"加载了 {new_comments - current_comments} 条新评论")

                previous_comments = new_comments

        except Exception as e:
            logger.error(f"滚动评论区时出错: {str(e)}")

    def extract_comment_info(self, comment_element):
        """提取评论信息"""
        try:
            return {
                'user': self.get_element_text(comment_element, '.user-name'),
                'content': self.get_element_text(comment_element, '.content'),
                'likes': self.get_element_text(comment_element, '.like-count', "0"),
                'time': self.get_element_text(comment_element, '.time'),
                'location': self.get_element_text(comment_element, '.location'),
                'replies': []
            }
        except Exception as e:
            logger.error(f"Error extracting comment info: {str(e)}")
            return {}

    def get_video_title(self):
        """获取视频标题"""
        try:
            title_element = self.page.query_selector('.title')
            return title_element.inner_text() if title_element else "Unknown Title"
        except Exception:
            return "Unknown Title"

