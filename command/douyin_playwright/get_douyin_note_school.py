import json
import os
import sys
import time
import traceback
from datetime import datetime

from loguru import logger
from playwright.sync_api import sync_playwright

from core.localhost_fp_project import session
from models.models import PgyUser, PgyNoteDetail, PgyUserFans
from unitl.common import Common

"""
抖音博主笔记数据抓取脚本
功能：从PgyUser表获取platform_id=2的用户，抓取博主信息和笔记数据
"""

def get_base_path():
    """获取基础路径"""
    try:
        return os.path.dirname(os.path.abspath(sys.argv[0])) if hasattr(sys, '_MEIPASS') else os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.path.abspath("../..")

class DouyinNoteSpider:
    def __init__(self):
        self.setup_logger()
        base_path = get_base_path()
        self.cookie_file = os.path.join(base_path, 'cookies.json')
        self.data_dir = os.path.join(base_path, 'data')
        os.makedirs(self.data_dir, exist_ok=True)

        self.base_url = 'https://www.douyin.com'
        self.is_logged_in = False
        self.api_data = {}
        self.common = Common()

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.current_user = None

        self.stats = {'total_users': 0, 'success_users': 0, 'total_notes': 0, 'new_notes': 0, 'updated_notes': 0}

    def setup_logger(self):
        """设置日志"""
        base_path = get_base_path()
        log_path = os.path.join(base_path, 'logs')
        os.makedirs(log_path, exist_ok=True)
        logger.remove()
        logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>", level="INFO")
        logger.add(os.path.join(log_path, "douyin_notes_{time:YYYY-MM-DD}.log"), rotation="1 day", retention="7 days",
                   format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}", level="DEBUG", encoding="utf-8")

    def setup_browser(self):
        """初始化浏览器"""
        if self.browser and self.context and self.page:
            return
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--disable-gpu', '--no-sandbox'])
        self.context = self.browser.new_context(viewport={'width': 1512, 'height': 768}, user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        if self._load_cookies():
            self.page = self.context.new_page()
            try:
                self.page.goto(self.base_url)
                self.common.random_sleep(2, 3)
                avatar = self.page.locator('[data-e2e="live-avatar"]').first
                self.is_logged_in = avatar and avatar.is_visible(timeout=3000)
                if self.is_logged_in:
                    logger.info("Cookie有效，已登录")
            except:
                self.is_logged_in = False
        else:
            self.page = self.context.new_page()
            self.is_logged_in = False

        self.page.set_default_timeout(20000)
        self.page.on("response", self._handle_api_response)
        logger.info("浏览器初始化完成")

    def login(self):
        """等待手动登录"""
        try:
            if self.is_logged_in:
                logger.info("已登录")
                return True
            self.page.goto(self.base_url)
            self.common.random_sleep(2, 3)
            logger.info("请手动登录抖音，最多等待5分钟...")
            for _ in range(30):
                try:
                    avatar = self.page.locator('[data-e2e="live-avatar"]').first
                    if avatar and avatar.is_visible(timeout=2000):
                        logger.info("✓ 登录成功！")
                        self.is_logged_in = True
                        self._save_cookies()
                        return True
                except:
                    pass
                time.sleep(10)
            logger.error("登录超时")
            return False
        except Exception as e:
            logger.error(f"登录出错: {str(e)}")
            return False

    def _handle_api_response(self, response):
        """处理API响应"""
        try:
            url = response.url
            if not url or response.status != 200:
                return

            api_type = None
            if 'aweme/v1/web/user/profile/other/' in url:
                api_type = 'profile'
            elif 'aweme/v1/web/aweme/post' in url:
                api_type = 'post'

            if not api_type:
                return

            if response.request.resource_type in ['fetch', 'xhr']:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        if api_type not in self.api_data:
                            self.api_data[api_type] = []
                        self.api_data[api_type].append({'url': url, 'data': data, 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
                        logger.info(f"✓ 捕获 {api_type} 接口")
                except:
                    pass
        except Exception as e:
            logger.error(f"处理API响应出错: {str(e)}")

    def save_user_info(self, user, profile_data):
        """保存用户信息"""
        try:
            if not profile_data:
                return False
            user_obj = profile_data.get('user', {})
            if not user_obj:
                return False

            nickname = user_obj.get('nickname', '')
            if nickname:
                user.douyin_name = nickname
                logger.info(f"✓ 更新昵称: {nickname}")

            max_follower_count = user_obj.get('max_follower_count', 0)
            if max_follower_count:
                fans_record = PgyUserFans(user_id=user.id, fans=str(max_follower_count), platform_id=2, create_time=datetime.now(), update_time=datetime.now())
                session.add(fans_record)
                logger.info(f"✓ 保存粉丝数: {max_follower_count}")

            session.commit()
            return True
        except Exception as e:
            logger.error(f"保存用户信息失败: {str(e)}")
            logger.error(traceback.format_exc())
            session.rollback()
            return False

    def save_notes(self, user, aweme_list):
        """保存笔记数据"""
        try:
            if not aweme_list:
                return False
            logger.info(f"准备保存 {len(aweme_list)} 条笔记")

            for item in aweme_list:
                try:
                    note_id = item.get('aweme_id', '')
                    if not note_id:
                        continue

                    tag = ''
                    if 'video_tag' in item and item['video_tag']:
                        tag_list = [tag_item.get('tag_name', '') for tag_item in item['video_tag'] if 'tag_name' in tag_item]
                        tag = ','.join(tag_list)

                    statistics = item.get('statistics', {})
                    like_count = statistics.get('digg_count', 0)
                    collect_count = statistics.get('collect_count', 0)
                    share_count = statistics.get('share_count', 0)

                    create_time = item.get('create_time', 0)
                    note_date = time.strftime('%Y-%m-%d', time.localtime(create_time)) if create_time else ''
                    current_timestamp = int(time.time())

                    existing_note = session.query(PgyNoteDetail).filter(PgyNoteDetail.note_id == note_id).first()

                    if existing_note:
                        existing_note.pgy_id = user.id
                        existing_note.note_title = item.get('desc', '')
                        existing_note.like_num = like_count
                        existing_note.collect_num = collect_count
                        existing_note.share_num = share_count
                        existing_note.note_message = item.get('desc', '')
                        existing_note.note_date = note_date
                        existing_note.update_time = current_timestamp
                        existing_note.platform_id = 2
                        logger.info(f"✓ 更新笔记: {note_id}")
                        self.stats['updated_notes'] += 1
                    else:
                        new_note = PgyNoteDetail(pgy_id=user.id, note_id=note_id, note_title=item.get('desc', ''), note_type='video', like_num=like_count,
                                                  collect_num=collect_count, share_num=share_count, note_message=item.get('desc', ''), note_date=note_date,
                                                  create_time=current_timestamp, update_time=current_timestamp, platform_id=2)
                        session.add(new_note)
                        logger.info(f"✓ 新增笔记: {note_id}")
                        self.stats['new_notes'] += 1

                    self.stats['total_notes'] += 1
                except Exception as e:
                    logger.error(f"保存单条笔记失败: {str(e)}")
                    continue

            session.commit()
            logger.info(f"✓ 成功保存 {len(aweme_list)} 条笔记")
            return True
        except Exception as e:
            logger.error(f"保存笔记失败: {str(e)}")
            logger.error(traceback.format_exc())
            session.rollback()
            return False

    def scroll_and_load_more(self):
        """滚动加载更多"""
        try:
            logger.info("滚动加载...")
            self.page.evaluate('() => { window.scrollTo({top: document.documentElement.scrollHeight, behavior: "smooth"}); }')
            self.common.random_sleep(3, 5)
            return True
        except Exception as e:
            logger.error(f"滚动失败: {str(e)}")
            return False

    def process_user(self, user):
        """处理单个用户"""
        try:
            self.current_user = user
            user_id = user.douyin_xsec_token
            nick_name = user.douyin_name
            logger.info(f"===== 开始处理用户: {nick_name} (userId: {user_id}) =====")

            douyin_url = f"https://www.douyin.com/user/{user_id}"
            logger.info(f"访问: {douyin_url}")
            self.api_data = {}
            self.page.goto(douyin_url)
            # 等待页面加载完成
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.warning(f"等待页面加载完成时出错: {str(e)}")

            # 处理profile接口
            if 'profile' in self.api_data and len(self.api_data['profile']) > 0:
                self.save_user_info(user, self.api_data['profile'][0]['data'])
            else:
                logger.warning("未捕获profile接口")

            # 处理post接口，循环滚动加载
            max_scrolls = 20
            scroll_count = 0
            no_new_data_count = 0

            while scroll_count < max_scrolls:
                logger.info(f"===== 第 {scroll_count + 1} 次加载 =====")

                if 'post' in self.api_data and len(self.api_data['post']) > 0:
                    aweme_list = self.api_data['post'][-1]['data'].get('aweme_list', [])
                    if aweme_list and len(aweme_list) > 0:
                        logger.info(f"获取 {len(aweme_list)} 条笔记")
                        self.save_notes(user, aweme_list)
                        no_new_data_count = 0
                    else:
                        no_new_data_count += 1
                else:
                    no_new_data_count += 1
                    logger.warning("未捕获post接口")

                if no_new_data_count >= 3:
                    logger.info("连续3次无新数据，停止")
                    break

                self.scroll_and_load_more()
                scroll_count += 1

            logger.info(f"===== 用户 {nick_name} 处理完成 =====")
            self.stats['success_users'] += 1
            self.current_user = None
            return True
        except Exception as e:
            logger.error(f"处理用户出错: {str(e)}")
            logger.error(traceback.format_exc())
            self.current_user = None
            return False

    def run(self):
        """运行爬虫"""
        try:
            logger.info("=" * 60)
            logger.info("开始运行抖音笔记爬虫")
            logger.info("=" * 60)

            users = session.query(PgyUser).filter(PgyUser.douyin_xsec_token is not None).all()
            logger.info(f"查询到 {len(users)} 个抖音用户")

            if len(users) == 0:
                logger.warning("未查询到用户")
                return False

            self.stats['total_users'] = len(users)

            for index, user in enumerate(users):
                logger.info(f"处理第 {index + 1}/{len(users)} 个用户")
                self.process_user(user)
                if index < len(users) - 1:
                    self.common.random_sleep(3, 5)

            self._print_stats()
            logger.info("=" * 60)
            logger.info("所有用户处理完成")
            logger.info("=" * 60)
            return True
        except Exception as e:
            logger.error(f"运行出错: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _print_stats(self):
        """打印统计"""
        logger.info("=" * 60)
        logger.info(f"总用户: {self.stats['total_users']}, 成功: {self.stats['success_users']}")
        logger.info(f"总笔记: {self.stats['total_notes']}, 新增: {self.stats['new_notes']}, 更新: {self.stats['updated_notes']}")
        logger.info("=" * 60)

    def close(self):
        """关闭资源"""
        try:
            if self.is_logged_in:
                self._save_cookies()
            if hasattr(self, 'page') and self.page:
                self.page.close()
            if hasattr(self, 'context') and self.context:
                self.context.close()
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()
            logger.info("资源已关闭")
        except Exception as e:
            logger.error(f"关闭资源出错: {str(e)}")

    def _save_cookies(self):
        """保存Cookie"""
        try:
            cookies = self.context.cookies()
            cookie_dir = os.path.dirname(self.cookie_file)
            if cookie_dir and not os.path.exists(cookie_dir):
                os.makedirs(cookie_dir, exist_ok=True)
            with open(self.cookie_file, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            logger.info("Cookie已保存")
        except Exception as e:
            logger.error(f"保存Cookie出错: {str(e)}")

    def _load_cookies(self):
        """加载Cookie"""
        try:
            if os.path.exists(self.cookie_file):
                with open(self.cookie_file, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                if cookies:
                    self.context.add_cookies(cookies)
                    logger.info(f"加载 {len(cookies)} 个Cookie")
                    return True
            return False
        except Exception as e:
            logger.error(f"加载Cookie出错: {str(e)}")
            try:
                if os.path.exists(self.cookie_file):
                    os.remove(self.cookie_file)
            except:
                pass
            return False


def main():
    """主函数"""
    spider = None
    try:
        logger.info("=== 抖音笔记爬虫启动 ===")
        spider = DouyinNoteSpider()
        spider.setup_browser()
        if not spider.login():
            logger.error("登录失败")
            return False
        if not spider.run():
            logger.error("执行失败")
            return False
        logger.info("执行完成")
        return True
    except KeyboardInterrupt:
        logger.warning("用户中断")
        return False
    except Exception as e:
        logger.error(f"运行出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    finally:
        if spider:
            try:
                spider.close()
            except Exception as e:
                logger.error(f"清理资源出错: {str(e)}")


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"启动失败: {str(e)}")
        sys.exit(1)
