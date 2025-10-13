import json
import os
import sys
import configparser
import time
import cv2
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from urllib.parse import urlparse
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import traceback

from core.database_onlone_tibao_2 import session
from unitl.common import Common

"""
    蒲公英邀约表单批量发送工具
    功能：从Excel导入蒲公英链接，提取ID，批量发送邀约表单
    基于Playwright模拟浏览器操作
"""

class PGYSpider:
    def __init__(self):
        # 设置cookie和数据目录，支持exe打包
        if hasattr(sys, '_MEIPASS'):
            # exe环境下，使用exe文件所在目录（不是临时解压目录）
            exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            self.cookie_file = os.path.join(exe_dir, 'cookies.json')
            self.data_dir = os.path.join(exe_dir, 'data')
            self.config_file = os.path.join(exe_dir, 'config.ini')
        else:
            # 开发环境下，使用当前文件同级目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.cookie_file = os.path.join(current_dir, 'cookies.json')
            self.data_dir = os.path.join(current_dir, 'data')
            self.config_file = os.path.join(current_dir, 'config.ini')

        # 确保数据目录存在
        os.makedirs(self.data_dir, exist_ok=True)

        # 加载配置
        self.setup_logger()
        self.load_config()

        self.is_logged_in = False
        self.common = Common()
        
        # Excel处理相关
        self.excel_data = None
        self.excel_file_path = None

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

        logger.info(f"日志文件保存路径: {log_path}")

    def load_config(self):
        """加载配置文件"""
        try:
            self.config = configparser.ConfigParser()
            self.config.read(self.config_file, encoding='utf-8')
            logger.info(f"成功加载配置文件: {self.config_file}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise

    def get_config_value(self, section, key):
        """获取配置值"""
        return self.config.get(section, key)

    def select_excel_file(self):
        """选择Excel文件"""
        try:
            # 创建隐藏的根窗口
            root = tk.Tk()
            root.withdraw()  # 隐藏主窗口
            
            # 显示提示信息
            messagebox.showinfo("Excel导入", "请选择包含蒲公英链接的Excel文件\n\n文件应包含以下列：\n• 蒲公英链接（必填）")
            
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
            
            # 检查是否包含蒲公英链接列
            if '蒲公英链接' not in self.excel_data.columns:
                logger.error("Excel文件缺少'蒲公英链接'列")
                return False
            
            # 处理nan值
            self.excel_data['蒲公英链接'] = self.excel_data['蒲公英链接'].fillna('')
            
            # 显示前几行数据用于调试
            logger.info(f"前3行数据预览:")
            for i in range(min(3, len(self.excel_data))):
                pgy_url = self.excel_data.iloc[i]['蒲公英链接']
                logger.info(f"第{i+1}行 蒲公英链接: {pgy_url}")
            
            return True
            
        except Exception as e:
            logger.error(f"加载Excel数据时出错: {str(e)}")
            return False

    def _extract_id_from_url(self, pgy_url):
        """从蒲公英链接中提取ID"""
        try:
            if not pgy_url or pd.isna(pgy_url):
                return None
            
            # 解析URL
            parsed = urlparse(pgy_url)
            path_parts = parsed.path.split('/')
            
            # 查找包含ID的路径部分
            for part in path_parts:
                if part and len(part) > 10 and not part.startswith('blogger-detail'):
                    return part
            
            return None
        except Exception as e:
            logger.error(f"提取ID时出错: {str(e)}")
            return None

    def setup_browser(self):
        """初始化浏览器"""
        try:
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
            
            logger.info("成功启动浏览器")
        except Exception as e:
            logger.error(f"启动浏览器失败: {str(e)}")
            raise
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
                self.page.goto('https://pgy.xiaohongshu.com')
                self.common.random_sleep(2, 3)

                # 检查是否存在用户头像元素
                if self.page.locator(".home_head_user_info").is_visible(timeout=5000):
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

    def login(self):
        """
        等待用户手动登录，最多等待5分钟
        """
        try:
            if self.is_logged_in:
                logger.info("已处于登录状态")
                return True

            logger.info("开始等待用户手动登录...")
            logger.info("请在5分钟内完成登录操作，程序将自动检测登录状态")
            
            try:
                # 访问首页
                self.page.goto('https://pgy.xiaohongshu.com')
                self.common.random_sleep(2, 3)

                # 等待5分钟，每10秒检查一次登录状态
                max_wait_time = 300  # 5分钟 = 300秒
                check_interval = 10   # 每10秒检查一次
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
                        
                        # 如果还没登录成功，等待10秒后继续检查
                        logger.info(f"等待登录中... ({elapsed_time}/{max_wait_time}秒)")
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


    def process_excel_invites(self):
        """处理Excel数据，为每行发送邀约表单"""
        try:
            if not self.is_logged_in:
                logger.error("未登录状态，无法发送邀约")
                return False
            
            if self.excel_data is None:
                logger.error("Excel数据未加载")
                return False
            
            logger.info(f'开始处理Excel数据，共 {len(self.excel_data)} 行')
            
            # 遍历每一行数据
            for index, row in self.excel_data.iterrows():
                try:
                    pgy_url = row.get('蒲公英链接', '')
                    if not pgy_url or pd.isna(pgy_url):
                        logger.info(f"第 {index + 1} 行：蒲公英链接为空，跳过")
                        continue
                    
                    # 提取ID
                    extracted_id = self._extract_id_from_url(pgy_url)
                    if not extracted_id:
                        logger.warning(f"第 {index + 1} 行：无法从链接中提取ID，跳过")
                        continue
                    
                    logger.info(f"处理第 {index + 1} 行，提取的ID: {extracted_id}")
                    
                    # 发送邀约表单
                    if self._send_invite_form(extracted_id, index + 1):
                        logger.info(f"第 {index + 1} 行邀约表单发送成功")
                    else:
                        logger.error(f"第 {index + 1} 行邀约表单发送失败")
                    
                    # 添加延迟，避免请求过于频繁
                    self.common.random_sleep(5, 10)
                    
                except Exception as e:
                    logger.error(f"处理第 {index + 1} 行时出错: {str(e)}")
                    continue
            
            logger.info("所有邀约表单处理完成")
            return True
            
        except Exception as e:
            logger.error(f"处理Excel邀约时出错: {str(e)}")
            return False

    def _send_invite_form(self, extracted_id, row_number):
        """发送单个邀约表单"""
        try:
            logger.info(f'开始发送第 {row_number} 行的邀约表单，ID: {extracted_id}')

            # 构建邀约表单URL
            page_url = f"https://pgy.xiaohongshu.com/solar/pre-trade/invite-form?id={extracted_id}&trackId="
            logger.info(f"访问邀约表单页面: {page_url}")
            self.page.goto(page_url)

            # 等待页面加载完成
            logger.info("等待页面加载完成...")
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logger.error(f"等待页面加载完成时出错: {str(e)}")

            # 尝试复用上回填写内容
            if not self._try_reuse_previous_content():
                logger.warning("无法复用上回内容，将重新填写表单")
                return False

            # 填写邀约表单
            if not self._fill_invite_form():
                logger.error("填写邀约表单失败")
                return False
            
            # 点击确认发起邀约按钮
            try:
                logger.info("准备点击确认发起邀约按钮...")
                submit_button = self.page.locator("button:has-text('确认发起邀约')").first
                
                if submit_button and submit_button.is_visible():
                    logger.info("找到确认发起邀约按钮，点击提交")
                    submit_button.click()
                    
                    # 等待提交完成
                    self.common.random_sleep(3, 5)
                    logger.info("邀约表单提交完成")
                else:
                    logger.warning("未找到确认发起邀约按钮")
                    return False
                    
            except Exception as e:
                logger.error(f"点击确认发起邀约按钮时出错: {str(e)}")
                return False

            logger.info(f"第 {row_number} 行邀约表单发送成功")
            return True

        except Exception as e:
            logger.error(f"发送第 {row_number} 行邀约表单时出错: {str(e)}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return False

    def _try_reuse_previous_content(self):
        """尝试复用上回填写内容"""
        try:
            # 等待复用按钮出现
            reuse_button = self.page.wait_for_selector("text=复用上回填写内容", timeout=5000)
            if not reuse_button:
                logger.warning("未找到复用上回填写内容按钮")
                return False
                
            logger.info("找到复用按钮，点击复用上回内容")
            reuse_button.click()
            self.common.random_sleep(2, 3)
            
            # 检查是否成功复用
            filled_inputs = self.page.locator("input[value], textarea:not(:empty)").all()
            logger.info(f"复用成功，发现 {len(filled_inputs)} 个已填充字段")
            return True
                
        except Exception as e:
            logger.error(f"复用上回内容时出错: {str(e)}")
            return False

    def _fill_invite_form(self):
        """填写邀约表单"""
        try:
            logger.info("开始填写邀约表单...")
            
            # 从配置获取表单内容
            product_name = self.get_config_value('FORM_CONFIG', 'product_name')
            product_type_description = self.get_config_value('FORM_CONFIG', 'product_type_description')
            
            product_name_input = self.page.wait_for_selector("input[placeholder='请输入产品名称']", timeout=3000)
            
            # 填写产品名称
            if product_name_input:
                logger.info(f"填写产品名称: {product_name}")
                product_name_input.fill(product_name)
                self.common.random_sleep(1, 2)
            else:
                logger.warning("未找到产品名称输入框")
            
            product_type_input = self.page.wait_for_selector("textarea.d-text", timeout=3000)
            
            # 填写产品类型
            if product_type_input:
                logger.info(f"填写产品类型描述: {product_type_description}")
                product_type_input.fill(product_type_description)
                self.common.random_sleep(1, 2)
            else:
                logger.warning("未找到产品类型输入框")
            
            logger.info("邀约表单填写完成")
            return True
            
        except Exception as e:
            logger.error(f"填写邀约表单时出错: {str(e)}")
            return False


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
                logger.info(f"找到Cookie文件: {self.cookie_file}")
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
                logger.info(f"Cookie文件不存在: {self.cookie_file}")
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

def run_spider_task():
    """执行爬虫任务"""
    spider = None
    try:
        logger.info("=== 蒲公英邀约表单发送程序启动 ===")
        
        # 初始化爬虫实例
        spider = PGYSpider()
        logger.info("爬虫实例初始化成功")

        # 选择Excel文件
        if not spider.select_excel_file():
            logger.error("未选择Excel文件，程序退出")
            return False

        # 加载Excel数据
        if not spider.load_excel_data():
            logger.error("加载Excel数据失败，程序退出")
            return False

        # 初始化浏览器
        logger.info("开始初始化浏览器...")
        spider.setup_browser()
        logger.info("浏览器初始化成功")

        # 执行登录
        logger.info("开始登录流程...")
        login_success = spider.login()
        if not login_success:
            logger.error("登录失败，程序退出")
            return False

        logger.info("登录成功，开始处理Excel数据...")

        # 处理Excel数据并发送邀约表单
        spider.process_excel_invites()

        logger.info("所有邀约表单处理完成")
        return True

    except KeyboardInterrupt:
        logger.warning("用户手动中断程序")
        return False
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
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
    run_spider_task()