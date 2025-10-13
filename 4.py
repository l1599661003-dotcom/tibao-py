# -*- coding: utf-8 -*-
"""
抖音MCN详情数据抓取程序
从DouyinMcn表获取mcn_id，抓取每个MCN的top_follower_authors数据存入DouyinMcnDetail表
"""

import json
import time
import requests
import urllib3
from datetime import datetime
from loguru import logger
import sys

from core.localhost_fp_project import session
from models.models import DouyinMcn, DouyinMcnDetail
from unitl.unitl import random_sleep
from cookie_config import COOKIE_CONFIGS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    "Content-Type": "application/json"
}

API_URL = "https://www.xingtu.cn/gw/api/mcn/mcn_main_page_author_list"
PAGE_SIZE = 200  # 每页数量
REQUEST_DELAY = 10  # 每次请求延迟（秒）

# Cookie管理器
class CookieManager:
    def __init__(self, cookie_configs):
        self.cookie_configs = cookie_configs
        self.current_index = 0
        self.current_date = datetime.now().date()
        
    def get_current_headers(self):
        """获取当前可用的请求头"""
        current_config = self.cookie_configs[self.current_index]
        headers = BASE_HEADERS.copy()
        headers["cookie"] = current_config["cookie"]
        return headers
    
    def get_current_cookie_name(self):
        """获取当前cookie名称"""
        return self.cookie_configs[self.current_index]["name"]
    
    def check_and_reset_daily_count(self):
        """检查并重置每日计数"""
        today = datetime.now().date()
        
        for config in self.cookie_configs:
            if config["last_reset_date"] != today:
                config["used_count"] = 0
                config["last_reset_date"] = today
                logger.info(f"🔄 {config['name']} 每日计数已重置")
    
    def increment_count(self):
        """增加当前cookie的使用计数"""
        current_config = self.cookie_configs[self.current_index]
        current_config["used_count"] += 1
        logger.info(f"📊 {current_config['name']} 使用次数: {current_config['used_count']}/{current_config['daily_limit']}")
    
    def is_current_cookie_available(self):
        """检查当前cookie是否可用"""
        current_config = self.cookie_configs[self.current_index]
        return current_config["used_count"] < current_config["daily_limit"]
    
    def switch_to_next_cookie(self):
        """切换到下一个可用cookie"""
        self.current_index = (self.current_index + 1) % len(self.cookie_configs)
        logger.info(f"🔄 切换到下一个cookie: {self.get_current_cookie_name()}")
    
    def get_available_cookie(self):
        """获取可用的cookie，如果没有可用cookie则返回None"""
        self.check_and_reset_daily_count()
        
        # 检查当前cookie是否可用
        if self.is_current_cookie_available():
            return self.get_current_headers()
        
        # 尝试切换到其他cookie
        original_index = self.current_index
        self.switch_to_next_cookie()
        
        while self.current_index != original_index:
            if self.is_current_cookie_available():
                return self.get_current_headers()
            self.switch_to_next_cookie()
        
        # 所有cookie都用完了
        logger.error("❌ 所有cookie今日调用次数已用完")
        return None
    
    def log_status(self):
        """记录所有cookie的状态"""
        logger.info("📊 Cookie使用状态:")
        for config in self.cookie_configs:
            status = "✅ 可用" if config["used_count"] < config["daily_limit"] else "❌ 已用完"
            logger.info(f"   {config['name']}: {config['used_count']}/{config['daily_limit']} {status}")

# 初始化Cookie管理器
cookie_manager = CookieManager(COOKIE_CONFIGS)


def setup_logger():
    """设置日志"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )


def save_mcn_detail_to_db(mcn_user_id, author_data):
    """保存MCN详情数据到数据库，使用author_id查重"""
    try:
        author_id = author_data.get('author_id')
        
        # 先查询DouyinMcn表的id（主键）
        mcn_record = session.query(DouyinMcn).filter(DouyinMcn.user_id == mcn_user_id).first()
        if not mcn_record:
            logger.error(f"未找到MCN记录: user_id={mcn_user_id}")
            return 'error'
            
        mcn_db_id = mcn_record.id  # 使用DouyinMcn表的主键id
        
        # 查询是否已存在
        existing_detail = session.query(DouyinMcnDetail).filter(
            DouyinMcnDetail.author_id == author_id
        ).first()
        
        # 拼接标签
        tags = '、'.join(author_data.get('tags', [])) if author_data.get('tags') else ''
        
        current_time = datetime.now()
        
        if existing_detail:
            # 更新现有记录
            existing_detail.mcn_id = mcn_db_id
            existing_detail.avatar_uri = author_data.get('avatar_uri')
            existing_detail.nick_name = author_data.get('nick_name')
            existing_detail.tags = tags
            existing_detail.sum_follower = author_data.get('sum_follower')
            existing_detail.update_time = current_time
            
            logger.debug(f"更新MCN详情: {author_data.get('nick_name')} (author_id: {author_id})")
            return 'updated'
        else:
            # 创建新记录
            new_detail = DouyinMcnDetail(
                mcn_id=mcn_db_id,  # 使用DouyinMcn表的主键id
                author_id=author_id,
                avatar_uri=author_data.get('avatar_uri'),
                nick_name=author_data.get('nick_name'),
                tags=tags,
                sum_follower=author_data.get('sum_follower'),
                create_time=current_time,
                update_time=current_time
            )
            
            session.add(new_detail)
            logger.debug(f"新增MCN详情: {author_data.get('nick_name')} (author_id: {author_id})")
            return 'inserted'
            
    except Exception as e:
        logger.error(f"保存MCN详情数据时出错: {str(e)}")
        # 回滚当前事务
        session.rollback()
        return 'error'


def update_mcn_status(mcn_id, status=1):
    """更新MCN状态"""
    try:
        mcn = session.query(DouyinMcn).filter(DouyinMcn.user_id == mcn_id).first()
        if mcn:
            mcn.status = status
            mcn.update_time = datetime.now()
            logger.info(f"更新MCN状态: {mcn_id} -> {status}")
            return True
        return False
    except Exception as e:
        logger.error(f"更新MCN状态失败: {str(e)}")
        session.rollback()
        return False


def fetch_mcn_detail_data(mcn_id, page):
    """获取指定MCN的详情数据"""
    try:
        # 获取可用的cookie
        headers = cookie_manager.get_available_cookie()
        if not headers:
            logger.error("❌ 没有可用的cookie，停止请求")
            return None
            
        url = f"{API_URL}?page={page}&limit={PAGE_SIZE}&mcn_id={mcn_id}"
        
        logger.info(f"🌐 使用 {cookie_manager.get_current_cookie_name()} 请求: {url}")

        response = requests.get(
            url,
            headers=headers,
            verify=False,
            timeout=30
        )
        
        # 增加cookie使用计数
        cookie_manager.increment_count()
        
        if response.status_code != 200:
            logger.error(f"请求失败，状态码: {response.status_code}")
            return None

        return response.json()

    except requests.RequestException as e:
        logger.error(f"请求MCN详情数据失败: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"获取MCN详情数据出错: {str(e)}")
        return None


def get_unprocessed_mcns():
    """获取未处理的MCN列表"""
    try:
        mcns = session.query(DouyinMcn).filter(DouyinMcn.status == 0).all()
        logger.info(f"获取到 {len(mcns)} 个未处理的MCN")
        return mcns
    except Exception as e:
        logger.error(f"获取未处理MCN失败: {str(e)}")
        return []


def process_mcn_detail(mcn):
    """处理单个MCN的详情数据"""
    mcn_id = mcn.user_id
    logger.info(f"\n🏢 开始处理MCN: {mcn_id}")
    
    page = 1
    max_pages = 50  # 设置最大页数限制，防止无限循环
    total_authors = 0
    inserted_count = 0
    updated_count = 0
    error_count = 0
    consecutive_empty_pages = 0  # 连续空页计数
    
    while page <= max_pages:
        logger.info(f"📄 正在处理MCN {mcn_id} 第 {page} 页...")
        
        # 检查是否有可用cookie
        headers = cookie_manager.get_available_cookie()
        if not headers:
            logger.error("❌ 没有可用的cookie，停止处理")
            # 标记MCN为未处理状态，等待明天重试
            update_mcn_status(mcn_id, 0)
            session.commit()
            return None
        
        # 获取数据
        response_data = fetch_mcn_detail_data(mcn_id, page)
        
        if not response_data:
            logger.error(f"获取MCN {mcn_id} 数据失败")
            break
            
        # 检查响应格式
        if 'base_resp' not in response_data or response_data['base_resp'].get('status_code') != 0:
            logger.error(f"MCN {mcn_id} API响应异常: {response_data}")
            break
            
        # 获取作者列表
        authors = response_data.get('top_follower_authors', [])
        pagination = response_data.get('pagination', {})
        
        logger.info(f"📊 MCN {mcn_id} 第 {page} 页获取到 {len(authors)} 条作者数据")
        
        if len(authors) == 0:
            consecutive_empty_pages += 1
            logger.info(f"MCN {mcn_id} 第 {page} 页没有数据 (连续空页: {consecutive_empty_pages})")
            
            # 如果连续3页都没有数据，认为处理完成
            if consecutive_empty_pages >= 3:
                logger.info(f"✅ MCN {mcn_id} 连续{consecutive_empty_pages}页无数据，该MCN处理完成")
                break
        else:
            consecutive_empty_pages = 0  # 重置连续空页计数
            
        # 处理每条作者数据
        for author in authors:
            result = save_mcn_detail_to_db(mcn_id, author)
            
            if result == 'inserted':
                inserted_count += 1
            elif result == 'updated':
                updated_count += 1
            elif result == 'error':
                error_count += 1
                
            total_authors += 1
            
        # 提交数据库事务
        try:
            session.commit()
            logger.success(f"✅ MCN {mcn_id} 第 {page} 页数据保存成功")
        except Exception as commit_error:
            session.rollback()
            logger.error(f"❌ MCN {mcn_id} 第 {page} 页数据保存失败: {str(commit_error)}")
            
        # 检查是否还有更多页
        has_more = pagination.get('has_more', False)
        total_count = pagination.get('total_count', 0)
        current_count = pagination.get('current_count', 0)
        
        # 如果没有has_more字段，尝试其他判断方式
        if not has_more:
            # 如果当前页获取的数据少于PAGE_SIZE，可能是最后一页
            if len(authors) < PAGE_SIZE and len(authors) > 0:
                logger.info(f"✅ MCN {mcn_id} 当前页数据少于{PAGE_SIZE}条，可能是最后一页")
                break
            elif len(authors) == PAGE_SIZE:
                logger.info(f"⚠️ MCN {mcn_id} has_more为False但当前页数据等于{PAGE_SIZE}条，继续尝试下一页")
            else:
                logger.info(f"✅ MCN {mcn_id} 当前页无数据，停止分页")
                break
        else:
            logger.info(f"📄 MCN {mcn_id} 还有更多页数据")
            
        # 翻页
        page += 1
        
        # 添加延迟，避免请求过快
        logger.info(f"⏳ 等待 {REQUEST_DELAY} 秒后继续...")
        random_sleep(REQUEST_DELAY, 15)
    
    # 检查是否达到最大页数限制
    if page > max_pages:
        logger.warning(f"⚠️ MCN {mcn_id} 达到最大页数限制 {max_pages}，可能还有更多数据未处理")
    
    # 更新MCN状态为已处理
    update_mcn_status(mcn_id, 1)
    session.commit()
    
    logger.info(f"📈 MCN {mcn_id} 处理完成: 总作者数 {total_authors}, 新增 {inserted_count}, 更新 {updated_count}, 错误 {error_count}")
    
    return {
        'total_authors': total_authors,
        'inserted_count': inserted_count,
        'updated_count': updated_count,
        'error_count': error_count
    }


def main():
    """主函数"""
    setup_logger()

    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info("🚀 抖音MCN详情数据抓取程序启动")
    logger.info(f"⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # 显示初始Cookie状态
    cookie_manager.log_status()

    try:
        # 获取未处理的MCN列表
        unprocessed_mcns = get_unprocessed_mcns()
        
        if not unprocessed_mcns:
            logger.info("没有未处理的MCN，程序结束")
            return True
            
        total_mcns = len(unprocessed_mcns)
        processed_mcns = 0
        total_inserted = 0
        total_updated = 0
        total_errors = 0
        
        # 处理每个MCN
        for mcn in unprocessed_mcns:
            try:
                # 在处理每个MCN前检查Cookie状态
                headers = cookie_manager.get_available_cookie()
                if not headers:
                    logger.error("❌ 所有Cookie今日调用次数已用完，程序停止")
                    logger.info("💡 请明天再运行程序，或添加更多Cookie")
                    break
                
                result = process_mcn_detail(mcn)
                
                if result:
                    total_inserted += result['inserted_count']
                    total_updated += result['updated_count']
                    total_errors += result['error_count']
                    
                processed_mcns += 1
                
                logger.info(f"📊 进度: {processed_mcns}/{total_mcns} MCN已处理")
                
                # 每处理10个MCN显示一次Cookie状态
                if processed_mcns % 10 == 0:
                    cookie_manager.log_status()
                
                # 添加延迟，避免请求过快
                if processed_mcns < total_mcns:
                    logger.info(f"⏳ 等待 {REQUEST_DELAY} 秒后处理下一个MCN...")
                    random_sleep(REQUEST_DELAY, 15)
                    
            except Exception as e:
                logger.error(f"处理MCN {mcn.user_id} 时出错: {str(e)}")
                # 标记该MCN为错误状态
                update_mcn_status(mcn.user_id, -1)
                session.commit()
                continue

        # 输出统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 70)
        logger.info("✅ MCN详情数据抓取完成")
        logger.info("📊 执行统计:")
        logger.info(f"   ⏱️  执行时长: {duration:.2f} 秒")
        logger.info(f"   🏢 总MCN数: {total_mcns}")
        logger.info(f"   ✅ 已处理MCN: {processed_mcns}")
        logger.info(f"   ➕ 新增记录: {total_inserted}")
        logger.info(f"   🔄 更新记录: {total_updated}")
        logger.info(f"   ❌ 错误记录: {total_errors}")
        logger.info(f"   🏁 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 显示最终Cookie状态
        logger.info("\n🍪 最终Cookie使用状态:")
        cookie_manager.log_status()
        
        logger.info("=" * 70)

        return True

    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户手动中断程序")
        session.rollback()
        return False
    except Exception as e:
        logger.error(f"❌ 程序运行出错: {str(e)}")
        session.rollback()
        import traceback
        logger.debug(traceback.format_exc())
        return False
    finally:
        # 关闭数据库连接
        session.close()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"程序异常退出: {str(e)}")
        sys.exit(1)
