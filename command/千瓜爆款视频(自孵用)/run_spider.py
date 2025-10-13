import time
from datetime import datetime
import signal
import sys
from sqlalchemy import distinct, desc, and_, or_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import schedule
from loguru import logger
from spider import XiaohongshuSpider
from core.database_text_fangpian import session, engine
from models.models_tibao import SpiderQianguaHotNote

# 全局变量用于控制程序运行
running = True
MAX_RETRIES = 3
RETRY_DELAY = 10
BATCH_SIZE = 50  # 每次处理的数据量

def signal_handler(signum, frame):
    """处理进程信号"""
    global running
    logger.info("收到退出信号，准备安全退出...")
    running = False

def get_kols_to_scrape():
    """
    获取需要爬取的KOL列表，使用事务和行锁保证线程安全
    返回：(kol列表, 记录ID列表)
    """
    local_session = sessionmaker(bind=engine)()
    try:
        # 开启事务
        with local_session.begin():
            today_start = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_timestamp = int(today_start.timestamp())

            # 使用SELECT FOR UPDATE锁定要处理的记录
            records = local_session.query(
                SpiderQianguaHotNote.id,
                SpiderQianguaHotNote.kol_name
            ).filter(
                and_(
                    or_(
                        SpiderQianguaHotNote.xiaohongshu_user_id.is_(None),
                        SpiderQianguaHotNote.xiaohongshu_note_id.is_(None)
                    ),
                    SpiderQianguaHotNote.hot_date == today_start_timestamp,
                    SpiderQianguaHotNote.status.is_(None)  # 只获取未处理的记录
                )
            ).order_by(
                desc(SpiderQianguaHotNote.note_interact)
            ).limit(BATCH_SIZE).with_for_update(skip_locked=True).all()

            if not records:
                return [], []

            # 提取KOL名称和记录ID
            kols = []
            record_ids = []
            for record in records:
                if record.kol_name:  # 确保KOL名称不为空
                    kols.append(record.kol_name)
                    record_ids.append(record.id)

            # 立即将这些记录的状态更新为0（处理中）
            if record_ids:
                local_session.query(SpiderQianguaHotNote).filter(
                    SpiderQianguaHotNote.id.in_(record_ids)
                ).update(
                    {"status": 0},
                    synchronize_session=False
                )

            # 提交事务，释放锁
            local_session.commit()
            logger.info(f"成功锁定并获取 {len(kols)} 条记录进行处理")
            return kols, record_ids

    except SQLAlchemyError as e:
        logger.error(f"数据库操作出错: {str(e)}")
        local_session.rollback()
        return [], []
    except Exception as e:
        logger.error(f"获取KOL列表时出错: {str(e)}")
        local_session.rollback()
        return [], []
    finally:
        local_session.close()

def process_kol(spider, kol, record_id, retry_count=0):
    """处理单个KOL的数据，包含重试逻辑"""
    try:
        logger.info(f"开始处理KOL: {kol}")
        success = spider.scrape_user_notes(kol)
        
        if success:
            logger.info(f"成功处理KOL: {kol}")
            return True
        elif retry_count < MAX_RETRIES:
            logger.warning(f"处理KOL {kol} 失败，{retry_count + 1}/{MAX_RETRIES} 次重试")
            time.sleep(RETRY_DELAY)
            return process_kol(spider, kol, record_id, retry_count + 1)
        else:
            logger.error(f"处理KOL {kol} 失败，已达到最大重试次数")
            return False
    except Exception as e:
        logger.error(f"处理KOL {kol} 时出错: {str(e)}")
        return False

def main():
    """主任务函数"""
    spider = None
    try:
        # 初始化爬虫
        spider = XiaohongshuSpider()
        
        # 尝试登录
        if not spider.is_logged_in:
            logger.info("尝试登录...")
            if not spider.login():
                logger.error("登录失败，程序退出")
                return

        total_success = 0
        total_fail = 0
        batch_count = 0
        
        while True:
            # 获取需要爬取的KOL列表
            kols, record_ids = get_kols_to_scrape()
            if not kols:
                if batch_count == 0:
                    logger.warning("没有找到需要更新的KOL数据")
                else:
                    logger.info(f"所有数据处理完成，共处理 {batch_count} 批数据")
                    logger.info(f"总计：成功 {total_success} 条，失败 {total_fail} 条")
                break

            batch_count += 1
            logger.info(f"开始处理第 {batch_count} 批数据，本批包含 {len(kols)} 个KOL")

            # 处理每个KOL
            success_count = 0
            fail_count = 0

            for index, (kol, record_id) in enumerate(zip(kols, record_ids), 1):
                try:
                    logger.info(f"批次 {batch_count} 进度: {index}/{len(kols)}")
                    if process_kol(spider, kol, record_id):
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    logger.error(f"处理KOL {kol} 时出错: {str(e)}")
                    fail_count += 1
                    continue

            total_success += success_count
            total_fail += fail_count
            
            logger.info(f"第 {batch_count} 批数据处理完成")
            logger.info(f"本批结果：成功 {success_count} 条，失败 {fail_count} 条")
            logger.info(f"累计结果：成功 {total_success} 条，失败 {total_fail} 条")
            
            # 每批数据处理完后短暂休息，避免请求过于频繁
            time.sleep(2)
        
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
    finally:
        if spider:
            spider.close()
            logger.info("爬虫资源已关闭")

def run_scheduler():
    """运行调度器"""
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 设置定时任务
    schedule.every().day.at('06:20').do(main)
    
    logger.info("自动化调度已启动，每天06:20运行，按 Ctrl+C 退出...")
    
    # 主循环
    while running:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.error(f"调度器运行出错: {str(e)}")
            time.sleep(60)  # 出错后等待1分钟再继续

if __name__ == '__main__':
    # 配置日志
    logger.add(
        "logs/spider_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
        encoding="utf-8"
    )
    main()
    
    try:
        run_scheduler()
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        sys.exit(1)