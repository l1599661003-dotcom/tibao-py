from loguru import logger

from service.feishu_service import read_table_content
from spider import DouYinSpider
from core.database_text_fangpian import session


def get_kols_to_scrape():
    """获取需要爬取的KOL列表"""
    try:
        kols = read_table_content('PJISbPC5OaihG8sCfMpc4Wohnyb', 'tbl17lQ70FPsRrC7', 'vewFqHiuMo')
        return kols
    except Exception as e:
        logger.error(f"获取KOL列表时出错: {str(e)}")
        return []


def main():
    try:
        # 初始化爬虫
        spider = DouYinSpider()

        # 尝试登录
        if not spider.is_logged_in:
            logger.info("尝试登录...")
            if not spider.login():
                logger.error("登录失败，程序退出")
                return

        # 获取需要爬取的KOL列表
        kols = get_kols_to_scrape()
        if not kols:
            logger.warning("没有找到需要更新的KOL数据")
            return

        logger.info(f"找到 {len(kols)} 个需要处理的KOL")

        # 遍历KOL列表进行爬取
        for kol in kols:
            kol_name = kol.get('fields', {}).get('矩阵号昵称', [])[0]['text']
            kol_id = kol.get('fields', {}).get('账号id', [])[0]['text']
            logger.info(f"开始处理KOL: {kol_name}")
            try:
                # 爬取笔记
                success = spider.scrape_user_notes(kol_id)
                if success:
                    logger.info(f"KOL {kol_name} 数据更新完成")
                else:
                    logger.warning(f"KOL {kol_name} 数据更新失败")

            except Exception as e:
                logger.error(f"处理KOL {kol_name} 时出错: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
    finally:
        # 确保资源被正确关闭
        if 'spider' in locals():
            spider.close()
        try:
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()


if __name__ == "__main__":
    # 设置日志格式
    logger.add(
        "logs/spider_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
        encoding="utf-8"
    )

    # 运行主程序
    main() 