"""
获取公司博主笔记信息并保存到数据库
"""

import time
from datetime import datetime

from loguru import logger

from service.pgy_service import get_notes_detail
from models.models_tibao import CreatorBusinessOut
from models.models import CreatorNoteDetail
from core.localhost_fp_project import session

"""
    获取公司博主的笔记信息并保存到数据库
"""


def get_kols_data():
    """获取博主笔记数据并保存到数据库"""
    try:
        # 查询 creator_mcn > 3 的所有创作者
        kols = session.query(CreatorBusinessOut).filter(CreatorBusinessOut.creator_mcn == 6).all()
        logger.info(f"查询到 {len(kols)} 个博主需要处理")

        total_kols = len(kols)
        processed_kols = 0
        total_notes = 0
        failed_kols = 0

        for index, kol in enumerate(kols, 1):
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"[{index}/{total_kols}] 开始处理博主: {kol.creator_nickname} (ID: {kol.platform_user_id})")
                logger.info(f"{'='*50}")

                # 调用接口获取笔记列表
                notes = get_notes_detail(kol.platform_user_id)

                if not notes or len(notes) == 0:
                    logger.warning(f"博主 {kol.creator_nickname} 没有返回笔记数据")
                    processed_kols += 1
                    time.sleep(3)
                    continue

                logger.info(f"获取到 {len(notes)} 条笔记数据")

                # 保存笔记数据
                saved_count = 0
                skipped_count = 0
                current_time = int(time.time())

                for note in notes:
                    try:
                        # 检查是否已存在（根据 creator_id + note_id）
                        existing = session.query(CreatorNoteDetail).filter(
                            CreatorNoteDetail.creator_id == kol.id,
                            CreatorNoteDetail.note_id == note.get('noteId')
                        ).first()

                        if existing:
                            # 如果已存在，更新数据
                            existing.note_title = note.get('title')
                            existing.brand_name = note.get('brandName')
                            existing.note_date = note.get('date')
                            existing.img_url = note.get('imgUrl')
                            existing.is_advertise = note.get('isAdvertise', False)
                            existing.is_video = note.get('isVideo', False)
                            existing.like_num = note.get('likeNum')
                            existing.collect_num = note.get('collectNum')
                            existing.read_num = note.get('readNum')
                            existing.update_time = current_time
                            skipped_count += 1
                        else:
                            # 创建新记录
                            note_detail = CreatorNoteDetail(
                                creator_id=kol.id,
                                note_id=note.get('noteId'),
                                note_title=note.get('title'),
                                brand_name=note.get('brandName'),
                                note_date=note.get('date'),
                                img_url=note.get('imgUrl'),
                                is_advertise=note.get('isAdvertise', False),
                                is_video=note.get('isVideo', False),
                                like_num=note.get('likeNum'),
                                collect_num=note.get('collectNum'),
                                read_num=note.get('readNum'),
                                create_time=current_time,
                                update_time=current_time
                            )
                            session.add(note_detail)
                            saved_count += 1

                    except Exception as note_error:
                        logger.error(f"保存笔记失败: {note.get('noteId', 'unknown')}, 错误: {str(note_error)}")
                        continue

                # 提交事务
                session.commit()
                total_notes += saved_count
                processed_kols += 1

                logger.info(f"博主 {kol.creator_nickname} 处理完成:")
                logger.info(f"  - 新增笔记: {saved_count} 条")
                logger.info(f"  - 更新笔记: {skipped_count} 条")
                logger.info(f"  - 总计: {saved_count + skipped_count} 条")

                # 延迟以避免请求过快
                time.sleep(6)

            except Exception as kol_error:
                failed_kols += 1
                logger.error(f"处理博主 {kol.creator_nickname} 失败: {str(kol_error)}")
                session.rollback()
                time.sleep(3)
                continue

        logger.info(f"\n{'='*50}")
        logger.info("所有数据处理完成!")
        logger.info(f"{'='*50}")
        logger.info(f"统计信息:")
        logger.info(f"  - 成功处理博主: {processed_kols}/{total_kols}")
        logger.info(f"  - 失败博主: {failed_kols}")
        logger.info(f"  - 新增笔记总数: {total_notes}")

    except Exception as e:
        logger.error(f"get_kols_data 执行失败: {str(e)}")
        session.rollback()
        raise


def main():
    """主函数"""
    try:
        logger.info("开始获取公司博主笔记信息...")
        start_time = datetime.now()

        get_kols_data()

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"程序执行完成,耗时: {duration}")

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        # 确保数据库连接正确关闭
        try:
            session.close()
            logger.info("数据库连接已关闭")
        except Exception as close_error:
            logger.error(f"关闭数据库连接失败: {str(close_error)}")


if __name__ == "__main__":
    get_notes_detail('5731b86b1c07df0465fd0874')
    main()
