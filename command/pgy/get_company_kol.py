"""
获取公司博主报价等信息
"""

import time
import json
from datetime import datetime
from loguru import logger

from models.models_tibao import KolFangpianPrice
from service.pgy_service import get_mcn_detail, get_notes_detail
from core.localhost_fp_project import session

"""
    获取公司博主的信息
"""
# 方片新媒体 方片家居 方片母婴 方片时尚 方片家居
mcn_ids = ['589ddd6e6a6a69130bf6dd6c', '590e88cd5e87e73c862ccf34', '61c68e7d0000000010008663',
           '5cf211ba000000000503e73f', '6117229e000000000100aab6']
# mcn_ids = ['5cf211ba000000000503e73f']


def get_kols_data():
    """获取kols数据，保存所有接口原始数据到JSON文件"""
    all_mcn_data = {}  # 存储所有MCN的原始数据
    total_success = 0
    total_failed = 0

    for mcn_id in mcn_ids:
        try:
            logger.info(f"开始获取MCN {mcn_id} 的数据...")
            kols1 = get_mcn_detail(mcn_id)

            if kols1 and len(kols1) > 0:
                # 保存原始接口数据
                all_mcn_data[mcn_id] = {
                    'mcn_id': mcn_id,
                    'fetch_time': datetime.now().isoformat(),
                    'total_count': len(kols1),
                    'raw_data': kols1
                }

                success_count = len(kols1)
                total_success += success_count
                logger.info(f"MCN {mcn_id} 返回了 {len(kols1)} 个博主数据")

                # 调试：打印第一个博主的数据结构
                if len(kols1) > 0:
                    logger.debug(f"第一个博主数据结构: {kols1[0]}")

                # 仍然保存到数据库（保持原有功能）
                for kol in kols1:
                    try:
                        # 验证必要字段
                        user_id = kol.get('userId')
                        name = kol.get('name')
                        pictureShowState = kol.get('pictureShowState')
                        videoShowState = kol.get('videoShowState')
                        if pictureShowState:
                            picturePrice = kol.get('picturePrice', 0)
                        else:
                            picturePrice = 0
                        if videoShowState:
                            videoPrice = kol.get('videoPrice', 0)
                        else:
                            videoPrice = 0
                        if not user_id:
                            logger.warning(f"博主数据缺少userId字段: {kol}")
                            continue

                        if not name:
                            logger.warning(f"博主数据缺少name字段: {kol}")
                            continue

                        # 创建新的数据库对象
                        kol_data = KolFangpianPrice(
                            kol_name=name,
                            kol_id=user_id,
                            picture_price=picturePrice,  # 使用图文价格作为固定价格
                            video_price=videoPrice,  # 使用视频价格作为1-1价格
                            redId=kol.get('redId', ''),  # 地址信息
                            currentLevel=kol.get('currentLevel', ''),  # 地址信息
                            created_time=datetime.now(),
                            updated_time=datetime.now(),
                        )
                        session.add(kol_data)
                        logger.debug(f"新增博主数据: {name}")

                    except Exception as kol_error:
                        logger.error(f"处理博主 {kol.get('name', 'Unknown')} 数据时出错: {str(kol_error)}")
                        continue

                # 提交事务
                session.commit()
                logger.info(f"获取MCN {mcn_id} 数据成功，处理了 {success_count} 个博主")
            else:
                logger.warning(f"MCN {mcn_id} 没有返回数据")
                # 即使没有数据也记录到JSON中
                all_mcn_data[mcn_id] = {
                    'mcn_id': mcn_id,
                    'fetch_time': datetime.now().isoformat(),
                    'total_count': 0,
                    'raw_data': []
                }

            time.sleep(6)  # 避免请求过快

        except Exception as e:
            total_failed += 1
            logger.error(f"获取MCN {mcn_id} 数据失败: {str(e)}")
            session.rollback()  # 出错时回滚事务

            # 记录错误信息到JSON中
            all_mcn_data[mcn_id] = {
                'mcn_id': mcn_id,
                'fetch_time': datetime.now().isoformat(),
                'total_count': 0,
                'raw_data': [],
                'error': str(e)
            }

            time.sleep(3)  # 出错后稍微等待一下再继续

    # 保存所有数据到JSON文件
    try:
        json_filename = f"mcn_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_mcn_data, f, ensure_ascii=False, indent=2)
        logger.info(f"所有MCN数据已保存到JSON文件: {json_filename}")
    except Exception as json_error:
        logger.error(f"保存JSON文件时出错: {str(json_error)}")

    logger.info(f"数据获取完成！成功处理 {total_success} 个博主，失败 {total_failed} 个MCN")


def main():
    """主函数"""
    try:
        logger.info("开始获取公司博主信息...")
        start_time = datetime.now()

        get_kols_data()

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"程序执行完成，耗时: {duration}")

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        # 确保数据库连接正确关闭
        try:
            session.close()
        except:
            pass


if __name__ == "__main__":
    get_notes_detail('64b0b59e000000000a022263')
    main()